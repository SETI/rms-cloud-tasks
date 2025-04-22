"""
Worker module for processing tasks from queues.

This module runs on worker instances and processes tasks from the queue.
It uses multiprocessing to achieve true parallelism across multiple CPU cores.
"""

import argparse
import asyncio
import importlib
import json
import logging
import multiprocessing
from multiprocessing import Process, Queue, Event, Value, Manager
import os
import signal
import sys
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple, Set, Awaitable, Callable, cast, Union

from cloud_tasks.common.logging_config import configure_logging
from cloud_tasks.queue_manager import create_queue


# Type aliases for multiprocessing objects
# We use Any because MyPy doesn't handle multiprocessing types well
MP_Queue = Any  # multiprocessing.Queue
MP_Event = Any  # multiprocessing.Event
MP_Value = Any  # multiprocessing.Value

# Set up logging with proper microsecond support
configure_logging(level=logging.INFO)

logger = logging.getLogger(__name__)


class Worker:
    """Worker class for processing tasks from queues using multiprocessing."""

    def __init__(self, user_worker_function: Callable[[str, Dict[str, Any]], bool]):
        """
        Initialize the worker.

        Args:
            user_worker_function: The function to execute for each task. It will be called
                with the task_id and task_data dictionary as arguments.
        """
        self._user_worker_function = user_worker_function

        self._provider = os.getenv("RMS_CLOUD_TASKS_PROVIDER")
        if self._provider is None:
            logger.error("RMS_CLOUD_TASKS_PROVIDER environment variable is not set")
            sys.exit(1)
        self._provider = self._provider.upper()
        logger.info(f"Provider: {self._provider}")

        self._job_id = os.getenv("RMS_CLOUD_TASKS_JOB_ID")
        if self._job_id is None:
            logger.error("RMS_CLOUD_TASKS_JOB_ID environment variable is not set")
            sys.exit(1)
        logger.info(f"Job ID: {self._job_id}")
        self._queue_name = os.getenv("RMS_CLOUD_TASKS_QUEUE_NAME")
        if self._queue_name is None:
            logger.error("RMS_CLOUD_TASKS_QUEUE_NAME environment variable is not set")
            sys.exit(1)
        logger.info(f"Queue name: {self._queue_name}")

        self._project_id = os.getenv("RMS_CLOUD_TASKS_PROJECT_ID")  # Optional - only for GCP
        logger.info(f"Project ID: {self._project_id}")

        if os.getenv("RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE") is not None:
            self._tasks_per_worker = int(os.getenv("RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE"))
            logger.info(
                f"Tasks per worker (RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE): {self._tasks_per_worker}"
            )
        elif os.getenv("RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS") is not None:
            self._tasks_per_worker = int(os.getenv("RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS"))
            logger.info(
                f"Tasks per worker (RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS): {self._tasks_per_worker}"
            )
        else:
            self._tasks_per_worker = 1
            logger.info(f"Tasks per worker (default): {self._tasks_per_worker}")

        self._visibility_timeout_seconds = int(
            os.getenv("RMS_CLOUD_TASKS_VISIBILITY_TIMEOUT_SECONDS", 30)
        )
        logger.info(f"Visibility timeout: {self._visibility_timeout_seconds} seconds")

        self._shutdown_grace_period = int(os.getenv("RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD", 120))
        logger.info(f"Shutdown grace period: {self._shutdown_grace_period} seconds")

        # Check if we should use new process for each task
        self._use_new_process = os.getenv("RMS_CLOUD_WORKER_USE_NEW_PROCESS", "False").lower()
        self._use_new_process = self._use_new_process not in ("false", "0")
        logger.info(f"Use new process per task: {self._use_new_process}")

        # State tracking
        self._running = False
        self._task_queue: Any = None

        # Multiprocessing coordination
        self._manager = Manager()
        self._shutdown_event: MP_Event = Event()  # type: ignore
        self._termination_event: MP_Event = Event()  # type: ignore

        # Track processes
        self._processes: List[Process] = []
        self._num_active_tasks: MP_Value = Value("i", 0)  # type: ignore

        # For results from worker processes
        self._num_tasks_processed: MP_Value = Value("i", 0)  # type: ignore
        self._num_tasks_failed: MP_Value = Value("i", 0)  # type: ignore

        # Task queue for inter-process communication
        self._task_queue_mp: MP_Queue = Queue()  # type: ignore
        self._result_queue: MP_Queue = Queue()  # type: ignore

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle termination signals."""
        signal_name = signal.Signals(signum).name
        logger.info(f"Received signal {signal_name}, initiating graceful shutdown")
        self._shutdown_event.set()
        signal.signal(signal.SIGINT, signal.SIG_DFL)  # So a second time will kill the process
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    async def start(self) -> None:
        """Start the worker processes and result handler."""
        logger.info(f"Starting worker for {self._provider.upper()} queue '{self._queue_name}'")

        try:
            # Create and initialize the task queue
            self._task_queue = await create_queue(
                provider=self._provider, queue_name=self._queue_name, project_id=self._project_id
            )
        except Exception as e:
            logger.error(f"Error initializing task queue: {e}", exc_info=True)
            sys.exit(1)

        self._running = True

        if not self._use_new_process:
            # Start worker processes if using process pool
            self._start_worker_processes()

        # Start the result handler in the main process
        asyncio.create_task(self._handle_results())

        # Start the task feeder to get tasks from the cloud queue
        asyncio.create_task(self._feed_tasks_to_workers())

        # Start the termination check loop
        asyncio.create_task(self._check_termination_loop())

        # Process tasks until shutdown
        await self._wait_for_shutdown()

        logger.info(
            f"Worker shutdown complete. Processed: {self._num_tasks_processed.value}, "
            f"failed: {self._num_tasks_failed.value}"
        )

    def _start_worker_processes(self) -> None:
        """Start worker processes for task processing."""
        for i in range(self._tasks_per_worker):
            p = Process(
                target=self._worker_process_main,
                args=(
                    i,
                    self._user_worker_function,
                    self._task_queue_mp,
                    self._result_queue,
                    self._shutdown_event,
                    self._termination_event,
                    self._num_active_tasks,
                    False,  # is_single_task
                ),
            )
            p.daemon = True
            p.start()
            self._processes.append(p)
            logger.info(f"Started worker process #{i} (PID: {p.pid})")

    async def _handle_results(self) -> None:
        """Handle results from worker processes."""
        while self._running and not self._shutdown_event.is_set():
            try:
                # Use asyncio to check the queue without blocking
                while not self._result_queue.empty():
                    process_id, task_id, ack_id, success, result = self._result_queue.get_nowait()

                    if success:
                        self._num_tasks_processed.value += 1
                        logger.info(
                            f"Task {task_id} completed successfully by process #{process_id}: {result}"
                        )
                        await self._task_queue.complete_task(ack_id)
                    else:
                        self._num_tasks_failed.value += 1
                        logger.error(f"Task {task_id} failed in process #{process_id}: {result}")
                        await self._task_queue.fail_task(ack_id)
                # Sleep briefly to avoid CPU hogging
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error handling results: {e}", exc_info=True)
                await asyncio.sleep(1)  # Wait a bit longer on error

    async def _wait_for_shutdown(self) -> None:
        """Wait for the shutdown event and then clean up."""
        # Wait until shutdown is requested
        while self._running and not self._shutdown_event.is_set():
            await asyncio.sleep(0.5)

        logger.info("Shutdown requested, stopping worker processes")
        self._running = False

        # Allow processes some time to finish current tasks
        shutdown_start = time.time()
        while (
            self._num_active_tasks.value > 0
            and time.time() - shutdown_start < self._shutdown_grace_period
        ):
            logger.info(f"Waiting for {self._num_active_tasks.value} active tasks to complete...")
            await asyncio.sleep(1)

        # Terminate any remaining processes
        for p in self._processes:
            if p.is_alive():
                logger.info(f"Terminating process {p.pid}")
                p.terminate()

        # Wait for processes to exit
        for p in self._processes:
            p.join(timeout=5)
            if p.is_alive():
                logger.warning(f"Process {p.pid} did not exit, killing")
                p.kill()

    async def _check_termination_loop(self) -> None:
        """Periodically check if the instance is scheduled for termination."""
        while self._running and not self._shutdown_event.is_set():
            try:
                termination_notice = await self._check_termination_notice()

                if termination_notice and not self._termination_event.is_set():
                    logger.warning("Instance termination notice received")
                    self._termination_event.set()

                    # Give some time to finish processing before shutdown
                    asyncio.create_task(self._delayed_shutdown(grace_period=60))

            except Exception as e:
                logger.error(f"Error checking for termination: {e}", exc_info=True)

            # Check every 15 seconds
            await asyncio.sleep(15)

    async def _delayed_shutdown(self, grace_period: int) -> None:
        """Trigger shutdown after a grace period to allow for task completion."""
        logger.info(f"Initiating delayed shutdown in {grace_period} seconds")
        await asyncio.sleep(grace_period)
        logger.info("Grace period expired, initiating shutdown")
        self._shutdown_event.set()

    async def _check_termination_notice(self) -> bool:
        """
        Check if the instance is scheduled for termination.

        This varies by cloud provider:
        - AWS: Check the instance metadata service
        - GCP: Check the metadata server
        - Azure: Check for scheduled events

        Returns:
            True if the instance is scheduled for termination, False otherwise
        """
        try:
            # Only import requests when needed to avoid dependency issues
            # using type: ignore to avoid mypy errors for missing stubs
            import requests  # type: ignore

            if self._provider == "aws":
                # AWS spot termination check
                response = requests.get(
                    "http://169.254.169.254/latest/meta-data/spot/instance-action", timeout=2
                )
                return response.status_code == 200

            elif self._provider == "gcp":
                # GCP preemption check
                response = requests.get(
                    "http://metadata.google.internal/computeMetadata/v1/instance/preempted",
                    headers={"Metadata-Flavor": "Google"},
                    timeout=2,
                )
                return response.text.strip().lower() == "true"

            elif self._provider == "azure":
                # TODO Azure doesn't have a direct API yet
                return False

        except Exception:
            # Request failed - likely not on a cloud instance
            pass

        return False

    async def _feed_tasks_to_workers(self) -> None:
        """Fetch tasks from the cloud queue and feed them to worker processes."""
        while (
            self._running
            and not self._shutdown_event.is_set()
            and not self._termination_event.is_set()
        ):
            try:
                # Ensure task_queue is available
                if self._task_queue is None:
                    logger.error("Task queue not initialized")
                    await asyncio.sleep(1)
                    continue

                # Only fetch new tasks if we have capacity
                max_concurrent = self._tasks_per_worker if not self._use_new_process else 1
                if self._num_active_tasks.value < max_concurrent:
                    # Receive tasks
                    tasks = await self._task_queue.receive_tasks(
                        max_count=min(5, max_concurrent - self._num_active_tasks.value),
                        visibility_timeout_seconds=self._visibility_timeout_seconds,
                    )

                    if tasks:
                        for task in tasks:
                            if self._use_new_process:
                                # Start a new process for this task
                                process_id = (
                                    self._num_tasks_processed.value
                                    + self._num_tasks_failed.value
                                    + self._num_active_tasks.value
                                )
                                p = Process(
                                    target=self._worker_process_main,
                                    args=(
                                        process_id,
                                        self._user_worker_function,
                                        self._task_queue_mp,
                                        self._result_queue,
                                        self._shutdown_event,
                                        self._termination_event,
                                        self._num_active_tasks,
                                        True,  # is_single_task
                                    ),
                                )
                                p.daemon = True
                                p.start()
                                self._processes.append(p)
                                logger.info(
                                    f"Started single-task process #{process_id} (PID: {p.pid})"
                                )

                            # Put task on the worker queue
                            self._task_queue_mp.put(task)
                            with self._num_active_tasks.get_lock():
                                self._num_active_tasks.value += 1
                            logger.debug(
                                f"Queued task {task['task_id']}, active tasks: {self._num_active_tasks.value}"
                            )
                    else:
                        # If no tasks, sleep to avoid hammering the queue
                        await asyncio.sleep(1)
                else:
                    # Wait for workers to process tasks
                    await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error fetching tasks: {e}", exc_info=True)
                await asyncio.sleep(1)  # Wait a bit longer on error

    @staticmethod
    def _worker_process_main(
        process_id: int,
        user_worker_function: Callable[[str, Dict[str, Any]], bool],
        task_queue: MP_Queue,
        result_queue: MP_Queue,
        shutdown_event: MP_Event,
        termination_event: MP_Event,
        active_tasks: MP_Value,
        is_single_task: bool,
    ) -> None:
        """Main function for worker processes."""
        # Set up logging for this process
        logging.basicConfig(
            level=logging.INFO,
            format=f"%(asctime)s - Process-{process_id} - %(levelname)s - %(message)s",
        )
        logger = logging.getLogger(f"worker-{process_id}")

        # Initialize task execution environment
        try:
            logger.info(f"Worker process #{process_id} started")

            # Main processing loop
            while not shutdown_event.is_set() and not termination_event.is_set():
                try:
                    # Get task with timeout
                    try:
                        task = task_queue.get(timeout=1)
                    except Exception:
                        # No task available or timeout
                        if is_single_task:
                            # If this is a single-task process and no task is available, exit
                            break
                        continue

                    # Extract task info
                    task_id = task["task_id"]
                    task_data = task["data"]
                    ack_id = task["ack_id"]  # For removing from the main queue
                    receipt = (
                        task.get("receipt_handle") or task.get("lock_token") or task.get("ack_id")
                    )

                    logger.info(f"Processing task {task_id} in process #{process_id}")
                    start_time = time.time()

                    # Process the task
                    try:
                        # Execute task in isolated environment
                        success, result = Worker._execute_task_isolated(
                            task_id, task_data, user_worker_function
                        )
                        processing_time = time.time() - start_time

                        logger.info(
                            f"Task {task_id} completed in {processing_time:.2f}s in process "
                            f"#{process_id}, success: {success}"
                        )

                        # Send result back to main process
                        result_queue.put((process_id, task_id, ack_id, success, result))

                    except Exception as e:
                        logger.error(f"Error executing task {task_id}: {e}")
                        # Send failure back to main process
                        result_queue.put((process_id, task_id, ack_id, False, str(e)))

                    finally:
                        # Update active task count
                        with active_tasks.get_lock():
                            active_tasks.value -= 1

                        if is_single_task:
                            # If this is a single-task process, exit after processing
                            break

                except Exception as e:
                    logger.error(f"Unhandled error in worker process: {e}")
                    if is_single_task:
                        break
                    time.sleep(1)

            logger.info(f"Worker process #{process_id} shutting down")

        except Exception as e:
            logger.error(f"Fatal error in worker process {process_id}: {e}")
            traceback.print_exc()

    @staticmethod
    def _execute_task_isolated(
        task_id: str,
        task_data: Dict[str, Any],
        user_worker_function: Callable[[str, Dict[str, Any]], bool],
    ) -> Tuple[bool, str]:
        """
        Execute a task in isolation.

        This static method executes a task without dependencies on the main Worker class,
        allowing it to run in a separate process.

        Args:
            task_id: Unique ID for the task
            task_data: Task data to process

        Returns:
            success flag
        """
        try:
            return user_worker_function(task_id, task_data)

        except Exception as e:
            return False, str(e)
