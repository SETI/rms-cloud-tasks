"""
Worker module for processing tasks from queues.

This module runs on worker instances and processes tasks from the queue.
It uses multiprocessing to achieve true parallelism across multiple CPU cores.
"""

import argparse
import asyncio
import json
import json_stream
import logging
import os
import signal
import sys
import time
import traceback
from typing import Any, Dict, Iterable, List, Optional, Tuple, Callable, Sequence
import uuid
import yaml
from multiprocessing import Process, Queue, Manager, Event, Value

from filecache import FCPath

from cloud_tasks.common.logging_config import configure_logging
from cloud_tasks.queue_manager import create_queue


# Type aliases for multiprocessing objects
# We use Any because MyPy doesn't handle multiprocessing types well
MP_Queue = Any  # multiprocessing.Queue
MP_Event = Any  # multiprocessing.Event
MP_Value = Any  # multiprocessing.Value

configure_logging(level=logging.INFO)

logger = logging.getLogger(__name__)


def _parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Worker for processing tasks from a queue")

    parser.add_argument(
        "--provider",
        help="Cloud provider (AWS, GCP, or AZURE) [overrides $RMS_CLOUD_TASKS_PROVIDER]",
    )
    parser.add_argument(
        "--project-id", help="Project ID (required for GCP) [overrides $RMS_CLOUD_TASKS_PROJECT_ID]"
    )
    parser.add_argument("--tasks", help="Path to JSON file containing tasks to process")
    parser.add_argument("--job-id", help="Job ID [overrides $RMS_CLOUD_TASKS_JOB_ID]")
    parser.add_argument(
        "--queue-name",
        help="Queue name [overrides $RMS_CLOUD_TASKS_QUEUE_NAME]; if not specified will be "
        "derived from the job ID",
    )
    parser.add_argument(
        "--instance-type", help="Instance type [overrides $RMS_CLOUD_TASKS_INSTANCE_TYPE]"
    )
    parser.add_argument(
        "--num-cpus",
        type=int,
        help="Number of vCPUs on this computer [overrides $RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS]",
    )
    parser.add_argument(
        "--memory",
        type=float,
        help="Memory in GB on this computer [overrides $RMS_CLOUD_TASKS_INSTANCE_MEM_GB]",
    )
    parser.add_argument(
        "--local-ssd",
        type=float,
        help="Local SSD in GB on this computer [overrides $RMS_CLOUD_TASKS_INSTANCE_SSD_GB]",
    )
    parser.add_argument(
        "--boot-disk",
        type=float,
        help="Boot disk size in GB on this computer "
        "[overrides $RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB]",
    )
    parser.add_argument(
        "--is-spot",
        action="store_true",
        help="Use spot instances [overrides $RMS_CLOUD_TASKS_INSTANCE_IS_SPOT]",
    )
    parser.add_argument(
        "--price",
        type=float,
        help="Price per hour on this computer [overrides $RMS_CLOUD_TASKS_INSTANCE_PRICE]",
    )
    parser.add_argument(
        "--num-simultaneous-tasks",
        type=int,
        help="Number of tasks that can be run simutaneously "
        "[overrides $RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE]",
    )
    parser.add_argument(
        "--max-runtime",
        type=int,
        default=30,
        help="Maximum allowed runtime in seconds "
        "[overrides $RMS_CLOUD_TASKS_MAX_RUNTIME]; used to determine queue visibility"
        "timeout and to kill tasks that are running too long",
    )
    parser.add_argument(
        "--shutdown-grace-period",
        type=int,
        default=120,
        help="Shutdown grace period in seconds "
        "[overrides $RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD]",
    )
    parser.add_argument(
        "--use-new-process",
        action="store_true",
        help="Use new process for each task [overrides $RMS_CLOUD_WORKER_USE_NEW_PROCESS]",
    )

    return parser.parse_args(args)


class LocalTaskQueue:
    """A local task queue that reads tasks from a JSON file."""

    def __init__(self, tasks_file: str):
        """Initialize the local task queue.

        Args:
            tasks_file: Path to JSON file containing tasks.
        """
        self._tasks_file = tasks_file
        self._tasks_iter = self._yield_tasks_from_file(tasks_file)

    def _yield_tasks_from_file(self, tasks_file: str) -> Iterable[Dict[str, Any]]:
        """
        Yield tasks from a JSON or YAML file as an iterator.

        This function uses streaming to read tasks files so that very large files can be
        processed without using a lot of memory or running slowly.

        Parameters:
            tasks_file: Path to the tasks file

        Yields:
            Task dictionaries (expected to have "id" and "data" keys)

        Raises:
            ValueError: If the file cannot be read
        """
        if not tasks_file.endswith((".json", ".yaml", ".yml")):
            raise ValueError(
                f"Unsupported file format for tasks: {tasks_file}; must be .json, .yml, or .yaml"
            )
        with FCPath(tasks_file).open(mode="r") as fp:
            if tasks_file.endswith(".json"):
                for task in json_stream.load(fp):
                    yield json_stream.to_standard_types(task)  # Convert to a dict
            else:
                # See https://stackoverflow.com/questions/429162/how-to-process-a-yaml-stream-in-python
                y = fp.readline()
                cont = True
                while cont:
                    l = fp.readline()
                    if len(l) == 0:
                        cont = False
                    elif l.startswith((" ", "-")):
                        y = y + l
                    elif len(y) > 0:
                        yield yaml.load(y)
                        y = l

    async def receive_tasks(self, max_count: int, visibility_timeout: int) -> List[Dict[str, Any]]:
        """Get a batch of tasks from the queue.

        Args:
            max_count: Maximum number of tasks to receive.
            visibility_timeout: Not used for local queue.

        Returns:
            List of tasks.
        """
        tasks = []
        for _ in range(max_count):
            try:
                task = next(self._tasks_iter)
            except StopIteration:
                return tasks
            task["ack_id"] = str(uuid.uuid4())
            tasks.append(task)
        return tasks

    async def complete_task(self, ack_id: str) -> None:
        """Mark a task as completed.

        Args:
            ack_id: The acknowledgement ID of the task.
        """
        # For local queue, we don't need to do anything
        pass

    async def fail_task(self, ack_id: str) -> None:
        """Mark a task as failed.

        Args:
            ack_id: The acknowledgement ID of the task.
        """
        # For local queue, we don't need to do anything
        pass


class Worker:
    """Worker class for processing tasks from queues using multiprocessing."""

    def __init__(
        self,
        user_worker_function: Callable[[str, Dict[str, Any]], bool],
        args: Optional[Sequence[str]] = None,
    ):
        """
        Initialize the worker.

        Args:
            user_worker_function: The function to execute for each task. It will be called
                with the task_id and task_data dictionary as arguments.
            args: Optional list of command line arguments (sys.argv[1:]).
        """
        self._user_worker_function = user_worker_function

        # Parse command line arguments if provided
        parsed_args = _parse_args(args)

        # Get provider from args or environment variable
        self._provider = parsed_args.provider or os.getenv("RMS_CLOUD_TASKS_PROVIDER")
        if self._provider is None and not parsed_args.tasks:
            logger.error("Provider not specified via --provider or RMS_CLOUD_TASKS_PROVIDER")
            sys.exit(1)
        if self._provider is not None:
            self._provider = self._provider.upper()
        logger.info(f"Provider: {self._provider}")

        # Get project ID from args or environment variable (optional - only for GCP)
        self._project_id = parsed_args.project_id or os.getenv("RMS_CLOUD_TASKS_PROJECT_ID")
        logger.info(f"Project ID: {self._project_id}")

        # Get job ID from args or environment variable
        self._job_id = parsed_args.job_id or os.getenv("RMS_CLOUD_TASKS_JOB_ID")
        if self._job_id is None and not parsed_args.tasks:
            logger.error("Job ID not specified via --job-id or RMS_CLOUD_TASKS_JOB_ID")
            sys.exit(1)
        logger.info(f"Job ID: {self._job_id}")

        # Get queue name from args or environment variable
        self._queue_name = parsed_args.queue_name or os.getenv("RMS_CLOUD_TASKS_QUEUE_NAME")
        if self._queue_name is None:
            self._queue_name = self._job_id
        logger.info(f"Queue name: {self._queue_name}")

        # Get instance type from args or environment variable
        self._instance_type = parsed_args.instance_type or os.getenv(
            "RMS_CLOUD_TASKS_INSTANCE_TYPE"
        )
        logger.info(f"Instance type: {self._instance_type}")

        # Get number of vCPUs from args or environment variable
        self._num_cpus = parsed_args.num_cpus
        if self._num_cpus is None:
            self._num_cpus = os.getenv("RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS")
        if self._num_cpus is not None:
            self._num_cpus = int(self._num_cpus)
        logger.info(f"Num CPUs: {self._num_cpus}")

        # Get memory from args or environment variable
        self._memory_gb = parsed_args.memory
        if self._memory_gb is None:
            self._memory_gb = os.getenv("RMS_CLOUD_TASKS_INSTANCE_MEM_GB")
        if self._memory_gb is not None:
            self._memory_gb = float(self._memory_gb)
        logger.info(f"Memory: {self._memory_gb} GB")

        # Get local SSD from args or environment variable
        self._local_ssd_gb = parsed_args.local_ssd
        if self._local_ssd_gb is None:
            self._local_ssd_gb = os.getenv("RMS_CLOUD_TASKS_INSTANCE_SSD_GB")
        if self._local_ssd_gb is not None:
            self._local_ssd_gb = float(self._local_ssd_gb)
        logger.info(f"Local SSD: {self._local_ssd_gb} GB")

        # Get boot disk size from args or environment variable
        self._boot_disk_gb = parsed_args.boot_disk
        if self._boot_disk_gb is None:
            self._boot_disk_gb = os.getenv("RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB")
        if self._boot_disk_gb is not None:
            self._boot_disk_gb = float(self._boot_disk_gb)
        logger.info(f"Boot disk size: {self._boot_disk_gb} GB")

        # Get spot instance flag from args or environment variable
        self._is_spot = parsed_args.is_spot
        if self._is_spot is None:
            self._is_spot = os.getenv("RMS_CLOUD_TASKS_INSTANCE_IS_SPOT")
            if self._is_spot is not None:
                self._is_spot = self._is_spot.lower() == "true"
        logger.info(f"Spot instance: {self._is_spot}")

        # Get price per hour from args or environment variable
        self._price_per_hour = parsed_args.price
        if self._price_per_hour is None:
            self._price_per_hour = os.getenv("RMS_CLOUD_TASKS_INSTANCE_PRICE")
        if self._price_per_hour is not None:
            self._price_per_hour = float(self._price_per_hour)
        logger.info(f"Price per hour: {self._price_per_hour}")

        # Determine number of tasks per worker
        self._num_simultaneous_tasks = parsed_args.num_simultaneous_tasks
        if self._num_simultaneous_tasks is None:
            self._num_simultaneous_tasks = os.getenv("RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE")
        if self._num_simultaneous_tasks is not None:
            self._num_simultaneous_tasks = int(self._num_simultaneous_tasks)
            logger.info(f"Num simultaneous tasks: {self._num_simultaneous_tasks}")
        else:
            self._num_simultaneous_tasks = 1
            logger.info(f"Num simultaneous tasks (default): {self._num_simultaneous_tasks}")

        # Get maximum runtime from args or environment variable
        self._max_runtime = parsed_args.max_runtime
        if self._max_runtime is None:
            self._max_runtime = os.getenv("RMS_CLOUD_TASKS_MAX_RUNTIME")
        if self._max_runtime is not None:
            self._max_runtime = int(self._max_runtime)
        logger.info(f"Maximum runtime: {self._max_runtime} seconds")

        # Get shutdown grace period from args or environment variable
        self._shutdown_grace_period = (
            parsed_args.shutdown_grace_period
            if parsed_args.shutdown_grace_period is not None
            else int(os.getenv("RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD", 120))
        )
        logger.info(f"Shutdown grace period: {self._shutdown_grace_period} seconds")

        # Check if we should use new process for each task
        self._use_new_process = (
            parsed_args.use_new_process
            if parsed_args.use_new_process
            else os.getenv("RMS_CLOUD_WORKER_USE_NEW_PROCESS", "False").lower()
            not in ("false", "0")
        )
        logger.info(f"Use new process per task: {self._use_new_process}")

        # Check if we're using a local tasks file
        self._tasks_file = parsed_args.tasks
        if self._tasks_file:
            logger.info(f"Using local tasks file: {self._tasks_file}")

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
        if self._tasks_file:
            logger.info(f"Starting worker for local tasks file '{self._tasks_file}'")
            try:
                self._task_queue = LocalTaskQueue(self._tasks_file)
            except Exception as e:
                logger.error(f"Error initializing local task queue: {e}", exc_info=True)
                sys.exit(1)
        else:
            logger.info(f"Starting worker for {self._provider.upper()} queue '{self._queue_name}'")
            try:
                self._task_queue = await create_queue(
                    provider=self._provider,
                    queue_name=self._queue_name,
                    project_id=self._project_id,
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

        # Start the task feeder to get tasks from the queue
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
        for i in range(self._num_simultaneous_tasks):
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
                max_concurrent = self._num_simultaneous_tasks
                if self._num_active_tasks.value < max_concurrent:
                    # Receive tasks
                    tasks = await self._task_queue.receive_tasks(
                        max_count=min(5, max_concurrent - self._num_active_tasks.value),
                        visibility_timeout=self._max_runtime,
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
                            print(task)
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
