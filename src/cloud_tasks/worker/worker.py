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

# Type aliases for multiprocessing objects
# We use Any because MyPy doesn't handle multiprocessing types well
MP_Queue = Any  # multiprocessing.Queue
MP_Event = Any  # multiprocessing.Event
MP_Value = Any  # multiprocessing.Value

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Worker:
    """Worker class for processing tasks from queues using multiprocessing."""

    def __init__(self, config_file: str):
        """
        Initialize the worker with configuration.

        Args:
            config_file: Path to JSON configuration file
        """
        self.config_file = config_file
        self.config = self._load_config()

        self.provider = self.config['provider']
        self.queue_name = self.config['queue_name']
        self.job_id = self.config['job_id']

        # Optional worker settings
        worker_options = self.config.get('worker_options', {})
        self.tasks_per_worker = worker_options.get('tasks_per_worker', 5)
        self.shutdown_grace_period = worker_options.get('shutdown_grace_period', 120)

        # State tracking
        self.running = False
        self.task_queue: Any = None

        # Multiprocessing coordination
        self.manager = Manager()
        self.shutdown_event: MP_Event = Event()  # type: ignore
        self.termination_event: MP_Event = Event()  # type: ignore

        # Track processes
        self.processes: List[Process] = []
        self.active_tasks: MP_Value = Value('i', 0)  # type: ignore

        # For results from worker processes
        self.task_results = self.manager.dict()
        self.tasks_processed: MP_Value = Value('i', 0)  # type: ignore
        self.tasks_failed: MP_Value = Value('i', 0)  # type: ignore

        # Task queue for inter-process communication
        self.task_queue_mp: MP_Queue = Queue()  # type: ignore
        self.result_queue: MP_Queue = Queue()  # type: ignore

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file."""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_file}")
            sys.exit(1)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in configuration file: {self.config_file}")
            sys.exit(1)

    def _signal_handler(self, signum, frame):
        """Handle termination signals."""
        signal_name = signal.Signals(signum).name
        logger.info(f"Received signal {signal_name}, initiating graceful shutdown")
        self.shutdown_event.set()

    async def initialize(self) -> None:
        """Initialize the worker, task queue, and result handler."""
        logger.info(f"Initializing worker for {self.provider} queue: {self.queue_name}")

        # Import the task queue
        try:
            module_name = f"cloud_tasks.queue_manager.{self.provider}"
            queue_module = importlib.import_module(module_name)

            # Get provider-specific config
            provider_config = self.config.get(self.provider, {})

            # Create and initialize the task queue
            from cloud_tasks.queue_manager import create_queue
            self.task_queue = await create_queue(self.provider, self.queue_name, provider_config)
        except ImportError:
            logger.error(f"Provider module not found: {module_name}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error initializing task queue: {e}")
            sys.exit(1)

        logger.info("Worker initialization complete")

    async def start(self) -> None:
        """Start the worker processes and result handler."""
        logger.info("Starting worker")
        self.running = True

        # Start worker processes
        self._start_worker_processes()

        # Start the result handler in the main process
        asyncio.create_task(self._handle_results())

        # Start the task feeder to get tasks from the cloud queue
        asyncio.create_task(self._feed_tasks_to_workers())

        # Start the termination check loop
        asyncio.create_task(self._check_termination_loop())

        # Process tasks until shutdown
        await self._wait_for_shutdown()

        logger.info(f"Worker shutdown complete. Processed: {self.tasks_processed.value}, Failed: {self.tasks_failed.value}")

    def _start_worker_processes(self) -> None:
        """Start worker processes for task processing."""
        for i in range(self.tasks_per_worker):
            p = Process(
                target=self._worker_process_main,
                args=(
                    i,
                    self.task_queue_mp,
                    self.result_queue,
                    self.shutdown_event,
                    self.termination_event,
                    self.active_tasks,
                    self.config_file
                )
            )
            p.daemon = True
            p.start()
            self.processes.append(p)
            logger.info(f"Started worker process {i} (PID: {p.pid})")

    async def _handle_results(self) -> None:
        """Handle results from worker processes."""
        while self.running and not self.shutdown_event.is_set():
            try:
                # Use asyncio to check the queue without blocking
                while not self.result_queue.empty():
                    process_id, task_id, success, result = self.result_queue.get_nowait()

                    if success:
                        self.tasks_processed.value += 1
                        logger.info(f"Task {task_id} completed successfully by process {process_id}")
                    else:
                        self.tasks_failed.value += 1
                        logger.error(f"Task {task_id} failed in process {process_id}: {result}")

                    # Store result in shared dictionary
                    self.task_results[task_id] = (success, result)

                # Sleep briefly to avoid CPU hogging
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error handling results: {e}")
                await asyncio.sleep(1)  # Wait a bit longer on error

    async def _wait_for_shutdown(self) -> None:
        """Wait for the shutdown event and then clean up."""
        # Wait until shutdown is requested
        while self.running and not self.shutdown_event.is_set():
            await asyncio.sleep(0.5)

        logger.info("Shutdown requested, stopping worker processes")
        self.running = False

        # Allow processes some time to finish current tasks
        shutdown_start = time.time()
        while self.active_tasks.value > 0 and time.time() - shutdown_start < self.shutdown_grace_period:
            logger.info(f"Waiting for {self.active_tasks.value} active tasks to complete...")
            await asyncio.sleep(1)

        # Terminate any remaining processes
        for p in self.processes:
            if p.is_alive():
                logger.info(f"Terminating process {p.pid}")
                p.terminate()

        # Wait for processes to exit
        for p in self.processes:
            p.join(timeout=5)
            if p.is_alive():
                logger.warning(f"Process {p.pid} did not exit, killing")
                p.kill()

    async def _check_termination_loop(self) -> None:
        """Periodically check if the instance is scheduled for termination."""
        while self.running and not self.shutdown_event.is_set():
            try:
                termination_notice = await self._check_termination_notice()

                if termination_notice and not self.termination_event.is_set():
                    logger.warning("Instance termination notice received")
                    self.termination_event.set()

                    # Give some time to finish processing before shutdown
                    asyncio.create_task(self._delayed_shutdown(grace_period=60))

            except Exception as e:
                logger.error(f"Error checking for termination: {e}")

            # Check every 15 seconds
            await asyncio.sleep(15)

    async def _delayed_shutdown(self, grace_period: int) -> None:
        """Trigger shutdown after a grace period to allow for task completion."""
        logger.info(f"Initiating delayed shutdown in {grace_period} seconds")
        await asyncio.sleep(grace_period)
        logger.info("Grace period expired, initiating shutdown")
        self.shutdown_event.set()

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

            if self.provider == 'aws':
                # AWS spot termination check
                response = requests.get(
                    "http://169.254.169.254/latest/meta-data/spot/instance-action",
                    timeout=2
                )
                return response.status_code == 200

            elif self.provider == 'gcp':
                # GCP preemption check
                response = requests.get(
                    "http://metadata.google.internal/computeMetadata/v1/instance/preempted",
                    headers={"Metadata-Flavor": "Google"},
                    timeout=2
                )
                return response.text.strip().lower() == "true"

            elif self.provider == 'azure':
                # Azure doesn't have a direct API yet
                return False

        except Exception:
            # Request failed - likely not on a cloud instance
            pass

        return False

    async def _feed_tasks_to_workers(self) -> None:
        """Fetch tasks from the cloud queue and feed them to worker processes."""
        while self.running and not self.shutdown_event.is_set() and not self.termination_event.is_set():
            try:
                # Ensure task_queue is available
                if self.task_queue is None:
                    logger.error("Task queue not initialized")
                    await asyncio.sleep(1)
                    continue

                # Only fetch new tasks if we have capacity
                if self.active_tasks.value < self.tasks_per_worker:
                    # Receive tasks
                    tasks = await self.task_queue.receive_tasks(max_count=min(5, self.tasks_per_worker - self.active_tasks.value))

                    if tasks:
                        for task in tasks:
                            # Put task on the worker queue
                            self.task_queue_mp.put(task)
                            with self.active_tasks.get_lock():
                                self.active_tasks.value += 1
                            logger.debug(f"Queued task {task['task_id']}, active tasks: {self.active_tasks.value}")
                    else:
                        # If no tasks, sleep to avoid hammering the queue
                        await asyncio.sleep(1)
                else:
                    # Wait for workers to process tasks
                    await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error fetching tasks: {e}")
                await asyncio.sleep(1)  # Wait a bit longer on error

    @staticmethod
    def _worker_process_main(
        process_id: int,
        task_queue: MP_Queue,
        result_queue: MP_Queue,
        shutdown_event: MP_Event,
        termination_event: MP_Event,
        active_tasks: MP_Value,
        config_file: str
    ) -> None:
        """Main function for worker processes."""
        # Set up logging for this process
        logging.basicConfig(
            level=logging.INFO,
            format=f'%(asctime)s - Process-{process_id} - %(levelname)s - %(message)s'
        )
        logger = logging.getLogger(f"worker-{process_id}")

        # Initialize task execution environment
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)

            logger.info(f"Worker process {process_id} started")

            # Main processing loop
            while not shutdown_event.is_set() and not termination_event.is_set():
                try:
                    # Get task with timeout
                    try:
                        task = task_queue.get(timeout=1)
                    except Exception:
                        # No task available or timeout
                        continue

                    # Extract task info
                    task_id = task['task_id']
                    task_data = task['data']
                    receipt = task.get('receipt_handle') or task.get('lock_token') or task.get('ack_id')

                    logger.info(f"Processing task {task_id}")
                    start_time = time.time()

                    # Process the task
                    try:
                        # Execute task in isolated environment
                        success, result = Worker._execute_task_isolated(task_id, task_data, config)
                        processing_time = time.time() - start_time

                        logger.info(f"Task {task_id} completed in {processing_time:.2f}s, success: {success}")

                        # Send result back to main process
                        result_queue.put((process_id, task_id, success, result))

                    except Exception as e:
                        logger.error(f"Error executing task {task_id}: {e}")
                        # Send failure back to main process
                        result_queue.put((process_id, task_id, False, str(e)))

                    finally:
                        # Update active task count
                        with active_tasks.get_lock():
                            active_tasks.value -= 1

                except Exception as e:
                    logger.error(f"Unhandled error in worker process: {e}")
                    time.sleep(1)

            logger.info(f"Worker process {process_id} shutting down")

        except Exception as e:
            logger.error(f"Fatal error in worker process {process_id}: {e}")
            traceback.print_exc()

    @staticmethod
    def _execute_task_isolated(task_id: str, task_data: Dict[str, Any], config: Dict[str, Any]) -> Tuple[bool, Any]:
        """
        Execute a task in isolation.

        This static method executes a task without dependencies on the main Worker class,
        allowing it to run in a separate process.

        Args:
            task_id: Unique ID for the task
            task_data: Task data to process
            config: Configuration dict

        Returns:
            Tuple of (success, result)
        """
        try:
            # This would be replaced with actual task execution logic
            # In a real implementation, you might:
            # 1. Import a specific module based on the task type
            # 2. Call a function with the task data
            # 3. Return the result

            # Simple example implementation:
            if 'task_type' in task_data:
                task_type = task_data['task_type']

                if task_type == 'addition':
                    result = task_data.get('num1', 0) + task_data.get('num2', 0)
                    return True, result

                elif task_type == 'multiplication':
                    result = task_data.get('num1', 0) * task_data.get('num2', 0)
                    return True, result

                else:
                    return False, f"Unknown task type: {task_type}"

            # Default implementation - just return success with the task data
            return True, task_data

        except Exception as e:
            return False, f"Error processing task: {str(e)}"


async def main():
    """Main entry point for the worker."""
    parser = argparse.ArgumentParser(description='Process tasks from a queue')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    args = parser.parse_args()

    worker = Worker(args.config)
    await worker.initialize()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())