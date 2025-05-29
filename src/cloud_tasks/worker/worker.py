"""
Worker module for processing tasks from queues.

This module runs on worker instances and processes tasks from the queue.
It uses multiprocessing to achieve true parallelism across multiple CPU cores.
"""

import argparse
import asyncio
import datetime
import json
import json_stream
import logging
import os
import signal
import socket
import sys
import time
import traceback
from typing import Any, Dict, Iterable, List, Optional, Tuple, Callable, Sequence
import uuid
import yaml
from multiprocessing import Process, Queue, Manager, Event

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
        help="Cloud provider (AWS, GCP, or AZURE); used primarily to test for instance "
        "termination notices [overrides $RMS_CLOUD_TASKS_PROVIDER]",
    )
    parser.add_argument(
        "--project-id", help="Project ID (required for GCP) [overrides $RMS_CLOUD_TASKS_PROJECT_ID]"
    )
    parser.add_argument(
        "--task-file",
        help="Path to JSON file containing tasks to process; if specified, cloud-based task "
        "queues are ignored",
    )
    parser.add_argument(
        "--job-id",
        help="Job ID; used to identify the cloud-based task queue name "
        "[overrides $RMS_CLOUD_TASKS_JOB_ID]",
    )
    parser.add_argument(
        "--queue-name",
        help="Cloud-based task queue name; if not specified will be derived from the job ID "
        "[overrides $RMS_CLOUD_TASKS_QUEUE_NAME]",
    )
    parser.add_argument(
        "--event-log-file",
        help="File to write events to; if not specified will not write events to a file "
        "[overrides $RMS_CLOUD_TASKS_EVENT_LOG_FILE]",
    )
    parser.add_argument(
        "--event-log-to-queue",
        action="store_true",
        default=None,
        help="If specified, events will be written to a cloud-based queue "
        "[overrides $RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE]",
    )
    parser.add_argument(
        "--no-event-log-to-queue",
        action="store_false",
        dest="event_log_to_queue",
        help="If specified, events will not be written to a cloud-based queue (default) "
        "[overrides $RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE]",
    )
    parser.add_argument(
        "--instance-type",
        help="Instance type; optional information for the worker processes "
        "[overrides $RMS_CLOUD_TASKS_INSTANCE_TYPE]",
    )
    parser.add_argument(
        "--num-cpus",
        type=int,
        help="Number of vCPUs on this computer; optional information for the worker processes "
        "[overrides $RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS]",
    )
    parser.add_argument(
        "--memory",
        type=float,
        help="Memory in GB on this computer; optional information for the worker processes "
        "[overrides $RMS_CLOUD_TASKS_INSTANCE_MEM_GB]",
    )
    parser.add_argument(
        "--local-ssd",
        type=float,
        help="Local SSD in GB on this computer; optional information for the worker processes "
        "[overrides $RMS_CLOUD_TASKS_INSTANCE_SSD_GB]",
    )
    parser.add_argument(
        "--boot-disk",
        type=float,
        help="Boot disk size in GB on this computer; optional information for the worker processes "
        "[overrides $RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB]",
    )
    parser.add_argument(
        "--is-spot",
        action="store_true",
        default=None,
        help="If supported by the provider, specify that this is a spot instance and subject "
        "to unexpected termination [overrides $RMS_CLOUD_TASKS_INSTANCE_IS_SPOT]",
    )
    parser.add_argument(
        "--no-is-spot",
        action="store_false",
        dest="is_spot",
        help="If supported by the provider, specify that this is not a spot instance "
        "and is not subject to unexpected termination (default) "
        "[overrides $RMS_CLOUD_TASKS_INSTANCE_IS_SPOT]",
    )
    parser.add_argument(
        "--price",
        type=float,
        help="Price per hour on this computer; optional information for the worker processes "
        "[overrides $RMS_CLOUD_TASKS_INSTANCE_PRICE]",
    )
    parser.add_argument(
        "--num-simultaneous-tasks",
        type=int,
        help="Number of tasks that can be run simutaneously; used to create worker processes "
        "[overrides $RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE]",
    )
    parser.add_argument(
        "--max-runtime",
        type=int,
        help="Maximum allowed runtime in seconds; used to determine queue visibility "
        "timeout and to kill tasks that are running too long [overrides "
        "$RMS_CLOUD_TASKS_MAX_RUNTIME] (default 600 seconds)",
    )
    parser.add_argument(
        "--shutdown-grace-period",
        type=int,
        help="How long to wait in seconds for processes to gracefully finish after shutdown is "
        "requested [overrides $RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD] (default 30 seconds)",
    )
    parser.add_argument(
        "--tasks-to-skip",
        type=int,
        help="Number of tasks to skip before processing any from the queue [overrides $RMS_CLOUD_TASKS_TO_SKIP]",
    )
    parser.add_argument(
        "--max-num-tasks",
        type=int,
        help="Maximum number of tasks to process [overrides $RMS_CLOUD_TASKS_MAX_NUM_TASKS]",
    )
    parser.add_argument(
        "--retry-on-exit",
        action="store_true",
        default=None,
        help="If specified, tasks will be retried if the user function exits "
        "prematurely [overrides $RMS_CLOUD_TASKS_RETRY_ON_EXIT]",
    )
    parser.add_argument(
        "--no-retry-on-exit",
        action="store_false",
        dest="retry_on_exit",
        help="If specified, tasks will not be retried if the user function exits "
        "prematurely (default) [overrides $RMS_CLOUD_TASKS_RETRY_ON_EXIT]",
    )
    parser.add_argument(
        "--retry-on-exception",
        action="store_true",
        default=None,
        help="If specified, tasks will be retried if the user function raises an unhandled "
        "exception [overrides $RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION]",
    )
    parser.add_argument(
        "--no-retry-on-exception",
        action="store_false",
        dest="retry_on_exception",
        help="If specified, tasks will not be retried if the user function raises an unhandled "
        "exception (default) [overrides $RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION]",
    )
    parser.add_argument(
        "--retry-on-timeout",
        action="store_true",
        default=None,
        help="If specified, tasks will be retried if they exceed the maximum runtime specified"
        "by --max-runtime [overrides $RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT]",
    )
    parser.add_argument(
        "--no-retry-on-timeout",
        action="store_false",
        dest="retry_on_timeout",
        help="If specified, tasks will not be retried if they exceed the maximum runtime specified "
        "by --max-runtime (default) [overrides $RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT]",
    )
    parser.add_argument(
        "--simulate-spot-termination-after",
        type=float,
        help="Number of seconds after worker start to simulate a spot termination notice "
        "[overrides $RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER]",
    )
    parser.add_argument(
        "--simulate-spot-termination-delay",
        type=float,
        help="Number of seconds after a simulated spot termination notice to forcibly kill "
        "all running tasks "
        "[overrides $RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY]",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="If specified, set the log level to DEBUG",
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
            Task dictionaries (expected to have "task_id" and "data" keys)

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
                    ln = fp.readline()
                    if len(ln) == 0:
                        cont = False
                    if not ln.startswith("-") and len(ln) != 0:
                        y = y + ln
                    else:
                        yield yaml.load(y, Loader=yaml.Loader)[0]
                        y = ln

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
                with the task_id, task_data dictionary, and Worker object as arguments.
            args: Optional list of command line arguments (sys.argv[1:]).
        """
        self._user_worker_function = user_worker_function

        # Parse command line arguments if provided
        parsed_args = _parse_args(args)

        if parsed_args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)

        logger.info("Configuration:")

        # Get provider from args or environment variable
        self._provider = parsed_args.provider or os.getenv("RMS_CLOUD_TASKS_PROVIDER")
        if self._provider is None:
            if not parsed_args.task_file:
                logger.error(
                    "Provider not specified via --provider or RMS_CLOUD_TASKS_PROVIDER "
                    "and no tasks file specified via --task-file"
                )
                sys.exit(1)
            if parsed_args.event_log_to_queue:
                logger.error(
                    "--event-log-to-queue requires either --provider or RMS_CLOUD_TASKS_PROVIDER"
                )
                sys.exit(1)
        if self._provider is not None:
            self._provider = self._provider.upper()
        logger.info(f"  Provider: {self._provider}")

        # Get project ID from args or environment variable (optional - only for GCP)
        self._project_id = parsed_args.project_id or os.getenv("RMS_CLOUD_TASKS_PROJECT_ID")
        logger.info(f"  Project ID: {self._project_id}")

        # Get job ID from args or environment variable
        self._job_id = parsed_args.job_id or os.getenv("RMS_CLOUD_TASKS_JOB_ID")
        logger.info(f"  Job ID: {self._job_id}")

        # Get queue name from args or environment variable
        self._queue_name = parsed_args.queue_name or os.getenv("RMS_CLOUD_TASKS_QUEUE_NAME")
        if self._queue_name is None:
            self._queue_name = self._job_id
        logger.info(f"  Queue name: {self._queue_name}")

        if self._queue_name is None and not parsed_args.task_file:
            logger.error(
                "Queue name not specified via --queue-name or RMS_CLOUD_TASKS_QUEUE_NAME "
                "or --job-id or RMS_CLOUD_TASKS_JOB_ID and no tasks file specified via --task-file"
            )
            sys.exit(1)

        # Get event log file from args or environment variable
        self._event_log_file = parsed_args.event_log_file or os.getenv(
            "RMS_CLOUD_TASKS_EVENT_LOG_FILE"
        )
        logger.info(f"  Event log file: {self._event_log_file}")

        self._event_log_to_queue = parsed_args.event_log_to_queue
        if self._event_log_to_queue is None:
            self._event_log_to_queue = os.getenv("RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE")
            if self._event_log_to_queue is not None:
                self._event_log_to_queue = self._event_log_to_queue.lower() in ("true", "1")
        logger.info(f"  Event log to queue: {self._event_log_to_queue}")

        self._event_log_queue_name = None
        if self._event_log_to_queue:
            if self._queue_name:
                self._event_log_queue_name = f"{self._queue_name}-events"
                logger.info(f"  Event log queue name: {self._event_log_queue_name}")
            else:
                logger.error("--event-log-to-queue requires either --job-id or --queue-name")
                sys.exit(1)

        # Get instance type from args or environment variable
        self._instance_type = parsed_args.instance_type or os.getenv(
            "RMS_CLOUD_TASKS_INSTANCE_TYPE"
        )
        logger.info(f"  Instance type: {self._instance_type}")

        # Get number of vCPUs from args or environment variable
        self._num_cpus = parsed_args.num_cpus
        if self._num_cpus is None:
            self._num_cpus = os.getenv("RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS")
        if self._num_cpus is not None:
            self._num_cpus = int(self._num_cpus)
        logger.info(f"  Num CPUs: {self._num_cpus}")

        # Get memory from args or environment variable
        self._memory_gb = parsed_args.memory
        if self._memory_gb is None:
            self._memory_gb = os.getenv("RMS_CLOUD_TASKS_INSTANCE_MEM_GB")
        if self._memory_gb is not None:
            self._memory_gb = float(self._memory_gb)
        logger.info(f"  Memory: {self._memory_gb} GB")

        # Get local SSD from args or environment variable
        self._local_ssd_gb = parsed_args.local_ssd
        if self._local_ssd_gb is None:
            self._local_ssd_gb = os.getenv("RMS_CLOUD_TASKS_INSTANCE_SSD_GB")
        if self._local_ssd_gb is not None:
            self._local_ssd_gb = float(self._local_ssd_gb)
        logger.info(f"  Local SSD: {self._local_ssd_gb} GB")

        # Get boot disk size from args or environment variable
        self._boot_disk_gb = parsed_args.boot_disk
        if self._boot_disk_gb is None:
            self._boot_disk_gb = os.getenv("RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB")
        if self._boot_disk_gb is not None:
            self._boot_disk_gb = float(self._boot_disk_gb)
        logger.info(f"  Boot disk size: {self._boot_disk_gb} GB")

        # Get spot instance flag from args or environment variable
        self._is_spot = parsed_args.is_spot
        if self._is_spot is None:
            self._is_spot = os.getenv("RMS_CLOUD_TASKS_INSTANCE_IS_SPOT")
            if self._is_spot is not None:
                self._is_spot = self._is_spot.lower() in ("true", "1")
        logger.info(f"  Spot instance: {self._is_spot}")

        # Get price per hour from args or environment variable
        self._price_per_hour = parsed_args.price
        if self._price_per_hour is None:
            self._price_per_hour = os.getenv("RMS_CLOUD_TASKS_INSTANCE_PRICE")
        if self._price_per_hour is not None:
            self._price_per_hour = float(self._price_per_hour)
        logger.info(f"  Price per hour: {self._price_per_hour}")

        # Determine number of tasks per worker
        self._num_simultaneous_tasks = parsed_args.num_simultaneous_tasks
        if self._num_simultaneous_tasks is None:
            self._num_simultaneous_tasks = os.getenv("RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE")
        if self._num_simultaneous_tasks is not None:
            self._num_simultaneous_tasks = int(self._num_simultaneous_tasks)
            logger.info(f"  Num simultaneous tasks: {self._num_simultaneous_tasks}")
        else:
            if self._num_cpus is not None:
                self._num_simultaneous_tasks = self._num_cpus
            else:
                self._num_simultaneous_tasks = 1
            logger.info(f"  Num simultaneous tasks (default): {self._num_simultaneous_tasks}")

        # Get maximum runtime from args or environment variable
        self._max_runtime = parsed_args.max_runtime
        if self._max_runtime is None:
            self._max_runtime = os.getenv("RMS_CLOUD_TASKS_MAX_RUNTIME")
        if self._max_runtime is None:
            self._max_runtime = 600  # Default to 10 minutes
        else:
            self._max_runtime = int(self._max_runtime)
        logger.info(f"  Maximum runtime: {self._max_runtime} seconds")

        # Get shutdown grace period from args or environment variable
        self._shutdown_grace_period = (
            parsed_args.shutdown_grace_period
            if parsed_args.shutdown_grace_period is not None
            else int(os.getenv("RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD", 30))
        )
        logger.info(f"  Shutdown grace period: {self._shutdown_grace_period} seconds")

        # Get retry on exit from args or environment variable
        self._retry_on_exit = parsed_args.retry_on_exit
        if self._retry_on_exit is None:
            self._retry_on_exit = os.getenv("RMS_CLOUD_TASKS_RETRY_ON_EXIT")
            if self._retry_on_exit is not None:
                self._retry_on_exit = self._retry_on_exit.lower() in ("true", "1")
        logger.info(f"  Retry on exit: {self._retry_on_exit}")

        # Get retry on exception from args or environment variable
        self._retry_on_exception = parsed_args.retry_on_exception
        if self._retry_on_exception is None:
            self._retry_on_exception = os.getenv("RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION")
            if self._retry_on_exception is not None:
                self._retry_on_exception = self._retry_on_exception.lower() in ("true", "1")
        logger.info(f"  Retry on exception: {self._retry_on_exception}")

        # Get retry on timeout from args or environment variable
        self._retry_on_timeout = parsed_args.retry_on_timeout
        if self._retry_on_timeout is None:
            self._retry_on_timeout = os.getenv("RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT")
            if self._retry_on_timeout is not None:
                self._retry_on_timeout = self._retry_on_timeout.lower() in ("true", "1")
        logger.info(f"  Retry on timeout: {self._retry_on_timeout}")

        # Get simulate spot termination after from args or environment variable
        self._simulate_spot_termination_after = parsed_args.simulate_spot_termination_after
        self._simulate_spot_termination_delay = None
        if self._simulate_spot_termination_after is None:
            delay_str = os.getenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER")
            if delay_str is not None:
                self._simulate_spot_termination_after = float(delay_str)
        if self._simulate_spot_termination_after is not None:
            logger.info(
                f"  Simulating spot termination after {self._simulate_spot_termination_after} seconds"
            )

            # Get simulate spot termination delay from args or environment variable
            self._simulate_spot_termination_delay = parsed_args.simulate_spot_termination_delay
            if self._simulate_spot_termination_delay is None:
                delay_str = os.getenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY")
                if delay_str is not None:
                    self._simulate_spot_termination_delay = float(delay_str)
            if self._simulate_spot_termination_delay is not None:
                logger.info(
                    "    Simulating spot termination delay of "
                    f"{self._simulate_spot_termination_delay} seconds"
                )
            else:
                logger.warning(
                    "  Simulating spot termination after but no delay specified; "
                    "tasks will never be killed"
                )

        # Check if we're using a local tasks file
        self._tasks_file = parsed_args.task_file
        if self._tasks_file:
            logger.info(f'  Using local tasks file: "{self._tasks_file}"')

        # Get number of tasks to skip from args or environment variable
        self._tasks_to_skip = parsed_args.tasks_to_skip
        if self._tasks_to_skip is None:
            self._tasks_to_skip = os.getenv("RMS_CLOUD_TASKS_TO_SKIP")
        if self._tasks_to_skip is not None:
            self._tasks_to_skip = int(self._tasks_to_skip)
        logger.info(f"  Tasks to skip: {self._tasks_to_skip}")

        # Get maximum number of tasks to process from args or environment variable
        self._max_num_tasks = parsed_args.max_num_tasks
        if self._max_num_tasks is None:
            self._max_num_tasks = os.getenv("RMS_CLOUD_TASKS_MAX_NUM_TASKS")
        if self._max_num_tasks is not None:
            self._max_num_tasks = int(self._max_num_tasks)
        logger.info(f"  Maximum number of tasks: {self._max_num_tasks}")
        self._task_skip_count = self._tasks_to_skip
        self._tasks_remaining = self._max_num_tasks

        # State tracking
        self._running = False
        self._task_queue: Any = None

        # Multiprocessing coordination
        self._manager = Manager()
        self._shutdown_event: MP_Event = Event()  # type: ignore
        self._termination_event: MP_Event = Event()  # type: ignore

        # Track processes
        self._next_worker_id: int = 0
        self._processes: Dict[int, Dict[str, Any]] = {}  # Maps worker # to process and task
        self._num_tasks_not_retried: int = 0
        self._num_tasks_retried: int = 0
        self._num_tasks_timed_out: int = 0
        self._num_tasks_exited: int = 0
        self._num_tasks_exception: int = 0

        # Task queue for inter-process communication
        self._result_queue: MP_Queue = Queue()  # type: ignore

        # Semaphores for synchronizing process operations
        self._process_ops_semaphore = asyncio.Semaphore(1)  # For process creation/monitoring
        self._task_queue_semaphore = asyncio.Semaphore(1)  # For task queue operations

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self._hostname = socket.gethostname()
        self._event_logger_fp = None
        self._event_logger_queue = None

    @property
    def provider(self) -> str | None:
        """The provider (AWS, GCP, or AZURE) to communicate with for queues"""
        return self._provider

    @property
    def project_id(self) -> str | None:
        """The project ID (GCP only))"""
        return self._project_id

    @property
    def job_id(self) -> str | None:
        """The job ID"""
        return self._job_id

    @property
    def queue_name(self) -> str | None:
        """The task queue name"""
        return self._queue_name

    @property
    def event_log_to_queue(self) -> bool:
        """Whether to write events to a queue"""
        return self._event_log_to_queue

    @property
    def event_log_queue_name(self) -> str | None:
        """The queue to write events to"""
        return self._event_log_queue_name

    @property
    def event_log_file(self) -> str | None:
        """The file to write events to"""
        return self._event_log_file

    @property
    def instance_type(self) -> str | None:
        """The instance type this task is running on"""
        return self._instance_type

    @property
    def num_cpus(self) -> int | None:
        """The number of vCPUs on this computer"""
        return self._num_cpus

    @property
    def memory_gb(self) -> float | None:
        """The amount of memory on this computer"""
        return self._memory_gb

    @property
    def local_ssd_gb(self) -> float | None:
        """The size of the extra local SSD, if any, in GB"""
        return self._local_ssd_gb

    @property
    def boot_disk_gb(self) -> float | None:
        """The size of the boot disk in GB"""
        return self._boot_disk_gb

    @property
    def is_spot(self) -> bool:
        """Whether this is a spot instance and might be preempted"""
        return self._is_spot or self._simulate_spot_termination_after is not None

    @property
    def price_per_hour(self) -> float | None:
        """The price per hour for this instance"""
        return self._price_per_hour

    @property
    def num_simultaneous_tasks(self) -> int:
        """The number of tasks to run simultaneously"""
        return self._num_simultaneous_tasks

    @property
    def max_runtime(self) -> int:
        """The maximum runtime for a task in seconds"""
        return self._max_runtime

    @property
    def shutdown_grace_period(self) -> int:
        """The grace period for shutting down the worker in seconds"""
        return self._shutdown_grace_period

    @property
    def received_termination_notice(self) -> bool:
        """Whether the worker has received a termination notice"""
        return self._termination_event.is_set()

    @property
    def received_shutdown_request(self) -> bool:
        """Whether the worker has received a shutdown request"""
        return self._shutdown_event.is_set()

    def _signal_handler(self, signum, frame):
        """Handle termination signals."""
        signal_name = signal.Signals(signum).name
        logger.warning(f"Received signal {signal_name}, initiating graceful shutdown")
        self._shutdown_event.set()
        signal.signal(signal.SIGINT, signal.SIG_DFL)  # So a second time will kill the process
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    _EVENT_TYPE_TASK_COMPLETED = "task_completed"
    _EVENT_TYPE_TASK_EXCEPTION = "task_exception"
    _EVENT_TYPE_TASK_TIMED_OUT = "task_timed_out"
    _EVENT_TYPE_TASK_EXITED = "task_exited"
    _EVENT_TYPE_NON_FATAL_EXCEPTION = "non_fatal_exception"
    _EVENT_TYPE_FATAL_EXCEPTION = "fatal_exception"
    _EVENT_TYPE_SPOT_TERMINATION = "spot_termination"

    async def _log_event(self, event: Dict[str, Any]) -> None:
        """Log an event to the event log."""
        # Reorder so these fields are first in the diction to make the display nicer
        new_event = {
            "timestamp": datetime.datetime.now().isoformat(),
            "hostname": self._hostname,
            "event_type": event["event_type"],
            **event,
        }
        if self._event_logger_fp:
            self._event_logger_fp.write(json.dumps(new_event) + "\n")
            self._event_logger_fp.flush()
        if self._event_logger_queue:
            await self._event_logger_queue.send_message(json.dumps(new_event))

    async def _log_task_completed(
        self, task_id: str, *, retry: bool, elapsed_time: float, result: Any
    ) -> None:
        """Log a task completed event."""
        await self._log_event(
            {
                "event_type": self._EVENT_TYPE_TASK_COMPLETED,
                "task_id": task_id,
                "retry": retry,
                "elapsed_time": elapsed_time,
                "result": result,
            }
        )

    async def _log_task_exception(
        self, task_id: str, *, retry: bool, elapsed_time: float, exception: str
    ) -> None:
        """Log a task exception event."""
        await self._log_event(
            {
                "event_type": self._EVENT_TYPE_TASK_EXCEPTION,
                "task_id": task_id,
                "retry": retry,
                "elapsed_time": elapsed_time,
                "exception": exception,
            }
        )

    async def _log_task_timed_out(self, task_id: str, *, retry: bool, runtime: float) -> None:
        """Log a task timed out event."""
        await self._log_event(
            {
                "event_type": self._EVENT_TYPE_TASK_TIMED_OUT,
                "task_id": task_id,
                "retry": retry,
                "elapsed_time": runtime,
            }
        )

    async def _log_task_exited(
        self, task_id: str, *, retry: bool, elapsed_time: float, exit_code: int
    ) -> None:
        """Log a task exited event."""
        await self._log_event(
            {
                "event_type": self._EVENT_TYPE_TASK_EXITED,
                "task_id": task_id,
                "retry": retry,
                "elapsed_time": elapsed_time,
                "exit_code": exit_code,
            }
        )

    async def _log_non_fatal_exception(self, exception: str) -> None:
        """Log a non-fatal exception event."""
        await self._log_event(
            {
                "event_type": self._EVENT_TYPE_NON_FATAL_EXCEPTION,
                "exception": exception,
            }
        )

    async def _log_fatal_exception(self, exception: str) -> None:
        """Log a fatal exception event."""
        await self._log_event(
            {
                "event_type": self._EVENT_TYPE_FATAL_EXCEPTION,
                "exception": exception,
            }
        )

    async def _log_spot_termination(self) -> None:
        """Log a spot termination event."""
        await self._log_event({"event_type": self._EVENT_TYPE_SPOT_TERMINATION})

    async def start(self) -> None:
        """Start the worker and begin processing tasks."""

        if self._event_log_file:
            logger.debug(f'Starting event logger for file "{self._event_log_file}"')
            try:
                self._event_logger_fp = open(self._event_log_file, "a")
            except Exception as e:
                logger.error(f"Error opening event log file: {e}", exc_info=True)
                sys.exit(1)

        if self._event_log_to_queue:
            logger.debug(f'Starting event logger for queue "{self._event_log_queue_name}"')
            try:
                self._event_logger_queue = await create_queue(
                    provider=self._provider,
                    queue_name=self._event_log_queue_name,
                    project_id=self._project_id,
                )
            except Exception as e:
                logger.error(f"Error initializing event log queue: {e}", exc_info=True)
                sys.exit(1)

        if self._tasks_file:
            logger.debug(f'Starting task scheduler for local tasks file "{self._tasks_file}"')
            try:
                self._task_queue = LocalTaskQueue(self._tasks_file)
            except Exception as e:
                logger.error(f"Error initializing local task queue: {e}", exc_info=True)
                await self._log_fatal_exception(traceback.format_exc())
                sys.exit(1)
        else:
            logger.debug(
                f"Starting task scheduler for {self._provider.upper()} queue "
                f'"{self._queue_name}"'
            )
            try:
                self._task_queue = await create_queue(
                    provider=self._provider,
                    queue_name=self._queue_name,
                    project_id=self._project_id,
                )
            except Exception as e:
                logger.error(f"Error initializing task queue: {e}", exc_info=True)
                await self._log_fatal_exception(traceback.format_exc())
                sys.exit(1)

        self._start_time = time.time()
        self._running = True

        # Start the result handler in the main process
        asyncio.create_task(self._handle_results())

        # Start the task feeder to get tasks from the queue
        asyncio.create_task(self._feed_tasks_to_workers())

        # Start the process runtime monitor
        asyncio.create_task(self._monitor_process_runtimes())

        # Start the termination check loop
        if self.is_spot:
            asyncio.create_task(self._check_termination_loop())

        # Process tasks until shutdown
        await self._wait_for_shutdown()

        total = self._num_tasks_not_retried + self._num_tasks_retried
        logger.info(
            f"Task scheduler shutdown complete. Not retried: {self._num_tasks_not_retried}, "
            f"Retried: {self._num_tasks_retried}, Timed out: {self._num_tasks_timed_out}, "
            f"Exited: {self._num_tasks_exited}, Total: {total}"
        )
        # We don't log an event here because this only happens when the user hits Ctrl-C

    async def _handle_results(self) -> None:
        """Handle results from worker processes."""
        while self._running:
            try:
                # We primarily look for processes that have exited. Once we find one,
                # we check the result queue to find the results from that process. If the
                # results aren't there, then the process exited prematurely.
                # Update the number of active processes in case one of them has exited
                # prematurely
                async with self._process_ops_semaphore:
                    exited_processes = {}
                    for worker_id, process_data in self._processes.items():
                        p = process_data["process"]
                        if not p.is_alive():
                            logger.debug(f"Worker #{worker_id} (PID {p.pid}) has exited")
                            exited_processes[worker_id] = process_data

                    # Now go through the result queue
                    while not self._result_queue.empty():
                        worker_id, retry, result = self._result_queue.get_nowait()
                        print(worker_id, retry, result)

                        if worker_id not in self._processes:
                            # Race condition with max_runtime most likely
                            logger.debug(
                                f"Worker #{worker_id} reported results but process had previously "
                                "exited; this is probably due to a race condition with "
                                "max_runtime and should be ignored"
                            )
                            continue

                        process_data = self._processes[worker_id]
                        p = process_data["process"]
                        task = process_data["task"]

                        if worker_id not in exited_processes:
                            # We caught this process between the time it sent a result and the
                            # time it exited. It's possible it's wedged, or that we were
                            # just lucky. Either way, we'll kill it off just to be safe.
                            p.kill()

                        elapsed_time = time.time() - process_data["start_time"]
                        if retry == "exception":
                            self._num_tasks_exception += 1
                            logger.warning(
                                f"Worker #{worker_id} reported task {task['task_id']} raised "
                                f"an unhandled exception in {elapsed_time:.1f} seconds, "
                                f"{'retrying' if self._retry_on_exception else 'not retrying'}: "
                                f"{result}"
                            )
                            await self._log_task_exception(
                                task["task_id"],
                                retry=self._retry_on_exception,
                                elapsed_time=elapsed_time,
                                exception=result,
                            )
                            if self._retry_on_exception:
                                self._num_tasks_retried += 1
                                async with self._task_queue_semaphore:
                                    await self._task_queue.fail_task(task["ack_id"])
                            else:
                                self._num_tasks_not_retried += 1
                                async with self._task_queue_semaphore:
                                    await self._task_queue.complete_task(task["ack_id"])
                        elif retry:
                            self._num_tasks_retried += 1
                            logger.info(
                                f"Worker #{worker_id} reported task {task['task_id']} completed "
                                f"in {elapsed_time:.1f} seconds but will be retried; result: "
                                f"{result}"
                            )
                            await self._log_task_completed(
                                task["task_id"],
                                retry=True,
                                elapsed_time=elapsed_time,
                                result=result,
                            )
                            async with self._task_queue_semaphore:
                                await self._task_queue.fail_task(task["ack_id"])
                        else:
                            self._num_tasks_not_retried += 1
                            logger.info(
                                f"Worker #{worker_id} reported task {task['task_id']} completed "
                                f"in {elapsed_time:.1f} seconds with no retry; result: {result}"
                            )
                            await self._log_task_completed(
                                task["task_id"],
                                retry=False,
                                elapsed_time=elapsed_time,
                                result=result,
                            )
                            async with self._task_queue_semaphore:
                                await self._task_queue.complete_task(task["ack_id"])

                        del self._processes[worker_id]
                        if worker_id in exited_processes:
                            del exited_processes[worker_id]

                    # Check for processes that exited prematurely; we didn't get result messages
                    # from these
                    for worker_id, process_data in exited_processes.items():
                        self._num_tasks_exited += 1
                        try:
                            exit_code = process_data["process"].exitcode
                        except Exception:
                            exit_code = None
                        task = process_data["task"]
                        elapsed_time = time.time() - process_data["start_time"]
                        logger.warning(
                            f"Worker #{worker_id} (PID {p.pid}) processing task "
                            f'"{task["task_id"]}" exited prematurely in {elapsed_time:.1f} seconds '
                            f"with exit code {exit_code}; "
                            f"{'retrying' if self._retry_on_exit else 'not retrying'}"
                        )
                        await self._log_task_exited(
                            task["task_id"],
                            retry=self._retry_on_exit,
                            elapsed_time=elapsed_time,
                            exit_code=exit_code,
                        )

                        async with self._task_queue_semaphore:
                            if self._retry_on_exit:
                                self._num_tasks_retried += 1
                                # If we're retrying on exit, mark it as failed
                                await self._task_queue.fail_task(task["ack_id"])
                            else:
                                self._num_tasks_not_retried += 1
                                # If we're not retrying on exit, mark it as complete
                                await self._task_queue.complete_task(task["ack_id"])

                        del self._processes[worker_id]

                # Sleep briefly to avoid CPU hogging
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error handling results: {e}", exc_info=True)
                await self._log_non_fatal_exception(traceback.format_exc())
                await asyncio.sleep(1)  # Wait a bit longer on error

    async def _wait_for_shutdown(self, interval: float = 0.5) -> None:
        """Wait for the shutdown event and then clean up."""
        # Wait until shutdown is requested
        while self._running and not self.received_shutdown_request:
            if self.received_termination_notice and len(self._processes) == 0:
                logger.info("Termination event set and all processes complete; exiting")
                return
            await asyncio.sleep(interval)

        logger.info("Shutdown requested, stopping worker processes")
        self._shutdown_event.set()

        # Allow processes some time to finish current tasks
        shutdown_start = time.time()
        while (
            len(self._processes) > 0 and time.time() - shutdown_start < self._shutdown_grace_period
        ):
            remaining_time = self._shutdown_grace_period - (time.time() - shutdown_start)
            logger.info(
                f"Waiting for {len(self._processes)} active tasks to complete; "
                f"{remaining_time:.0f} seconds remaining"
            )
            await asyncio.sleep(1)

        # Terminate any remaining processes
        async with self._process_ops_semaphore:
            for worker_id, process_data in self._processes.items():
                p = process_data["process"]
                if p.is_alive():
                    logger.info(f"Terminating process worker #{worker_id} (PID {p.pid})")
                    p.terminate()

            # Wait for processes to exit
            for worker_id, process_data in self._processes.items():
                p = process_data["process"]
                p.join(timeout=5)
                if p.is_alive():
                    logger.warning(
                        f"Process worker #{worker_id} (PID {p.pid}) did not exit, killing"
                    )
                    p.kill()

            self._processes = {}
            self._running = False

    async def _check_termination_loop(self) -> None:
        """Periodically check if the instance is scheduled for termination."""
        while self._running and not self.received_shutdown_request:
            try:
                termination_notice = await self._check_termination_notice()
                if termination_notice and not self.received_termination_notice:
                    logger.warning("Instance termination notice received")
                    self._termination_event.set()
                    await self._log_spot_termination()
                    # When the termination actually occurs, we don't need to do anything;
                    # this instance will simply stop running. If the workers were in the
                    # middle of doing something, they will be aborted at a random point.
                    # They had better be checking termination_event periodically or before
                    # they do something important.
                    break

            except Exception as e:
                logger.error(f"Error checking for termination: {e}", exc_info=True)
                await self._log_non_fatal_exception(traceback.format_exc())
            # Check every 5 seconds for real instance, .1 second for simulated
            if self._simulate_spot_termination_after is not None:
                await asyncio.sleep(0.1)
            else:
                await asyncio.sleep(5)

        if self._running and self._simulate_spot_termination_delay is not None:
            # If we're simulating a spot termination, wait for the delay and then kill all
            # running processes
            await asyncio.sleep(self._simulate_spot_termination_delay)
            if self._running:
                logger.info("Simulated spot termination delay complete, killing all processes")
                async with self._process_ops_semaphore:
                    for worker_id, process_data in self._processes.items():
                        p = process_data["process"]
                        if p.is_alive():
                            logger.info(f"Terminating process worker #{worker_id} (PID {p.pid})")
                            p.terminate()

                    # Wait for processes to exit
                    for worker_id, process_data in self._processes.items():
                        p = process_data["process"]
                        p.join(timeout=5)
                        if p.is_alive():
                            logger.warning(
                                f"Process worker #{worker_id} (PID {p.pid}) did not exit, killing"
                            )
                            p.kill()

                    self._processes = {}
                    self._running = False

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
        # Check for simulated termination first
        if self._simulate_spot_termination_after is not None:
            elapsed_time = time.time() - self._start_time
            if elapsed_time >= self._simulate_spot_termination_after:
                logger.info(
                    f"Simulating spot termination notice received after {elapsed_time:.1f} seconds"
                )
                return True
            return False

        try:
            import requests  # type: ignore

            if self._provider == "AWS":
                # AWS spot termination check
                response = requests.get(
                    "http://169.254.169.254/latest/meta-data/spot/instance-action", timeout=2
                )
                return response.status_code == 200

            elif self._provider == "GCP":
                # GCP preemption check
                response = requests.get(
                    "http://metadata.google.internal/computeMetadata/v1/instance/preempted",
                    headers={"Metadata-Flavor": "Google"},
                    timeout=2,
                )
                return response.text.strip().lower() == "true"

            elif self._provider == "AZURE":
                # TODO Azure doesn't have a direct API yet
                return False

        except Exception:
            pass

        return False

    async def _feed_tasks_to_workers(self) -> None:
        """Fetch tasks from the cloud queue and feed them to worker processes."""
        while self._running:
            try:
                if self.received_shutdown_request or self.received_termination_notice:
                    # If we're shutting down for any reason, don't start any new tasks
                    await asyncio.sleep(1)
                    continue

                # Only fetch new tasks if we have capacity
                max_concurrent = self._num_simultaneous_tasks
                if len(self._processes) >= max_concurrent:
                    # Wait for workers to process tasks
                    await asyncio.sleep(0.1)
                    continue

                # Receive tasks
                async with self._task_queue_semaphore:
                    tasks = await self._task_queue.receive_tasks(
                        max_count=min(5, max_concurrent - len(self._processes)),
                        visibility_timeout=self._max_runtime,
                    )

                if tasks:
                    for task in tasks:
                        if self._task_skip_count is not None and self._task_skip_count > 0:
                            self._task_skip_count -= 1
                            logger.info("Skipping")
                            continue
                        if self._tasks_remaining is not None:
                            if self._tasks_remaining <= 0:
                                break
                            self._tasks_remaining -= 1
                            logger.info("Remaining tasks: %d", self._tasks_remaining)

                        async with self._process_ops_semaphore:
                            # Start a new process for this task
                            worker_id = self._next_worker_id
                            self._next_worker_id += 1
                            p = Process(
                                target=self._worker_process_main,
                                args=(
                                    worker_id,
                                    self._user_worker_function,
                                    self,
                                    task,
                                    self._result_queue,
                                    self._shutdown_event,
                                    self._termination_event,
                                ),
                            )
                            p.daemon = True  # Guarantees exit when main process dies
                            p.start()
                            self._processes[worker_id] = {
                                "worker_id": worker_id,
                                "process": p,
                                "start_time": time.time(),
                                "task": task,
                            }
                            logger.info(f"Started single-task worker #{worker_id} (PID {p.pid})")
                            logger.debug(
                                f"Queued task {task['task_id']}, active tasks: "
                                f"{len(self._processes)}"
                            )
                else:
                    # If no tasks, sleep to avoid hammering the queue
                    await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error fetching tasks: {e}", exc_info=True)
                await self._log_non_fatal_exception(traceback.format_exc())
                await asyncio.sleep(1)  # Wait a bit longer on error

    async def _monitor_process_runtimes(self) -> None:
        """Monitor process runtimes and kill processes that exceed max_runtime."""
        while self._running:
            current_time = time.time()

            # Check each process's runtime
            processes_to_delete = []
            async with self._process_ops_semaphore:
                for worker_id, process_data in self._processes.items():
                    start_time = process_data["start_time"]
                    p = process_data["process"]
                    task = process_data["task"]
                    runtime = current_time - start_time
                    if runtime <= self._max_runtime:
                        continue

                    self._num_tasks_timed_out += 1
                    logger.warning(
                        f"Worker #{worker_id} (PID {p.pid}), task "
                        f"{process_data['task']['task_id']} exceeded max runtime of "
                        f"{self._max_runtime} seconds (actual runtime {runtime:.1f} seconds); "
                        "terminating"
                    )
                    await self._log_task_timed_out(task["task_id"], retry=False, runtime=runtime)

                    # Kill the process that exceeded runtime
                    try:
                        p.terminate()
                        p.join(timeout=1)
                        if p.is_alive():
                            logger.warning(
                                f"Worker #{worker_id} (PID {p.pid}) did not terminate, killing"
                            )
                            p.kill()
                            p.join(timeout=1)
                    except Exception as e:
                        logger.error(
                            f"Error terminating process worker #{worker_id} (PID " f"{p.pid}): {e}"
                        )
                        await self._log_non_fatal_exception(traceback.format_exc())

                    # Mark task as failed in the queue
                    try:
                        if self._retry_on_timeout:
                            self._num_tasks_retried += 1
                            async with self._task_queue_semaphore:
                                logger.info(
                                    f"Worker #{worker_id}: Task {task['task_id']} will be retried"
                                )
                                await self._task_queue.fail_task(task["ack_id"])
                        else:
                            self._num_tasks_not_retried += 1
                            async with self._task_queue_semaphore:
                                logger.info(
                                    f"Worker #{worker_id}: Task {task['task_id']} will not be retried"
                                )
                                await self._task_queue.complete_task(task["ack_id"])
                    except Exception as e:
                        logger.error(f"Error marking task {task['task_id']} as completed: {e}")
                        await self._log_non_fatal_exception(traceback.format_exc())

                    # Remove from tracking
                    processes_to_delete.append(worker_id)

                for worker_id in processes_to_delete:
                    del self._processes[worker_id]

            await asyncio.sleep(1)  # Check every second

    @staticmethod
    def _worker_process_main(
        worker_id: int,
        user_worker_function: Callable[[str, Dict[str, Any]], bool],
        worker: "Worker",
        task: Dict[str, Any],
        result_queue: MP_Queue,
        shutdown_event: MP_Event,
        termination_event: MP_Event,
    ) -> None:
        """Main function for worker processes."""
        # We inherited signal catching from the parent process, but we don't want that
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        # Set up logging for this process
        logging.basicConfig(
            level=logging.INFO,
            format=f"%(asctime)s - Process-{worker_id} - %(levelname)s - %(message)s",
        )
        logger = logging.getLogger(f"worker-{worker_id}")

        # Initialize task execution environment
        try:
            logger.info(f"Worker #{worker_id}: Started")

            # Extract task info
            task_id = task["task_id"]
            task_data = task["data"]

            logger.info(f"Worker #{worker_id}: Processing task {task_id}")
            start_time = time.time()

            # Process the task
            try:
                # Execute task in isolated environment
                retry, result = Worker._execute_task_isolated(
                    task_id, task_data, worker, user_worker_function
                )
                processing_time = time.time() - start_time

                logger.info(
                    f"Worker #{worker_id}: Completed task {task_id} in "
                    f"{processing_time:.2f} seconds, retry {retry}"
                )

                # Send result back to main process
                result_queue.put((worker_id, retry, result))

            except Exception as e:
                logger.error(
                    f"Worker #{worker_id}: Unhandled exception executing task {task_id}: {e}",
                    exc_info=True,
                )
                # Send failure back to main process
                result_queue.put((worker_id, "exception", str(traceback.format_exc())))

        except Exception as e:
            logger.error(f"Worker #{worker_id}: Unhandled exception - {e}", exc_info=True)

        logger.info(f"Worker #{worker_id}: Exiting")
        sys.exit(0)

    @staticmethod
    def _execute_task_isolated(
        task_id: str,
        task_data: Dict[str, Any],
        worker: "Worker",
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
            Tuple of (retry, result)
        """
        return user_worker_function(task_id, task_data, worker)
