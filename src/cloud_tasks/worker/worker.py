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
from pathlib import Path
import requests
import signal
import socket
import sys
import time
import traceback
from typing import Any, Dict, Iterable, List, Optional, Tuple, Callable, Sequence
import uuid
import yaml
import multiprocessing

from filecache import FCPath

from ..common.logging_config import configure_logging
from ..queue_manager import create_queue


MP_CTX = multiprocessing.get_context("spawn")


# Type aliases for multiprocessing objects
# We use Any because MyPy doesn't handle multiprocessing types well
MP_Queue = Any  # multiprocessing.Queue
MP_Event = Any  # multiprocessing.Event
MP_Value = Any  # multiprocessing.Value

configure_logging(level=logging.INFO)

logger = logging.getLogger(__name__)


def _parse_args(
    parser: argparse.ArgumentParser | None,
    args: Sequence[str] | None,
) -> argparse.Namespace:
    """Parse command line arguments."""

    if parser is None:
        parser = argparse.ArgumentParser(description="Worker for processing tasks from a queue")

    parser.add_argument(
        "--provider",
        help="Cloud provider (AWS or GCP); used to test for instance "
        "termination notices and to know which cloud-based queueing service to use "
        "[overrides $RMS_CLOUD_TASKS_PROVIDER]",
    )
    parser.add_argument(
        "--project-id", help="Project ID (required for GCP) [overrides $RMS_CLOUD_TASKS_PROJECT_ID]"
    )
    parser.add_argument(
        "--task-file",
        help="The name of a local JSON or YAML file containing tasks to process; if not "
        "specified, the worker will pull tasks from the cloud provider queue. "
        "The filename can also be a cloud storage path like ``gs://bucket/file``, "
        "``s3://bucket/file``, or ``https://path/to/file``. If not specified, the worker "
        "will pull tasks from the cloud provider queue.",
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
        "--exactly-once-queue",
        action="store_true",
        default=None,
        help="If specified, task and event queue messages are guaranteed to be delivered exactly "
        "once to any recipient [overrides $RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE]",
    )
    parser.add_argument(
        "--no-exactly-once-queue",
        action="store_false",
        dest="exactly_once_queue",
        help="If specified, task and event queue messages are delivered at least once, but could "
        "be delivered multiple times [overrides $RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE]",
    )
    parser.add_argument(
        "--event-log-file",
        help='File to write events to if --event-log-to-file is specified (defaults to "events.log") '
        "[overrides $RMS_CLOUD_TASKS_EVENT_LOG_FILE]",
    )
    parser.add_argument(
        "--event-log-to-file",
        action="store_true",
        default=None,
        help="If specified, events will be written to the file specified by --event-log-file "
        "or $RMS_CLOUD_TASKS_EVENT_LOG_FILE (default if --task-file is specified) "
        "[overrides $RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE]",
    )
    parser.add_argument(
        "--no-event-log-to-file",
        action="store_false",
        dest="event_log_to_file",
        help="If specified, events will not be written to a file "
        "[overrides $RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE]",
    )
    parser.add_argument(
        "--event-log-to-queue",
        action="store_true",
        default=None,
        help="If specified, events will be written to a cloud-based queue (default if "
        "--task-file is not specified) [overrides $RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE]",
    )
    parser.add_argument(
        "--no-event-log-to-queue",
        action="store_false",
        dest="event_log_to_queue",
        help="If specified, events will not be written to a cloud-based queue "
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
        help="Price in USD/hour on this computer; optional information for the worker processes "
        "[overrides $RMS_CLOUD_TASKS_INSTANCE_PRICE]",
    )
    parser.add_argument(
        "--num-simultaneous-tasks",
        type=int,
        help="Number of concurrent tasks to process (defaults to number of vCPUs, or 1 if not "
        "specified) [overrides $RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE]",
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
        help="How long to wait in seconds for processes to gracefully finish after shutdown (SIGINT, "
        "SIGTERM, or Ctrl-C) is requested before killing them [overrides "
        "$RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD] (default 30 seconds)",
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
        help="If specified, retry tasks on premature exit [overrides $RMS_CLOUD_TASKS_RETRY_ON_EXIT]",
    )
    parser.add_argument(
        "--no-retry-on-exit",
        action="store_false",
        dest="retry_on_exit",
        help="If specified, do not retry tasks on premature exit (default) [overrides $RMS_CLOUD_TASKS_RETRY_ON_EXIT]",
    )
    parser.add_argument(
        "--retry-on-exception",
        action="store_true",
        default=None,
        help="If specified, retry tasks on unhandled exception [overrides $RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION]",
    )
    parser.add_argument(
        "--no-retry-on-exception",
        action="store_false",
        dest="retry_on_exception",
        help="If specified, do not retry tasks on unhandled exception (default) [overrides $RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION]",
    )
    parser.add_argument(
        "--retry-on-timeout",
        action="store_true",
        default=None,
        help="If specified, tasks will be retried if they exceed the maximum runtime specified "
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
        help="Set the log level to DEBUG",
    )

    return parser.parse_args(args)


class LocalTaskQueue:
    """A local task queue that reads tasks from a JSON file or a factory function."""

    def __init__(self, task_source: FCPath | Callable[[], Iterable[Dict[str, Any]]]):
        """Initialize the local task queue.

        Args:
            task_source: FCPath to JSON file containing tasks, or a function that returns an
                iterator of tasks.
        """
        if isinstance(task_source, FCPath):
            self._tasks_iter = self._yield_tasks_from_file(task_source)
        elif callable(task_source):
            self._tasks_iter = task_source()
        else:
            raise TypeError(
                f"task_source must be FCPath or callable, got {type(task_source).__name__}"
            )

    def _yield_tasks_from_file(self, task_file: FCPath) -> Iterable[Dict[str, Any]]:
        """
        Yield tasks from a JSON or YAML file as an iterator.

        This function uses streaming to read tasks files so that very large files can be
        processed without using a lot of memory or running slowly.

        Parameters:
            task_file: Path to the tasks file

        Yields:
            Task dictionaries (expected to have "task_id" and "data" keys)

        Raises:
            ValueError: If the file cannot be read
        """
        if task_file.suffix not in (".json", ".yaml", ".yml"):
            raise ValueError(
                f"Unsupported file format for tasks: {task_file}; must be .json, .yml, or .yaml"
            )
        with task_file.open(mode="r") as fp:
            if task_file.suffix == ".json":
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

    async def receive_tasks(self, max_count: int) -> List[Dict[str, Any]]:
        """Get a batch of tasks from the queue.

        Args:
            max_count: Maximum number of tasks to receive.

        Returns:
            List of tasks.
        """
        tasks: List[Dict[str, Any]] = []
        for _ in range(max_count):
            try:
                task = next(self._tasks_iter)
            except StopIteration:
                return tasks
            task["ack_id"] = str(uuid.uuid4())
            tasks.append(task)
        return tasks

    async def acknowledge_task(self, ack_id: str) -> None:
        """Mark a task as completed.

        Args:
            ack_id: The acknowledgement ID of the task.
        """
        # For local queue, we don't need to do anything
        pass

    async def retry_task(self, ack_id: str) -> None:
        """Mark a task as failed.

        Args:
            ack_id: The acknowledgement ID of the task.
        """
        # For local queue, we don't need to do anything
        pass


class WorkerData:
    """Class containing properties that can be safely inherited by child processes."""

    def __init__(self):
        # Initialize all attributes to None

        #: argparse.Namespace containing the command line arguments, including any additional
        #: arguments specified by the user
        self.args: argparse.Namespace | None = None
        self.provider: str | None = None  #: The cloud provider to use (AWS or GCP)
        self.project_id: str | None = None  #: The project ID to use (only for GCP)
        self.job_id: str | None = None  #: The job ID to use
        self.queue_name: str | None = None  #: The queue name to use
        self.exactly_once_queue: bool = False  #: Whether to use an exactly-once queue
        self.event_log_to_queue: bool = False  #: Whether to log events to a cloud-based queue
        #: The name of the cloud-based queue to log events to
        self.event_log_queue_name: str | None = None
        self.event_log_to_file: bool = False  #: Whether to log events to a file
        self.event_log_file: str | None = None  #: The name of the file to log events to
        self.instance_type: str | None = None  #: The instance type this task is running on
        self.num_cpus: int | None = None  #: The number of vCPUs on this computer
        self.memory_gb: int | None = None  #: The amount of memory on this computer
        self.local_ssd_gb: int | None = None  #: The amount of local SSD on this computer
        self.boot_disk_gb: int | None = None  #: The amount of boot disk on this computer
        self.is_spot: bool = False  #: Whether the instance is a spot instance
        self.price_per_hour: float | None = None  #: The price per hour for the instance
        self.num_simultaneous_tasks: int = 1  #: The number of simultaneous tasks to process
        self.max_runtime: int = 600  #: The maximum runtime for a task in seconds
        #: The time in seconds to wait for tasks to complete during shutdown
        self.shutdown_grace_period: int = 30
        self.retry_on_exit: bool = False  #: Whether to retry tasks on premature exit
        self.retry_on_exception: bool = False  #: Whether to retry tasks on unhandled exception
        self.retry_on_timeout: bool = False  #: Whether to retry tasks on timeout
        #: The number of seconds after worker start to simulate a spot termination notice
        self.simulate_spot_termination_after: float | None = None
        #: The number of seconds after a simulated spot termination notice to forcibly kill all running tasks
        self.simulate_spot_termination_delay: float | None = None
        self.shutdown_event: MP_Event | None = None  # Will be set to MP_Event
        self.termination_event: MP_Event | None = None  # Will be set to MP_Event

    @property
    def received_termination_notice(self) -> bool:
        """Whether the worker has received a termination notice. This is for a spot instance or
        system maintenance."""
        return self.termination_event.is_set()

    @property
    def received_shutdown_request(self) -> bool:
        """Whether the worker has received a shutdown request. This is for the user hitting
        Ctrl-C at the terminal or otherwise receiving a SIGINT or SIGTERM signal."""
        return self.shutdown_event.is_set()


class Worker:
    """Worker class for processing tasks from queues using multiprocessing."""

    def __init__(
        self,
        user_worker_function: Callable[[str, Dict[str, Any]], bool],
        *,
        task_source: Optional[str | Path | FCPath | Callable[[], Iterable[Dict[str, Any]]]] = None,
        args: Optional[Sequence[str]] = None,
        argparser: Optional[argparse.ArgumentParser] = None,
    ):
        """
        Initialize the worker.

        Args:
            user_worker_function: The function to execute for each task. It will be called
                with the task_id, task_data dictionary, and Worker object as arguments.
            task_source: Optional task source. Can be a filename, a pathlib.Path, a
                filecache.FCPath, or a function that returns an iterator of tasks. If specified,
                this will override the command line and environment variable task sources.
            args: Optional list of command line arguments (sys.argv[1:]).
            argparser: Optional argument parser to use. If provided, the command line
                arguments used by Worker will be added before arguments are parsed.
                The resulting argparse.Namespace can be retrieved from the WorkerData
                structure.
        """
        self._user_worker_function = user_worker_function

        # Parse command line arguments if provided
        parsed_args = _parse_args(argparser, args)

        if parsed_args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)

        logger.info("Configuration:")

        # Create the inheritable properties object first
        self._data = WorkerData()
        self._data.args = parsed_args

        # Set up multiprocessing events
        self._data.shutdown_event = MP_CTX.Event()  # type: ignore
        self._data.termination_event = MP_CTX.Event()  # type: ignore

        # Check if we're using a local tasks file
        self._task_source = None
        if task_source is not None:
            # Override both the command line and the environment variable
            if callable(task_source):
                self._task_source = task_source
            else:
                self._task_source = FCPath(task_source)
        else:
            self._task_source = parsed_args.task_file
            if self._task_source is None:
                self._task_source = os.getenv("RMS_CLOUD_TASKS_TASK_FILE")
            if self._task_source is not None:
                self._task_source = FCPath(self._task_source)
        if self._task_source:
            if callable(self._task_source):
                logger.info("  Using task factory function")
            else:
                logger.info(f'  Using local tasks file: "{self._task_source}"')
        self._task_source_is_empty = False

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

        # Get provider from args or environment variable
        self._data.provider = parsed_args.provider or os.getenv("RMS_CLOUD_TASKS_PROVIDER")
        if self._data.provider is None:
            if not self._task_source:
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
        if self._data.provider is not None:
            self._data.provider = self._data.provider.upper()
        logger.info(f"  Provider: {self._data.provider}")

        # Get project ID from args or environment variable (optional - only for GCP)
        self._data.project_id = parsed_args.project_id or os.getenv("RMS_CLOUD_TASKS_PROJECT_ID")
        logger.info(f"  Project ID: {self._data.project_id}")

        # Get job ID from args or environment variable
        self._data.job_id = parsed_args.job_id or os.getenv("RMS_CLOUD_TASKS_JOB_ID")
        logger.info(f"  Job ID: {self._data.job_id}")

        # Get queue name from args or environment variable
        self._data.queue_name = parsed_args.queue_name or os.getenv("RMS_CLOUD_TASKS_QUEUE_NAME")
        if self._data.queue_name is None:
            self._data.queue_name = self._data.job_id
        logger.info(f"  Queue name: {self._data.queue_name}")

        if self._data.queue_name is None and not self._task_source:
            logger.error(
                "Queue name not specified via --queue-name or RMS_CLOUD_TASKS_QUEUE_NAME "
                "or --job-id or RMS_CLOUD_TASKS_JOB_ID and no tasks file specified via --task-file"
            )
            sys.exit(1)

        # Get exactly-once queue flag from args or environment variable
        self._data.exactly_once_queue = parsed_args.exactly_once_queue
        if self._data.exactly_once_queue is None:
            self._data.exactly_once_queue = os.getenv("RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE")
            if self._data.exactly_once_queue is None:
                self._data.exactly_once_queue = False
            else:
                self._data.exactly_once_queue = self._data.exactly_once_queue.lower() in (
                    "true",
                    "1",
                )
        logger.info(f"  Exactly-once queue: {self._data.exactly_once_queue}")

        # Get event log to file from args or environment variable
        self._data.event_log_to_file = parsed_args.event_log_to_file
        if self._data.event_log_to_file is None:
            self._data.event_log_to_file = os.getenv("RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE")
            if self._data.event_log_to_file is not None:
                self._data.event_log_to_file = self._data.event_log_to_file.lower() in (
                    "true",
                    "1",
                )
            else:
                self._data.event_log_to_file = self._task_source is not None
        logger.info(f"  Event log to file: {self._data.event_log_to_file}")

        # Get event log file from args or environment variable
        self._data.event_log_file = parsed_args.event_log_file or os.getenv(
            "RMS_CLOUD_TASKS_EVENT_LOG_FILE"
        )
        if not self._data.event_log_file:
            self._data.event_log_file = "events.log"
        logger.info(f"  Event log file: {self._data.event_log_file}")

        self._data.event_log_to_queue = parsed_args.event_log_to_queue
        if self._data.event_log_to_queue is None:
            self._data.event_log_to_queue = os.getenv("RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE")
            if self._data.event_log_to_queue is not None:
                self._data.event_log_to_queue = self._data.event_log_to_queue.lower() in (
                    "true",
                    "1",
                )
            else:
                self._data.event_log_to_queue = self._task_source is None
        logger.info(f"  Event log to queue: {self._data.event_log_to_queue}")

        self._data.event_log_queue_name = None
        if self._data.event_log_to_queue:
            if self._data.queue_name:
                self._data.event_log_queue_name = f"{self._data.queue_name}-events"
                logger.info(f"  Event log queue name: {self._data.event_log_queue_name}")
            else:
                logger.error("--event-log-to-queue requires either --job-id or --queue-name")
                sys.exit(1)

        # Get instance type from args or environment variable
        self._data.instance_type = parsed_args.instance_type or os.getenv(
            "RMS_CLOUD_TASKS_INSTANCE_TYPE"
        )
        logger.info(f"  Instance type: {self._data.instance_type}")

        # Get number of vCPUs from args or environment variable
        self._data.num_cpus = parsed_args.num_cpus
        if self._data.num_cpus is None:
            self._data.num_cpus = os.getenv("RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS")
        if self._data.num_cpus is not None:
            self._data.num_cpus = int(self._data.num_cpus)
        logger.info(f"  Num CPUs: {self._data.num_cpus}")

        # Get memory from args or environment variable
        self._data.memory_gb = parsed_args.memory
        if self._data.memory_gb is None:
            self._data.memory_gb = os.getenv("RMS_CLOUD_TASKS_INSTANCE_MEM_GB")
        if self._data.memory_gb is not None:
            self._data.memory_gb = float(self._data.memory_gb)
        logger.info(f"  Memory: {self._data.memory_gb} GB")

        # Get local SSD from args or environment variable
        self._data.local_ssd_gb = parsed_args.local_ssd
        if self._data.local_ssd_gb is None:
            self._data.local_ssd_gb = os.getenv("RMS_CLOUD_TASKS_INSTANCE_SSD_GB")
        if self._data.local_ssd_gb is not None:
            self._data.local_ssd_gb = float(self._data.local_ssd_gb)
        logger.info(f"  Local SSD: {self._data.local_ssd_gb} GB")

        # Get boot disk size from args or environment variable
        self._data.boot_disk_gb = parsed_args.boot_disk
        if self._data.boot_disk_gb is None:
            self._data.boot_disk_gb = os.getenv("RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB")
        if self._data.boot_disk_gb is not None:
            self._data.boot_disk_gb = float(self._data.boot_disk_gb)
        logger.info(f"  Boot disk size: {self._data.boot_disk_gb} GB")

        # Get spot instance flag from args or environment variable
        self._data.is_spot = parsed_args.is_spot
        if self._data.is_spot is None:
            self._data.is_spot = os.getenv("RMS_CLOUD_TASKS_INSTANCE_IS_SPOT")
            if self._data.is_spot is not None:
                self._data.is_spot = self._data.is_spot.lower() in ("true", "1")
        logger.info(f"  Spot instance: {self._data.is_spot}")

        # Get price per hour from args or environment variable
        self._data.price_per_hour = parsed_args.price
        if self._data.price_per_hour is None:
            self._data.price_per_hour = os.getenv("RMS_CLOUD_TASKS_INSTANCE_PRICE")
        if self._data.price_per_hour is not None:
            self._data.price_per_hour = float(self._data.price_per_hour)
        logger.info(f"  Price per hour: {self._data.price_per_hour}")

        # Determine number of tasks per worker
        self._data.num_simultaneous_tasks = parsed_args.num_simultaneous_tasks
        if self._data.num_simultaneous_tasks is None:
            self._data.num_simultaneous_tasks = os.getenv("RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE")
        if self._data.num_simultaneous_tasks is not None:
            self._data.num_simultaneous_tasks = int(self._data.num_simultaneous_tasks)
            logger.info(f"  Num simultaneous tasks: {self._data.num_simultaneous_tasks}")
        else:
            if self._data.num_cpus is not None:
                self._data.num_simultaneous_tasks = self._data.num_cpus
            else:
                self._data.num_simultaneous_tasks = 1
            logger.info(f"  Num simultaneous tasks (default): {self._data.num_simultaneous_tasks}")

        # Get maximum runtime from args or environment variable
        self._data.max_runtime = parsed_args.max_runtime
        if self._data.max_runtime is None:
            self._data.max_runtime = os.getenv("RMS_CLOUD_TASKS_MAX_RUNTIME")
        if self._data.max_runtime is None:
            self._data.max_runtime = 600  # Default to 10 minutes
        else:
            self._data.max_runtime = int(self._data.max_runtime)
        logger.info(f"  Maximum runtime: {self._data.max_runtime} seconds")

        # Get shutdown grace period from args or environment variable
        self._data.shutdown_grace_period = (
            parsed_args.shutdown_grace_period
            if parsed_args.shutdown_grace_period is not None
            else int(os.getenv("RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD", 30))
        )
        logger.info(f"  Shutdown grace period: {self._data.shutdown_grace_period} seconds")

        # Get retry on exit from args or environment variable
        self._data.retry_on_exit = parsed_args.retry_on_exit
        if self._data.retry_on_exit is None:
            retry_str = os.getenv("RMS_CLOUD_TASKS_RETRY_ON_EXIT")
            if retry_str is not None:
                self._data.retry_on_exit = retry_str.lower() in ("true", "1")
        logger.info(f"  Retry on exit: {self._data.retry_on_exit}")

        # Get retry on exception from args or environment variable
        self._data.retry_on_exception = parsed_args.retry_on_exception
        if self._data.retry_on_exception is None:
            retry_str = os.getenv("RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION")
            if retry_str is not None:
                self._data.retry_on_exception = retry_str.lower() in ("true", "1")
        logger.info(f"  Retry on exception: {self._data.retry_on_exception}")

        # Get retry on timeout from args or environment variable
        self._data.retry_on_timeout = parsed_args.retry_on_timeout
        if self._data.retry_on_timeout is None:
            retry_str = os.getenv("RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT")
            if retry_str is not None:
                self._data.retry_on_timeout = retry_str.lower() in ("true", "1")
        logger.info(f"  Retry on timeout: {self._data.retry_on_timeout}")

        # Get simulate spot termination after from args or environment variable
        self._data.simulate_spot_termination_after = parsed_args.simulate_spot_termination_after
        if self._data.simulate_spot_termination_after is None:
            after_str = os.getenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER")
            if after_str is not None:
                self._data.simulate_spot_termination_after = float(after_str)
        if self._data.simulate_spot_termination_after is not None:
            logger.info(
                f"  Simulating spot termination after {self._data.simulate_spot_termination_after} seconds"
            )

            # Get simulate spot termination delay from args or environment variable
            self._data.simulate_spot_termination_delay = parsed_args.simulate_spot_termination_delay
            if self._data.simulate_spot_termination_delay is None:
                delay_str = os.getenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY")
                if delay_str is not None:
                    self._data.simulate_spot_termination_delay = float(delay_str)
            if self._data.simulate_spot_termination_delay is not None:
                logger.info(
                    "    Simulating spot termination delay of "
                    f"{self._data.simulate_spot_termination_delay} seconds"
                )
            else:
                logger.warning(
                    "  Simulating spot termination after but no delay specified; "
                    "tasks will never be killed"
                )

        # State tracking
        self._running = False
        self._task_queue: Any = None

        # Track processes
        self._next_worker_id: int = 0
        self._processes: Dict[int, Dict[str, Any]] = {}  # Maps worker # to process and task
        self._num_tasks_not_retried: int = 0
        self._num_tasks_retried: int = 0
        self._num_tasks_timed_out: int = 0
        self._num_tasks_exited: int = 0
        self._num_tasks_exception: int = 0

        # Task queue for inter-process communication
        self._result_queue: MP_Queue = MP_CTX.Queue()  # type: ignore

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
    def _is_spot(self) -> bool:
        """Whether the worker is running on a spot instance."""
        return self._data.is_spot or self._data.simulate_spot_termination_after is not None

    def _signal_handler(self, signum, frame):
        """Handle termination signals."""
        signal_name = signal.Signals(signum).name
        logger.warning(f"Received signal {signal_name}, initiating graceful shutdown")
        self._data.shutdown_event.set()
        signal.signal(signal.SIGINT, signal.SIG_DFL)  # So a second time will kill the process
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    async def _queue_acknowledge_task_with_logging(self, task: Dict[str, Any]) -> None:
        """Complete a task with logging of errors."""
        try:
            async with self._task_queue_semaphore:
                await self._task_queue.acknowledge_task(task["ack_id"])
        except Exception as e:
            logger.error(f"Error completing task {task['task_id']}: {e}", exc_info=True)
            await self._log_non_fatal_exception(traceback.format_exc())

    async def _queue_retry_task_with_logging(self, task: Dict[str, Any]) -> None:
        """Fail a task with logging of errors."""
        try:
            async with self._task_queue_semaphore:
                await self._task_queue.retry_task(task["ack_id"])
        except Exception as e:
            logger.error(f"Error failing task {task['task_id']}: {e}", exc_info=True)
            await self._log_non_fatal_exception(traceback.format_exc())

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
        self._task_list = []

        if self._data.event_log_to_file:
            logger.debug(f'Starting event logger for file "{self._data.event_log_file}"')
            try:
                self._event_logger_fp = open(self._data.event_log_file, "a")
            except Exception as e:
                logger.error(f"Error opening event log file: {e}", exc_info=True)
                sys.exit(1)

        if self._data.event_log_to_queue:
            logger.debug(f'Starting event logger for queue "{self._data.event_log_queue_name}"')
            try:
                self._event_logger_queue = await create_queue(
                    provider=self._data.provider,
                    queue_name=self._data.event_log_queue_name,
                    project_id=self._data.project_id,
                    exactly_once=self._data.exactly_once_queue,
                )
            except Exception as e:
                logger.error(f"Error initializing event log queue: {e}", exc_info=True)
                sys.exit(1)

        if self._task_source:
            if isinstance(self._task_source, FCPath):
                logger.debug(f'Starting task scheduler for local tasks file "{self._task_source}"')
            else:
                logger.debug("Starting task scheduler for factory task queue")

            try:
                self._task_queue = LocalTaskQueue(self._task_source)
            except Exception as e:
                logger.error(f"Error initializing local task queue: {e}", exc_info=True)
                await self._log_fatal_exception(traceback.format_exc())
                sys.exit(1)
        else:
            logger.debug(
                f"Starting task scheduler for {self._data.provider.upper()} queue "
                f'"{self._data.queue_name}"'
            )
            try:
                self._task_queue = await create_queue(
                    provider=self._data.provider,
                    queue_name=self._data.queue_name,
                    project_id=self._data.project_id,
                    visibility_timeout=self._data.max_runtime
                    + 5,  # Add 5 seconds to account for the
                    # time it takes to notice an event is over time, kill it, and fail the task.
                    # We only want the message to timeout if something goes really wrong with the
                    # task manager.
                    exactly_once=self._data.exactly_once_queue,
                )
            except Exception as e:
                logger.error(f"Error initializing task queue: {e}", exc_info=True)
                await self._log_fatal_exception(traceback.format_exc())
                sys.exit(1)

        self._start_time = time.time()
        self._running = True

        # Start the result handler in the main process
        self._task_list.append(asyncio.create_task(self._handle_results()))

        # Start the task feeder to get tasks from the queue
        self._task_list.append(asyncio.create_task(self._feed_tasks_to_workers()))

        # Start the process runtime monitor
        self._task_list.append(asyncio.create_task(self._monitor_process_runtimes()))

        # Start the termination check loop
        if self._is_spot:
            self._task_list.append(asyncio.create_task(self._check_termination_loop()))

        # Process tasks until shutdown
        await self._wait_for_shutdown()
        await self._cleanup_tasks()

        total = self._num_tasks_not_retried + self._num_tasks_retried
        logger.info(
            f"Task scheduler shutdown complete. Not retried: {self._num_tasks_not_retried}, "
            f"Retried: {self._num_tasks_retried}, Timed out: {self._num_tasks_timed_out}, "
            f"Exited: {self._num_tasks_exited}, Total: {total}"
        )
        # We don't log an event here because this only happens when the user hits Ctrl-C

    async def _cleanup_tasks(self) -> None:
        """Cleanup all tasks."""
        self._running = False
        for task in self._task_list:
            # Needed because of mocks used during testing
            try:
                await task
            except Exception:  # pragma: no cover
                pass
        self._task_list = []

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

                        if worker_id not in self._processes:  # pragma: no cover
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
                                f"{'retrying' if self._data.retry_on_exception else 'not retrying'}: "
                                f"{result}"
                            )
                            await self._log_task_exception(
                                task["task_id"],
                                retry=self._data.retry_on_exception,
                                elapsed_time=elapsed_time,
                                exception=result,
                            )
                            if self._data.retry_on_exception:
                                self._num_tasks_retried += 1
                                await self._queue_retry_task_with_logging(task)
                            else:
                                self._num_tasks_not_retried += 1
                                await self._queue_acknowledge_task_with_logging(task)
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
                            await self._queue_retry_task_with_logging(task)
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
                            await self._queue_acknowledge_task_with_logging(task)
                        del self._processes[worker_id]
                        if worker_id in exited_processes:
                            del exited_processes[worker_id]

                    # Check for processes that exited prematurely; we didn't get result messages
                    # from these
                    for worker_id, process_data in exited_processes.items():
                        self._num_tasks_exited += 1
                        try:
                            exit_code = process_data["process"].exitcode
                        except Exception:  # pragma: no cover
                            exit_code = None
                        task = process_data["task"]
                        elapsed_time = time.time() - process_data["start_time"]
                        logger.warning(
                            f"Worker #{worker_id} (PID {p.pid}) processing task "
                            f'"{task["task_id"]}" exited prematurely in {elapsed_time:.1f} seconds '
                            f"with exit code {exit_code}; "
                            f"{'retrying' if self._data.retry_on_exit else 'not retrying'}"
                        )
                        await self._log_task_exited(
                            task["task_id"],
                            retry=self._data.retry_on_exit,
                            elapsed_time=elapsed_time,
                            exit_code=exit_code,
                        )

                        if self._data.retry_on_exit:
                            self._num_tasks_retried += 1
                            await self._queue_retry_task_with_logging(task)
                        else:
                            self._num_tasks_not_retried += 1
                            # If we're not retrying on exit, mark it as complete
                            await self._queue_acknowledge_task_with_logging(task)

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
        while self._running and not self._data.received_shutdown_request:
            if self._task_source_is_empty:
                if len(self._processes) == 0:
                    logger.info("Local task source is empty and all processes complete; exiting")
                    self._running = False
                    return
                logger.info(
                    f"Local task source is empty; waiting for {len(self._processes)} processes to "
                    "complete"
                )
                await asyncio.sleep(interval)
                continue

            if len(self._processes) == 0 and self._data.received_termination_notice:
                logger.info("Termination event set and all processes complete; exiting")
                self._running = False
                return
            await asyncio.sleep(interval)

        logger.info("Shutdown requested, stopping worker processes")
        self._data.shutdown_event.set()

        # Allow processes some time to finish current tasks
        shutdown_start = time.time()
        while (
            len(self._processes) > 0
            and time.time() - shutdown_start < self._data.shutdown_grace_period
        ):
            remaining_time = self._data.shutdown_grace_period - (time.time() - shutdown_start)
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
        while self._running and not self._data.received_shutdown_request:
            try:
                termination_notice = await self._check_termination_notice()
                if termination_notice and not self._data.received_termination_notice:
                    logger.warning("Instance termination notice received")
                    self._data.termination_event.set()
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
            if self._data.simulate_spot_termination_after is not None:
                await asyncio.sleep(0.1)
            else:
                await asyncio.sleep(5)

        if self._running and self._data.simulate_spot_termination_delay is not None:
            # If we're simulating a spot termination, wait for the delay and then kill all
            # running processes
            await asyncio.sleep(self._data.simulate_spot_termination_delay)
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
        if self._data.simulate_spot_termination_after is not None:
            elapsed_time = time.time() - self._start_time
            if elapsed_time >= self._data.simulate_spot_termination_after:
                logger.info(
                    f"Simulating spot termination notice received after {elapsed_time:.1f} seconds"
                )
                return True
            return False

        try:
            if self._data.provider == "AWS":
                # AWS spot termination check
                response = requests.get(
                    "http://169.254.169.254/latest/meta-data/spot/instance-action", timeout=2
                )
                return response.status_code == 200

            elif self._data.provider == "GCP":
                # GCP preemption check
                response = requests.get(
                    "http://metadata.google.internal/computeMetadata/v1/instance/preempted",
                    headers={"Metadata-Flavor": "Google"},
                    timeout=2,
                )
                return response.text.strip().lower() == "true"

            elif self._data.provider == "AZURE":
                # TODO Azure doesn't have a direct API yet
                return False

        except Exception:  # pragma: no cover
            pass

        # Can't happen
        return False  # pragma: no cover

    async def _feed_tasks_to_workers(self) -> None:
        """Fetch tasks from the cloud queue and feed them to worker processes."""
        while self._running:
            try:
                if self._data.received_shutdown_request or self._data.received_termination_notice:
                    # If we're shutting down for any reason, don't start any new tasks
                    await asyncio.sleep(1)
                    continue

                # Only fetch new tasks if we have capacity
                max_concurrent = self._data.num_simultaneous_tasks
                if len(self._processes) >= max_concurrent:
                    # Wait for workers to process tasks
                    await asyncio.sleep(0.1)
                    continue

                # Receive tasks
                async with self._task_queue_semaphore:
                    tasks = await self._task_queue.receive_tasks(
                        max_count=max(0, min(5, max_concurrent - len(self._processes)))
                    )

                if tasks:
                    for task in tasks:
                        if self._task_skip_count is not None and self._task_skip_count > 0:
                            self._task_skip_count -= 1
                            logger.debug(f"Skipping task {task['task_id']}")
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
                            p = MP_CTX.Process(
                                target=self._worker_process_main,
                                args=(
                                    worker_id,
                                    self._user_worker_function,
                                    self._data,
                                    task["task_id"],  # Can't pass in task because it may have
                                    task["data"],  # non-serializable fields
                                    self._result_queue,
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
                    # If no tasks and using a local task file or function, we're done
                    if self._task_source is not None:
                        self._task_source_is_empty = True
                    # If no tasks, sleep to avoid hammering the queue
                    await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error feeding tasks to workers: {e}", exc_info=True)
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
                    if runtime <= self._data.max_runtime:
                        continue

                    self._num_tasks_timed_out += 1
                    logger.warning(
                        f"Worker #{worker_id} (PID {p.pid}), task "
                        f"{process_data['task']['task_id']} exceeded max runtime of "
                        f"{self._data.max_runtime} seconds (actual runtime {runtime:.1f} seconds); "
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
                        if self._data.retry_on_timeout:
                            self._num_tasks_retried += 1
                            logger.info(
                                f"Worker #{worker_id}: Task {task['task_id']} will be retried"
                            )
                            try:
                                await self._queue_retry_task_with_logging(task)
                            except Exception as e:
                                logger.error(
                                    f"Error failing task {task['task_id']} after "
                                    f"exception: {e}",
                                    exc_info=True,
                                )
                                await self._log_non_fatal_exception(traceback.format_exc())
                        else:
                            self._num_tasks_not_retried += 1
                            logger.info(
                                f"Worker #{worker_id}: Task {task['task_id']} will not be retried"
                            )
                            try:
                                await self._queue_acknowledge_task_with_logging(task)
                            except Exception as e:
                                logger.error(
                                    f"Error completing task {task['task_id']} after "
                                    f"exception: {e}",
                                    exc_info=True,
                                )
                                await self._log_non_fatal_exception(traceback.format_exc())
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
        worker_data: WorkerData,
        task_id: str,
        task_data: Dict[str, Any],
        result_queue: MP_Queue,
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
            logger.info(f"Worker #{worker_id}: Started, processing task {task_id}")
            start_time = time.time()

            # Process the task
            try:
                # Execute task in isolated environment
                retry, result = Worker._execute_task_isolated(
                    task_id,
                    task_data,
                    worker_data,
                    user_worker_function,
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
        worker_data: WorkerData,
        user_worker_function: Callable[[str, Dict[str, Any]], bool],
    ) -> Tuple[bool, str]:
        """
        Execute a task in isolation.

        This static method executes a task without dependencies on the main Worker class,
        allowing it to run in a separate process.

        Args:
            task_id: Unique ID for the task
            task_data: Task data to process
            worker_data: WorkerData object containing properties that can be safely inherited
            user_worker_function: The function to execute for each task

        Returns:
            Tuple of (retry, result)
        """
        return user_worker_function(task_id, task_data, worker_data)
