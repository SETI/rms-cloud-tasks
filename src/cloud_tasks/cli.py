"""
Command-line interface for the multi-cloud task processing system.
"""

import argparse
import asyncio
from datetime import datetime
import json
import json_stream
import logging
import os
from pathlib import Path
import signal
import sys
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, Iterable, Optional, Tuple
import yaml  # type: ignore

from filecache import FCPath
from prettytable import PrettyTable, TableStyle
import pydantic

from .common.config import Config, load_config
from .common.logging_config import configure_logging
from .common.task_db import TaskDatabase
from .common.time_utils import parse_utc, utc_now
from .instance_manager import create_instance_manager
from .instance_manager.orchestrator import InstanceOrchestrator
from .queue_manager import create_queue


# Use custom logging configuration
configure_logging(level=logging.WARNING)
logger = logging.getLogger(__name__)


def yield_tasks_from_file(
    task_file: str, start_task: Optional[int] = None, limit: Optional[int] = None
) -> Iterable[Dict[str, Any]]:
    """
    Yield tasks from a JSON or YAML file as an iterator.

    This function uses streaming to read tasks files so that very large files can be
    processed without using a lot of memory or running slowly.

    Parameters:
        tasks_file: Path to the tasks file
        start_task: Index of the first task to yield
        limit: Number of tasks to yield

    Yields:
        Task dictionaries (expected to have "task_id" and "data" keys)

    Raises:
        ValueError: If the file cannot be read
    """
    if not task_file.endswith((".json", ".yaml", ".yml")):
        raise ValueError(
            f"Unsupported file format for tasks: {task_file}; must be .json, .yml, or .yaml"
        )
    if limit is not None and limit <= 0:
        return

    with FCPath(task_file).open(mode="r") as fp:
        if task_file.endswith(".json"):
            for task in json_stream.load(fp):
                ret = json_stream.to_standard_types(task)  # Convert to a dict
                if start_task is not None and start_task > 0:
                    start_task -= 1
                    continue
                if limit is not None:
                    if limit == 0:
                        return
                    limit -= 1
                yield ret
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
                    ret = yaml.load(y, Loader=yaml.Loader)[0]
                    y = ln
                    if start_task is not None and start_task > 0:
                        start_task -= 1
                        continue
                    if limit is not None:
                        if limit == 0:
                            return
                        limit -= 1
                    yield ret


async def show_queue_cmd(args: argparse.Namespace, config: Config) -> None:
    """Show the current depth of a task queue.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)
    queue_name = provider_config.queue_name
    print(f"Checking queue depth for '{queue_name}'...")

    try:
        task_queue = await create_queue(config)
    except Exception as e:
        logger.fatal(f"Error connecting to queue: {e}", exc_info=True)
        print(f"\nError connecting to queue: {e}")
        print("\nPlease check your configuration and ensure the queue exists.")
        sys.exit(1)

    # Get queue depth
    try:
        queue_depth = await task_queue.get_queue_depth()
    except Exception as e:
        logger.fatal(f"Error getting queue depth: {e}", exc_info=True)
        print(f"\nError retrieving queue depth: {e}")
        print("\nThe queue may exist but you might not have permission to access it.")
        sys.exit(1)

    if queue_depth is None:
        print("Failed to get queue depth.")
        sys.exit(1)

    print(f"Queue depth: {queue_depth}")

    if queue_depth == 0:
        print("\nQueue is empty. No messages available.")
    elif args.detail:
        # If verbose, try to get a sample message without removing it
        # We try this even if the queue depth failed
        print("\nAttempting to peek at first message...")
        try:
            messages = await task_queue.receive_tasks(max_count=1)

            if messages:
                message = messages[0]
                await task_queue.retry_task(message["ack_id"])  # Return to queue
                task_id = message.get("task_id", "unknown")

                print("\n" + "-" * 50)
                print("SAMPLE MESSAGE")
                print("-" * 50)
                print(f"Task ID: {task_id}")

                # Get receipt handle info based on provider
                receipt_info = ""
                if "receipt_handle" in message:  # AWS
                    receipt_info = (
                        f"Receipt Handle: {message['receipt_handle'][:50]}..."
                        if len(message.get("receipt_handle", "")) > 50
                        else f"Receipt Handle: {message.get('receipt_handle', '')}"
                    )
                elif "ack_id" in message:  # GCP
                    receipt_info = (
                        f"Ack ID: {message['ack_id'][:50]}..."
                        if len(message.get("ack_id", "")) > 50
                        else f"Ack ID: {message.get('ack_id', '')}"
                    )
                elif "lock_token" in message:  # Azure
                    receipt_info = (
                        f"Lock Token: {message['lock_token'][:50]}..."
                        if len(message.get("lock_token", "")) > 50
                        else f"Lock Token: {message.get('lock_token', '')}"
                    )

                if receipt_info:
                    print(f"{receipt_info}")

                try:
                    data = message.get("data", {})
                    print("\nData:")
                    if isinstance(data, dict):
                        print(json.dumps(data, indent=2))
                    else:
                        print(data)
                except Exception as e:
                    print(f"Error displaying data: {e}")
                    print(f"Raw data: {message.get('data', {})}")

                print("\nNote: Message was not removed from the queue.")
            else:
                print("\nCould not retrieve a sample message. This might happen if:")
                print("  - Another consumer received the message")
                print("  - The message is not available for immediate delivery")
                print("  - There's an issue with queue visibility settings")
        except Exception as e:
            logger.fatal(f"Error peeking at message: {e}", exc_info=True)


async def purge_queue_cmd(args: argparse.Namespace, config: Config) -> None:
    """Empty a task queue by removing all messages from it.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)
    task_queue_name = provider_config.queue_name
    event_queue_name = f"{task_queue_name}-events"

    if not args.task_queue_only:
        task_queue = await create_queue(config)
    if not args.event_queue_only:
        event_queue = await create_queue(config, queue_name=event_queue_name)

    if not args.event_queue_only:
        queue_depth = await task_queue.get_queue_depth()

        if queue_depth is None:
            print(f"Failed to get queue depth for task queue '{task_queue_name}'.")
        else:
            print(f"Task queue '{task_queue_name}' has {queue_depth} messages.")

        # Confirm with the user if not using --force
        if not args.force:
            confirm = input(
                f"\nWARNING: This will permanently delete all {queue_depth}+ messages from queue "
                f"'{task_queue_name}' on '{provider}'."
                f"\nType 'EMPTY {task_queue_name}' to confirm: "
            )
            if confirm != f"EMPTY {task_queue_name}":
                print("Operation cancelled.")
                return

        print(f"Emptying queue '{task_queue_name}'...")
        await task_queue.purge_queue()

    if not args.task_queue_only:
        queue_depth = await event_queue.get_queue_depth()

        if queue_depth is None:
            print(f"Failed to get queue depth for event queue '{event_queue_name}'.")
        else:
            print(f"Event queue '{event_queue_name}' has {queue_depth} messages.")

        # Confirm with the user if not using --force
        if not args.force:
            confirm = input(
                f"\nWARNING: This will permanently delete all {queue_depth}+ messages from queue "
                f"'{event_queue_name}' on '{provider}'."
                f"\nType 'EMPTY {event_queue_name}' to confirm: "
            )
            if confirm != f"EMPTY {event_queue_name}":
                print("Operation cancelled.")
                return

        print(f"Emptying queue '{event_queue_name}'...")
        await event_queue.purge_queue()


async def delete_queue_cmd(args: argparse.Namespace, config: Config) -> None:
    """Delete a task queue entirely from the cloud provider.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)
    task_queue_name = provider_config.queue_name
    event_queue_name = f"{task_queue_name}-events"

    if not args.task_queue_only:
        task_queue = await create_queue(config)
    if not args.event_queue_only:
        event_queue = await create_queue(config, queue_name=event_queue_name)

    if not args.event_queue_only:
        # Confirm with the user if not using --force
        if not args.force:
            confirm = input(
                f"\nWARNING: This will permanently delete the queue '{task_queue_name}' from {provider}.\n"
                f"This operation cannot be undone and will remove all infrastructure.\n"
                f"Type 'DELETE {task_queue_name}' to confirm: "
            )
            if confirm != f"DELETE {task_queue_name}":
                print("Operation cancelled.")
                return

        try:
            print(f"Deleting queue '{task_queue_name}' from {provider}...")
            await task_queue.delete_queue()
            print(f"Queue '{task_queue_name}' has been deleted.")
        except Exception as e:
            logger.fatal(f"Error deleting task queue: {e}", exc_info=True)
            sys.exit(1)

    if not args.task_queue_only:
        # Confirm with the user if not using --force
        if not args.force:
            confirm = input(
                f"\nWARNING: This will permanently delete the queue '{event_queue_name}' from {provider}.\n"
                f"This operation cannot be undone and will remove all infrastructure.\n"
                f"Type 'DELETE {event_queue_name}' to confirm: "
            )
            if confirm != f"DELETE {event_queue_name}":
                print("Operation cancelled.")
                return

        try:
            print(f"Deleting queue '{event_queue_name}' from {provider}...")
            await event_queue.delete_queue()
            print(f"Queue '{event_queue_name}' has been deleted.")
        except Exception as e:
            logger.fatal(f"Error deleting results queue: {e}", exc_info=True)
            sys.exit(1)


async def list_running_instances_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    List all running instances for the specified provider.

    Parameters:
        args: Command-line arguments
    """
    try:
        # Create instance manager
        instance_manager = await create_instance_manager(config)

        # Get list of running instances
        tag_filter = {}
        if args.job_id:
            tag_filter["rms_cloud_tasks_job_id"] = args.job_id
            print(f"Listing instances with job ID: {args.job_id}\n")
        else:
            if args.all_instances:
                print("Listing all instances including ones not created by cloud tasks\n")
            else:
                print("Listing all instances created by cloud tasks\n")

        try:
            # # For GCP, pass the region parameter explicitly
            # if args.provider == "gcp" and args.region and not instance_manager.zone:
            #     # List instances with specific region filter
            #     instances = await instance_manager.list_running_instances(
            #         tag_filter=tag_filter, region=config.region
            #     )
            # else:
            instances = await instance_manager.list_running_instances(
                job_id=args.job_id, include_non_job=args.all_instances
            )
        except Exception as e:
            logger.fatal(f"Error listing instances: {e}", exc_info=True)
            sys.exit(1)

        # Display instances
        if instances:
            # Define field mapping for sorting
            field_mapping = {
                "id": "id",
                "i": "id",
                "type": "type",
                "t": "type",
                "state": "state",
                "s": "state",
                "zone": "zone",
                "z": "zone",
                "created": "creation_time",
                "creation": "creation_time",
                "c": "creation_time",
                "time": "creation_time",
                "creation_time": "creation_time",
            }

            # Apply custom sorting if specified
            if args.sort_by:
                sort_fields = args.sort_by.split(",")
                if sort_fields:
                    for sort_field in sort_fields[::-1]:
                        if sort_field.startswith("-"):
                            descending = True
                            sort_field = sort_field[1:]
                        else:
                            descending = False
                        field_name = field_mapping.get(sort_field.lower())
                        if field_name is None:
                            print(f"Invalid sort field: {sort_field}")
                            sys.exit(1)
                        instances.sort(key=lambda x: x.get(field_name, ""), reverse=descending)
                else:
                    # Default sort if no fields specified
                    instances.sort(key=lambda x: x.get("id", ""))
            else:
                # Default sort by ID if no sort-by specified
                instances.sort(key=lambda x: x.get("id", ""))

            instance_count = 0
            state_counts = {}

            if args.detail:
                for instance in instances:
                    if not args.include_terminated and instance.get("state") == "terminated":
                        continue
                    instance_count += 1

                    instance_id = instance.get("id", "N/A")[:64]
                    instance_type = instance.get("type", "N/A")
                    state = instance.get("state", "N/A")
                    created_at = instance.get("creation_time", "N/A")
                    zone = instance.get("zone", "N/A")
                    private_ip = instance.get("private_ip", "N/A")
                    public_ip = instance.get("public_ip", "N/A")
                    job_id = instance.get("job_id", "N/A")

                    state_counts[state] = state_counts.get(state, 0) + 1

                    print(f"Instance ID: {instance_id}")
                    print(f"Type:        {instance_type}")
                    print(f"State:       {state}")

                    if zone:
                        print(f"Zone:        {zone}")

                    if job_id:
                        print(f"Job ID:      {job_id}")

                    print(f"Created:     {created_at}")

                    if private_ip:
                        print(f"Private IP:  {private_ip}")
                    if public_ip:
                        print(f"Public IP:   {public_ip}")
                    print()
            else:
                headers = ["Job ID", "ID", "Type", "State", "Zone", "Created"]
                rows = []
                for instance in instances:
                    if not args.include_terminated and instance.get("state") == "terminated":
                        continue
                    instance_count += 1
                    state = instance.get("state", "N/A")
                    state_counts[state] = state_counts.get(state, 0) + 1
                    rows.append(
                        [
                            instance.get("job_id", "N/A")[:25],
                            instance.get("id", "N/A")[:64],
                            instance.get("type", "N/A")[:15],
                            instance.get("state", "N/A")[:11],
                            instance.get("zone", "N/A")[:15],
                            instance.get("creation_time", instance.get("created_at", "N/A"))[:30],
                        ]
                    )
                table = PrettyTable()
                table.field_names = headers
                table.add_rows(rows)
                table.align = "l"
                table.set_style(TableStyle.SINGLE_BORDER)
                print(table)

            print(f"\nSummary: {instance_count} total instances")
            for state, count in sorted(state_counts.items()):
                print(f"  {count} {state}")
        else:
            if args.job_id:
                print(f"\nNo instances found for job ID: {args.job_id}")
            else:
                print("\nNo instances found")

    except Exception as e:
        logger.error(f"Error listing running instances: {e}", exc_info=True)
        sys.exit(1)


class EventMonitor:
    """Helper class for monitoring events from the event queue and updating the database."""

    def __init__(
        self,
        events_queue,
        task_db,
        output_file_path: Optional[str] = None,
        print_events: bool = True,
        print_summary: bool = True,
    ):
        """
        Initialize the event monitor.

        Args:
            events_queue: Queue to receive events from
            task_db: TaskDatabase instance
            output_file_path: Optional path to write events to
            print_events: Whether to print events to stdout
            print_summary: Whether to print summary statistics
        """
        self.events_queue = events_queue
        self.task_db = task_db
        self.output_file_path = output_file_path
        self.print_events = print_events
        self.print_summary = print_summary
        self.output_file = None
        self.something_changed = True

    async def start(self) -> None:
        """Start monitoring events."""
        if self.output_file_path:
            try:
                self.output_file = open(self.output_file_path, "a")
                logger.info(f'Writing events to "{self.output_file_path}"')
            except Exception as e:
                logger.fatal(
                    f'Error opening events file "{self.output_file_path}": {e}', exc_info=True
                )
                sys.exit(1)

    async def process_events_batch(self) -> int:
        """
        Process a batch of events from the queue.

        Returns:
            Number of events processed
        """
        try:
            # Receive a batch of messages
            messages = await self.events_queue.receive_messages(max_count=100)

            if messages:
                self.something_changed = True
                for message in messages:
                    try:
                        # Extract and parse the JSON data
                        data = json.loads(message.get("data", "{}"))

                        # Write to file if specified
                        if self.output_file:
                            self.output_file.write(json.dumps(data) + "\n")

                        # Print to stdout if requested
                        if self.print_events:
                            print(json.dumps(data))

                        # Update database
                        self.task_db.insert_event(data)
                        self.task_db.update_task_from_event(data)

                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding message: {e}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

                if self.output_file:
                    self.output_file.flush()

                return len(messages)
            else:
                return 0

        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
            raise

    def print_status_summary(self, force: bool = False) -> None:
        """
        Print a summary of the current status.
        Uses the shared task stats output also used when the job completes.

        Args:
            force: If True, always print even if nothing changed
        """
        if not self.print_summary:
            return

        if not force and not self.something_changed:
            return

        logger.info("")
        log_task_stats(self.task_db, header="Summary:", include_remaining_ids=True)
        self.something_changed = False

    def close(self) -> None:
        """Close the output file if open."""
        if self.output_file:
            self.output_file.close()


async def run_event_monitoring_loop(
    event_monitor: EventMonitor,
    task_db: TaskDatabase,
    check_completion: bool = True,
    stop_signal: Optional[asyncio.Event] = None,
) -> None:
    """
    Run the event monitoring loop.

    This is a shared function used by both the `run` and `monitor_event_queue` commands.

    Args:
        event_monitor: EventMonitor instance to use
        task_db: TaskDatabase instance for checking completion
        check_completion: Whether to check for task completion and stop when done
        stop_signal: Optional asyncio.Event to signal stopping the loop
    """
    job_complete = False

    while not job_complete:
        # Check stop signal
        if stop_signal and stop_signal.is_set():
            break

        try:
            did_something = False
            for _loop_count in range(20):
                count = await event_monitor.process_events_batch()
                if count == 0:
                    break
                did_something = True

            # Always print status summary (force=True if no new events)
            event_monitor.print_status_summary(force=True)

            # Check if all tasks are complete
            if check_completion and task_db.is_all_tasks_complete():
                logger.info("All tasks complete")
                job_complete = True
                break

            if did_something:
                await asyncio.sleep(1)
            else:
                await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"Error in event monitoring loop: {e}", exc_info=True)
            await asyncio.sleep(5)


async def monitor_event_queue_cmd(args, config: Config) -> None:
    """
    Monitor the event queue and update task status in SQLite database.

    This command is useful when running workers locally rather than using
    cloud-managed instances. It provides the same event monitoring and
    SQLite tracking as the unified `run` command, but without instance
    management.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)
    queue_name = provider_config.queue_name
    event_queue_name = f"{queue_name}-events"

    # Determine database file path
    db_file = args.db_file if args.db_file else config.run.db_file
    if not db_file:
        db_file = f"{provider_config.job_id}.db"

    logger.info(f"Using database file: {db_file}")

    # Initialize or open task database
    if not Path(db_file).exists():
        print(f"Error: Database file '{db_file}' does not exist.")
        print("You must first create tasks using the 'run' command or load them manually.")
        sys.exit(1)

    # Initialize variables that will be used in finally block
    task_db = TaskDatabase(db_file)
    event_monitor = None

    try:
        total_tasks = task_db.get_total_tasks()
        if total_tasks == 0:
            print(f"Warning: Database '{db_file}' has no tasks.")

        print(f"Monitoring event queue '{event_queue_name}'...")
        print(f"Database: {db_file} ({total_tasks} tasks)")

        # Create event queue
        events_queue = await create_queue(config, queue_name=event_queue_name)

        # Set up event monitor
        event_monitor = EventMonitor(
            events_queue,
            task_db,
            output_file_path=getattr(args, "output_file", None),
            print_events=getattr(args, "print_events", False),
            print_summary=True,
        )
        await event_monitor.start()

        # Run monitoring loop
        print("Starting event monitoring (Ctrl+C to stop)...")
        await run_event_monitoring_loop(
            event_monitor,
            task_db,
            check_completion=not getattr(args, "no_auto_complete", False),
            stop_signal=None,
        )

        print("\n=== Monitoring stopped ===")
        print_final_report(task_db)

    except KeyboardInterrupt:
        print("\n\nMonitoring interrupted by user.")
        print_final_report(task_db)
    except Exception as e:
        logger.error(f"Error monitoring event queue: {e}", exc_info=True)
        print(f"\nFatal error: {e}")
        sys.exit(1)
    finally:
        if event_monitor is not None:
            event_monitor.close()
        if task_db is not None:
            task_db.close()


async def load_queue_common(
    config: Config,
    db_file: str,
    task_file: str,
    start_task: Optional[int],
    limit: Optional[int],
    max_concurrent_queue_operations: int,
    force: bool = False,
) -> Tuple[TaskDatabase, Any, Any, int]:
    """
    Common logic: delete db, (confirm queue overwrite), delete/create queues,
    load tasks from file into database, enqueue to cloud.
    Used by both load_queue_cmd and run_cmd (fresh run).

    Returns:
        (task_db, task_queue, events_queue, num_tasks)
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)
    queue_name = provider_config.queue_name
    event_queue_name = f"{queue_name}-events"

    # Delete existing database
    db_path = Path(db_file)
    if db_path.exists():
        logger.info(f"Deleting existing database '{db_file}'...")
        os.remove(db_file)

    # Check existing queue depth before deletion
    queue_depth = None
    try:
        temp_queue = await create_queue(config)
        queue_depth = await temp_queue.get_queue_depth()
    except Exception as e:
        logger.debug(f"Could not check queue depth (queue may not exist): {e}")
        queue_depth = None

    if queue_depth is not None and queue_depth > 0 and not force:
        logger.info(
            f"WARNING: Task queue '{queue_name}' currently has at least {queue_depth} message(s)."
        )
        logger.info(
            "Starting a fresh run will DELETE the existing queue and all its messages."
        )
        confirm = input("Type 'YES' to confirm deletion: ")
        if confirm != "YES":
            logger.info("Operation cancelled.")
            sys.exit(0)

    # Delete existing queues
    logger.info("Deleting existing queues if they exist...")
    try:
        task_queue = await create_queue(config)
        await task_queue.delete_queue()
        logger.info(f"Deleted task queue '{queue_name}'")
    except Exception as e:
        logger.info(f"Task queue deletion: {e}")

    try:
        events_queue = await create_queue(config, queue_name=event_queue_name)
        await events_queue.delete_queue()
        logger.info(f"Deleted event queue '{event_queue_name}'")
    except Exception as e:
        logger.info(f"Event queue deletion: {e}")

    await asyncio.sleep(2)

    # Create new queues
    logger.info(f"Creating task queue '{queue_name}'...")
    task_queue = await create_queue(config)
    logger.info(f"Creating event queue '{event_queue_name}'...")
    events_queue = await create_queue(config, queue_name=event_queue_name)

    task_db = TaskDatabase(db_file)

    # Load tasks from file into database
    logger.info(f"Loading tasks from '{task_file}' into database...")
    num_tasks = 0
    for task in yield_tasks_from_file(task_file, start_task, limit):
        task_db.insert_task(task["task_id"], task["data"], status="pending")
        num_tasks += 1

    logger.info(f"Loaded {num_tasks} tasks into database")

    await task_queue.ensure_queue_ready()

    logger.info(f"Enqueueing tasks to cloud queue '{queue_name}'...")
    semaphore = asyncio.Semaphore(max_concurrent_queue_operations)
    pending_tasks = set()

    async def enqueue_task(task):
        async with semaphore:
            await task_queue.send_task(task["task_id"], task["data"])
            task_db.update_task_enqueued(task["task_id"])

    with tqdm(desc="Enqueueing tasks", total=num_tasks) as pbar:
        for task in yield_tasks_from_file(task_file, start_task, limit):
            task_obj = asyncio.create_task(enqueue_task(task))
            pending_tasks.add(task_obj)
            task_obj.add_done_callback(pending_tasks.discard)

            while len(pending_tasks) >= max_concurrent_queue_operations:
                done, pending_tasks = await asyncio.wait(
                    pending_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                pbar.update(len(done))

        if pending_tasks:
            done, pending_tasks = await asyncio.wait(pending_tasks)
            pbar.update(len(done))

    logger.info(f"Enqueued {num_tasks} tasks to cloud queue")
    return (task_db, task_queue, events_queue, num_tasks)


async def load_queue_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    Load tasks into the database and task queue without starting instances.
    With --continue, open existing database and show status (no load).

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)
    queue_name = provider_config.queue_name

    db_file = getattr(args, "db_file", None) or config.run.db_file
    if not db_file:
        db_file = f"{provider_config.job_id}.db"

    task_db = None

    try:
        if getattr(args, "continue_run", False):
            # Continue mode: open existing db, show status and queue depth
            if not Path(db_file).exists():
                logger.info(f"Error: Database file '{db_file}' does not exist.")
                logger.info("Run without --continue to create and load from a task file.")
                sys.exit(1)

            task_db = TaskDatabase(db_file)
            total_tasks = task_db.get_total_tasks()
            if total_tasks == 0:
                logger.info(f"Warning: Database '{db_file}' has no tasks.")

            logger.info(f"Database: {db_file} ({total_tasks} tasks)")
            log_task_stats(task_db, header="Status:", include_remaining_ids=True)

            task_queue = await create_queue(config)
            queue_depth = await task_queue.get_queue_depth()
            if queue_depth is None:
                logger.info("Tasks loaded. Failed to get queue depth.")
            else:
                logger.info(f"Queue depth (may be approximate): {queue_depth}")
        else:
            # Fresh load
            if not getattr(args, "task_file", None):
                logger.fatal("--task-file is required unless --continue is specified")
                logger.info("Error: --task-file is required unless --continue is specified")
                sys.exit(1)

            task_db, task_queue, _events_queue, num_tasks = await load_queue_common(
                config=config,
                db_file=db_file,
                task_file=args.task_file,
                start_task=getattr(args, "start_task", None),
                limit=getattr(args, "limit", None),
                max_concurrent_queue_operations=getattr(
                    args, "max_concurrent_queue_operations", 100
                ),
                force=getattr(args, "force", False),
            )
            queue_depth = await task_queue.get_queue_depth()
            if queue_depth is None:
                logger.info(f"Loaded {num_tasks} tasks. Failed to get queue depth.")
            else:
                logger.info(f"Loaded {num_tasks} tasks. Queue depth (may be approximate): {queue_depth}")
    except Exception as e:
        logger.fatal(f"Error loading tasks: {e}", exc_info=True)
        logger.info(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if task_db is not None:
            task_db.close()


def add_load_queue_args(
    parser: argparse.ArgumentParser,
    task_required: bool = True,
    include_max_concurrent: bool = True,
) -> None:
    """Add load-queue specific arguments (task file, start, limit, concurrency)."""
    parser.add_argument(
        "--task-file",
        required=task_required,
        help="Path to tasks file (JSON or YAML); required unless --continue",
    )
    parser.add_argument(
        "--start-task", type=int, help="Skip tasks until this task number (1-based indexing)"
    )
    parser.add_argument("--limit", type=int, help="Maximum number of tasks to enqueue")
    if include_max_concurrent:
        parser.add_argument(
            "--max-concurrent-queue-operations",
            type=int,
            default=100,
            help="Maximum number of concurrent queue operations while loading (default: 100)",
        )


async def run_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    Run a job with the specified configuration.
    This is a unified command that handles queue management, instance orchestration,
    and event monitoring with SQLite-based task tracking.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    # Validate arguments
    if not args.continue_run and not args.task_file:
        logger.fatal("--task-file is required unless --continue is specified")
        logger.info("Error: --task-file is required unless --continue is specified")
        sys.exit(1)

    provider = config.provider
    provider_config = config.get_provider_config(provider)
    queue_name = provider_config.queue_name
    event_queue_name = f"{queue_name}-events"

    # Determine database file path
    db_file = args.db_file if args.db_file else config.run.db_file
    if not db_file:
        db_file = f"{provider_config.job_id}.db"

    logger.info(f"Using database file: {db_file}")

    # Initialize variables that will be used in finally block
    task_db = None
    event_monitor = None

    try:
        if args.continue_run:
            # CONTINUE MODE: Resume from previous run
            logger.info(f"Continuing job '{provider_config.job_id}' from database '{db_file}'")

            # Initialize database (must exist)
            task_db = TaskDatabase(db_file)

            # Check if database exists and has tasks
            total_tasks = task_db.get_total_tasks()
            if total_tasks == 0:
                logger.info(f"Error: Database '{db_file}' has no tasks. Cannot continue.")
                sys.exit(1)

            logger.info(f"Found {total_tasks} tasks in database")

            # Drain event queue to catch up on any missed events
            # print("Draining event queue to update task statuses...")
            events_queue = await create_queue(config, queue_name=event_queue_name)

            # drained_count = 0
            # while True:
            #     messages = await events_queue.receive_messages(max_count=100)
            #     if not messages:
            #         break
            #     for message in messages:
            #         try:
            #             data = json.loads(message.get("data", "{}"))
            #             task_db.insert_event(data)
            #             task_db.update_task_from_event(data)
            #             drained_count += 1
            #         except Exception as e:
            #             logger.error(f"Error processing drained event: {e}")

            # print(f"Drained {drained_count} events from queue")

            # Show current status
            counts = task_db.get_task_counts()
            logger.info("Current task status:")
            for status, count in counts.items():
                logger.info(f"  {status}: {count}")

            # Create task queue (don't delete/recreate)
            logger.info(f"Connecting to task queue '{queue_name}'...")
            task_queue = await create_queue(config)

        else:
            # FRESH RUN MODE: use common load logic
            logger.info(f"Starting fresh job '{provider_config.job_id}'")
            task_db, task_queue, events_queue, _num_tasks = await load_queue_common(
                config=config,
                db_file=db_file,
                task_file=args.task_file,
                start_task=args.start_task,
                limit=args.limit,
                max_concurrent_queue_operations=args.max_concurrent_queue_operations,
                force=getattr(args, "force", False),
            )

        # Create orchestrator (disable auto-termination since we control completion via SQLite)
        logger.info("Creating instance orchestrator...")

        # Provide callback to get remaining task count from database
        def get_remaining_task_count():
            return len(task_db.get_remaining_task_ids())

        orchestrator = InstanceOrchestrator(
            config=config,
            dry_run=args.dry_run,
            auto_terminate_on_empty=False,
            get_remaining_task_count=get_remaining_task_count,
        )

        # Set up event monitor
        event_monitor = EventMonitor(
            events_queue,
            task_db,
            output_file_path=getattr(args, "output_file", None),
            print_events=False,  # Don't print individual events
            print_summary=True,
        )
        await event_monitor.start()

        # Track if we should stop
        job_complete = False
        interrupted = False
        stop_signal = asyncio.Event()

        async def monitor_events_wrapper():
            """Wrapper for the monitoring loop."""
            nonlocal job_complete
            await run_event_monitoring_loop(
                event_monitor, task_db, check_completion=True, stop_signal=stop_signal
            )
            job_complete = True

        async def run_orchestrator():
            """Run the instance orchestrator."""
            try:
                await orchestrator.start()
                # Keep checking until job is complete or interrupted
                while orchestrator.is_running and not job_complete and not interrupted:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error in orchestrator: {e}", exc_info=True)
                raise

        # Set up signal handler for graceful shutdown
        def signal_handler(signum, frame):
            """Handle SIGINT (Ctrl+C) by raising KeyboardInterrupt."""
            raise KeyboardInterrupt()

        # Install signal handler
        old_handler = signal.signal(signal.SIGINT, signal_handler)

        # Start both tasks concurrently
        logger.info("Starting instance orchestrator and event monitor...")
        orchestrator_task = asyncio.create_task(run_orchestrator())
        event_monitor_task = asyncio.create_task(monitor_events_wrapper())

        try:
            # Wait for completion or interrupt
            await asyncio.gather(orchestrator_task, event_monitor_task)
        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            # Handle both KeyboardInterrupt and CancelledError (which asyncio may raise instead)
            logger.info("Received interrupt.")

            interrupted = True
            stop_signal.set()

            # Cancel the tasks if they're still running
            if not orchestrator_task.done():
                orchestrator_task.cancel()
            if not event_monitor_task.done():
                event_monitor_task.cancel()

            # Wait for tasks to finish cancelling
            try:
                await asyncio.gather(orchestrator_task, event_monitor_task, return_exceptions=True)
            except Exception:
                pass

            # Prompt user for action (force valid input)
            choice = None
            while choice not in ("T", "L", "C"):
                print("\n\nChoose action:")
                print("  [T] Terminate all instances and delete queues")
                print("  [L] Leave instances running (can resume with --continue)")
                print("  [C] Cancel and continue running")
                try:
                    choice = input("\nEnter choice (T/L/C): ").strip().upper()
                except KeyboardInterrupt:
                    choice = "L"
                    print("Defaulting to [L] Leave instances running")
                    break
                if choice not in ("T", "L", "C"):
                    print(f"Invalid choice '{choice}'. Please enter T, L, or C.")

            if choice == "T":
                logger.info("Terminating all instances and deleting queues...")
                await orchestrator.stop(terminate_instances=True)
                await task_queue.delete_queue()
                await events_queue.delete_queue()
                logger.info("Job terminated")
            elif choice == "L":
                logger.info("Leaving instances running. Use --continue to resume.")
                await orchestrator.stop(terminate_instances=False)
                logger.info(f"Database saved to: {db_file}")
            elif choice == "C":
                logger.info("Continuing job...")
                interrupted = False
                stop_signal.clear()
                # Resume the tasks
                orchestrator_task = asyncio.create_task(run_orchestrator())
                event_monitor_task = asyncio.create_task(monitor_events_wrapper())
                try:
                    await asyncio.gather(orchestrator_task, event_monitor_task)
                except (KeyboardInterrupt, asyncio.CancelledError):
                    # If interrupted again, default to leaving instances running
                    logger.info("Received second interrupt. Leaving instances running.")
                    await orchestrator.stop(terminate_instances=False)
                    logger.info(f"Database saved to: {db_file}")
            else:
                # Can't get here
                raise RuntimeError("Can't get here")

            # Offer to dump task files by status (when exiting, not when continuing)
            if choice != "C":
                dump_choice = None
                while dump_choice not in ("y", "yes", "n", "no"):
                    print("\n\nDump task files by status? Each file contains full task definitions")
                    print("loadable with --task-file (e.g. for retrying failed or pending tasks).")
                    try:
                        dump_choice = input("Dump? (Y/N): ").strip().lower()
                    except KeyboardInterrupt:
                        dump_choice = "n"
                        print("Skipping dump.")
                        break
                    if dump_choice not in ("y", "yes", "n", "no"):
                        print("Please enter y or n.")
                if dump_choice in ("y", "yes"):
                    base_path = provider_config.job_id
                    dump_tasks_by_status(task_db, base_path)
        finally:
            # Restore original signal handler
            signal.signal(signal.SIGINT, old_handler)

        # If job completed normally, clean up
        if job_complete and not interrupted:
            logger.info(
                "Job complete! Cleaning up...this may take a few minutes; don't interrupt the process"
            )
            await orchestrator.stop(terminate_instances=True)

            # Delete queues
            logger.info("Deleting queues...")
            await task_queue.delete_queue()
            await events_queue.delete_queue()

            # Print final report
            print_final_report(task_db)

    except Exception as e:
        logger.fatal(f"Error running job: {e}", exc_info=True)
        logger.info(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if event_monitor is not None:
            event_monitor.close()
        if task_db is not None:
            task_db.close()


def dump_tasks_by_status(task_db: TaskDatabase, output_base_path: str) -> None:
    """
    Write one JSON task file per status containing full task definitions.
    Each file is loadable into the task queue (same format as --task-file).

    Args:
        task_db: TaskDatabase instance
        output_base_path: Base path for output files; each file will be
            {output_base_path}_{status}.json
    """
    counts = task_db.get_task_counts()
    if not counts:
        logger.info("  No tasks in database.")
        return

    written = []
    for status in sorted(counts.keys()):
        if counts[status] == 0:
            continue
        tasks = task_db.get_tasks_by_status(status)
        # Build loadable format: list of {"task_id": ..., "data": ...}
        task_list = []
        for row in tasks:
            task_data = row.get("task_data")
            if task_data is None:
                task_data = {}
            elif isinstance(task_data, str):
                try:
                    task_data = json.loads(task_data)
                except json.JSONDecodeError:
                    task_data = {}
            task_list.append({"task_id": row["task_id"], "data": task_data})

        # Sanitize status for filename (replace any path-unsafe chars)
        safe_status = status.replace("/", "_").replace("\\", "_")
        path = f"{output_base_path}_{safe_status}.json"
        with open(path, "w") as f:
            json.dump(task_list, f, indent=2)
        written.append(f"  {path} ({len(task_list)} tasks)")
        logger.info(f"Dumped {len(task_list)} tasks with status '{status}' to {path}")

    if written:
        logger.info("Task files by status:")
        for line in written:
            logger.info(line)
    else:
        logger.info("  No task files written.")


def log_task_stats(
    task_db: TaskDatabase,
    *,
    header: str = "Summary:",
    include_remaining_ids: bool = True,
) -> None:
    """
    Log task statistics (counts, exceptions, elapsed time stats).
    Used by both print_status_summary and print_final_report.

    Args:
        task_db: TaskDatabase instance
        header: Section header (e.g. "Summary:" or "Final:")
        include_remaining_ids: Whether to list remaining task IDs if < 50
    """
    counts = task_db.get_task_counts()
    total_tasks = task_db.get_total_tasks()
    remaining_task_ids = task_db.get_remaining_task_ids()
    stats = task_db.get_task_statistics()

    logger.info(header)
    logger.info(f"  Total tasks: {total_tasks}")
    heard_from = total_tasks - counts.get("in_queue_original", 0)
    logger.info(f"  Heard from: {heard_from}")
    for status in counts:
        if status != "in_queue_original":
            logger.info(f"    {status}: {counts[status]}")
    logger.info(f"  Still in original queue: {total_tasks - heard_from}")

    if stats["exception_counts"]:
        logger.info("  Exceptions:")
        for exception, count in list(stats["exception_counts"].items())[:10]:
            exc_lines = exception.strip().split("\n")
            if len(exc_lines) > 2:
                exc_display = f"{exc_lines[0]}...{exc_lines[-1]}"
            else:
                exc_display = exception
            logger.info(f"    {count:6d}: {exc_display[:100]}")

    if stats["time_stats"]["avg_time"]:
        logger.info("  Elapsed time statistics:")
        logger.info(
            f"    Range:  {stats['time_stats']['min_time']:.2f} to "
            f"{stats['time_stats']['max_time']:.2f} seconds"
        )
        logger.info(
            f"    Mean:   {stats['time_stats']['avg_time']:.2f} +/- "
            f"{stats['percentiles'].get('std', 0):.2f} seconds"
        )
        if stats["percentiles"]:
            logger.info(f"    Median: {stats['percentiles']['median']:.2f} seconds")
            logger.info(f"    90th %: {stats['percentiles']['p90']:.2f} seconds")
            logger.info(f"    95th %: {stats['percentiles']['p95']:.2f} seconds")

    if include_remaining_ids and len(remaining_task_ids) > 0 and len(remaining_task_ids) < 50:
        logger.info(f"  Remaining tasks: {', '.join(remaining_task_ids)}")

    # Wall-clock elapsed time and throughput (from time_range)
    time_range = stats.get("time_range") or {}
    start_time = time_range.get("start_time")
    end_time = time_range.get("end_time")
    if start_time:
        start = parse_utc(start_time)
        end = parse_utc(end_time) if end_time else utc_now()
        if start is not None and end is not None:
            total_elapsed = (end - start).total_seconds()
            hours = int(total_elapsed // 3600)
            minutes = int((total_elapsed % 3600) // 60)
            seconds = int(total_elapsed % 60)
            logger.info(f"  Elapsed time: {hours}h {minutes}m {seconds}s")
            completed_count = counts.get("completed", 0)
            if completed_count > 0 and total_elapsed > 0:
                tasks_per_hour = completed_count / (total_elapsed / 3600)
                logger.info(f"  Tasks/hour: {tasks_per_hour:.1f}")

    if stats.get("spot_terminations"):
        logger.info(f"  Spot terminations: {len(stats['spot_terminations'])} hosts")
        for host in stats["spot_terminations"]:
            logger.info(f"    {host}")

    logger.info("")


def print_final_report(task_db: TaskDatabase) -> None:
    """
    Print final report with task statistics.
    Uses the same stats block as print_status_summary via log_task_stats.

    Args:
        task_db: TaskDatabase instance
    """
    counts = task_db.get_task_counts()
    total_tasks = task_db.get_total_tasks()
    stats = task_db.get_task_statistics()

    logger.info("")
    logger.info("=" * 60)
    logger.info("")
    logger.info("********************")
    logger.info("*** JOB COMPLETE ***")
    logger.info("********************")
    logger.info("")
    logger.info(f"Total tasks: {total_tasks}")
    for status in counts:
        logger.info(f"  {status.capitalize()}: {counts[status]}")

    # Same stats block as during run (exceptions, time stats, remaining, elapsed, tasks/hour, spot terminations)
    log_task_stats(task_db, header="Final:", include_remaining_ids=True)

    logger.info("=" * 60)


async def status_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    Check the status of a running job.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    print(f"Checking job status for job '{config.get_provider_config().job_id}'")

    try:
        # Create orchestrator using only the config
        orchestrator = InstanceOrchestrator(config=config)
        await orchestrator.initialize()

        num_running, running_cpus, running_price, job_status = (
            await orchestrator.get_job_instances()
        )
        print(job_status)

        queue_depth = await orchestrator._task_queue.get_queue_depth()
        if queue_depth is None:
            print("Failed to get queue depth for task queue.")
        else:
            print(f"Current queue depth: {queue_depth}")

    except Exception as e:
        logger.error(f"Error checking job status: {e}", exc_info=True)
        sys.exit(1)


async def stop_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    Stop a running job and terminate its instances.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    try:
        # Create orchestrator using only the config
        orchestrator = InstanceOrchestrator(config=config)

        await orchestrator.initialize()

        # Stop orchestrator (terminates instances)
        job_id_to_stop = orchestrator.job_id  # Get job_id from orchestrator
        print(f"Stopping job '{job_id_to_stop}'...this could take a few minutes")
        await orchestrator.stop()  # stop already handles termination

        # Purge queue if requested
        if args.purge_queue:
            queue_name_to_purge = orchestrator.queue_name  # Get queue_name from orchestrator
            logger.info(f"Purging queue {queue_name_to_purge}")
            # Ensure task_queue is available before purging
            await orchestrator.task_queue.purge_queue()

        print(f"Job '{job_id_to_stop}' stopped")

    except Exception as e:
        logger.error(f"Error stopping job: {e}", exc_info=True)
        sys.exit(1)


async def list_images_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    List available VM images for the specified provider.
    Shows only standard images and user-owned images, not third-party images.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    try:
        # Create instance manager for the provider
        instance_manager = await create_instance_manager(config)

        # Get images
        print("Retrieving images...")
        images = await instance_manager.list_available_images()

        if not images:
            print("No images found")
            return

        # Apply filters if specified
        if not args.user:
            images = [img for img in images if img.get("source", "").lower() != "user"]

        if args.filter:
            # Filter by any field containing the filter string
            filter_text = args.filter.lower()
            filtered_images = []
            for img in images:
                # Check if any field contains the filter string
                for key, value in img.items():
                    if isinstance(value, (str, int, float)) and filter_text in str(value).lower():
                        filtered_images.append(img)
                        break
            images = filtered_images

        # Apply custom sorting if specified
        if args.sort_by:
            # Define field mapping specific to images
            field_mapping = {
                "family": "family",  # GCP
                "fam": "family",
                "f": "family",
                "name": "name",  # AWS, Azure, GCP
                "n": "name",
                "project": "project",  # GCP
                "proj": "project",
                "p": "project",
                "source": "source",  # AWS, Azure, GCP
                "s": "source",
                "id": "id",  # AWS, Azure, GCP
                "description": "description",  # AWS, GCP
                "descr": "description",
                "desc": "description",
                "creation_date": "creation_date",  # AWS, GCP
                "date": "creation_date",
                "self_link": "self_link",  # GCP
                "link": "self_link",
                "url": "self_link",
                "status": "status",  # AWS, GCP
                "platform": "platform",  # AWS
                "publisher": "publisher",  # Azure
                "pub": "publisher",
                "offer": "offer",  # Azure
                "o": "offer",
                "sku": "sku",  # Azure
                "version": "version",  # Azure
                "v": "version",
                "location": "location",  # Azure
                "loc": "location",
            }

            sort_fields = args.sort_by.split(",")

            if sort_fields:
                for sort_field in sort_fields[::-1]:
                    if sort_field.startswith("-"):
                        descending = True
                        sort_field = sort_field[1:]
                    else:
                        descending = False
                    field_name = field_mapping.get(sort_field)
                    if field_name is None:
                        print(f"Invalid sort field: {sort_field}")
                        sys.exit(1)
                    images.sort(key=lambda x: x[field_name], reverse=descending)
            else:
                # Default sort if no fields specified
                images.sort(key=lambda x: x["name"])
        else:
            # Default sort by name if no sort-by specified
            images.sort(key=lambda x: x["name"])

        # Limit results if specified - applied after sorting
        if args.limit:
            images = images[: args.limit]

        # Display results
        print(
            f"Found {len(images)} {'filtered ' if args.filter else ''}images for "
            f"{args.provider}:"
        )
        print()

        # Format output based on provider
        if args.provider == "AWS":
            if args.detail:
                for img in images:
                    print(f"Name:   {img.get('name', 'N/A')}")
                    print(f"Source: {img.get('source', 'N/A')}")
                    print(f"{img.get('description', 'N/A')}")
                    print(
                        f"ID: {img.get('id', 'N/A'):<24}  CREATION DATE: "
                        f"{img.get('creation_date', 'N/A')[:32]:<34}  STATUS: "
                        f"{img.get('status', 'N/A'):<20}"
                    )
                    print(f"URL: {img.get('self_link', 'N/A')}")
                    print()
            else:
                headers = ["Name", "Source"]
                rows = []
                for img in images:
                    rows.append([img.get("name", "N/A"), img.get("source", "N/A")])
                table = PrettyTable()
                table.field_names = headers
                table.add_rows(rows)
                table.align = "l"
                table.set_style(TableStyle.SINGLE_BORDER)
                print(table)

        elif args.provider == "GCP":
            if args.detail:
                for img in images:
                    print(f"Family:  {img.get('family', 'N/A')}")
                    print(f"Name:    {img.get('name', 'N/A')}")
                    print(f"Project: {img.get('project', 'N/A')}")
                    print(f"Source:  {img.get('source', 'N/A')}")
                    print(f"{img.get('description', 'N/A')}")
                    print(
                        f"ID: {img.get('id', 'N/A'):<24}  CREATION DATE: "
                        f"{img.get('creation_date', 'N/A')[:32]:<34}  STATUS: "
                        f"{img.get('status', 'N/A'):<20}"
                    )
                    print(f"URL: {img.get('self_link', 'N/A')}")
                    print()
            else:
                headers = ["Family", "Name", "Project", "Source"]
                rows = []
                for img in images:
                    rows.append(
                        [
                            img.get("family", "N/A")[:38],
                            img.get("name", "N/A")[:48],
                            img.get("project", "N/A")[:19],
                            img.get("source", "N/A"),
                        ]
                    )
                table = PrettyTable()
                table.field_names = headers
                table.add_rows(rows)
                table.align = "l"
                table.set_style(TableStyle.SINGLE_BORDER)
                print(table)

        elif args.provider == "AZURE":
            if any(img.get("source") == "Azure" for img in images):
                print("MARKETPLACE IMAGES (Reference format: publisher:offer:sku:version)")
                print(f"{'Publisher':<24} {'Offer':<24} {'SKU':<24} {'Latest Version':<16}")
                print("-" * 90)
                for img in images:
                    if img.get("source") == "Azure":
                        print(
                            f"{img.get('publisher', 'N/A')[:22]:<24} "
                            f"{img.get('offer', 'N/A')[:22]:<24} {img.get('sku', 'N/A')[:22]:<24} "
                            f"{img.get('version', 'N/A')[:14]:<16}"
                        )
                        # TODO Update for --detail
            if any(img.get("source") == "User" for img in images):
                if any(img.get("source") == "Azure" for img in images):
                    print("\nCUSTOM IMAGES")
                print(f"{'Name':<30} {'Resource Group':<30} {'OS Type':<10} {'Location':<16}")
                print("-" * 90)
                for img in images:
                    if img.get("source") == "User":
                        print(
                            f"{img.get('name', 'N/A')[:28]:<30} "
                            f"{img.get('resource_group', 'N/A')[:28]:<30} "
                            f"{img.get('os_type', 'N/A')[:8]:<10} "
                            f"{img.get('location', 'N/A')[:14]:<16}"
                        )
                        # TODO Update for --detail

        print(
            "\nTo use a custom image with the 'run' or 'manage_pool' commands, use the "
            "--image parameter."
        )
        if args.provider == "AWS":
            print("For AWS, specify the AMI ID: --image ami-12345678")
        elif args.provider == "GCP":
            print(
                "For GCP, specify the image family or full URI: --image ubuntu-2404-lts or "
                "--image https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/"
                "global/images/ubuntu-2404-lts-amd64-v20240416"
            )
        elif args.provider == "AZURE":
            print(
                "For Azure, specify as publisher:offer:sku:version or full resource ID: "
                "--image Canonical:UbuntuServer:24_04-lts:latest"
            )

    except Exception as e:
        logger.error(f"Error listing images: {e}", exc_info=True)
        sys.exit(1)


async def list_instance_types_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    List available compute instance types for the specified provider with pricing information.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    try:
        # Create instance manager for the provider
        instance_manager = await create_instance_manager(config)

        # Get available instance types
        print("Retrieving instance types...")

        # Turn command line arguments into a dictionary
        # Note we can do this because the names of the command line arguments and the constraints
        # are the same
        constraints = vars(args)
        instances = await instance_manager.get_available_instance_types(constraints)

        if not instances:
            print("No instance types found")
            return

        # Apply text filter to all fields if specified
        if args.filter:
            filter_text = args.filter.lower()
            filtered_instances = {}
            for instance_name, instance in instances.items():
                # Check if any field contains the filter string
                for key, value in instance.items():
                    if isinstance(value, (str, int, float)) and filter_text in str(value).lower():
                        filtered_instances[instance_name] = instance
                        break
            instances = filtered_instances

        # Try to get pricing information where available
        print("Retrieving pricing information...")
        pricing_data = await instance_manager.get_instance_pricing(
            instances, use_spot=args.use_spot, boot_disk_constraints=constraints
        )

        pricing_data_list = []
        for zone_prices in pricing_data.values():
            if zone_prices is None:
                continue
            for zone_price in zone_prices.values():
                for boot_disk_price in zone_price.values():
                    pricing_data_list.append(boot_disk_price)

        # Apply custom sorting if specified
        if args.sort_by:
            # Define field mapping for case-insensitive and prefix matching
            field_mapping = {
                # name
                "type": "name",
                "t": "name",
                "name": "name",
                # cpu_family
                "cpu_family": "cpu_family",
                "family": "cpu_family",
                "processor_type": "cpu_family",
                "processor": "cpu_family",
                "p_type": "cpu_family",
                "ptype": "cpu_family",
                "pt": "cpu_family",
                # cpu_rank
                "cpu_rank": "cpu_rank",
                "performance_rank": "cpu_rank",
                "cr": "cpu_rank",
                "pr": "cpu_rank",
                "rank": "cpu_rank",
                "r": "cpu_rank",
                # vcpu
                "vcpu": "vcpu",
                "v": "vcpu",
                "cpu": "vcpu",
                "c": "vcpu",
                # mem_gb
                "mem_gb": "mem_gb",
                "memory": "mem_gb",
                "mem": "mem_gb",
                "m": "mem_gb",
                "ram": "mem_gb",
                # local_ssd_gb
                "local_ssd": "local_ssd_gb",
                "local_ssd_gb": "local_ssd_gb",
                "lssd": "local_ssd_gb",
                "ssd": "local_ssd_gb",
                # Can't sort on available_boot_disk_types
                # Can't sort on boot_disk_iops
                # Can't sort on boot_disk_throughput
                # boot_disk_gb
                "boot_disk": "boot_disk_gb",
                "boot_disk_gb": "boot_disk_gb",
                "disk": "boot_disk_gb",
                # architecture
                "architecture": "architecture",
                "arch": "architecture",
                "a": "architecture",
                # description
                "description": "description",
                "d": "description",
                "desc": "description",
                # Added by get_instance_pricing
                # cpu_price
                "cpu_price": "cpu_price",
                "cp": "cpu_price",
                "vcpu_price": "cpu_price",
                # per_cpu_price
                "per_cpu_price": "per_cpu_price",
                "cpu_price_per_cpu": "per_cpu_price",
                "vcpu_price_per_cpu": "per_cpu_price",
                # mem_price
                "mem_price": "mem_price",
                "mp": "mem_price",
                # mem_per_gb_price
                "mem_per_gb_price": "mem_per_gb_price",
                # boot_disk_type
                "boot_disk_type": "boot_disk_type",
                # boot_disk_price
                "boot_disk_price": "boot_disk_price",
                # boot_disk_per_gb_price
                "boot_disk_per_gb_price": "boot_disk_per_gb_price",
                # boot_disk_iops_price
                "boot_disk_iops_price": "boot_disk_iops_price",
                # boot_disk_throughput_price
                "boot_disk_throughput_price": "boot_disk_throughput_price",
                # local_ssd_price
                "local_ssd_price": "local_ssd_price",
                "lssd_price": "local_ssd_price",
                # local_ssd_per_gb_price
                "local_ssd_per_gb_price": "local_ssd_per_gb_price",
                "lssd_per_gb_price": "local_ssd_per_gb_price",
                "ssd_price": "local_ssd_price",
                # total_price
                "total_price": "total_price",
                "cost": "total_price",
                "p": "total_price",
                "tp": "total_price",
                # total_price_per_cpu
                "total_price_per_cpu": "total_price_per_cpu",
                "tp_per_cpu": "total_price_per_cpu",
                # zone
                "zone": "zone",
                "z": "zone",
                # Can't sort on supports_spot
                # Can't sort on URL
            }

            # Parse the sort fields
            sort_fields = args.sort_by.split(",")

            if sort_fields:
                for sort_field in sort_fields[::-1]:
                    if sort_field.startswith("-"):
                        descending = True
                        sort_field = sort_field[1:]
                    else:
                        descending = False
                    field_name = field_mapping.get(sort_field)
                    if field_name is None:
                        print(f"Invalid sort field: {sort_field}")
                        sys.exit(1)
                    pricing_data_list.sort(key=lambda x: x[field_name], reverse=descending)
            else:
                # Default sort if no fields specified
                pricing_data_list.sort(key=lambda x: (x["vcpu"], x["mem_gb"], x["name"], x["zone"]))
        else:
            # Default sort by vCPU, then memory if no sort-by specified
            pricing_data_list.sort(key=lambda x: (x["vcpu"], x["mem_gb"], x["name"], x["zone"]))

        # Limit results if specified - applied after sorting
        if args.limit and len(pricing_data_list) > args.limit:
            pricing_data_list = pricing_data_list[: args.limit]

        # Display results with pricing if available
        print()

        has_lssd = any(price_data["local_ssd_gb"] > 0 for price_data in pricing_data_list)
        has_iops = any(price_data["boot_disk_iops_price"] > 0 for price_data in pricing_data_list)
        has_throughput = any(
            price_data["boot_disk_throughput_price"] > 0 for price_data in pricing_data_list
        )

        # All of this complexity is because 1) prettytable doesn't support multi-line headers and
        # 2) prettytable doesn't handle column alignment well when there aren't header fields
        left_fields = []
        field_num = 1
        header1 = ["Instance Type", "Arch", "vCPU", "Mem"]
        header2 = [
            "",
            "",
            "",
            "(GB)",
        ]
        left_fields += [f"Field {field_num}", f"Field {field_num+1}"]
        field_num += 4

        if has_lssd:
            header1 += ["LSSD"]
            header2 += ["(GB)"]
            field_num += 1

        header1 += [
            "Disk",
            "Boot",
        ]
        header2 += [
            "(GB)",
            "Disk Type",
        ]
        left_fields += [f"Field {field_num+1}"]
        field_num += 2

        if args.detail:
            header1 += ["vCPU $", "Mem $"]
            header2 += [
                "(/vCPU/Hr)",
                "(/GB/Hr)",
            ]
            field_num += 2

            if has_lssd:
                header1 += ["LSSD $"]
                header2 += ["(/GB/Hr)"]
                field_num += 1

            header1 += ["Disk $"]
            header2 += ["(/GB/Hr)"]
            field_num += 1
            if has_iops:
                header1 += ["IOPS $"]
                header2 += ["(/Hr)"]
                field_num += 1
            if has_throughput:
                header1 += ["Thruput $"]
                header2 += ["(/Hr)"]
                field_num += 1

        header1 += ["Total $"]
        header2 += ["(/Hr)"]
        field_num += 1

        if args.detail:
            header1 += ["Total $"]
            header2 += ["(/vCPU/Hr)"]
            field_num += 1

        header1 += ["Zone"]
        header2 += [""]
        left_fields += [f"Field {field_num}"]
        field_num += 1

        if args.detail:
            header1 += ["Processor", "Perf", "Description"]
            header2 += ["", "Rank", ""]
        left_fields += [f"Field {field_num}", f"Field {field_num+2}"]

        rows = []
        for price_data in pricing_data_list:
            vcpu_str = f"{price_data['vcpu']:d}"
            mem_gb_str = f"{price_data['mem_gb']:.2f}"
            local_ssd_gb_str = f"{price_data['local_ssd_gb']:.2f}"
            boot_disk_gb_str = f"{price_data['boot_disk_gb']:.2f}"
            cpu_price_str = f"${price_data['per_cpu_price']:.5f}"
            mem_price_str = f"${price_data['mem_per_gb_price']:.5f}"
            total_price_str = f"${price_data['total_price']:.4f}"
            total_price_per_cpu_str = f"${price_data['total_price_per_cpu']:.5f}"
            local_ssd_price_str = f"${price_data['local_ssd_per_gb_price']:.6f}"
            boot_disk_price_str = f"${price_data['boot_disk_per_gb_price']:.6f}"
            boot_disk_iops_price_str = f"${price_data['boot_disk_iops_price']:.5f}"
            boot_disk_throughput_price_str = f"${price_data['boot_disk_throughput_price']:.6f}"
            cpu_rank_str = f"{price_data['cpu_rank']:d}"

            row = [price_data["name"], price_data["architecture"], vcpu_str, mem_gb_str]
            if has_lssd:
                row += [local_ssd_gb_str]
            row += [
                boot_disk_gb_str,
                price_data["boot_disk_type"],
            ]
            if args.detail:
                row += [cpu_price_str, mem_price_str]
                if has_lssd:
                    row += [local_ssd_price_str]
                row += [boot_disk_price_str]
                if has_iops:
                    row += [boot_disk_iops_price_str]
                if has_throughput:
                    row += [boot_disk_throughput_price_str]
            row += [total_price_str]
            if args.detail:
                row += [total_price_per_cpu_str]
            row += [price_data["zone"]]
            if args.detail:
                row += [price_data["cpu_family"], cpu_rank_str, price_data["description"]]
            rows.append(row)

        table = PrettyTable()
        table.add_row(header1)
        table.add_row(header2)
        table.add_divider()
        table.add_rows(rows)
        table.set_style(TableStyle.SINGLE_BORDER)
        table.align = "r"
        for left_field in left_fields:
            table.align[left_field] = "l"
        table.header = False
        print(table)

    except Exception as e:
        logger.error(f"Error listing instance types: {e}", exc_info=True)
        sys.exit(1)


async def list_regions_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    List available regions for the specified provider.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    try:
        # Create instance manager for the provider
        instance_manager = await create_instance_manager(config)

        # Get regions with prefix filtering applied in the provider implementation
        regions = await instance_manager.get_available_regions(prefix=args.prefix)

        if not regions:
            if args.prefix:
                print(f"No regions found with prefix '{args.prefix}'")
            else:
                print("No regions found")
            return

        # Display results
        if args.prefix:
            print(f"Found {len(regions)} regions (filtered by prefix: {args.prefix})")
        else:
            print(f"Found {len(regions)} regions:")
        print()

        if args.detail:
            for region_name in sorted(regions):
                region = regions[region_name]
                print(f"Region: {region['name']}")
                print(f"Description: {region['description']}")
                print(f"Zones: {', '.join(sorted(region['zones'])) if region['zones'] else 'None'}")
                if args.provider == "AWS":
                    print(f"Opt-in Status: {region.get('opt_in_status', 'N/A')}")
                elif args.provider == "AZURE" and region.get("metadata"):
                    print(f"Geography: {region['metadata'].get('geography', 'N/A')}")
                    print(f"Geography Group: {region['metadata'].get('geography_group', 'N/A')}")
                    print(
                        f"Physical Location: {region['metadata'].get('physical_location', 'N/A')}"
                    )
                elif args.provider == "GCP":
                    print(f"Endpoint: {region['endpoint']}")
                    print(f"Status: {region['status']}")
                print()
        else:
            headers = ["Region", "Description"]
            if args.zones:
                headers.append("Zones")
            rows = []
            for region_name in sorted(regions):
                region = regions[region_name]
                row = [region["name"], region["description"]]
                if args.zones:
                    if region["zones"]:
                        row.append(", ".join(sorted(region["zones"])))
                    else:
                        row.append("None found")
                rows.append(row)
            table = PrettyTable()
            table.field_names = headers
            table.add_rows(rows)
            table.align = "l"
            table.set_style(TableStyle.SINGLE_BORDER)
            print(table)

    except Exception as e:
        logger.error(f"Error listing regions: {e}", exc_info=True)
        sys.exit(1)


# Helper functions for argument parsing


def add_common_args(
    parser: argparse.ArgumentParser,
    include_job_id: bool = True,
    include_region: bool = True,
    include_zone=True,
) -> None:
    """Add common arguments to all command parsers."""
    parser.add_argument("--config", help="Path to configuration file")

    # From main Config class
    parser.add_argument("--provider", choices=["aws", "gcp", "azure"], help="Cloud provider")

    # From ProviderConfig class
    if include_job_id:
        parser.add_argument("--job-id", help="The job ID used to group tasks and compute instances")
        parser.add_argument(
            "--queue-name",
            help="The name of the task queue to use (derived from job ID if not provided)",
        )
    if include_region:
        parser.add_argument(
            "--region", help="Specific region to use (derived from zone if not provided)"
        )
    if include_zone:
        parser.add_argument("--zone", help="Specific zone to use")
    parser.add_argument(
        "--exactly-once-queue",
        action="store_true",
        default=None,
        help="If specified, task and event queue messages are guaranteed to be delivered exactly "
        "once to any recipient",
    )
    parser.add_argument(
        "--no-exactly-once-queue",
        action="store_false",
        dest="exactly_once_queue",
        help="If specified, task and event queue messages are delivered at least once, but could "
        "be delivered multiple times",
    )

    # AWS-specific arguments - from AWSConfig class
    parser.add_argument("--access-key", help="AWS only: access key")
    parser.add_argument("--secret-key", help="AWS only: secret key")

    # GCP-specific arguments - from GCPConfig class
    parser.add_argument("--project-id", help="GCP only: project name")
    parser.add_argument(
        "--credentials-file",
        help="GCP only: Path to credentials file",
    )
    parser.add_argument(
        "--service-account",
        help="GCP only: The service account to use for the worker",
    )

    # TODO Add Azure-specific arguments here

    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase verbosity level (-v for warning, -vv for info, -vvv for debug)",
    )


def add_instance_pool_args(parser: argparse.ArgumentParser) -> None:
    """Add instance pool management specific arguments."""

    # From RunConfig class
    # Constraints on number of instances
    parser.add_argument(
        "--min-instances", type=int, help="Minimum number of compute instances (default: 0)"
    )
    parser.add_argument(
        "--max-instances", type=int, help="Maximum number of compute instances (default: 10)"
    )
    parser.add_argument(
        "--min-total-cpus",
        type=int,
        help="Filter instance types by minimum total number of vCPUs",
    )
    parser.add_argument(
        "--max-total-cpus",
        type=int,
        help="Filter instance types by maximum total number of vCPUs",
    )
    parser.add_argument("--cpus-per-task", type=int, help="Number of vCPUs per task")
    parser.add_argument(
        "--min-tasks-per-instance", type=int, help="Minimum number of tasks per instance"
    )
    parser.add_argument(
        "--max-tasks-per-instance",
        type=int,
        help="Maximum number of tasks per instance",
    )
    parser.add_argument(
        "--min-simultaneous-tasks",
        type=int,
        help="Minimum number of simultaneous tasks in the entire system",
    )
    parser.add_argument(
        "--max-simultaneous-tasks",
        type=int,
        help="Maximum number of simultaneous tasks in the entire system",
    )
    parser.add_argument(
        "--min-total-price-per-hour",
        type=float,
        help="Filter instance types by minimum total price per hour",
    )
    parser.add_argument(
        "--max-total-price-per-hour",
        type=float,
        help="Filter instance types by maximum total price per hour",
    )

    # Instance startup and run information
    parser.add_argument("--startup-script-file", help="Path to custom startup script file")
    # We don't bother with --startup-script because any startup script is too long to be
    # passed as a command line argument

    parser.add_argument(
        "--scaling-check-interval",
        type=int,
        help="Interval in seconds between scaling checks (default: 60)",
    )
    parser.add_argument("--image", help="VM image to use")
    parser.add_argument(
        "--instance-termination-delay",
        type=int,
        help="Delay in seconds before terminating instances after queue is empty (default: 60)",
    )
    parser.add_argument(
        "--max-runtime",
        type=int,
        help="Maximum seconds a single worker job is allowed to run (default: 3600)",
    )
    parser.add_argument(
        "--retry-on-exit",
        action="store_true",
        default=None,
        help="If specified, tasks will be retried if the worker exits prematurely",
    )
    parser.add_argument(
        "--no-retry-on-exit",
        action="store_false",
        dest="retry_on_exit",
        help="If specified, tasks will not be retried if the worker exits prematurely (default)",
    )
    parser.add_argument(
        "--retry-on-exception",
        action="store_true",
        default=None,
        help="If specified, tasks will be retried if the user function raises an unhandled "
        "exception",
    )
    parser.add_argument(
        "--no-retry-on-exception",
        action="store_false",
        dest="retry_on_exception",
        help="If specified, tasks will not be retried if the user function raises an unhandled "
        "exception (default)",
    )
    parser.add_argument(
        "--retry-on-timeout",
        action="store_true",
        default=None,
        help="If specified, tasks will be retried if they exceed the maximum runtime specified "
        "by --max-runtime",
    )
    parser.add_argument(
        "--no-retry-on-timeout",
        action="store_false",
        dest="retry_on_timeout",
        help="If specified, tasks will not be retried if they exceed the maximum runtime specified "
        "by --max-runtime (default)",
    )


def add_instance_args(parser: argparse.ArgumentParser) -> None:
    """Add compute instance-specific arguments."""
    # From RunConfig class
    # Constraints on instance types
    parser.add_argument(
        "--architecture",
        choices=["x86_64", "arm64", "X86_64", "ARM64"],
        help="Architecture to use (default: X86_64)",
    )
    parser.add_argument(
        "--min-cpu", type=int, help="Filter instance types by minimum number of vCPUs"
    )
    parser.add_argument(
        "--max-cpu", type=int, help="Filter instance types by maximum number of vCPUs"
    )
    parser.add_argument(
        "--min-total-memory",
        type=float,
        help="Filter instance types by minimum amount of total memory (GB)",
    )
    parser.add_argument(
        "--max-total-memory",
        type=float,
        help="Filter instance types by maximum amount of total memory (GB)",
    )
    parser.add_argument(
        "--min-memory-per-cpu",
        type=float,
        help="Filter instance types by minimum memory (GB) per vCPU",
    )
    parser.add_argument(
        "--max-memory-per-cpu",
        type=float,
        help="Filter instance types by maximum memory (GB) per vCPU",
    )
    parser.add_argument(
        "--min-local-ssd",
        type=float,
        help="Filter instance types by minimum local-SSD storage (GB)",
    )
    parser.add_argument(
        "--max-local-ssd",
        type=float,
        help="Filter instance types by maximum local-SSD storage (GB)",
    )
    parser.add_argument(
        "--min-local-ssd-per-cpu",
        type=float,
        help="Filter instance types by minimum local-SSD storage per vCPU",
    )
    parser.add_argument(
        "--max-local-ssd-per-cpu",
        type=float,
        help="Filter instance types by maximum local-SSD storage per vCPU",
    )
    parser.add_argument(
        "--cpu-family",
        help="Filter instance types by CPU family (e.g., Intel Cascade Lake, AMD Genoa)",
    )
    parser.add_argument(
        "--min-cpu-rank",
        type=int,
        help="Filter instance types by minimum CPU performance rank",
    )
    parser.add_argument(
        "--max-cpu-rank",
        type=int,
        help="Filter instance types by maximum CPU performance rank",
    )
    parser.add_argument(
        "--instance-types",
        nargs="+",
        help='Filter instance types by name prefix (e.g., "t3 m5" for AWS)',
    )
    parser.add_argument(
        "--boot-disk-types",
        nargs="+",
        help="Specify the boot disk type(s)",
    )
    parser.add_argument(
        "--boot-disk-iops",
        type=int,
        help="Specify the boot disk provisioned IOPS (GCP only)",
    )
    parser.add_argument(
        "--boot-disk-throughput",
        type=int,
        help="Specify the boot disk provisioned throughput (GCP only)",
    )
    parser.add_argument(
        "--total-boot-disk-size",
        type=float,
        help="Specify the total boot disk size (GB) (default: 10 for GCP)",
    )
    parser.add_argument(
        "--boot-disk-base-size",
        type=float,
        help="Specify the base boot disk size (GB) (default: 0)",
    )
    parser.add_argument(
        "--boot-disk-per-cpu",
        type=float,
        help="Specify the boot disk size (GB) per vCPU",
    )
    parser.add_argument(
        "--boot-disk-per-task",
        type=float,
        help="Specify the boot disk size (GB) per task",
    )
    parser.add_argument(
        "--use-spot",
        action="store_true",
        help="Use spot/preemptible instances (cheaper but can be terminated)",
    )


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Multi-Cloud Task Processing System")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    subparsers.required = True

    # ------------------------- #
    # QUEUE MANAGEMENT COMMANDS #
    # ------------------------- #

    # --- Show queue command ---

    show_queue_parser = subparsers.add_parser(
        "show_queue",
        help="Show the current depth of a task queue's contents",
    )
    add_common_args(show_queue_parser)
    show_queue_parser.add_argument(
        "--detail", action="store_true", help="Attempt to show a sample message"
    )
    show_queue_parser.set_defaults(func=show_queue_cmd)

    # --- Purge queue command ---

    purge_queue_parser = subparsers.add_parser(
        "purge_queue", help="Purge a task queue by removing all messages"
    )
    add_common_args(purge_queue_parser)
    me_group = purge_queue_parser.add_mutually_exclusive_group()
    me_group.add_argument(
        "--task-queue-only",
        action="store_true",
        help="Purge only the task queue (not the results queue)",
    )
    me_group.add_argument(
        "--event-queue-only",
        action="store_true",
        help="Purge only the results queue (not the task queue)",
    )
    purge_queue_parser.add_argument(
        "--force", "-f", action="store_true", help="Purge the queue without confirmation prompt"
    )
    purge_queue_parser.set_defaults(func=purge_queue_cmd)

    # --- Delete queue command ---

    delete_queue_parser = subparsers.add_parser(
        "delete_queue", help="Permanently delete a task queue and its infrastructure"
    )
    add_common_args(delete_queue_parser)
    me_group = delete_queue_parser.add_mutually_exclusive_group()
    me_group.add_argument(
        "--task-queue-only",
        action="store_true",
        help="Delete only the task queue (not the results queue)",
    )
    me_group.add_argument(
        "--event-queue-only",
        action="store_true",
        help="Delete only the results queue (not the task queue)",
    )
    delete_queue_parser.add_argument(
        "--force", "-f", action="store_true", help="Delete the queue without confirmation prompt"
    )
    delete_queue_parser.set_defaults(func=delete_queue_cmd)

    # ---------------------------- #
    # INSTANCE MANAGEMENT COMMANDS #
    # ---------------------------- #

    # --- Run command ---
    run_parser = subparsers.add_parser(
        "run", help="Run a job (load tasks and manage instance pool)"
    )
    add_common_args(run_parser)
    run_parser.add_argument(
        "--task-file",
        help="Path to tasks file (JSON or YAML); required for fresh runs, not used with --continue",
    )
    run_parser.add_argument(
        "--start-task", type=int, help="Skip tasks until this task number (1-based indexing)"
    )
    run_parser.add_argument("--limit", type=int, help="Maximum number of tasks to enqueue")
    run_parser.add_argument(
        "--max-concurrent-queue-operations",
        type=int,
        default=100,
        help="Maximum number of concurrent queue operations while loading tasks (default: 100)",
    )
    add_instance_pool_args(run_parser)
    add_instance_args(run_parser)
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not actually load any tasks or create or delete any instances",
    )
    run_parser.add_argument(
        "--continue",
        dest="continue_run",
        action="store_true",
        help="Continue from a previous interrupted run using existing SQLite database and cloud state",
    )
    run_parser.add_argument(
        "--db-file",
        help="Path to SQLite database file (default: {job_id}.db)",
    )
    run_parser.add_argument(
        "--output-file",
        help="Optional file to write events to in JSON-lines format",
    )
    run_parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force fresh run without confirmation even if queue has existing messages",
    )
    run_parser.set_defaults(func=run_cmd)

    # --- Load queue command ---

    load_queue_parser = subparsers.add_parser(
        "load_queue",
        help="Load tasks into database and cloud queue (no instance management)",
    )
    add_common_args(load_queue_parser)
    add_load_queue_args(load_queue_parser, task_required=False, include_max_concurrent=True)
    load_queue_parser.add_argument(
        "--db-file",
        help="Path to SQLite database file (default: {job_id}.db)",
    )
    load_queue_parser.add_argument(
        "--continue",
        dest="continue_run",
        action="store_true",
        help="Open existing database and show status only (no load)",
    )
    load_queue_parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force fresh load without confirmation even if queue has existing messages",
    )
    load_queue_parser.set_defaults(func=load_queue_cmd)

    # --- Monitor event queue command ---

    monitor_events_parser = subparsers.add_parser(
        "monitor_event_queue",
        help="Monitor the event queue and update task database (for use with local workers)",
    )
    add_common_args(monitor_events_parser)
    monitor_events_parser.add_argument(
        "--db-file",
        help="Path to SQLite database file (default: {job_id}.db)",
    )
    monitor_events_parser.add_argument(
        "--output-file",
        help="Optional file to write events to in JSON-lines format",
    )
    monitor_events_parser.add_argument(
        "--print-events",
        action="store_true",
        help="Print events to stdout as they are received",
    )
    monitor_events_parser.add_argument(
        "--no-auto-complete",
        action="store_true",
        help="Don't stop automatically when all tasks complete (monitor indefinitely)",
    )
    monitor_events_parser.set_defaults(func=monitor_event_queue_cmd)

    # --- Status command ---

    status_parser = subparsers.add_parser("status", help="Check job status")
    add_common_args(status_parser)
    status_parser.set_defaults(func=status_cmd)

    # --- Stop command ---

    stop_parser = subparsers.add_parser("stop", help="Stop a running job")
    add_common_args(stop_parser)
    stop_parser.add_argument(
        "--purge-queue", action="store_true", help="Purge the queue after stopping"
    )
    stop_parser.set_defaults(func=stop_cmd)

    # --- List running instances command ---

    list_running_instances_parser = subparsers.add_parser(
        "list_running_instances",
        help="List all currently running instances for the specified provider",
    )
    add_common_args(list_running_instances_parser, include_job_id=False)
    list_running_instances_parser.add_argument("--job-id", help="Filter instances by job ID")
    list_running_instances_parser.add_argument(
        "--all-instances",
        action="store_true",
        help="Show all instances including ones that were not created by cloud tasks",
    )
    list_running_instances_parser.add_argument(
        "--include-terminated",
        action="store_true",
        help="Include terminated instances",
    )
    list_running_instances_parser.add_argument(
        "--sort-by",
        help='Sort results by comma-separated fields (e.g., "state,type" or "-created,id"). '
        "Available fields: id, type, state, zone, creation_time. "
        'Prefix with "-" for descending order. '
        'Partial field names like "t" for "type" or "s" for "state" are supported.',
    )
    list_running_instances_parser.add_argument(
        "--detail", action="store_true", help="Show additional provider-specific information"
    )
    list_running_instances_parser.set_defaults(func=list_running_instances_cmd)

    # ------------------------------ #
    # INFORMATION GATHERING COMMANDS #
    # ------------------------------ #

    # --- List regions command ---

    list_regions_parser = subparsers.add_parser(
        "list_regions", help="List available regions for the specified provider"
    )
    add_common_args(
        list_regions_parser, include_job_id=False, include_region=False, include_zone=False
    )
    list_regions_parser.add_argument(
        "--prefix", help="Filter regions to only show those with names starting with this prefix"
    )
    list_regions_parser.add_argument(
        "--zones", action="store_true", help="Show availability zones for each region"
    )
    list_regions_parser.add_argument(
        "--detail", action="store_true", help="Show additional provider-specific information"
    )
    list_regions_parser.set_defaults(func=list_regions_cmd)

    # --- List images command ---

    list_images_parser = subparsers.add_parser(
        "list_images", help="List available VM images for the specified provider"
    )
    add_common_args(
        list_images_parser, include_job_id=False, include_region=False, include_zone=False
    )
    list_images_parser.add_argument(
        "--user",
        action="store_true",
        help="Include user-created images in the list",
    )
    list_images_parser.add_argument(
        "--filter", help="Filter images containing this text in any field"
    )
    list_images_parser.add_argument(
        "--sort-by",
        help='Sort results by comma-separated fields (e.g., "family,name" or "-source,project"). '
        "Available fields: family, name, project, source. "
        'Prefix with "-" for descending order. '
        'Partial field names like "fam" for "family" or "proj" for "project" are supported.',
    )
    list_images_parser.add_argument(
        "--limit", type=int, help="Limit the number of images displayed"
    )
    # TODO Update --sort-by to be specific to each provider which have different fields
    list_images_parser.add_argument(
        "--detail", action="store_true", help="Show detailed information about each image"
    )
    list_images_parser.set_defaults(func=list_images_cmd)

    # --- List instance types command ---

    list_instance_types_parser = subparsers.add_parser(
        "list_instance_types",
        help="List compute instance types for the specified provider with pricing information",
    )
    add_common_args(list_instance_types_parser, include_job_id=False)
    add_instance_args(list_instance_types_parser)
    list_instance_types_parser.add_argument(
        "--filter", help="Filter instance types containing this text in any field"
    )
    list_instance_types_parser.add_argument(
        "--sort-by",
        help='Sort results by comma-separated fields (e.g., "price,vcpu" or "type,-memory"). '
        "Available fields: "
        "name, vcpu, mem, local_ssd, storage, "
        "vcpu_price, mem_price, local_ssd_price, boot_disk_type, boot_disk_price, "
        "boot_disk_iops_price, boot_disk_throughput_price, "
        "price_per_cpu, mem_per_gb_price, local_ssd_per_gb_price, boot_disk_per_gb_price, "
        "total_price, total_price_per_cpu, zone, processor_type, performance_rank, description. "
        'Prefix with "-" for descending order. '
        'Partial field names like "ram" or "mem" for "mem_gb" or "v" for "vcpu" are supported.',
    )
    list_instance_types_parser.add_argument(
        "--limit", type=int, help="Limit the number of instance types displayed"
    )
    list_instance_types_parser.add_argument(
        "--detail", action="store_true", help="Show additional cost information"
    )
    list_instance_types_parser.set_defaults(func=list_instance_types_cmd)

    # -------------- #
    # MAIN EXECUTION #
    # -------------- #

    # Parse arguments
    args = parser.parse_args()

    if hasattr(args, "instance_types") and args.instance_types:
        new_instance_types = []
        for str1 in args.instance_types:
            for str2 in str1.split(","):
                for str3 in str2.split(" "):
                    if str3.strip():
                        new_instance_types.append(str3.strip())
        args.instance_types = new_instance_types

    # Set up logging level based on verbosity
    # Force at least INFO when using run command so progress is visible
    if getattr(args, "func", None) is run_cmd and hasattr(args, "verbose") and args.verbose < 1:
        args.verbose = 1
    if hasattr(args, "verbose"):
        if args.verbose == 0:
            logging.getLogger().setLevel(logging.WARNING)
        elif args.verbose == 1:
            logging.getLogger().setLevel(logging.INFO)
        elif args.verbose > 1:
            logging.getLogger().setLevel(logging.DEBUG)

    # Load configuration
    logger.info(f"Loading configuration from {args.config}")
    try:
        config = load_config(args.config)
        config.overload_from_cli(vars(args))
        config.update_run_config_from_provider_config()
        config.validate_config()
    except (pydantic.ValidationError, ValueError) as e:
        logger.fatal(f"Invalid configuration: {e}")
        print(f"Invalid configuration: {e}")
        sys.exit(1)

    # Run the appropriate command
    try:
        asyncio.run(args.func(args, config))
    except KeyboardInterrupt:
        # KeyboardInterrupt should be handled within the command itself
        # If it propagates here, it means the command didn't handle it
        print("\n\nOperation interrupted by user.")
        sys.exit(130)  # Standard exit code for SIGINT

    sys.exit(0)


if __name__ == "__main__":
    main()
