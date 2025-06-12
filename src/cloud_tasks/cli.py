"""
Command-line interface for the multi-cloud task processing system.
"""

import argparse
import asyncio
from datetime import datetime
import json
import json_stream
import logging
import numpy as np
import sys
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, Iterable, Optional
import yaml  # type: ignore

from filecache import FCPath
from prettytable import PrettyTable, TableStyle
import pydantic

from .common.config import Config, load_config
from .common.logging_config import configure_logging
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


async def load_queue_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    Load tasks into a queue without starting instances.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    try:
        provider = config.provider
        provider_config = config.get_provider_config(provider)
        queue_name = provider_config.queue_name

        try:
            dry_run = args.dry_run
        except AttributeError:
            dry_run = False

        if dry_run:
            print("Dry run mode enabled. No task queue will be created.")
            task_queue = None
        else:
            print(f"Creating task queue '{queue_name}' on {provider} if necessary...")
            task_queue = await create_queue(config)

        if dry_run:
            print("Dry run mode enabled. No tasks will be loaded.")
        else:
            print(f"Populating task queue from {args.task_file}...")
        num_tasks = 0

        # Create a semaphore to limit concurrent tasks
        semaphore = asyncio.Semaphore(args.max_concurrent_queue_operations)
        pending_tasks = set()

        load_failed_exception = None

        async def enqueue_task(task):
            """Helper function to enqueue a single task with semaphore control."""
            nonlocal load_failed_exception
            if load_failed_exception:
                return
            async with semaphore:
                if "task_id" not in task:
                    logger.error(f"Task #{task['task_num']} does not have a 'task_id' key")
                    return
                if not isinstance(task["task_id"], str):
                    logger.error(
                        f"Task #{task['task_num']} has a non-string 'task_id' "
                        f"key: {task['task_id']}"
                    )
                    return
                if "data" not in task:
                    logger.error(f"Task #{task['task_num']} does not have a 'data' key")
                    return
                if not isinstance(task["data"], dict):
                    logger.error(
                        f"Task #{task['task_num']} has a non-dict 'data' key: {task['data']}"
                    )
                    return
                try:
                    if not dry_run:
                        await task_queue.send_task(task["task_id"], task["data"])
                except Exception as e:
                    load_failed_exception = e

        with tqdm(desc="Enqueueing tasks") as pbar:
            for task_num, task in enumerate(
                yield_tasks_from_file(args.task_file, args.start_task, args.limit)
            ):
                if load_failed_exception:
                    raise load_failed_exception

                if dry_run:
                    logger.debug(f"Dry run mode - would load task: {task}")
                else:
                    logger.debug(f"Loading task: {task}")

                # Create and track the task
                task["task_num"] = task_num  # For errors
                task_obj = asyncio.create_task(enqueue_task(task))
                pending_tasks.add(task_obj)
                task_obj.add_done_callback(pending_tasks.discard)

                # Update progress when tasks complete
                while len(pending_tasks) >= args.max_concurrent_queue_operations:
                    done, pending_tasks = await asyncio.wait(
                        pending_tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    pbar.update(len(done))
                    num_tasks += len(done)
                    logger.debug(f"Increment of {len(done)} task(s)")

            # Wait for remaining tasks to complete
            if pending_tasks:
                done, pending_tasks = await asyncio.wait(pending_tasks)
                pbar.update(len(done))
                num_tasks += len(done)
                logger.debug(f"Final increment of {len(done)} task(s)")

        print(f"Loaded {num_tasks} task(s)")

        if dry_run:
            print("Dry run mode enabled. No queue depth will be shown.")
        else:
            queue_depth = await task_queue.get_queue_depth()
            if queue_depth is None:
                print("Tasks loaded successfully. Failed to get queue depth.")
            else:
                print(f"Tasks loaded successfully. Queue depth (may be approximate): {queue_depth}")

    except Exception as e:
        logger.fatal(f"Error loading tasks: {e}", exc_info=True)
        sys.exit(1)


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


async def manage_pool_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    Manage an instance pool for processing tasks without loading tasks.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    try:
        logger.info(f"Starting pool management for job: {config.get_provider_config().job_id}")

        # Create the orchestrator using only the config object
        # Configuration (including startup script, region, etc.) is handled
        # during the config loading phase in main()
        orchestrator = InstanceOrchestrator(config=config, dry_run=args.dry_run)

        # Start orchestrator
        logger.info("Starting orchestrator")
        await orchestrator.start()
    except Exception as e:
        logger.fatal(f"Error starting instance pool: {e}", exc_info=True)
        sys.exit(1)

    # Monitor job progress (using orchestrator's task_queue)
    try:
        while orchestrator.is_running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received interrupt, stopping job management")
        print("Any instances are still running!")
        await orchestrator.stop(terminate_instances=False)
        sys.exit(1)
    except Exception as monitor_err:
        logger.fatal(f"Error during monitoring: {monitor_err}", exc_info=True)
        print("Any instances are still running!")
        await orchestrator.stop(terminate_instances=False)
        sys.exit(1)

    # We could call orchestrator.stop(terminate_instances=True) here, but it's not necessary
    # because it would just terminate the threads but we're about exit the program anyway
    # so we don't care.
    logger.info("Job management complete")


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


async def monitor_event_queue_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    Monitor the event queue and display or save events as they arrive.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)
    event_queue_name = f"{provider_config.queue_name}-events"

    # If a tasks file was specified, read it and get the list of task_ids
    task_ids = set()
    if args.task_file:
        print(f'Reading tasks from "{args.task_file}"')
        for task in yield_tasks_from_file(args.task_file, args.start_task, args.limit):
            task_ids.add(task["task_id"])

    event_type_data = {}
    task_exceptions = {}
    non_fatal_exceptions = {}
    fatal_exceptions = {}
    spot_termination_hosts = set()
    duplicate_completed_task_ids = set()
    earliest_event_time = None
    latest_event_time = None
    elapsed_times = []

    def _process_log_entry(log_entry: Dict[str, Any]) -> None:
        nonlocal earliest_event_time, latest_event_time
        event_time = log_entry.get("timestamp")
        if event_time:
            event_time = datetime.fromisoformat(event_time)

        if event_time and (earliest_event_time is None or event_time < earliest_event_time):
            earliest_event_time = event_time
        if event_time and (latest_event_time is None or event_time > latest_event_time):
            latest_event_time = event_time

        event_type = log_entry["event_type"]
        retry = log_entry.get("retry")
        task_id = log_entry.get("task_id")
        exception = log_entry.get("exception")
        if exception:
            exception_split = [line.strip() for line in exception.strip().split("\n")]
            exception_head = "; ".join(exception_split[1:3])
            exception_tail = "; ".join(exception_split[-2:])
            exception_str = f"{exception_head}; ...; {exception_tail}"
        else:
            exception_str = ""
        hostname = log_entry.get("hostname")
        elapsed_time = log_entry.get("elapsed_time")
        if elapsed_time is not None:
            elapsed_times.append(elapsed_time)

        match event_type:
            case "task_completed" | "task_timed_out" | "task_exited" | "task_exception":
                if event_type == "task_exception":
                    task_exceptions[exception_str] = task_exceptions.get(exception_str, 0) + 1
                key = (event_type, retry)
                if (
                    key == ("task_completed", False)
                    and key in event_type_data
                    and task_id in event_type_data[key]
                ):
                    # This was already completed once before with no retry, and now we see it
                    # again with no retry. This is a duplicate.
                    duplicate_completed_task_ids.add(task_id)
                else:
                    if key not in event_type_data:
                        event_type_data[key] = set()
                    event_type_data[key].add(task_id)
            # These are not task-specific
            case "non_fatal_exception":
                non_fatal_exceptions[exception_str] = non_fatal_exceptions.get(exception_str, 0) + 1
            case "fatal_exception":
                fatal_exceptions[exception_str] = fatal_exceptions.get(exception_str, 0) + 1
            case "spot_termination":
                key = (event_type, retry)
                if key not in event_type_data:
                    event_type_data[key] = set()
                event_type_data[key].add(hostname)
            case _:
                pass

    output_file = None
    if args.output_file:
        # If the results file already exists, read it and summarize the results
        try:
            with open(args.output_file, "r") as f:
                print(f'Reading previous events from "{args.output_file}"')
                while s := f.readline():
                    log_entry = json.loads(s)
                    _process_log_entry(log_entry)
        except FileNotFoundError:
            print("No previous events found...starting statistics from scratch")
            pass
        except json.decoder.JSONDecodeError as e:
            print(f'Error parsing results file "{args.output_file}"')
            print(e)
        except Exception as e:
            logger.fatal(f'Error reading results file "{args.output_file}": {e}', exc_info=True)
            sys.exit(1)

        try:
            output_file = open(args.output_file, "a")
            logger.info(f'Writing events to "{args.output_file}"')
        except Exception as e:
            logger.fatal(f'Error opening events file "{args.output_file}": {e}', exc_info=True)
            sys.exit(1)

    try:
        # Create results queue
        events_queue = await create_queue(config, queue_name=event_queue_name)
        print(f"Monitoring event queue '{event_queue_name}' on {provider}...")

        # Main monitoring loop
        something_changed = True  # Start out with a summary
        while True:
            if something_changed:
                print()
                if args.task_file:
                    if ("task_completed", False) in event_type_data:
                        tasks_remaining = task_ids - event_type_data[("task_completed", False)]
                    else:
                        tasks_remaining = task_ids
                else:
                    tasks_remaining = None
                print("Summary:")
                if tasks_remaining is not None:
                    print(
                        f"  {len(tasks_remaining)} tasks have not been completed with retry=False"
                    )
                if len(duplicate_completed_task_ids) > 0:
                    print(
                        f"  {len(duplicate_completed_task_ids)} tasks completed with retry=False "
                        "more than once but shouldn't have"
                    )
                if event_type_data:
                    print("  Task event status:")
                    for (event_type, retry), info in sorted(event_type_data.items()):
                        count = len(info)
                        print(f"    {event_type:<19s} (retry={str(retry):>5s}): {count:6d}")
                    if task_exceptions:
                        print("  Task exceptions:")
                        for exception, count in sorted(task_exceptions.items()):
                            print(f"    {count:6d}: {exception}")
                    if fatal_exceptions:
                        print("  Non-task fatal exceptions:")
                        for exception, count in sorted(fatal_exceptions.items()):
                            print(f"    {count:6d}: {exception}")
                    if non_fatal_exceptions:
                        print("  Non-task non-fatal exceptions:")
                        for exception, count in sorted(non_fatal_exceptions.items()):
                            print(f"    {count:6d}: {exception}")
                    if spot_termination_hosts:
                        print("  Spot terminations:")
                        for host in sorted(spot_termination_hosts):
                            print(f"    {host}")
                if ("task_completed", False) in event_type_data:
                    tasks_completed = len(event_type_data[("task_completed", False)])
                    if earliest_event_time and latest_event_time:
                        elapsed_time = (latest_event_time - earliest_event_time).total_seconds()
                        if elapsed_time > 0:
                            print(
                                f"  Tasks completed: {tasks_completed} in {elapsed_time:.2f} seconds "
                                f"({elapsed_time / tasks_completed:.2f} seconds/task)"
                            )
                if elapsed_times:
                    elapsed_times_arr = np.array(elapsed_times)
                    print("  Elapsed time statistics:")
                    print(
                        f"    Range:  {np.min(elapsed_times_arr):.2f} to "
                        f"{np.max(elapsed_times_arr):.2f} seconds"
                    )
                    print(
                        f"    Mean:   {np.mean(elapsed_times_arr):.2f} +/- "
                        f"{np.std(elapsed_times_arr):.2f} seconds"
                    )
                    print(f"    Median: {np.median(elapsed_times_arr):.2f} seconds")
                    print(f"    90th %: {np.percentile(elapsed_times_arr, 90):.2f} seconds")
                    print(f"    95th %: {np.percentile(elapsed_times_arr, 95):.2f} seconds")
                if tasks_remaining is not None and len(tasks_remaining) < 50:
                    print(f"  Remaining tasks: {', '.join(tasks_remaining)}")
                print()
                something_changed = False

            try:
                # Receive a batch of messages
                messages = await events_queue.receive_messages(max_count=100)

                if messages:
                    something_changed = True
                    for message in messages:
                        try:
                            # Extract and parse the JSON data
                            data = json.loads(message.get("data", "{}"))

                            # Format the output
                            output = json.dumps(data)

                            # Write to file if specified
                            if output_file:
                                output_file.write(output + "\n")

                            # Always print to stdout
                            print(output)

                            _process_log_entry(data)

                        except json.JSONDecodeError as e:
                            logger.error(f"Error decoding message: {e}")
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                    output_file.flush()
                else:
                    # Sleep briefly to avoid hammering the queue
                    await asyncio.sleep(1)

            except KeyboardInterrupt:
                print("\nMonitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error receiving messages: {e}")
                await asyncio.sleep(5)  # Wait longer on error

    except Exception as e:
        logger.fatal(f"Error monitoring event queue: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Clean up
        if output_file:
            output_file.close()


async def run_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    Run a job with the specified configuration.
    This is a combination of loading tasks into the queue and managing an instance pool.

    Parameters:
        args: Command-line arguments
    """
    await load_queue_cmd(args, config)
    await manage_pool_cmd(args, config)


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


def add_load_queue_args(
    parser: argparse.ArgumentParser, task_required: bool = True, include_max_concurrent: bool = True
) -> None:
    """Add load queue specific arguments."""
    parser.add_argument(
        "--task-file", required=task_required, help="Path to tasks file (JSON or YAML)"
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
            help="Maximum number of concurrent queue operations while loading tasks (default: 100)",
        )


def add_instance_pool_args(parser: argparse.ArgumentParser) -> None:
    """Add instance pool management specific arguments."""

    # From RunConfig class
    # Constraints on number of instances
    parser.add_argument(
        "--min-instances", type=int, help="Minimum number of compute instances (default: 1)"
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
        help="Maximum seconds a single worker job is allowed to run (default: 600)",
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

    # --- Load queue command ---

    load_queue_parser = subparsers.add_parser(
        "load_queue", help="Load tasks into a queue without starting instances"
    )
    add_common_args(load_queue_parser)
    add_load_queue_args(load_queue_parser)
    load_queue_parser.set_defaults(func=load_queue_cmd)

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

    # --- Run command (combines load_queue and manage_pool)
    run_parser = subparsers.add_parser(
        "run", help="Run a job (load tasks and manage instance pool)"
    )
    add_common_args(run_parser)
    add_load_queue_args(run_parser)
    add_instance_pool_args(run_parser)
    add_instance_args(run_parser)
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not actually load any tasks or create or delete any instances",
    )
    run_parser.set_defaults(func=run_cmd)

    # --- Status command ---

    status_parser = subparsers.add_parser("status", help="Check job status")
    add_common_args(status_parser)
    status_parser.set_defaults(func=status_cmd)

    # --- Manage pool command ---

    manage_pool_parser = subparsers.add_parser(
        "manage_pool", help="Manage an instance pool for processing tasks"
    )
    add_common_args(manage_pool_parser)
    add_instance_pool_args(manage_pool_parser)
    add_instance_args(manage_pool_parser)
    manage_pool_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not actually create or delete any instances",
    )
    manage_pool_parser.set_defaults(func=manage_pool_cmd)

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

    # --- Monitor results queue command ---

    monitor_events_parser = subparsers.add_parser(
        "monitor_event_queue",
        help="Monitor the event queue and display or save events as they arrive",
    )
    add_common_args(monitor_events_parser)
    add_load_queue_args(monitor_events_parser, task_required=False, include_max_concurrent=False)
    monitor_events_parser.add_argument(
        "--output-file",
        required=True,
        help="File to write events to (will be opened in append mode)",
    )

    monitor_events_parser.set_defaults(func=monitor_event_queue_cmd)

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
    asyncio.run(args.func(args, config))

    sys.exit(0)


if __name__ == "__main__":
    main()
