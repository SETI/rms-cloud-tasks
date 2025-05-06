"""
Command-line interface for the multi-cloud task processing system.
"""

import argparse
import asyncio
import json
import json_stream
import logging
import sys
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, Iterable
import yaml  # type: ignore

from filecache import FCPath
import pydantic

from cloud_tasks.common.config import Config, load_config
from cloud_tasks.common.logging_config import configure_logging
from cloud_tasks.instance_manager import create_instance_manager
from cloud_tasks.instance_manager.orchestrator import InstanceOrchestrator
from cloud_tasks.queue_manager import create_queue


# Use custom logging configuration
configure_logging(level=logging.WARNING)
logger = logging.getLogger(__name__)


def yield_tasks_from_file(tasks_file: str) -> Iterable[Dict[str, Any]]:
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
                elif ln.startswith((" ", "-")):
                    y = y + ln
                elif len(y) > 0:
                    yield yaml.load(y)
                    y = ln


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

        print(f"Creating task queue '{queue_name}' on {provider} if necessary...")
        task_queue = await create_queue(config)

        print(f"Populating task queue from {args.tasks}...")
        num_tasks = 0
        tasks_to_skip = args.start_task - 1 if args.start_task else 0
        tasks_remaining = args.limit

        # Create a semaphore to limit concurrent tasks
        semaphore = asyncio.Semaphore(args.max_concurrent_tasks)
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
                    await task_queue.send_task(task["task_id"], task["data"])
                except Exception as e:
                    load_failed_exception = e

        with tqdm(desc="Enqueueing tasks") as pbar:
            for task_num, task in enumerate(yield_tasks_from_file(args.tasks)):
                # Skip tasks until we reach the start_task
                if tasks_to_skip > 0:
                    tasks_to_skip -= 1
                    continue

                # Check if we've reached the limit
                if tasks_remaining is not None:
                    if tasks_remaining <= 0:
                        break
                    tasks_remaining -= 1

                if load_failed_exception:
                    raise load_failed_exception

                logger.debug(f"Loading task: {task}")

                # Create and track the task
                task["task_num"] = task_num  # For errors
                task_obj = asyncio.create_task(enqueue_task(task))
                pending_tasks.add(task_obj)
                task_obj.add_done_callback(pending_tasks.discard)

                # Update progress when tasks complete
                while len(pending_tasks) >= args.max_concurrent_tasks:
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

        queue_depth = await task_queue.get_queue_depth()
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

    # Display queue depth with some formatting
    print(f"Current depth: {queue_depth} message(s)")

    if queue_depth == 0:
        print("\nQueue is empty. No messages available.")
    else:
        # If verbose, try to get a sample message without removing it
        if args.detail:
            print("\nAttempting to peek at first message...")
            try:
                messages = await task_queue.receive_tasks(
                    max_count=1, visibility_timeout_seconds=10
                )

                if messages:
                    message = messages[0]
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
    queue_name = provider_config.queue_name
    task_queue = await create_queue(config)

    queue_depth = await task_queue.get_queue_depth()

    if queue_depth == 0:
        print(f"Queue '{queue_name}' on '{provider}' is already empty (0 messages).")
        return

    # Confirm with the user if not using --force
    if not args.force:
        confirm = input(
            f"\nWARNING: This will permanently delete all {queue_depth}+ messages from queue "
            f"'{queue_name}' on '{provider}'."
            f"\nType 'EMPTY {queue_name}' to confirm: "
        )
        if confirm != f"EMPTY {queue_name}":
            print("Operation cancelled.")
            return

    print(f"Emptying queue '{queue_name}'...")
    await task_queue.purge_queue()

    # Verify the queue is now empty
    new_depth = await task_queue.get_queue_depth()
    if new_depth == 0:
        print(f"Queue '{queue_name}' has been emptied. Removed {queue_depth}+ message(s).")
    else:
        print(f"WARNING: Queue purge operation completed but {new_depth} messages still remain.")
        print("Some messages may be in flight or locked by consumers.")


async def delete_queue_cmd(args: argparse.Namespace, config: Config) -> None:
    """Delete a task queue entirely from the cloud provider.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)
    queue_name = provider_config.queue_name

    # Confirm with the user if not using --force
    if not args.force:
        confirm = input(
            f"\nWARNING: This will permanently delete the queue '{queue_name}' from {provider}.\n"
            f"This operation cannot be undone and will remove all infrastructure.\n"
            f"Type 'DELETE {queue_name}' to confirm: "
        )
        if confirm != f"DELETE {queue_name}":
            print("Operation cancelled.")
            return

    try:
        print(f"Deleting queue '{queue_name}' from {provider}...")
        task_queue = await create_queue(config)
        await task_queue.delete_queue()
        print(f"Queue '{queue_name}' has been deleted.")
    except Exception as e:
        logger.fatal(f"Error deleting queue: {e}", exc_info=True)
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
        orchestrator = InstanceOrchestrator(config=config)

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

            if not args.detail:
                # Headers
                if args.provider == "gcp":
                    print(
                        f"{'Job ID':<16} {'ID':<64} {'Type':<15} {'State':<11} {'Zone':<15} {'Created':<30}"
                    )
                    print("-" * 155)
                else:
                    print(f"{'Job ID':<16} {'ID':<64} {'Type':<15} {'State':<11} {'Created':<30}")
                    print("-" * 101)

            instance_count = 0
            state_counts = {}
            for instance in instances:
                if not args.include_terminated and instance.get("state") == "terminated":
                    continue

                instance_count += 1

                # Extract common fields with safe defaults
                instance_id = instance.get("id", "N/A")
                instance_type = instance.get("type", "N/A")
                state = instance.get("state", "N/A")
                created_at = instance.get("creation_time", instance.get("created_at", "N/A"))
                zone = instance.get("zone", "N/A")
                private_ip = instance.get("private_ip", "N/A")
                public_ip = instance.get("public_ip", "N/A")
                job_id = instance.get("job_id", "N/A")
                state_counts[state] = state_counts.get(state, 0) + 1

                if args.detail:
                    # More detailed output in detail mode with aligned values
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
                    if args.provider == "gcp" and zone:
                        print(
                            f"{job_id:<16} {instance_id:<64} {instance_type:<15} {state:<11} "
                            f"{zone:<15} {created_at:<30}"
                        )
                    else:
                        print(
                            f"{job_id:<16} {instance_id:<26} {instance_type:<15} {state:<11} "
                            f"{created_at:<30} "
                        )

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


async def run_job_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    Run a job with the specified configuration.
    This is a combination of loading tasks into the queue and managing an instance pool.

    Parameters:
        args: Command-line arguments
    """
    load_queue_cmd(args, config)
    manage_pool_cmd(args, config)


async def status_job_cmd(args: argparse.Namespace, config: Config) -> None:
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

        # Get job status
        num_running, running_cpus, running_price, job_status = (
            await orchestrator.get_job_instances()
        )
        print(job_status)

        queue_depth = await orchestrator.task_queue.get_queue_depth()
        print(f"Current queue depth: {queue_depth}+")

    except Exception as e:
        logger.error(f"Error checking job status: {e}", exc_info=True)
        sys.exit(1)


async def stop_job_cmd(args: argparse.Namespace, config: Config) -> None:
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
        if args.provider == "aws":
            print(f"{'Name':<80} {'Source':<6}")
            print("-" * 90)
            for img in images:
                print(f"{img.get('name', 'N/A')[:78]:<80} {img.get('source', 'N/A'):<6}")
                if args.detail:
                    print(f"{img.get('description', 'N/A')}")
                    print(f"ID: {img.get('id', 'N/A')}")
                    print(
                        f"CREATION DATE: "
                        f"{img.get('creation_date', 'N/A')[:24]:<26}  STATUS: "
                        f"{img.get('status', 'N/A'):<20}"
                    )
                    print(f"URL: {img.get('self_link', 'N/A')}")
                    print()

        elif args.provider == "gcp":
            print(f"{'Family':<40} {'Name':<50} {'Project':<21} {'Source':<6}")
            print("-" * 114)
            for img in images:
                print(
                    f"{img.get('family', 'N/A')[:38]:<40} {img.get('name', 'N/A')[:48]:<50} "
                    f"{img.get('project', 'N/A')[:19]:<21} {img.get('source', 'N/A'):<6}"
                )
                if args.detail:
                    print(f"{img.get('description', 'N/A')}")
                    print(
                        f"ID: {img.get('id', 'N/A'):<24}  CREATION DATE: "
                        f"{img.get('creation_date', 'N/A')[:32]:<34}  STATUS: "
                        f"{img.get('status', 'N/A'):<20}"
                    )
                    print(f"URL: {img.get('self_link', 'N/A')}")
                    print()
        elif args.provider == "azure":
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
        if args.provider == "aws":
            print("For AWS, specify the AMI ID: --image ami-12345678")
        elif args.provider == "gcp":
            print(
                "For GCP, specify the image family or full URI: --image ubuntu-2404-lts or "
                "--image https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/"
                "global/images/ubuntu-2404-lts-amd64-v20240416"
            )
        elif args.provider == "azure":
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
        for zone_pricing in pricing_data.values():
            if zone_pricing is None:
                continue
            for x in zone_pricing.values():
                pricing_data_list.append(x)

        # Apply custom sorting if specified
        if args.sort_by:
            # Define field mapping for case-insensitive and prefix matching
            field_mapping = {
                "type": "name",
                "t": "name",
                "name": "name",
                "zone": "zone",
                "z": "zone",
                "vcpu": "vcpu",
                "v": "vcpu",
                "cpu": "vcpu",
                "c": "vcpu",
                "mem_gb": "mem_gb",
                "memory": "mem_gb",
                "mem": "mem_gb",
                "m": "mem_gb",
                "ram": "mem_gb",
                "local_ssd": "local_ssd_gb",
                "local_ssd_gb": "local_ssd_gb",
                "lssd": "local_ssd_gb",
                "ssd": "local_ssd_gb",
                "boot_disk": "boot_disk_gb",
                "boot_disk_gb": "boot_disk_gb",
                "disk": "boot_disk_gb",
                "cpu_price": "cpu_price",
                "cp": "cpu_price",
                "per_cpu_price": "cpu_price",
                "vcpu_price": "cpu_price",
                "mem_price": "mem_price",
                "mp": "mem_price",
                "per_gb_price": "mem_per_gb_price",
                "mem_per_gb_price": "mem_per_gb_price",
                "local_ssd_price": "local_ssd_price",
                "lssd_price": "local_ssd_price",
                "local_ssd_per_gb_price": "local_ssd_per_gb_price",
                "lssd_per_gb_price": "local_ssd_per_gb_price",
                "ssd_price": "local_ssd_price",
                "boot_disk_price": "boot_disk_price",
                "disk_price": "boot_disk_price",
                "boot_disk_per_gb_price": "boot_disk_per_gb_price",
                "disk_per_gb_price": "boot_disk_per_gb_price",
                "total_price": "total_price",
                "p": "total_price",
                "tp": "total_price",
                "cost": "total_price",
                "total_price_per_cpu": "total_price_per_cpu",
                "tp_per_cpu": "total_price_per_cpu",
                "description": "description",
                "d": "description",
                "desc": "description",
                "processor_type": "processor_type",
                "processor": "processor_type",
                "p_type": "processor_type",
                "ptype": "processor_type",
                "pt": "processor_type",
                "performance_rank": "performance_rank",
                "pr": "performance_rank",
                "rank": "performance_rank",
                "r": "performance_rank",
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

        underline = "-" * 107
        header = (
            f"{'Instance Type':<24} {'Arch':>10} {'vCPU':>4} {'Mem (GB)':>10} "
            f"{'LSSD (GB)':>10} {'Disk (GB)':>10} "
        )

        if args.detail:
            header += (
                f"{'$/vCPU/Hr':>10} {'Mem $/GB/Hr':>12} {'LSSD $/GB/Hr':>13} {'Disk $/GB/Hr':>13} "
            )
        header += f"{'Total $/Hr':>11} "
        if args.detail:
            header += f"{'& $/vCPU/Hr':>11} "
        header += f" {'Zone':<25} "
        if args.detail:
            header += f"{'Processor':<21} "
            header += f"{'Ranking':>8}  "
            header += f"{'Description':>10}"
            underline += "-" * 130

        print(header)
        print(underline)
        for price_data in pricing_data_list:
            cpu_price_str = f"${price_data['per_cpu_price']:.5f}"
            mem_price_str = f"${price_data['mem_per_gb_price']:.5f}"
            total_price_str = f"${price_data['total_price']:.4f}"
            total_price_per_cpu_str = f"${price_data['total_price_per_cpu']:.5f}"
            local_ssd_price_str = f"${price_data['local_ssd_per_gb_price']:.8f}"
            boot_disk_price_str = f"${price_data['boot_disk_per_gb_price']:.8f}"

            val = (
                f"{price_data['name']:<24} {price_data['architecture']:>10} {price_data['vcpu']:>4} "
                f"{price_data['mem_gb']:>10.1f} {price_data['local_ssd_gb']:>10} "
                f"{price_data['boot_disk_gb']:>10} "
            )
            if args.detail:
                val += (
                    f"{cpu_price_str:>10} {mem_price_str:>12} "
                    f"{local_ssd_price_str:>13} {boot_disk_price_str:>13} "
                )
            val += f"{total_price_str:>11} "
            if args.detail:
                val += f"{total_price_per_cpu_str:>11} "
            val += f" {price_data['zone']:<25} "
            if args.detail:
                val += f"{price_data['processor_type']:<21} "
                val += f"{price_data['performance_rank']:>8}  "
                val += f"{price_data['description']}"
            print(val)

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

        print(f"{'Region':<25} {'Description':<40}")
        print("-" * 100)

        for region_name in sorted(regions):
            region = regions[region_name]
            print(f"{region['name']:<25} {region['description']:<40}")

            skip_line = False
            if args.zones:
                if region["zones"]:
                    print(f"  Availability Zones: {', '.join(sorted(region['zones']))}")
                else:
                    print("  No availability zones found")
                skip_line = True

            if args.detail:
                if args.provider == "aws":
                    print(f"  Opt-in Status: {region.get('opt_in_status', 'N/A')}")
                    skip_line = True
                elif args.provider == "azure" and region.get("metadata"):
                    print(f"  Geography: {region['metadata'].get('geography', 'N/A')}")
                    print(f"  Geography Group: {region['metadata'].get('geography_group', 'N/A')}")
                    print(
                        f"  Physical Location: {region['metadata'].get('physical_location', 'N/A')}"
                    )
                elif args.provider == "gcp":
                    print(f"  Endpoint: {region['endpoint']}")
                    print(f"  Status: {region['status']}")
                    skip_line = True

            if skip_line:
                print()

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


def add_load_queue_args(parser: argparse.ArgumentParser) -> None:
    """Add load queue specific arguments."""
    parser.add_argument("--tasks", required=True, help="Path to tasks file (JSON or YAML)")
    parser.add_argument(
        "--start-task", type=int, help="Skip tasks until this task number (1-based indexing)"
    )
    parser.add_argument("--limit", type=int, help="Maximum number of tasks to enqueue")
    parser.add_argument(
        "--max-concurrent-tasks",
        type=int,
        default=100,
        help="Maximum number of concurrent tasks to enqueue (default: 100)",
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
        help="Maximum seconds a single worker job is allowed to run (default: 60)",
    )
    parser.add_argument(
        "--worker-use-new-process",
        action="store_true",
        default=None,
        help="Use a new process for each task on each worker",
    )
    parser.add_argument(
        "--no-worker-use-new-process",
        action="store_false",
        dest="worker_use_new_process",
        help="Do not use a new process for each task on each worker (default)",
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
        "--boot-disk-type",
        help="Specify the boot disk type (default: balanced for GCP)",
    )
    parser.add_argument(
        "--boot-disk-provisioned-iops",
        type=int,
        help="Specify the boot disk provisioned IOPS (GCP only)",
    )
    parser.add_argument(
        "--boot-disk-provisioned-throughput",
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
    purge_queue_parser.add_argument(
        "--force", "-f", action="store_true", help="Purge the queue without confirmation prompt"
    )
    purge_queue_parser.set_defaults(func=purge_queue_cmd)

    # --- Delete queue command ---

    delete_queue_parser = subparsers.add_parser(
        "delete_queue", help="Permanently delete a task queue and its infrastructure"
    )
    add_common_args(delete_queue_parser)
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
    run_parser.set_defaults(func=run_job_cmd)

    # --- Status command ---

    status_parser = subparsers.add_parser("status", help="Check job status")
    add_common_args(status_parser)
    status_parser.set_defaults(func=status_job_cmd)

    # --- Manage pool command ---

    manage_pool_parser = subparsers.add_parser(
        "manage_pool", help="Manage an instance pool for processing tasks"
    )
    add_common_args(manage_pool_parser)
    add_instance_pool_args(manage_pool_parser)
    add_instance_args(manage_pool_parser)
    manage_pool_parser.set_defaults(func=manage_pool_cmd)

    # --- Stop command ---

    stop_parser = subparsers.add_parser("stop", help="Stop a running job")
    add_common_args(stop_parser)
    stop_parser.add_argument(
        "--purge-queue", action="store_true", help="Purge the queue after stopping"
    )
    stop_parser.set_defaults(func=stop_job_cmd)

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
        "vcpu_price, mem_price, local_ssd_price, storage_price, "
        "price_per_cpu, mem_per_gb_price, local_ssd_per_gb_price, storage_per_gb_price, "
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
        if args.verbose == 1:
            logging.getLogger().setLevel(logging.WARNING)
        elif args.verbose == 2:
            logging.getLogger().setLevel(logging.INFO)
        elif args.verbose > 2:
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


if __name__ == "__main__":
    main()
