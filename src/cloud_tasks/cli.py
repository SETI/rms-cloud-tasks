"""
Command-line interface for the multi-cloud task processing system.
"""

import argparse
import asyncio
import json
import json_stream
import logging
import math
import sys
import time
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, Iterable
import yaml  # type: ignore

from cloud_tasks.common.config import Config, get_run_config, load_config
from cloud_tasks.queue_manager import create_queue

# from cloud_tasks.instance_orchestrator import create_instance_manager
# from cloud_tasks.instance_orchestrator.orchestrator import InstanceOrchestrator
from cloud_tasks.common.logging_config import configure_logging

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
        Task dictionaries (expected to have "id" and "data" keys)

    Raises:
        ValueError: If the file cannot be read
    """
    if not tasks_file.endswith((".json", ".yaml", ".yml")):
        raise ValueError(
            f"Unsupported file format for tasks: {tasks_file}; must be .json, .yml, or .yaml"
        )
    with open(tasks_file, "r") as fp:
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


async def load_queue_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    Load tasks into a queue without starting instances.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    try:
        provider = config.provider
        provider_config = config.get_provider_config()
        queue_name = provider_config.queue_name
        if queue_name is None:
            logger.fatal("Queue name must be specified")
            sys.exit(1)

        logger.info(f"Creating task queue {queue_name} on {provider}")
        task_queue = await create_queue(config)

        logger.info(f"Populating task queue from {args.tasks}")
        num_tasks = 0
        with tqdm(desc="Enqueueing tasks") as pbar:
            for task in yield_tasks_from_file(args.tasks):
                logger.info(task)
                await task_queue.send_task(task["id"], task["data"])
                pbar.update(1)
                num_tasks += 1

        logger.info(f"Loaded {num_tasks} tasks")

        queue_depth = await task_queue.get_queue_depth()
        logger.info(f"Tasks loaded successfully. Queue depth (may be approximate): {queue_depth}")

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
    provider_config = config.get_provider_config()
    queue_name = provider_config.queue_name
    print(f"Checking queue depth for {queue_name} on {provider}...")

    try:
        task_queue = await create_queue(config)
    except Exception as e:
        logger.error(f"Error connecting to queue: {e}")
        print(f"\nError connecting to queue: {e}")
        print("\nPlease check your configuration and ensure the queue exists.")
        sys.exit(1)

    # Get queue depth
    try:
        queue_depth = await task_queue.get_queue_depth()
    except Exception as e:
        logger.error(f"Error getting queue depth: {e}")
        print(f"\nError retrieving queue depth: {e}")
        print("\nThe queue may exist but you might not have permission to access it.")
        sys.exit(1)

    # Display queue depth with some formatting
    print("\n" + "=" * 50)
    print("QUEUE INFORMATION")
    print("=" * 50)
    print(f"Queue name:     {args.queue_name}")
    print(f"Provider:       {args.provider}")
    print(f"Current depth:  {queue_depth} message(s)")

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
                logger.error(f"Error peeking at message: {e}")
                print(f"\nError retrieving sample message: {e}")


async def purge_queue_cmd(args: argparse.Namespace, config: Config) -> None:
    """Empty a task queue by removing all messages from it.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    provider = config.provider
    provider_config = config.get_provider_config()
    queue_name = provider_config.queue_name
    task_queue = await create_queue(config)

    queue_depth = await task_queue.get_queue_depth()

    if queue_depth == 0:
        print(f"Queue '{queue_name}' on '{provider}' is already empty (0 messages).")
        return

    # Confirm with the user if not using --force
    if not args.force:
        confirm = input(
            f"\nWARNING: This will permanently delete all {queue_depth} messages from queue "
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
        print(
            f"SUCCESS: Queue '{queue_name}' has been emptied. Removed " f"{queue_depth} message(s)."
        )
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
    provider_config = config.get_provider_config()
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
        print(f"SUCCESS: Queue '{queue_name}' has been deleted.")
    except Exception as e:
        logger.error(f"Error deleting queue: {e}")
        print(f"\nError deleting queue: {e}")
        sys.exit(1)


async def manage_pool_cmd(args: argparse.Namespace, config: Config) -> None:
    """TODO
    Manage an instance pool for processing tasks without loading tasks.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    try:
        # Initialize cloud services
        provider = args.provider

        # Get the run configuration with proper overrides
        cli_args = vars(args)
        run_config = get_run_config(config, provider, cli_args)

        if not run_config.startup_script:
            logger.fatal("No startup script specified")
            sys.exit(1)

        logger.info(
            f"Using #CPU: {run_config.cpu}, Memory: {run_config.memory_gb} GB, "
            f"Disk: {run_config.disk_gb} GB"
        )
        logger.info(f"Using image: {run_config.image}")
        script_preview = run_config.startup_script[:50].replace("\n", " ") + (
            "..." if len(run_config.startup_script) > 50 else ""
        )
        logger.info(f"Using startup script: {script_preview}")

        # Log instance types restriction if set
        if hasattr(run_config, "instance_types") and run_config.instance_types:
            logger.info(f"Restricting instance types to: {' '.join(run_config.instance_types)}")

        # Handle region parameter
        if run_config.region:
            logger.info(f"Using specified region: {run_config.region}")
        else:
            logger.info("No region specified, will find cheapest")

        # Create task queue to monitor progress
        logger.info(f"Creating task queue {args.queue_name} on {provider}")
        task_queue = await create_queue(config)

        # Create job ID
        job_id = args.job_id or f"job-{int(time.time())}"
        logger.info(f"Starting job {job_id}")

        # Create the orchestrator with parameters from run_config
        orchestrator = InstanceOrchestrator(
            provider=provider,
            job_id=job_id,
            cpu_required=run_config.cpu,
            memory_required_gb=run_config.memory_gb,
            disk_required_gb=run_config.disk_gb,
            min_instances=args.min_instances,
            max_instances=args.max_instances,
            tasks_per_instance=args.tasks_per_instance,
            use_spot_instances=args.use_spot,
            region=run_config.region,
            queue_name=args.queue_name,
            custom_image=run_config.image,
            startup_script=run_config.startup_script,
            config=config,
        )

        # Set the task queue on the orchestrator
        orchestrator.task_queue = task_queue

        # Start orchestrator
        logger.info("Starting orchestrator")
        await orchestrator.start()

        # Monitor job progress
        try:
            queue_depth = await task_queue.get_queue_depth()
            initial_queue_depth = queue_depth

            if initial_queue_depth == 0:
                logger.warning("Queue is empty. Add tasks using the 'load_queue' command.")

            with tqdm(total=initial_queue_depth, desc="Processing tasks") as pbar:
                last_depth = queue_depth

                while queue_depth > 0:
                    await asyncio.sleep(5)
                    queue_depth = await task_queue.get_queue_depth()

                    # Update progress bar
                    processed = last_depth - queue_depth
                    if processed > 0:
                        pbar.update(processed)
                        last_depth = queue_depth

                    # Print job status
                    if args.verbose:
                        status = await orchestrator.get_job_status()
                        instances_info = (
                            f"{status['instances']['running']} running, "
                            f"{status['instances']['starting']} starting"
                        )
                        logger.info(f"Queue depth: {queue_depth}, Instances: {instances_info}")

                # Ensure progress bar reaches 100%
                if initial_queue_depth > 0:
                    pbar.update(pbar.total - pbar.n)

            logger.info("All tasks processed")

            # Stop orchestrator
            logger.info("Stopping orchestrator")
            await orchestrator.stop()

            logger.info(f"Job {job_id} completed successfully")

        except KeyboardInterrupt:
            logger.info("Received interrupt, stopping job")
            await orchestrator.stop()

    except Exception as e:
        logger.error(f"Error managing instance pool: {e}", exc_info=True)
        sys.exit(1)


async def list_running_instances_cmd(args: argparse.Namespace, config: Config) -> None:
    """
    List all running instances for the specified provider.

    Parameters:
        args: Command-line arguments
    """
    try:
        # Create instance manager
        instance_manager = await create_instance_manager(provider=args.provider, config=config)

        # Get list of running instances
        tag_filter = {}
        if args.job_id:
            tag_filter["job_id"] = args.job_id
            print(f"\nListing instances for {args.provider} with job_id: {args.job_id}")
        else:
            print(f"\nListing all instances for {args.provider}")

        try:
            # For GCP, pass the region parameter explicitly
            if args.provider == "gcp" and args.region and not instance_manager.zone:
                # List instances with specific region filter
                instances = await instance_manager.list_running_instances(
                    tag_filter=tag_filter, region=args.region
                )
            else:
                instances = await instance_manager.list_running_instances(tag_filter=tag_filter)
        except Exception as e:
            logger.error(f"Error listing instances: {e}")
            error_message = str(e)

            if args.provider == "gcp" and (
                "zone" in error_message or "no region" in error_message.lower()
            ):
                print("\nError: Zone/region information is required for GCP.")
                print("You have two options:")
                print("  1. Specify a region with --region (e.g., --region us-central1)")
                print("  2. Add 'zone' to your config file in the GCP section")
            else:
                print(f"\nError listing instances: {error_message}")
            sys.exit(1)

        # Display instances
        if instances:
            print(f"\nFound {len(instances)} instances for provider: {args.provider}")

            # Headers
            if args.provider == "gcp":
                print(
                    f"{'ID':<20} {'Type':<15} {'State':<10} {'Zone':<15} {'Created':<26} {'Tags'}"
                )
                print("-" * 95)
            else:
                print(f"{'ID':<20} {'Type':<15} {'State':<10} {'Created':<26} {'Tags'}")
                print("-" * 80)

            for instance in instances:
                # Extract common fields with safe defaults
                instance_id = instance.get("id", "N/A")
                instance_type = instance.get("type", "N/A")
                state = instance.get("state", "N/A")
                created_at = instance.get("creation_time", instance.get("created_at", "N/A"))
                zone = instance.get("zone", "")

                # Format tags as a comma-separated string of key=value pairs
                tags = instance.get("tags", {})
                tags_str = ", ".join([f"{k}={v}" for k, v in tags.items()]) if tags else "No tags"

                # Truncate tags if verbose mode is not enabled
                if not args.verbose and len(tags_str) > 40:
                    tags_str = tags_str[:37] + "..."

                if args.verbose:
                    # More detailed output in verbose mode
                    print(f"\nInstance ID: {instance_id}")
                    print(f"Type: {instance_type}")
                    print(f"State: {state}")

                    if zone:
                        print(f"Zone: {zone}")

                    print(f"Created: {created_at}")

                    if "private_ip" in instance:
                        print(f"Private IP: {instance['private_ip']}")
                    if "public_ip" in instance:
                        print(f"Public IP: {instance['public_ip']}")

                    print("Tags:")
                    if tags:
                        for k, v in tags.items():
                            print(f"  {k}: {v}")
                    else:
                        print("  No tags")
                    print("-" * 40)
                else:
                    if args.provider == "gcp" and zone:
                        print(
                            f"{instance_id:<20} {instance_type:<15} {state:<10} {zone:<15} "
                            f"{created_at:<26} {tags_str}"
                        )
                    else:
                        print(
                            f"{instance_id:<20} {instance_type:<15} {state:<10} {created_at:<26} "
                            f"{tags_str}"
                        )

            # Print count summary
            running_count = len([i for i in instances if i["state"] == "running"])
            starting_count = len([i for i in instances if i["state"] == "starting"])
            other_count = len(instances) - running_count - starting_count

            print(f"\nSummary: {len(instances)} total instances")
            print(f"  {running_count} running")
            print(f"  {starting_count} starting")
            if other_count > 0:
                print(f"  {other_count} in other states")
        else:
            if args.job_id:
                print(f"\nNo instances found for job ID: {args.job_id}")
            else:
                print(f"\nNo instances found for provider: {args.provider}")

    except Exception as e:
        logger.error(f"Error listing running instances: {e}", exc_info=True)
        print(f"Error listing running instances: {e}")
        sys.exit(1)


async def run_job(args: argparse.Namespace, config: Config) -> None:
    """TODO
    Run a job with the specified configuration.
    This is a combination of loading tasks and managing an instance pool.

    Parameters:
        args: Command-line arguments
    """
    try:
        # Load tasks
        logger.info(f"Loading tasks from {args.tasks}")
        tasks = yield_tasks_from_file(args.tasks)  # TODO
        logger.info(f"Loaded {len(tasks)} tasks")

        # Initialize cloud services
        provider = args.provider

        # Get the run configuration with proper overrides
        cli_args = vars(args)
        run_config = get_run_config(config, provider, cli_args)

        # Now we can use attribute access thanks to Config
        logger.info(
            f"Using CPU: {run_config.cpu}, Memory: {run_config.memory_gb} GB, "
            f"Disk: {run_config.disk_gb} GB"
        )
        logger.info(f"Using image: {run_config.image}")
        if run_config.startup_script:
            script_preview = run_config.startup_script[:50].replace("\n", " ") + (
                "..." if len(run_config.startup_script) > 50 else ""
            )
            logger.info(f"Using custom startup script: {script_preview}")

        # Log instance types restriction if set
        if hasattr(run_config, "instance_types") and run_config.instance_types:
            logger.info(f"Restricting instance types to: {run_config.instance_types}")

        # Handle region parameter
        if args.region:
            logger.info(f"Using specified region from command line: {args.region}")
            region_param = args.region
        else:
            region_param = None
            logger.info(
                "No region specified on command line, will use from config or find cheapest"
            )

        # Create task queue
        logger.info(f"Creating task queue on {provider}")
        task_queue = await create_queue(config)

        # Create job ID
        job_id = args.job_id or f"job-{int(time.time())}"
        logger.info(f"Starting job {job_id}")

        # Create the orchestrator with parameters from run_config
        orchestrator = InstanceOrchestrator(
            provider=provider,
            job_id=job_id,
            cpu_required=run_config.cpu,
            memory_required_gb=run_config.memory_gb,
            disk_required_gb=run_config.disk_gb,
            min_instances=args.min_instances,
            max_instances=args.max_instances,
            tasks_per_instance=args.tasks_per_instance,
            use_spot_instances=args.use_spot,
            region=region_param,
            queue_name=args.queue_name,
            config=config,
            custom_image=run_config.image,
            startup_script=run_config.startup_script,
        )

        # Set the task queue on the orchestrator
        orchestrator.task_queue = task_queue

        # Populate task queue
        logger.info("Populating task queue")
        with tqdm(total=len(tasks), desc="Enqueueing tasks") as pbar:
            for task in tasks:
                await task_queue.send_task(task["id"], task["data"])
                pbar.update(1)

        # Start orchestrator
        logger.info("Starting orchestrator")
        await orchestrator.start()

        # Monitor job progress
        try:
            queue_depth = await task_queue.get_queue_depth()
            with tqdm(total=len(tasks), desc="Processing tasks") as pbar:
                last_depth = queue_depth

                while queue_depth > 0:
                    await asyncio.sleep(5)
                    queue_depth = await task_queue.get_queue_depth()

                    # Update progress bar
                    processed = last_depth - queue_depth
                    if processed > 0:
                        pbar.update(processed)
                        last_depth = queue_depth

                    # Print job status
                    if args.verbose:
                        status = await orchestrator.get_job_status()
                        instances_info = (
                            f"{status['instances']['running']} running, "
                            f"{status['instances']['starting']} starting"
                        )
                        logger.info(f"Queue depth: {queue_depth}, Instances: {instances_info}")

                # Ensure progress bar reaches 100%
                pbar.update(pbar.total - pbar.n)

            logger.info("All tasks processed")

            # Stop orchestrator
            logger.info("Stopping orchestrator")
            await orchestrator.stop()

            logger.info(f"Job {job_id} completed successfully")

        except KeyboardInterrupt:
            logger.info("Received interrupt, stopping job")
            await orchestrator.stop()

    except Exception as e:
        logger.error(f"Error running job: {e}", exc_info=True)
        sys.exit(1)


async def status_job(args: argparse.Namespace, config: Config) -> None:
    """TODO
    Check the status of a running job.

    Parameters:
        args: Command-line arguments
    """
    try:
        # Create task queue to check depth
        task_queue = await create_queue(config)

        # Create orchestrator
        orchestrator = InstanceOrchestrator(
            provider=args.provider,
            job_id=args.job_id,
            region=args.region if hasattr(args, "region") else None,
            queue_name=args.queue_name,
            config=config,
        )

        # Set task queue on orchestrator
        orchestrator.task_queue = task_queue

        # Get instances with job ID tag
        instances = await orchestrator.instance_manager.list_running_instances(
            tag_filter={"job_id": args.job_id}
        )

        # Get queue depth
        queue_depth = await task_queue.get_queue_depth()

        # Print status
        running_count = len([i for i in instances if i["state"] == "running"])
        starting_count = len([i for i in instances if i["state"] == "starting"])

        print(f"Job: {args.job_id}")
        print(f"Queue: {args.queue_name}")
        print(f"Queue depth: {queue_depth}")
        print(
            f"Instances: {len(instances)} total ({running_count} running, "
            f"{starting_count} starting)"
        )

        if args.verbose:
            print("\nInstances:")
            for instance in instances:
                print(f"  {instance['id']}: {instance['type']} - {instance['state']}")
                if "public_ip" in instance and instance["public_ip"]:
                    print(f"    Public IP: {instance['public_ip']}")

    except Exception as e:
        logger.error(f"Error checking job status: {e}", exc_info=True)
        sys.exit(1)


async def stop_job(args: argparse.Namespace, config: Config) -> None:
    """TODO
    Stop a running job and terminate its instances.

    Parameters:
        args: Command-line arguments
    """
    try:
        # Create task queue
        task_queue = await create_queue(config)

        # Create orchestrator with the provider parameter and full config
        orchestrator = InstanceOrchestrator(
            provider=args.provider,
            job_id=args.job_id,
            # Minimum parameters needed for stopping
            min_instances=0,
            max_instances=0,
            queue_name=args.queue_name,
            config=config,  # Pass the full config
        )

        # Set the task_queue directly
        orchestrator.task_queue = task_queue

        # Create the instance manager
        orchestrator.instance_manager = await create_instance_manager(
            provider=args.provider, config=config
        )

        # Stop orchestrator (terminates instances)
        logger.info(f"Stopping job {args.job_id}")
        await orchestrator.stop()

        if args.purge_queue:
            logger.info(f"Purging queue {args.queue_name}")
            await task_queue.purge_queue()

        logger.info(f"Job {args.job_id} stopped")

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
        instance_manager = await create_instance_manager(provider=args.provider, config=config)

        # Get images
        images = await instance_manager.list_available_images()

        if not images:
            print(f"No images found for provider {args.provider}")
            return

        # Apply filters if specified
        if args.source:
            images = [img for img in images if img.get("source", "").lower() == args.source.lower()]

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
                        logger.warning(f"Invalid sort field: {sort_field}")
                        continue
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
            f"Found {len(images)} {'filtered ' if args.filter or args.source else ''}images for "
            f"{args.provider}:"
        )
        print()

        # Format output based on provider
        if args.provider == "aws":
            print(f"{'ID':<20} {'Name':<40} {'Creation Date':<24} {'Source':<6}")
            print("-" * 90)
            for img in images:
                print(
                    f"{img.get('id', 'N/A'):<20} {img.get('name', 'N/A')[:38]:<40} "
                    f"{img.get('creation_date', 'N/A')[:22]:<24} {img.get('source', 'N/A'):<6}"
                )
                # TODO Update for --detail

        elif args.provider == "gcp":
            print(f"{'Family':<35} {'Name':<50} {'Project':<20} {'Source':<6}")
            print("-" * 114)
            for img in images:
                print(
                    f"{img.get('family', 'N/A')[:33]:<35} {img.get('name', 'N/A')[:48]:<50} "
                    f"{img.get('project', 'N/A')[:18]:<20} {img.get('source', 'N/A'):<6}"
                )
                if args.detail:
                    print(f"{img.get('description', 'N/A')}")
                    print(f"{img.get('self_link', 'N/A')}")
                    print(
                        f"ID: {img.get('id', 'N/A'):<24}  CREATION DATE: "
                        f"{img.get('creation_date', 'N/A')[:32]:<34}  STATUS: "
                        f"{img.get('status', 'N/A'):<20}"
                    )
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
    """
    try:
        # Create instance manager for the provider
        instance_manager = await create_instance_manager(provider=args.provider, config=config)

        # Get available instance types
        instances = await instance_manager.list_available_instance_types()

        if not instances:
            print(f"No instance types found for provider {args.provider}")
            return

        # Filter based on minimum requirements
        instances = [
            inst
            for inst in instances
            if (args.min_cpu is None or inst["vcpu"] >= args.min_cpu)
            and (args.max_cpu is None or inst["vcpu"] <= args.max_cpu)
            and (args.min_memory is None or inst["memory_gb"] >= args.min_memory)
            and (args.max_memory is None or inst["memory_gb"] <= args.max_memory)
            and (
                args.min_memory_ratio is None
                or inst["memory_gb"] / inst["vcpu"] >= args.min_memory_ratio
            )
            and (
                args.max_memory_ratio is None
                or inst["memory_gb"] / inst["vcpu"] <= args.max_memory_ratio
            )
        ]

        # Apply instance type filter if specified
        if args.instance_types:
            new_instance_types = []
            for str1 in args.instance_types:
                for str2 in str1.split(","):
                    for str3 in str2.split(" "):
                        if str3.strip():
                            new_instance_types.append(str3.strip())
            filtered_instances = []
            for instance in instances:
                instance_name = instance["name"]
                # Check if instance matches any prefix or exact name
                for pattern in new_instance_types:
                    if instance_name.startswith(pattern):
                        filtered_instances.append(instance)
                        break
            instances = filtered_instances

        # Apply text filter if specified
        if args.filter:
            filter_text = args.filter.lower()
            filtered_instances = []
            for instance in instances:
                # Check if any field contains the filter string
                for key, value in instance.items():
                    if isinstance(value, (str, int, float)) and filter_text in str(value).lower():
                        filtered_instances.append(instance)
                        break
            instances = filtered_instances

        # Try to get pricing information where available
        pricing_data = {}
        try:
            # Create a config structure for the get_optimal_instance_type function to use
            provider_config = config.get_provider_config(args.provider)

            # For AWS and Azure, we need to set region explicitly for pricing
            if args.provider == "aws" and "region" not in provider_config:
                print(
                    "Note: Pricing information requires a region. Using default us-west-2 for "
                    "pricing data."
                )
                provider_config["region"] = "us-west-2"  # TODO Default region
            elif args.provider == "azure" and "location" not in provider_config:
                print(
                    "Note: Pricing information requires a location. Using default eastus for "
                    "pricing data."
                )
                provider_config["location"] = "eastus"  # TODO Default region

            # Get pricing data for all eligible instance types
            for instance in instances:
                try:
                    price = await instance_manager.get_instance_pricing(
                        instance["name"], args.use_spot
                    )
                    if price is not None:
                        pricing_data[instance["name"]] = price
                except Exception as e:
                    logger.warning(f"Could not get pricing for {instance['name']}: {e}")
        except Exception as e:
            logger.warning(f"Could not retrieve pricing information: {e}")

        # Add price data to instances for sorting
        for instance in instances:
            if instance["name"] in pricing_data:
                print(pricing_data[instance["name"]])
                cpu_price, ram_price = pricing_data[instance["name"]]
                instance["cpu_price"] = cpu_price
                instance["ram_price"] = ram_price
                instance["total_price"] = (
                    cpu_price * instance["vcpu"] + ram_price * instance["memory_gb"]
                )
            else:
                instance["cpu_price"] = math.inf
                instance["ram_price"] = math.inf
                instance["total_price"] = math.inf

        # Apply custom sorting if specified
        if args.sort_by:
            # Define field mapping for case-insensitive and prefix matching
            field_mapping = {
                "type": "name",
                "t": "name",
                "name": "name",
                "vcpu": "vcpu",
                "v": "vcpu",
                "cpu": "vcpu",
                "c": "vcpu",
                "memory": "memory_gb",
                "mem": "memory_gb",
                "m": "memory_gb",
                "ram": "memory_gb",
                "cpu_price": "cpu_price",
                "cp": "cpu_price",
                "mem_price": "ram_price",
                "mp": "ram_price",
                "total_price": "total_price",
                "p": "total_price",
                "tp": "total_price",
                "cost": "total_price",
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
                        logger.warning(f"Invalid sort field: {sort_field}")
                        continue
                    instances.sort(key=lambda x: x[field_name], reverse=descending)
            else:
                # Default sort if no fields specified
                instances.sort(key=lambda x: (x["vcpu"], x["memory_gb"]))
        else:
            # Default sort by vCPU, then memory if no sort-by specified
            instances.sort(key=lambda x: (x["vcpu"], x["memory_gb"]))

        # Limit results if specified - applied after sorting
        if args.limit and len(instances) > args.limit:
            instances = instances[: args.limit]

        # Display results with pricing if available
        print(f"Found {len(instances)} instance types for {args.provider}:")
        print()

        has_pricing = bool(pricing_data)

        # Format output based on provider
        if args.provider == "aws":
            print(
                f"{'Instance Type':<24} {'vCPU':>6} {'Memory (GB)':>12} {'Storage (GB)':>12} "
                f"{'Architecture':>12} {'Price/vCPU/Hour':>17}"
            )
            print("-" * 80)
            for inst in instances:
                price_str = "N/A"
                if inst["name"] in pricing_data:
                    price_str = f"${pricing_data[inst['name']]:.4f}"
                print(
                    f"{inst['name']:<24} {inst['vcpu']:>6} {inst['memory_gb']:>12.1f} "
                    f"{inst.get('storage_gb', 0):>12} {inst.get('architecture', 'x86_64'):>12} "
                    f"{price_str:>17}"
                )

        elif args.provider == "gcp":
            print(
                f"{'Machine Type':<24} {'vCPU':>4} {'Mem (GB)':>9} {'$/vCPU/Hr':>10} "
                f"{'$/GB/Hr':>8} {'Total $/Hr':>11}"
            )
            print("-" * 90)
            for inst in instances:
                if math.isinf(inst["cpu_price"]):
                    cpu_price_str = "N/A"
                else:
                    cpu_price_str = f"${inst['cpu_price']:.4f}"
                if math.isinf(inst["ram_price"]):
                    ram_price_str = "N/A"
                else:
                    ram_price_str = f"${inst['ram_price']:.4f}"
                if math.isinf(inst["total_price"]):
                    total_price_str = "N/A"
                else:
                    total_price_str = f"${inst['total_price']:.4f}"
                print(
                    f"{inst['name']:<24} {inst['vcpu']:>4} {inst['memory_gb']:>9.1f} "
                    f"{cpu_price_str:>10} {ram_price_str:>8} {total_price_str:>11}"
                )

        elif args.provider == "azure":
            print(
                f"{'VM Size':<24} {'vCPU':>6} {'Memory (GB)':>12} {'Storage (GB)':>12} "
                f"{'Price/Hour':>17}"
            )
            print("-" * 70)
            for inst in instances:
                price_str = "N/A"
                if inst["name"] in pricing_data:
                    price_str = f"${pricing_data[inst['name']]:.4f}"
                print(
                    f"{inst['name']:<24} {inst['vcpu']:>6} {inst['memory_gb']:>12.1f} "
                    f"{inst.get('storage_gb', 0):>12} {price_str:>17}"
                )

        # Show no pricing data info if we couldn't get pricing
        if not has_pricing:
            print(
                "\nNote: To show pricing information, configure credentials with pricing "
                "API access."
            )
            print("      Pricing varies by region and can change over time.")
        elif args.use_spot:
            print(
                "\nNote: Showing spot/preemptible instance pricing which is variable and "
                "subject to change."
            )
            print("      These instances can be terminated by the cloud provider at any time.")
        else:
            print(
                "\nNote: On-demand pricing shown. Use --use-spot to see spot/preemptible pricing."
            )

        if args.provider == "aws":
            print(
                "\nTo filter instance types with the 'run' or 'manage_pool' commands, use the "
                "--instance-types parameter:"
            )
            print("  --instance-types t3 m5 (will include all t3.* and m5.* instances)")
        elif args.provider == "gcp":
            print(
                "\nTo filter instance types with the 'run' or 'manage_pool' commands, use the "
                "--instance-types parameter:"
            )
            print(
                "  --instance-types n1 n2 e2 (will include all n1-*, n2-* and e2-* machine types)"
            )
        elif args.provider == "azure":
            print(
                "\nTo filter instance types with the 'run' or 'manage_pool' commands, use the "
                "--instance-types parameter:"
            )
            print(
                "  --instance-types Standard_B Standard_D (will include all Standard_B* and "
                "Standard_D* VM sizes)"
            )

    except Exception as e:
        logger.error(f"Error listing instance types: {e}", exc_info=True)
        sys.exit(1)


# Helper functions for argument parsing


def add_common_args(parser: argparse.ArgumentParser, include_queue_name: bool = True) -> None:
    """Add common arguments to all command parsers."""
    parser.add_argument(
        "--config", default="cloud_run_config.yaml", help="Path to configuration file"
    )
    parser.add_argument("--provider", choices=["aws", "gcp", "azure"], help="Cloud provider")
    if include_queue_name:
        parser.add_argument("--queue-name", help="Name of the task queue")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")


def add_tasks_args(parser: argparse.ArgumentParser) -> None:
    """Add task-loading specific arguments."""
    parser.add_argument("--tasks", required=True, help="Path to tasks file (JSON or YAML)")


def add_instance_pool_args(parser: argparse.ArgumentParser) -> None:
    """Add instance pool management specific arguments."""
    parser.add_argument("--job-id", help="Unique job ID (generated if not provided)")
    parser.add_argument("--max-instances", type=int, default=5, help="Maximum number of instances")
    parser.add_argument("--min-instances", type=int, default=0, help="Minimum number of instances")
    parser.add_argument("--cpu", type=int, help="Minimum CPU cores per instance (overrides config)")
    parser.add_argument(
        "--memory", type=float, help="Minimum memory (GB) per instance (overrides config)"
    )
    parser.add_argument(
        "--disk", type=int, help="Minimum disk space (GB) per instance (overrides config)"
    )
    parser.add_argument("--image", help="Custom VM image to use (overrides config)")
    parser.add_argument(
        "--startup-script-file", help="Path to custom startup script file (overrides config)"
    )
    parser.add_argument(
        "--tasks-per-instance", type=int, default=10, help="Number of tasks per instance"
    )
    parser.add_argument(
        "--use-spot",
        action="store_true",
        help="Use spot/preemptible instances (cheaper but can be terminated)",
    )
    parser.add_argument(
        "--region", help="Specific region to launch instances in (defaults to cheapest region)"
    )
    parser.add_argument(
        "--instance-types",
        nargs="+",
        help='List of instance type patterns to use (e.g., "t3" for all t3 instances, "t3.micro" '
        "for exact match)",
    )


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Multi-Cloud Task Processing System")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    subparsers.required = True

    #
    # QUEUE MANAGEMENT COMMANDS
    #

    # Load queue command
    load_queue_parser = subparsers.add_parser(
        "load_queue", help="Load tasks into a queue without starting instances"
    )
    add_common_args(load_queue_parser)
    add_tasks_args(load_queue_parser)
    load_queue_parser.set_defaults(func=load_queue_cmd)

    # Show queue command
    show_queue_parser = subparsers.add_parser(
        "show_queue",
        help="Show the current depth of a task queue; use --verbose to show sample message "
        "contents",
    )
    add_common_args(show_queue_parser)
    show_queue_parser.add_argument(
        "--detail", action="store_true", help="Attempt to show a sample message"
    )
    show_queue_parser.set_defaults(func=show_queue_cmd)

    # Purge queue command
    purge_queue_parser = subparsers.add_parser(
        "purge_queue", help="Purge a task queue by removing all messages"
    )
    add_common_args(purge_queue_parser)
    purge_queue_parser.add_argument(
        "--force", "-f", action="store_true", help="Purge the queue without confirmation prompt"
    )
    purge_queue_parser.set_defaults(func=purge_queue_cmd)

    # Delete queue command
    delete_queue_parser = subparsers.add_parser(
        "delete_queue", help="Permanently delete a task queue and its infrastructure"
    )
    add_common_args(delete_queue_parser)
    delete_queue_parser.add_argument(
        "--force", "-f", action="store_true", help="Delete the queue without confirmation prompt"
    )
    delete_queue_parser.set_defaults(func=delete_queue_cmd)

    #
    # JOB MANAGEMENT COMMANDS
    #

    # Run command (combines load_tasks and manage_pool)
    run_parser = subparsers.add_parser(
        "run", help="Run a job (load tasks and manage instance pool)"
    )
    add_common_args(run_parser)
    add_tasks_args(run_parser)
    add_instance_pool_args(run_parser)
    run_parser.set_defaults(func=run_job)

    # Manage pool command
    manage_pool_parser = subparsers.add_parser(
        "manage_pool", help="Manage an instance pool for processing tasks"
    )
    add_common_args(manage_pool_parser)
    add_instance_pool_args(manage_pool_parser)
    manage_pool_parser.set_defaults(func=manage_pool_cmd)

    # Status command
    status_parser = subparsers.add_parser("status", help="Check job status")
    add_common_args(status_parser)
    status_parser.add_argument("--job-id", required=True, help="Job ID to check")
    status_parser.add_argument("--region", help="Specific region to look for instances in")
    status_parser.set_defaults(func=status_job)

    # Stop command
    stop_parser = subparsers.add_parser("stop", help="Stop a running job")
    add_common_args(stop_parser)
    stop_parser.add_argument("--job-id", required=True, help="Job ID to stop")
    stop_parser.add_argument(
        "--purge-queue", action="store_true", help="Purge the queue after stopping"
    )
    stop_parser.set_defaults(func=stop_job)

    # List running instances command
    list_running_instances_parser = subparsers.add_parser(
        "list_running_instances", help="List currently running instances for the specified provider"
    )
    add_common_args(list_running_instances_parser, include_queue_name=False)
    list_running_instances_parser.add_argument("--job-id", help="Filter instances by job ID")
    list_running_instances_parser.add_argument("--region", help="Filter instances by region")
    list_running_instances_parser.set_defaults(func=list_running_instances_cmd)

    #
    # INFORMATION GATHERING COMMANDS
    #

    # List images command
    list_images_parser = subparsers.add_parser(
        "list_images", help="List available VM images for the specified provider"
    )
    add_common_args(list_images_parser, include_queue_name=False)
    # Additional filtering options
    list_images_parser.add_argument(
        "--source",
        choices=["aws", "gcp", "azure", "user"],
        help="Filter by image source (standard cloud images or user-created)",
    )
    list_images_parser.add_argument(
        "--filter", help="Filter images containing this text in any field"
    )
    list_images_parser.add_argument(
        "--limit", type=int, help="Limit the number of images displayed"
    )
    list_images_parser.add_argument(
        "--sort-by",
        help='Sort results by comma-separated fields (e.g., "family,name" or "-source,project"). '
        "Available fields: family, name, project, source. "
        'Prefix with "-" for descending order. '
        'Partial field names like "fam" for "family" or "proj" for "project" are supported.',
    )
    # TODO Update --sort-by to be specific to each provider which have different fields
    list_images_parser.add_argument(
        "--detail", action="store_true", help="Show detailed information about each image"
    )
    list_images_parser.set_defaults(func=list_images_cmd)

    # List instance types command
    list_instance_types_parser = subparsers.add_parser(
        "list_instance_types",
        help="List compute instance types for the specified provider with pricing information",
    )
    add_common_args(list_instance_types_parser, include_queue_name=False)
    list_instance_types_parser.add_argument(
        "--min-cpu", type=int, help="Filter instance types by minimum number of vCPUs"
    )
    list_instance_types_parser.add_argument(
        "--max-cpu", type=int, help="Filter instance types by maximum number of vCPUs"
    )
    list_instance_types_parser.add_argument(
        "--min-memory", type=int, help="Filter instance types by minimum amount of memory (GB)"
    )
    list_instance_types_parser.add_argument(
        "--max-memory", type=int, help="Filter instance types by maximum amount of memory (GB)"
    )
    list_instance_types_parser.add_argument(
        "--min-memory-ratio", type=int, help="Filter instance types by minimum memory:vCPU ratio"
    )
    list_instance_types_parser.add_argument(
        "--max-memory-ratio", type=int, help="Filter instance types by maximum memory:vCPU ratio"
    )
    list_instance_types_parser.add_argument(
        "--instance-types",
        nargs="+",
        help='List of instance type patterns to filter by (e.g., "t3 m5" for AWS)',
    )
    list_instance_types_parser.add_argument(
        "--filter", help="Filter instance types containing this text in any field"
    )
    list_instance_types_parser.add_argument(
        "--limit", type=int, help="Limit the number of instance types displayed"
    )
    list_instance_types_parser.add_argument(
        "--use-spot",
        action="store_true",
        help="Show spot/preemptible instance pricing instead of on-demand",
    )
    list_instance_types_parser.add_argument(
        "--sort-by",
        help='Sort results by comma-separated fields (e.g., "price,vcpu" or "type,-memory"). '
        "Available fields: type/name, vcpu, memory, cpu_price, mem_price, total_price. "
        'Prefix with "-" for descending order. '
        'Partial field names like "mem" for "memory" or "v" for "vcpu" are supported.',
    )
    list_instance_types_parser.set_defaults(func=list_instance_types_cmd)

    # Parse arguments
    args = parser.parse_args()

    # Set up logging level based on verbosity
    if hasattr(args, "verbose") and args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load configuration
    logger.info(f"Loading configuration from {args.config}")
    try:
        config = load_config(args.config, vars(args))
    except Exception as e:
        logger.fatal(f"Configuration error: {e}", exc_info=True)
        sys.exit(1)

    if args.provider is None:
        args.provider = config.get("provider")

    if args.provider is None:
        logger.fatal("Provider must be specified")
        sys.exit(1)

    # Run the appropriate command
    asyncio.run(args.func(args, config))


if __name__ == "__main__":
    main()
