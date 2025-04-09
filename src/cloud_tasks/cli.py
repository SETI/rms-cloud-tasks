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

import pydantic

from cloud_tasks.common.config import Config, load_config
from cloud_tasks.instance_manager import create_instance_manager
from cloud_tasks.queue_manager import create_queue

from cloud_tasks.common.logging_config import configure_logging
from cloud_tasks.instance_manager.orchestrator import InstanceOrchestrator

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

        async def enqueue_task(task):
            """Helper function to enqueue a single task with semaphore control."""
            async with semaphore:
                await task_queue.send_task(task["id"], task["data"])

        with tqdm(desc="Enqueueing tasks") as pbar:
            for task in yield_tasks_from_file(args.tasks):
                # Skip tasks until we reach the start_task
                if tasks_to_skip > 0:
                    tasks_to_skip -= 1
                    continue

                # Check if we've reached the limit
                if tasks_remaining is not None:
                    if tasks_remaining <= 0:
                        break
                    tasks_remaining -= 1

                logger.debug(f"Loading task: {task}")

                # Create and track the task
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
    print(f"Checking queue depth for '{queue_name}' on {provider}...")

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
                print(f"\nError retrieving sample message: {e}")


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
        print(f"Queue '{queue_name}' has been emptied. Removed " f"{queue_depth} message(s).")
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
        logger.info(f"Starting pool management for job: {config.get_provider_config().job_id}")

        # Create the orchestrator using only the config object
        # Configuration (including startup script, region, etc.) is handled
        # during the config loading phase in main()
        orchestrator = InstanceOrchestrator(config=config)

        # Start orchestrator
        logger.info("Starting orchestrator")
        await orchestrator.start()

        # Monitor job progress (using orchestrator's task_queue)
        try:
            queue_depth = await orchestrator.task_queue.get_queue_depth()
            initial_queue_depth = queue_depth

            if initial_queue_depth == 0:
                logger.warning(
                    "Queue is empty. Add tasks using the 'load_tasks' command before starting "
                    "the pool."
                )

            # We might want a way to keep this running indefinitely even if queue is empty
            # if min_instances > 0, or have a separate command just to maintain a pool.
            # For now, it exits if the queue becomes empty.
            with tqdm(total=initial_queue_depth, desc="Processing tasks") as pbar:
                last_depth = queue_depth

                while queue_depth > 0 or orchestrator.num_running_instances > 0:
                    # Check if the orchestrator is still running
                    if not orchestrator.running:
                        logger.info("Orchestrator stopped, exiting monitor loop.")
                        break

                    await asyncio.sleep(5)  # Shorter sleep for responsiveness

                    # Check instance health/count (optional, orchestrator loop does this)
                    status = await orchestrator.get_job_status()
                    running_count = status["instances"]["running"]
                    starting_count = status["instances"]["starting"]
                    total_instances = running_count + starting_count

                    try:
                        queue_depth = await orchestrator.task_queue.get_queue_depth()
                    except Exception as q_err:
                        logger.error(f"Error getting queue depth in monitor loop: {q_err}")
                        # Decide how to handle this - continue, break, etc.
                        continue  # Continue for now

                    # Update progress bar only if queue depth decreases
                    processed = last_depth - queue_depth
                    if processed > 0:
                        pbar.update(processed)
                        last_depth = queue_depth

                    # Print job status if verbose
                    if args.verbose >= 2:  # Use INFO level for status updates
                        instances_info = f"{running_count} running, {starting_count} starting"
                        logger.info(f"Queue depth: {queue_depth}, Instances: {instances_info}")

                    # Exit condition if queue is empty and no minimum instances required
                    if queue_depth == 0 and orchestrator.min_instances == 0:
                        logger.info("Queue is empty and min_instances is 0, finishing up.")
                        # Wait briefly for any final processing or scaling down
                        await asyncio.sleep(orchestrator.check_interval_seconds)
                        break

                # Ensure progress bar reaches 100% if there were initial tasks
                if initial_queue_depth > 0:
                    pbar.update(max(0, pbar.total - pbar.n))

            logger.info("Monitoring complete or queue is empty.")

            # Stop orchestrator gracefully
            logger.info("Stopping orchestrator")
            await orchestrator.stop()

            logger.info(f"Job {orchestrator.job_id} management finished.")

        except KeyboardInterrupt:
            logger.info("Received interrupt, stopping job management")
            await orchestrator.stop()
        except Exception as monitor_err:
            logger.error(f"Error during monitoring: {monitor_err}", exc_info=True)
            logger.info("Attempting to stop orchestrator due to monitoring error...")
            await orchestrator.stop()
            sys.exit(1)

    except Exception as e:
        logger.fatal(f"Error managing instance pool: {e}", exc_info=True)
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
            tag_filter["rms_cloud_run_job_id"] = args.job_id
            print(f"Listing instances with job ID: {args.job_id}\n")
        else:
            if args.all_instances:
                print("Listing all instances including ones not created by cloud run\n")
            else:
                print("Listing all instances created by cloud run\n")

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
                        f"{'Job ID':<16} {'ID':<26} {'Type':<15} {'State':<11} {'Zone':<15} {'Created':<30}"
                    )
                    print("-" * 117)
                else:
                    print(f"{'Job ID':<16} {'ID':<26} {'Type':<15} {'State':<11} {'Created':<30}")
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
                            f"{job_id:<16} {instance_id:<26} {instance_type:<15} {state:<11} "
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
                print(f"\nNo instances found")

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
        config: Configuration
    """
    try:
        # Create orchestrator using only the config
        orchestrator = InstanceOrchestrator(config=config)

        # Instance Manager and Task Queue are initialized within the orchestrator if needed,
        # but for status, we might need to initialize them explicitly if start() is not called.
        # Let's rely on get_job_status which should handle initialization or use existing state.

        # Start the orchestrator components needed for status without starting the scaling loop
        await orchestrator.instance_manager.initialize()  # Assuming an initialize method or similar
        await orchestrator.task_queue.initialize()  # Assuming an initialize method or similar

        # Get job status
        status = await orchestrator.get_job_status()

        # Print status
        print(f"Job: {status['job_id']}")
        print(f"Queue: {config.get_provider_config().queue_name}")  # Get queue name from config
        print(f"Queue depth: {status['queue_depth']}")
        print(
            f"Instances: {status['instances']['total']} total "
            f"({status['instances']['running']} running, "
            f"{status['instances']['starting']} starting)"
        )

        if args.verbose:
            print("\nInstance Details:")
            if status["instances"]["details"]:
                for instance in status["instances"]["details"]:
                    # Format instance details for display
                    job_id_str = instance.get("job_id", "N/A")
                    created_str = instance.get("creation_time", instance.get("created_at", "N/A"))
                    zone_str = instance.get("zone", "N/A")
                    print(
                        f"  ID: {instance['id']:<30} Type: {instance.get('type', 'N/A'):<15} "
                        f"State: {instance.get('state', 'N/A'):<10} Zone: {zone_str:<15} "
                        f"Created: {created_str:<30} JobID: {job_id_str}"
                    )
                    if args.verbose >= 2:  # More details with -vv
                        if instance.get("private_ip"):
                            print(f"    Private IP: {instance['private_ip']}")
                        if instance.get("public_ip"):
                            print(f"    Public IP: {instance['public_ip']}")
            else:
                print("  No instances found for this job.")

    except Exception as e:
        logger.error(f"Error checking job status: {e}", exc_info=True)
        sys.exit(1)


async def stop_job(args: argparse.Namespace, config: Config) -> None:
    """TODO
    Stop a running job and terminate its instances.

    Parameters:
        args: Command-line arguments
        config: Configuration
    """
    try:
        # Create orchestrator using only the config
        orchestrator = InstanceOrchestrator(config=config)

        # Initialize necessary components without starting the loop
        # This ensures instance_manager and task_queue are available
        await orchestrator.instance_manager.initialize()  # Assuming initialize
        await orchestrator.task_queue.initialize()  # Assuming initialize

        # Stop orchestrator (terminates instances)
        job_id_to_stop = orchestrator.job_id  # Get job_id from orchestrator
        logger.info(f"Stopping job {job_id_to_stop}")
        await orchestrator.stop()  # stop already handles termination

        # Purge queue if requested
        if args.purge_queue:
            queue_name_to_purge = orchestrator.queue_name  # Get queue_name from orchestrator
            logger.info(f"Purging queue {queue_name_to_purge}")
            # Ensure task_queue is available before purging
            if orchestrator.task_queue:
                await orchestrator.task_queue.purge_queue()
            else:
                logger.warning("Task queue not available for purging.")

        logger.info(f"Job {job_id_to_stop} stopped")

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
            print(f"No images found for provider {args.provider}")
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
            print(f"{'Family':<35} {'Name':<50} {'Project':<20} {'Source':<6}")
            print("-" * 114)
            for img in images:
                print(
                    f"{img.get('family', 'N/A')[:33]:<35} {img.get('name', 'N/A')[:48]:<50} "
                    f"{img.get('project', 'N/A')[:18]:<20} {img.get('source', 'N/A'):<6}"
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
        instances = await instance_manager.list_available_instance_types()

        if not instances:
            print(f"No instance types found for provider {config.provider}")
            return

        # Filter based on minimum requirements
        instances = [
            inst
            for inst in instances
            if (args.min_cpu is None or inst["vcpu"] >= args.min_cpu)
            and (args.max_cpu is None or inst["vcpu"] <= args.max_cpu)
            and (args.min_total_memory is None or inst["ram_gb"] >= args.min_total_memory)
            and (args.max_total_memory is None or inst["ram_gb"] <= args.max_total_memory)
            and (
                args.min_memory_per_cpu is None
                or inst["ram_gb"] / inst["vcpu"] >= args.min_memory_per_cpu
            )
            and (
                args.max_memory_per_cpu is None
                or inst["ram_gb"] / inst["vcpu"] <= args.max_memory_per_cpu
            )
            and (args.min_disk is None or inst["storage_gb"] >= args.min_disk)
            and (args.max_disk is None or inst["storage_gb"] <= args.max_disk)
            and (
                args.min_disk_per_cpu is None
                or inst["storage_gb"] / inst["vcpu"] >= args.min_disk_per_cpu
            )
            and (
                args.max_disk_per_cpu is None
                or inst["storage_gb"] / inst["vcpu"] <= args.max_disk_per_cpu
            )
            and (not args.use_spot or (args.use_spot and inst["supports_spot"]))
        ]

        # Apply instance type filter if specified
        if args.instance_types:
            filtered_instances = []
            for instance in instances:
                instance_name = instance["name"]
                # Check if instance matches any prefix or exact name
                for pattern in args.instance_types:
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
        print("Retrieving pricing information...")
        pricing_data = await instance_manager.get_instance_pricing(
            [x["name"] for x in instances], args.use_spot
        )

        instances_and_zones = []
        # Add price data to instances for sorting
        for instance in instances:
            inst_name = instance["name"]
            if inst_name in pricing_data and pricing_data[inst_name] is not None:
                for zone, zone_pricing in pricing_data[inst_name].items():
                    cpu_price = zone_pricing["cpu_price"]
                    per_cpu_price = zone_pricing["per_cpu_price"]
                    ram_price = zone_pricing["ram_price"]
                    per_gb_price = zone_pricing["ram_per_gb_price"]
                    total_price = zone_pricing["total_price"]
                    if cpu_price is None and per_cpu_price is not None:
                        cpu_price = per_cpu_price * instance["vcpu"]
                    elif per_cpu_price is None and cpu_price is not None:
                        per_cpu_price = cpu_price / instance["vcpu"]
                    if ram_price is None and per_gb_price is not None:
                        ram_price = per_gb_price * instance["ram_gb"]
                    elif per_gb_price is None and ram_price is not None:
                        per_gb_price = ram_price / instance["ram_gb"]
                    if total_price is None:
                        total_price = cpu_price + ram_price

                    data = {
                        **instance,
                        "name": inst_name,
                        "zone": zone,
                        "per_cpu_price": per_cpu_price,
                        "ram_per_gb_price": per_gb_price,
                        "total_price": total_price,
                    }
                    instances_and_zones.append(data)
            else:
                data = {
                    **instance,
                    "name": inst_name,
                    "zone": "N/A",
                    "per_cpu_price": math.inf,
                    "ram_per_gb_price": math.inf,
                    "total_price": math.inf,
                }
                instances_and_zones.append(data)

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
                "memory": "ram_gb",
                "mem": "ram_gb",
                "m": "ram_gb",
                "ram": "ram_gb",
                "cpu_price": "cpu_price",
                "cp": "cpu_price",
                "vcpu_price": "cpu_price",
                "mem_price": "ram_price",
                "mp": "ram_price",
                "total_price": "total_price",
                "p": "total_price",
                "tp": "total_price",
                "cost": "total_price",
                "description": "description",
                "d": "description",
                "desc": "description",
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
                    instances_and_zones.sort(key=lambda x: x[field_name], reverse=descending)
            else:
                # Default sort if no fields specified
                instances_and_zones.sort(
                    key=lambda x: (x["vcpu"], x["ram_gb"], x["name"], x["zone"])
                )
        else:
            # Default sort by vCPU, then memory if no sort-by specified
            instances_and_zones.sort(key=lambda x: (x["vcpu"], x["ram_gb"], x["name"], x["zone"]))

        # Limit results if specified - applied after sorting
        if args.limit and len(instances_and_zones) > args.limit:
            instances_and_zones = instances_and_zones[: args.limit]

        # Display results with pricing if available
        print(f"Found {len(instances_and_zones)} instance/zone pairs for {config.provider}:")
        print()

        has_pricing = bool(pricing_data)

        print(
            f"{'Instance Type':<24} {'Arch':>10} {'vCPU':>4} {'Mem (GB)':>9} "
            f"{'$/vCPU/Hr':>10} {'$/GB/Hr':>8} {'Total $/Hr':>11} {'Zone':>12}               "
            f"{'Description':>10}"
        )
        print("-" * 139)
        for inst in instances_and_zones:
            if math.isinf(inst["per_cpu_price"]):
                cpu_price_str = "N/A"
            else:
                cpu_price_str = f"${inst['per_cpu_price']:.4f}"
            if math.isinf(inst["ram_per_gb_price"]):
                ram_price_str = "N/A"
            else:
                ram_price_str = f"${inst['ram_per_gb_price']:.4f}"
            if math.isinf(inst["total_price"]):
                total_price_str = "N/A"
            else:
                total_price_str = f"${inst['total_price']:.4f}"
            print(
                f"{inst['name']:<24} {inst['architecture']:>10} {inst['vcpu']:>4} "
                f"{inst['ram_gb']:>9.1f} {cpu_price_str:>10} {ram_price_str:>8} "
                f"{total_price_str:>11}  {inst['zone']:<25} {inst['description'][:60]:<60}"
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
                print(f"No regions found with prefix '{args.prefix}' for provider {args.provider}")
            else:
                print(f"No regions found for provider {args.provider}")
            return

        # Display results
        if args.prefix:
            print(
                f"Found {len(regions)} regions for {args.provider} (filtered by prefix: {args.prefix})"
            )
        else:
            print(f"Found {len(regions)} regions for {args.provider}:")
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
                    skip_line = True

            if skip_line:
                print()

        if not args.zones:
            print("\nUse --zones to show availability zones for each region")
        if not args.detail:
            print("Use --detail to show additional provider-specific information")

    except Exception as e:
        logger.error(f"Error listing regions: {e}", exc_info=True)
        sys.exit(1)


# Helper functions for argument parsing


def add_common_args(
    parser: argparse.ArgumentParser, include_queue_name: bool = True, include_job_id: bool = True
) -> None:
    """Add common arguments to all command parsers."""
    parser.add_argument(
        "--config", default="cloud_run_config.yaml", help="Path to configuration file"
    )
    parser.add_argument("--provider", choices=["aws", "gcp", "azure"], help="Cloud provider")
    if include_queue_name:
        parser.add_argument(
            "--queue-name",
            help="The name of the task queue to use (derived from job ID if not provided)",
        )
    if include_job_id:
        parser.add_argument("--job-id", help="The job ID used to group tasks and compute instances")
    parser.add_argument(
        "--region", help="Specific region to use (derived from zone if not provided)"
    )
    parser.add_argument("--zone", help="Specific zone to use")
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase verbosity level (-v for warning, -vv for info, -vvv for debug)",
    )


def add_instance_pool_args(parser: argparse.ArgumentParser) -> None:
    """Add instance pool management specific arguments."""
    parser.add_argument(
        "--max-instances", type=int, default=5, help="Maximum number of compute instances"
    )
    parser.add_argument(
        "--min-instances", type=int, default=1, help="Minimum number of compute instances"
    )
    parser.add_argument("--image", help="Custom VM image to use (overrides config)")
    parser.add_argument(
        "--startup-script-file", help="Path to custom startup script file (overrides config)"
    )
    parser.add_argument("--cpus-per-task", type=int, default=1, help="Number of vCPUs per task")
    parser.add_argument(
        "--min-tasks-per-instance", type=int, default=1, help="Minimum number of tasks per instance"
    )
    parser.add_argument(
        "--max-tasks-per-instance",
        type=int,
        default=10,
        help="Maximum number of tasks per instance",
    )


def add_instance_args(parser: argparse.ArgumentParser) -> None:
    """Add compute instance-specific arguments."""
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
        "--min-disk",
        type=float,
        help="Filter instance types by minimum disk (GB)",
    )
    parser.add_argument(
        "--max-disk",
        type=float,
        help="Filter instance types by maximum disk (GB)",
    )
    parser.add_argument(
        "--min-disk-per-cpu",
        type=float,
        help="Filter instance types by minimum disk (GB) per vCPU",
    )
    parser.add_argument(
        "--max-disk-per-cpu",
        type=float,
        help="Filter instance types by maximum disk (GB) per vCPU",
    )
    parser.add_argument(
        "--instance-types",
        nargs="+",
        help='Filter instance types by name prefix (e.g., "t3 m5" for AWS)',
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
    load_queue_parser.add_argument(
        "--tasks", required=True, help="Path to tasks file (JSON or YAML)"
    )
    load_queue_parser.add_argument(
        "--start-task", type=int, help="Skip tasks until this task number (1-based indexing)"
    )
    load_queue_parser.add_argument(
        "--limit", type=int, default=None, help="Maximum number of tasks to enqueue"
    )
    load_queue_parser.add_argument(
        "--max-concurrent-tasks",
        type=int,
        default=100,
        help="Maximum number of concurrent tasks to enqueue (default: 100)",
    )
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

    # --- Manage pool command ---

    manage_pool_parser = subparsers.add_parser(
        "manage_pool", help="Manage an instance pool for processing tasks"
    )
    # --cpus-per-task
    # --startup-script-file
    add_common_args(manage_pool_parser)
    add_instance_pool_args(manage_pool_parser)
    add_instance_args(manage_pool_parser)
    manage_pool_parser.set_defaults(func=manage_pool_cmd)

    # --- List running instances command ---

    list_running_instances_parser = subparsers.add_parser(
        "list_running_instances", help="List currently running instances for the specified provider"
    )
    add_common_args(list_running_instances_parser, include_queue_name=False, include_job_id=False)
    list_running_instances_parser.add_argument("--job-id", help="Filter instances by job ID")
    list_running_instances_parser.add_argument(
        "--all-instances",
        action="store_true",
        help="Show all instances including ones that were not created by cloud run",
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
    add_common_args(list_regions_parser, include_queue_name=False, include_job_id=False)
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
    add_common_args(list_images_parser, include_queue_name=False, include_job_id=False)
    list_images_parser.add_argument(
        "--user",
        action="store_true",
        help="Include user-created images in the list",
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

    # --- List instance types command ---

    list_instance_types_parser = subparsers.add_parser(
        "list_instance_types",
        help="List compute instance types for the specified provider with pricing information",
    )
    add_common_args(list_instance_types_parser, include_queue_name=False, include_job_id=False)
    add_instance_args(list_instance_types_parser)
    list_instance_types_parser.add_argument(
        "--filter", help="Filter instance types containing this text in any field"
    )
    list_instance_types_parser.add_argument(
        "--limit", type=int, help="Limit the number of instance types displayed"
    )
    list_instance_types_parser.add_argument(
        "--sort-by",
        help='Sort results by comma-separated fields (e.g., "price,vcpu" or "type,-memory"). '
        "Available fields: type/name, vcpu, ram_gb, cpu_price, ram_price, total_price. "
        'Prefix with "-" for descending order. '
        'Partial field names like "ram" or "mem" for "ram_gb" or "v" for "vcpu" are supported.',
    )
    list_instance_types_parser.set_defaults(func=list_instance_types_cmd)

    # -------------- #
    # Main execution #
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

    """_summary_
    # --------------------------
    # JOB MANAGEMENT COMMANDS
    # --------------------------

    # --- Run command (combines load_tasks and manage_pool)
    run_parser = subparsers.add_parser(
        "run", help="Run a job (load tasks and manage instance pool)"
    )
    add_common_args(run_parser)
    add_tasks_args(run_parser)
    add_instance_pool_args(run_parser)
    run_parser.set_defaults(func=run_job)

    # --- Status command ---

    status_parser = subparsers.add_parser("status", help="Check job status")
    add_common_args(status_parser)
    status_parser.add_argument("--job-id", required=True, help="Job ID to check")
    status_parser.add_argument("--region", help="Specific region to look for instances in")
    status_parser.set_defaults(func=status_job)

    # --- Stop command ---

    stop_parser = subparsers.add_parser("stop", help="Stop a running job")
    add_common_args(stop_parser)
    stop_parser.add_argument("--job-id", required=True, help="Job ID to stop")
    stop_parser.add_argument(
        "--purge-queue", action="store_true", help="Purge the queue after stopping"
    )
    stop_parser.set_defaults(func=stop_job)



    """
