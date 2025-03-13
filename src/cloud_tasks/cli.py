"""
Command-line interface for the multi-cloud task processing system.
"""
import argparse
import asyncio
import json
import logging
import sys
import time
import traceback
from typing import Any, Dict, List, Optional, Union, Callable

import yaml  # type: ignore
from tqdm import tqdm  # type: ignore

from cloud_tasks.common.config import load_config, ConfigError, get_run_config, Config, ProviderConfig
from cloud_tasks.queue_manager import create_queue
from cloud_tasks.instance_orchestrator import create_instance_manager
from cloud_tasks.instance_orchestrator.orchestrator import InstanceOrchestrator
from cloud_tasks.common.logging_config import configure_logging

# Use custom logging configuration with proper microsecond support
configure_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_tasks_from_file(tasks_file: str) -> List[Dict[str, Any]]:
    """
    Load tasks from a JSON or YAML file.

    Args:
        tasks_file: Path to the tasks file

    Returns:
        List of task dictionaries

    Raises:
        Exception: If the file cannot be loaded
    """
    with open(tasks_file, 'r') as f:
        if tasks_file.endswith('.json'):
            tasks = json.load(f)
        elif tasks_file.endswith(('.yaml', '.yml')):
            tasks = yaml.safe_load(f)
        else:
            raise ValueError(f"Unsupported file format for tasks: {tasks_file}")

    if not isinstance(tasks, list):
        raise ValueError("Tasks file must contain a list of task dictionaries")

    return tasks


# Helper functions for argument parsing

def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add common arguments to all command parsers."""
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--provider', choices=['aws', 'gcp', 'azure'], required=True, help='Cloud provider')
    parser.add_argument('--queue-name', required=True, help='Name of the task queue')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')


def add_tasks_args(parser: argparse.ArgumentParser) -> None:
    """Add task-loading specific arguments."""
    parser.add_argument('--tasks', required=True, help='Path to tasks file (JSON or YAML)')


def add_instance_pool_args(parser: argparse.ArgumentParser) -> None:
    """Add instance pool management specific arguments."""
    parser.add_argument('--job-id', help='Unique job ID (generated if not provided)')
    parser.add_argument('--max-instances', type=int, default=5, help='Maximum number of instances')
    parser.add_argument('--min-instances', type=int, default=0, help='Minimum number of instances')
    parser.add_argument('--worker-repo', required=True, help='URL to GitHub repo with worker code')
    parser.add_argument('--cpu', type=int, help='Minimum CPU cores per instance (overrides config)')
    parser.add_argument('--memory', type=float, help='Minimum memory (GB) per instance (overrides config)')
    parser.add_argument('--disk', type=int, help='Minimum disk space (GB) per instance (overrides config)')
    parser.add_argument('--image', help='Custom VM image to use (overrides config)')
    parser.add_argument('--startup-script-file', help='Path to custom startup script file (overrides config)')
    parser.add_argument('--tasks-per-instance', type=int, default=10, help='Number of tasks per instance')
    parser.add_argument('--use-spot', action='store_true', help='Use spot/preemptible instances (cheaper but can be terminated)')
    parser.add_argument('--region', help='Specific region to launch instances in (defaults to cheapest region)')
    parser.add_argument('--instance-types', nargs='+', help='List of instance type patterns to use (e.g., "t3" for all t3 instances, "t3.micro" for exact match)')


async def load_tasks_cmd(args: argparse.Namespace) -> None:
    """
    Load tasks into a queue without starting instances.

    Args:
        args: Command-line arguments
    """
    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config = load_config(args.config)

        # Load tasks
        logger.info(f"Loading tasks from {args.tasks}")
        tasks = load_tasks_from_file(args.tasks)
        logger.info(f"Loaded {len(tasks)} tasks")

        # Initialize cloud services
        provider = args.provider

        # Create task queue
        logger.info(f"Creating task queue on {provider}")
        task_queue = await create_queue(
            provider=provider,
            queue_name=args.queue_name,
            config=config
        )

        # Populate task queue
        logger.info("Populating task queue")
        with tqdm(total=len(tasks), desc="Enqueueing tasks") as pbar:
            for task in tasks:
                await task_queue.send_task(task['id'], task['data'])
                pbar.update(1)

        # Get final queue depth to confirm
        queue_depth = await task_queue.get_queue_depth()
        logger.info(f"Tasks loaded successfully. Queue depth: {queue_depth}")

    except ConfigError as e:
        logger.error(f"Configuration error: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading tasks: {e}", exc_info=True)
        sys.exit(1)


async def manage_pool_cmd(args: argparse.Namespace) -> None:
    """
    Manage an instance pool for processing tasks without loading tasks.

    Args:
        args: Command-line arguments
    """
    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config = load_config(args.config)

        # Initialize cloud services
        provider = args.provider

        # Get the run configuration with proper overrides
        cli_args = vars(args)
        run_config = get_run_config(config, provider, cli_args)

        # Now we can use attribute access thanks to Config
        logger.info(f"Using CPU: {run_config.cpu}, Memory: {run_config.memory_gb} GB, Disk: {run_config.disk_gb} GB")
        logger.info(f"Using image: {run_config.image}")
        if run_config.startup_script:
            script_preview = run_config.startup_script[:50].replace('\n', ' ') + ('...' if len(run_config.startup_script) > 50 else '')
            logger.info(f"Using custom startup script: {script_preview}")

        # Log instance types restriction if set
        if hasattr(run_config, 'instance_types') and run_config.instance_types:
            logger.info(f"Restricting instance types to: {run_config.instance_types}")

        # Handle region parameter
        if args.region:
            logger.info(f"Using specified region from command line: {args.region}")
            region_param = args.region
        else:
            region_param = None
            logger.info("No region specified on command line, will use from config or find cheapest")

        # Create task queue to monitor progress
        logger.info(f"Creating task queue on {provider}")
        task_queue = await create_queue(
            provider=provider,
            queue_name=args.queue_name,
            config=config
        )

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
            worker_repo_url=args.worker_repo,
            queue_name=args.queue_name,
            config=config,
            custom_image=run_config.image,
            startup_script=run_config.startup_script
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
                logger.warning("Queue is empty. Add tasks using the 'load_tasks' command.")

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

    except ConfigError as e:
        logger.error(f"Configuration error: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error managing instance pool: {e}", exc_info=True)
        sys.exit(1)


async def run_job(args: argparse.Namespace) -> None:
    """
    Run a job with the specified configuration.
    This is a combination of loading tasks and managing an instance pool.

    Args:
        args: Command-line arguments
    """
    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config = load_config(args.config)

        # Load tasks
        logger.info(f"Loading tasks from {args.tasks}")
        tasks = load_tasks_from_file(args.tasks)
        logger.info(f"Loaded {len(tasks)} tasks")

        # Initialize cloud services
        provider = args.provider

        # Get the run configuration with proper overrides
        cli_args = vars(args)
        run_config = get_run_config(config, provider, cli_args)

        # Now we can use attribute access thanks to Config
        logger.info(f"Using CPU: {run_config.cpu}, Memory: {run_config.memory_gb} GB, Disk: {run_config.disk_gb} GB")
        logger.info(f"Using image: {run_config.image}")
        if run_config.startup_script:
            script_preview = run_config.startup_script[:50].replace('\n', ' ') + ('...' if len(run_config.startup_script) > 50 else '')
            logger.info(f"Using custom startup script: {script_preview}")

        # Log instance types restriction if set
        if hasattr(run_config, 'instance_types') and run_config.instance_types:
            logger.info(f"Restricting instance types to: {run_config.instance_types}")

        # Handle region parameter
        if args.region:
            logger.info(f"Using specified region from command line: {args.region}")
            region_param = args.region
        else:
            region_param = None
            logger.info("No region specified on command line, will use from config or find cheapest")

        # Create task queue
        logger.info(f"Creating task queue on {provider}")
        task_queue = await create_queue(
            provider=provider,
            queue_name=args.queue_name,
            config=config
        )

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
            worker_repo_url=args.worker_repo,
            queue_name=args.queue_name,
            config=config,
            custom_image=run_config.image,
            startup_script=run_config.startup_script
        )

        # Set the task queue on the orchestrator
        orchestrator.task_queue = task_queue

        # Populate task queue
        logger.info("Populating task queue")
        with tqdm(total=len(tasks), desc="Enqueueing tasks") as pbar:
            for task in tasks:
                await task_queue.send_task(task['id'], task['data'])
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

    except ConfigError as e:
        logger.error(f"Configuration error: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error running job: {e}", exc_info=True)
        sys.exit(1)


async def status_job(args: argparse.Namespace) -> None:
    """
    Check the status of a running job.

    Args:
        args: Command-line arguments
    """
    try:
        # Load configuration
        config = load_config(args.config)

        # Create task queue to check depth
        task_queue = await create_queue(
            provider=args.provider,
            queue_name=args.queue_name,
            config=config
        )

        # Create orchestrator
        orchestrator = InstanceOrchestrator(
            provider=args.provider,
            job_id=args.job_id,
            region=args.region if hasattr(args, 'region') else None,
            queue_name=args.queue_name,
            config=config
        )

        # Set task queue on orchestrator
        orchestrator.task_queue = task_queue

        # Get instances with job ID tag
        instances = await orchestrator.instance_manager.list_running_instances(
            tag_filter={'job_id': args.job_id}
        )

        # Get queue depth
        queue_depth = await task_queue.get_queue_depth()

        # Print status
        running_count = len([i for i in instances if i['state'] == 'running'])
        starting_count = len([i for i in instances if i['state'] == 'starting'])

        print(f"Job: {args.job_id}")
        print(f"Queue: {args.queue_name}")
        print(f"Queue depth: {queue_depth}")
        print(f"Instances: {len(instances)} total ({running_count} running, {starting_count} starting)")

        if args.verbose:
            print("\nInstances:")
            for instance in instances:
                print(f"  {instance['id']}: {instance['type']} - {instance['state']}")
                if 'public_ip' in instance and instance['public_ip']:
                    print(f"    Public IP: {instance['public_ip']}")

    except Exception as e:
        logger.error(f"Error checking job status: {e}", exc_info=True)
        sys.exit(1)


async def stop_job(args: argparse.Namespace) -> None:
    """
    Stop a running job and terminate its instances.

    Args:
        args: Command-line arguments
    """
    try:
        # Load configuration
        config = load_config(args.config)

        # Create task queue
        task_queue = await create_queue(
            provider=args.provider,
            queue_name=args.queue_name,
            config=config
        )

        # Create orchestrator with the provider parameter and full config
        orchestrator = InstanceOrchestrator(
            provider=args.provider,
            job_id=args.job_id,
            # Minimum parameters needed for stopping
            min_instances=0,
            max_instances=0,
            queue_name=args.queue_name,
            config=config  # Pass the full config
        )

        # Set the task_queue directly
        orchestrator.task_queue = task_queue

        # Create the instance manager
        orchestrator.instance_manager = await create_instance_manager(
            provider=args.provider,
            config=config
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


async def list_images_cmd(args: argparse.Namespace) -> None:
    """
    List available VM images for the specified provider.
    Shows only standard images and user-owned images, not third-party images.

    Args:
        args: Command-line arguments
    """
    try:
        # Load configuration
        config = load_config(args.config)

        # Create instance manager for the provider
        instance_manager = await create_instance_manager(
            provider=args.provider,
            config=config
        )

        # Get images
        images = await instance_manager.list_available_images()

        if not images:
            print(f"No images found for provider {args.provider}")
            return

        # Apply filters if specified
        if args.source:
            images = [img for img in images if img.get('source', '').lower() == args.source.lower()]

        if args.filter:
            # Filter by any field containing the filter string
            filter_text = args.filter.lower()
            filtered_images = []
            for img in images:
                # Check if any field contains the filter string
                for key, value in img.items():
                    if isinstance(value, str) and filter_text in value.lower():
                        filtered_images.append(img)
                        break
            images = filtered_images

        # Limit results if specified
        if args.limit and len(images) > args.limit:
            images = images[:args.limit]

        # Display results
        print(f"Found {len(images)} {'filtered ' if args.filter or args.source else ''}images for {args.provider}:")
        print()

        # Format output based on provider
        if args.provider == 'aws':
            print(f"{'ID':<20} {'Name':<40} {'Creation Date':<24} {'Source':<6}")
            print('-' * 90)
            for img in images:
                print(f"{img.get('id', 'N/A'):<20} {img.get('name', 'N/A')[:38]:<40} {img.get('creation_date', 'N/A')[:22]:<24} {img.get('source', 'N/A'):<6}")

        elif args.provider == 'gcp':
            print(f"{'Family':<16} {'Name':<40} {'Project':<20} {'Source':<6}")
            print('-' * 85)
            for img in images:
                print(f"{img.get('family', 'N/A')[:14]:<16} {img.get('name', 'N/A')[:38]:<40} {img.get('project', 'N/A')[:18]:<20} {img.get('source', 'N/A'):<6}")

        elif args.provider == 'azure':
            if any(img.get('source') == 'Azure' for img in images):
                print("MARKETPLACE IMAGES (Reference format: publisher:offer:sku:version)")
                print(f"{'Publisher':<24} {'Offer':<24} {'SKU':<24} {'Latest Version':<16}")
                print('-' * 90)
                for img in images:
                    if img.get('source') == 'Azure':
                        print(f"{img.get('publisher', 'N/A')[:22]:<24} {img.get('offer', 'N/A')[:22]:<24} {img.get('sku', 'N/A')[:22]:<24} {img.get('version', 'N/A')[:14]:<16}")

            if any(img.get('source') == 'User' for img in images):
                if any(img.get('source') == 'Azure' for img in images):
                    print("\nCUSTOM IMAGES")
                print(f"{'Name':<30} {'Resource Group':<30} {'OS Type':<10} {'Location':<16}")
                print('-' * 90)
                for img in images:
                    if img.get('source') == 'User':
                        print(f"{img.get('name', 'N/A')[:28]:<30} {img.get('resource_group', 'N/A')[:28]:<30} {img.get('os_type', 'N/A')[:8]:<10} {img.get('location', 'N/A')[:14]:<16}")

        print(f"\nTo use a custom image with the 'run' or 'manage_pool' commands, use the --image parameter.")
        if args.provider == 'aws':
            print("For AWS, specify the AMI ID: --image ami-12345678")
        elif args.provider == 'gcp':
            print("For GCP, specify the image family or full URI: --image ubuntu-2404-lts or --image https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts-amd64-v20240416")
        elif args.provider == 'azure':
            print("For Azure, specify as publisher:offer:sku:version or full resource ID: --image Canonical:UbuntuServer:24_04-lts:latest")

    except Exception as e:
        logger.error(f"Error listing images: {e}", exc_info=True)
        sys.exit(1)


async def list_instances_cmd(args: argparse.Namespace) -> None:
    """
    List available compute instance types for the specified provider with pricing information.

    Args:
        args: Command-line arguments
    """
    try:
        # Load configuration
        config = load_config(args.config)

        # Create instance manager for the provider
        instance_manager = await create_instance_manager(
            provider=args.provider,
            config=config
        )

        # Get available instance types
        instances = await instance_manager.list_available_instance_types()

        if not instances:
            print(f"No instance types found for provider {args.provider}")
            return

        # Apply source filter if specified
        if args.size_filter:
            # Filter instance types by minimum size requirements
            size_parts = args.size_filter.split(':')
            if len(size_parts) == 3:
                try:
                    min_cpu = int(size_parts[0])
                    min_memory = float(size_parts[1])
                    min_disk = int(size_parts[2])

                    # Filter based on minimum requirements
                    instances = [
                        inst for inst in instances
                        if inst['vcpu'] >= min_cpu
                        and inst['memory_gb'] >= min_memory
                        and inst.get('storage_gb', 0) >= min_disk
                    ]
                except ValueError:
                    print(f"Invalid size filter format: {args.size_filter}. Should be cpu:memory_gb:disk_gb")
            else:
                print(f"Invalid size filter format: {args.size_filter}. Should be cpu:memory_gb:disk_gb")

        # Apply instance type filter if specified
        if args.instance_types:
            instance_type_patterns = args.instance_types.split()
            filtered_instances = []
            for instance in instances:
                instance_name = instance['name']
                # Check if instance matches any prefix or exact name
                for pattern in instance_type_patterns:
                    if instance_name.startswith(pattern) or instance_name == pattern:
                        filtered_instances.append(instance)
                        break
            instances = filtered_instances

        # Apply text filter if specified
        if args.filter:
            filter_text = args.filter.lower()
            instances = [
                inst for inst in instances
                if any(str(value).lower().find(filter_text) >= 0
                      for key, value in inst.items() if isinstance(value, (str, int, float)))
            ]

        # Try to get pricing information where available
        pricing_data = {}
        try:
            # Create a config structure for the get_optimal_instance_type function to use
            provider_config = config.get_provider(args.provider)

            # For AWS and Azure, we need to set region explicitly for pricing
            if args.provider == 'aws' and 'region' not in provider_config:
                print("Note: Pricing information requires a region. Using default us-west-2 for pricing data.")
                provider_config['region'] = 'us-west-2'
            elif args.provider == 'azure' and 'location' not in provider_config:
                print("Note: Pricing information requires a location. Using default eastus for pricing data.")
                provider_config['location'] = 'eastus'

            # Get pricing data for all eligible instance types
            if hasattr(instance_manager, 'get_instance_pricing'):
                # If the provider has a pricing method, use it
                for instance in instances:
                    try:
                        price = await instance_manager.get_instance_pricing(instance['name'], args.use_spot)
                        pricing_data[instance['name']] = price
                    except Exception as e:
                        logger.debug(f"Could not get pricing for {instance['name']}: {e}")
        except Exception as e:
            logger.warning(f"Could not retrieve pricing information: {e}")

        # Add price data to instances for sorting
        for instance in instances:
            if instance['name'] in pricing_data:
                instance['price'] = pricing_data[instance['name']]
            else:
                instance['price'] = float('inf')  # Use infinity for sorting unknown prices to the end

        # Apply custom sorting if specified
        if args.sort_by:
            # Define field mapping for case-insensitive and prefix matching
            field_mapping = {
                'type': 'name',
                't': 'name',
                'name': 'name',
                'vcpu': 'vcpu',
                'v': 'vcpu',
                'cpu': 'vcpu',
                'c': 'vcpu',
                'memory': 'memory_gb',
                'mem': 'memory_gb',
                'm': 'memory_gb',
                'price': 'price',
                'p': 'price',
                'cost': 'price'
            }

            # Parse the sort fields
            sort_fields = args.sort_by.split(',')

            # Build a sort key function
            def build_sort_key(instance):
                result = []
                for field in sort_fields:
                    descending = field.startswith('-')
                    field_name = field[1:] if descending else field
                    field_name = field_name.lower()  # Case-insensitive matching

                    # Find the matching field using prefix matching
                    matched_field = None
                    for key, value in field_mapping.items():
                        if field_name == key or field_name.startswith(key):
                            matched_field = value
                            break

                    if matched_field:
                        value = instance.get(matched_field)
                        # Handle different types appropriately for sorting
                        if isinstance(value, (int, float)):
                            # For numbers, negate if descending
                            result.append(-value if descending else value)
                        elif value is None:
                            # None values come first in ascending, last in descending
                            result.append(float('inf') if descending else float('-inf'))
                        else:
                            # For strings, use lowercase for case-insensitive comparison
                            # In descending order, we'd ideally sort from Z-A
                            str_val = str(value).lower()
                            if descending:
                                # Create a tuple with a negation flag for descending string sort
                                result.append((1, str_val))  # 1 indicates descending
                            else:
                                result.append((0, str_val))  # 0 indicates ascending

                return tuple(result)

            # Apply sorting if we have valid fields
            if sort_fields:
                try:
                    instances.sort(key=build_sort_key)
                except Exception as e:
                    logger.warning(f"Error during sorting: {e}, using default sort")
                    # Fall back to default sort
                    instances.sort(key=lambda x: (x['vcpu'], x['memory_gb']))
            else:
                # Default sort if no fields specified
                instances.sort(key=lambda x: (x['vcpu'], x['memory_gb']))
        else:
            # Default sort by vCPU, then memory if no sort-by specified
            instances.sort(key=lambda x: (x['vcpu'], x['memory_gb']))

        # Limit results if specified - applied after sorting
        if args.limit and len(instances) > args.limit:
            instances = instances[:args.limit]

        # Display results with pricing if available
        print(f"Found {len(instances)} {'filtered ' if args.filter or args.instance_types or args.size_filter else ''}instance types for {args.provider}:")
        print()

        has_pricing = bool(pricing_data)

        # Format output based on provider
        if args.provider == 'aws':
            print(f"{'Instance Type':<20} {'vCPU':>6} {'Memory (GB)':>12} {'Storage (GB)':>12} {'Architecture':>12} {'Price/Hour':>17}")
            print('-' * 80)
            for inst in instances:
                price_str = "N/A"
                if inst['name'] in pricing_data:
                    price_str = f"${pricing_data[inst['name']]:.4f}"
                print(f"{inst['name']:<20} {inst['vcpu']:>6} {inst['memory_gb']:>12.1f} {inst.get('storage_gb', 0):>12} {inst.get('architecture', 'x86_64'):>12} {price_str:>17}")

        elif args.provider == 'gcp':
            print(f"{'Machine Type':<20} {'vCPU':>6} {'Memory (GB)':>12} {'Price/Hour':>17}")
            print('-' * 60)
            for inst in instances:
                price_str = "N/A"
                if inst['name'] in pricing_data:
                    price_str = f"${pricing_data[inst['name']]:.4f}"
                print(f"{inst['name']:<20} {inst['vcpu']:>6} {inst['memory_gb']:>12.1f} {price_str:>17}")

        elif args.provider == 'azure':
            print(f"{'VM Size':<20} {'vCPU':>6} {'Memory (GB)':>12} {'Storage (GB)':>12} {'Price/Hour':>17}")
            print('-' * 70)
            for inst in instances:
                price_str = "N/A"
                if inst['name'] in pricing_data:
                    price_str = f"${pricing_data[inst['name']]:.4f}"
                print(f"{inst['name']:<20} {inst['vcpu']:>6} {inst['memory_gb']:>12.1f} {inst.get('storage_gb', 0):>12} {price_str:>17}")

        # Show no pricing data info if we couldn't get pricing
        if not has_pricing:
            print("\nNote: To show pricing information, configure credentials with pricing API access.")
            print("      Pricing varies by region and can change over time.")
        elif args.use_spot:
            print("\nNote: Showing spot/preemptible instance pricing which is variable and subject to change.")
            print("      These instances can be terminated by the cloud provider at any time.")
        else:
            print("\nNote: On-demand pricing shown. Use --use-spot to see spot/preemptible pricing.")

        if args.provider == 'aws':
            print("\nTo filter instance types with the 'run' or 'manage_pool' commands, use the --instance-types parameter:")
            print("  --instance-types 't3 m5' (will include all t3.* and m5.* instances)")
        elif args.provider == 'gcp':
            print("\nTo filter instance types with the 'run' or 'manage_pool' commands, use the --instance-types parameter:")
            print("  --instance-types 'n1 n2 e2' (will include all n1-*, n2-* and e2-* machine types)")
        elif args.provider == 'azure':
            print("\nTo filter instance types with the 'run' or 'manage_pool' commands, use the --instance-types parameter:")
            print("  --instance-types 'Standard_B Standard_D' (will include all Standard_B* and Standard_D* VM sizes)")

    except Exception as e:
        logger.error(f"Error listing instance types: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description='Multi-Cloud Task Processing System')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    subparsers.required = True

    # Run command (combines load_tasks and manage_pool)
    run_parser = subparsers.add_parser('run', help='Run a job (load tasks and manage instance pool)')
    add_common_args(run_parser)
    add_tasks_args(run_parser)
    add_instance_pool_args(run_parser)
    run_parser.set_defaults(func=run_job)

    # Load tasks command
    load_tasks_parser = subparsers.add_parser('load_tasks', help='Load tasks into a queue without starting instances')
    add_common_args(load_tasks_parser)
    add_tasks_args(load_tasks_parser)
    load_tasks_parser.set_defaults(func=load_tasks_cmd)

    # Manage pool command
    manage_pool_parser = subparsers.add_parser('manage_pool', help='Manage an instance pool for processing tasks')
    add_common_args(manage_pool_parser)
    add_instance_pool_args(manage_pool_parser)
    manage_pool_parser.set_defaults(func=manage_pool_cmd)

    # Status command
    status_parser = subparsers.add_parser('status', help='Check job status')
    add_common_args(status_parser)
    status_parser.add_argument('--job-id', required=True, help='Job ID to check')
    status_parser.add_argument('--region', help='Specific region to look for instances in')
    status_parser.set_defaults(func=status_job)

    # Stop command
    stop_parser = subparsers.add_parser('stop', help='Stop a running job')
    add_common_args(stop_parser)
    stop_parser.add_argument('--job-id', required=True, help='Job ID to stop')
    stop_parser.add_argument('--purge-queue', action='store_true', help='Purge the queue after stopping')
    stop_parser.set_defaults(func=stop_job)

    # List images command
    list_images_parser = subparsers.add_parser('list_images', help='List available VM images for the specified provider')
    # Use common args helper but we'll need to override queue-name since we don't need it
    add_common_args(list_images_parser)
    # Override queue-name to make it optional since we don't need it for this command
    for action in list_images_parser._actions:
        if action.dest == 'queue_name':
            action.required = False
            action.help = 'Name of the task queue (not used for this command)'
    # Additional filtering options
    list_images_parser.add_argument('--source', choices=['aws', 'gcp', 'azure', 'user'], help='Filter by image source (standard cloud images or user-created)')
    list_images_parser.add_argument('--filter', help='Filter images containing this text in any field')
    list_images_parser.add_argument('--limit', type=int, help='Limit the number of images displayed')
    list_images_parser.set_defaults(func=list_images_cmd)

    # List instances command
    list_instances_parser = subparsers.add_parser('list_instances', help='List available compute instance types for the specified provider with pricing information')
    add_common_args(list_instances_parser)
    # Override queue-name to make it optional since we don't need it for this command
    for action in list_instances_parser._actions:
        if action.dest == 'queue_name':
            action.required = False
            action.help = 'Name of the task queue (not used for this command)'
    list_instances_parser.add_argument('--size-filter', help='Filter instance types by minimum size requirements (format: cpu:memory_gb:disk_gb)')
    list_instances_parser.add_argument('--instance-types', help='Space-separated list of instance type patterns to filter by (e.g., "t3 m5" for AWS)')
    list_instances_parser.add_argument('--filter', help='Filter instance types containing this text in any field')
    list_instances_parser.add_argument('--limit', type=int, help='Limit the number of instance types displayed')
    list_instances_parser.add_argument('--use-spot', action='store_true', help='Show spot/preemptible instance pricing instead of on-demand')
    list_instances_parser.add_argument('--sort-by',
                                     help='Sort results by comma-separated fields (e.g., "price,vcpu" or "type,-memory"). '
                                          'Available fields: type/name, vcpu, memory, price. '
                                          'Prefix with "-" for descending order. '
                                          'Partial field names like "mem" for "memory" or "v" for "vcpu" are supported.')
    list_instances_parser.set_defaults(func=list_instances_cmd)

    args = parser.parse_args()

    # Set up logging level based on verbosity
    if hasattr(args, 'verbose') and args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run the appropriate command
    asyncio.run(args.func(args))


if __name__ == '__main__':
    main()
