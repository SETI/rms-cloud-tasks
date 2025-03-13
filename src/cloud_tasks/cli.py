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
from typing import Any, Dict, List, Optional, Union

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


def load_tasks(tasks_file: str) -> List[Dict[str, Any]]:
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


async def run_job(args: argparse.Namespace) -> None:
    """
    Run a job with the specified configuration.

    Args:
        args: Command-line arguments
    """
    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config = load_config(args.config)

        # Load tasks
        logger.info(f"Loading tasks from {args.tasks}")
        tasks = load_tasks(args.tasks)
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

        # Handle region parameter - add it to the config if specified on command line
        if args.region:
            logger.info(f"Using specified region from command line: {args.region}")
            # Store region directly in InstanceOrchestrator constructor
            region_param = args.region
        else:
            # No need to extract region, it will be handled by InstanceOrchestrator
            region_param = None
            logger.info("No region specified on command line, will use from config or find cheapest")

        # Create task queue
        logger.info(f"Creating task queue on {provider}")
        task_queue = await create_queue(
            provider=provider,
            queue_name=args.queue_name,
            config=config
        )

        # Create instance manager
        logger.info(f"Creating instance manager on {provider}")
        instance_manager = await create_instance_manager(
            provider=provider,
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
            config=config,  # Pass the full config dictionary (already a Config)
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


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description='Multi-Cloud Task Processing System')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    subparsers.required = True

    # Run command
    run_parser = subparsers.add_parser('run', help='Run a job')
    run_parser.add_argument('--config', required=True, help='Path to configuration file')
    run_parser.add_argument('--tasks', required=True, help='Path to tasks file (JSON or YAML)')
    run_parser.add_argument('--provider', choices=['aws', 'gcp', 'azure'], required=True, help='Cloud provider')
    run_parser.add_argument('--queue-name', required=True, help='Name of the task queue')
    run_parser.add_argument('--worker-repo', required=True, help='URL to GitHub repo with worker code')
    run_parser.add_argument('--job-id', help='Unique job ID (generated if not provided)')
    run_parser.add_argument('--max-instances', type=int, default=5, help='Maximum number of instances')
    run_parser.add_argument('--min-instances', type=int, default=0, help='Minimum number of instances')
    run_parser.add_argument('--cpu', type=int, help='Minimum CPU cores per instance (overrides config)')
    run_parser.add_argument('--memory', type=float, help='Minimum memory (GB) per instance (overrides config)')
    run_parser.add_argument('--disk', type=int, help='Minimum disk space (GB) per instance (overrides config)')
    run_parser.add_argument('--image', help='Custom VM image to use (overrides config)')
    run_parser.add_argument('--startup-script-file', help='Path to custom startup script file (overrides config)')
    run_parser.add_argument('--tasks-per-instance', type=int, default=10, help='Number of tasks per instance')
    run_parser.add_argument('--use-spot', action='store_true', help='Use spot/preemptible instances (cheaper but can be terminated)')
    run_parser.add_argument('--region', help='Specific region to launch instances in (defaults to cheapest region)')
    run_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    run_parser.set_defaults(func=run_job)

    # Status command
    status_parser = subparsers.add_parser('status', help='Check job status')
    status_parser.add_argument('--config', required=True, help='Path to configuration file')
    status_parser.add_argument('--provider', choices=['aws', 'gcp', 'azure'], required=True, help='Cloud provider')
    status_parser.add_argument('--queue-name', required=True, help='Name of the task queue')
    status_parser.add_argument('--job-id', required=True, help='Job ID to check')
    status_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    status_parser.set_defaults(func=status_job)

    # Stop command
    stop_parser = subparsers.add_parser('stop', help='Stop a running job')
    stop_parser.add_argument('--config', required=True, help='Path to configuration file')
    stop_parser.add_argument('--provider', choices=['aws', 'gcp', 'azure'], required=True, help='Cloud provider')
    stop_parser.add_argument('--queue-name', required=True, help='Name of the task queue')
    stop_parser.add_argument('--job-id', required=True, help='Job ID to stop')
    stop_parser.add_argument('--purge-queue', action='store_true', help='Purge the queue after stopping')
    stop_parser.set_defaults(func=stop_job)

    args = parser.parse_args()

    # Set up logging level based on verbosity
    if hasattr(args, 'verbose') and args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run the appropriate command
    asyncio.run(args.func(args))


if __name__ == '__main__':
    main()
