"""
Instance Orchestrator core module.
"""
import asyncio
import base64
import json
import logging
import time
import traceback
from typing import Any, Dict, List, Optional, Set
import datetime

from cloud_tasks.common.base import InstanceManager, TaskQueue
from cloud_tasks.instance_orchestrator import create_instance_manager
from cloud_tasks.common.logging_config import configure_logging

# Configure logging with proper microsecond support
configure_logging(level=logging.INFO)

# Remove the old logging configuration
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S.%f'  # Explicitly use period for fractions
# )
logger = logging.getLogger(__name__)


class InstanceOrchestrator:
    """
    Class that manages a pool of worker instances based on queue status.
    Determines when to scale up (start new instances) and down (terminate instances).
    """

    def __init__(
        self,
        provider: str,
        job_id: str,
        cpu_required: int = 1,
        memory_required_gb: int = 2,
        disk_required_gb: int = 10,
        min_instances: int = 0,
        max_instances: int = 5,
        scale_up_threshold: int = 10,
        scale_down_threshold: int = 2,
        use_spot_instances: bool = False,
        region: Optional[str] = None,
        tasks_per_instance: int = 5,
        worker_repo_url: Optional[str] = None,
        queue_name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        """
        Initialize the orchestrator.

        Args:
            provider: Cloud provider ('aws', 'gcp', or 'azure')
            job_id: Unique identifier for this job
            cpu_required: Minimum number of vCPUs for instances
            memory_required_gb: Minimum amount of memory in GB
            disk_required_gb: Minimum amount of disk space in GB
            min_instances: Minimum number of worker instances to maintain
            max_instances: Maximum number of worker instances to allow
            scale_up_threshold: Tasks per instance that triggers scale up
            scale_down_threshold: Tasks per instance that triggers scale down
            use_spot_instances: Whether to use spot/preemptible instances
            region: Specific region to launch instances in (defaults to cheapest)
            tasks_per_instance: Number of tasks each worker instance can process
            worker_repo_url: URL to Git repository with worker code
            queue_name: Name of the task queue (defaults to {job_id}-queue)
            config: Full configuration dictionary with all provider settings
            **kwargs: Additional provider-specific configuration that will be merged
                     with config[provider] if config is provided
        """
        self.provider = provider
        self.job_id = job_id
        self.cpu_required = cpu_required
        self.memory_required_gb = memory_required_gb
        self.disk_required_gb = disk_required_gb
        self.min_instances = min_instances
        self.max_instances = max_instances
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.use_spot_instances = use_spot_instances
        self.region = region
        self.tasks_per_instance = tasks_per_instance
        self.worker_repo_url = worker_repo_url
        self.queue_name = queue_name or f"{job_id}-queue"

        # Store the full configuration dictionary
        self.config = config or {}

        # Get or create the provider-specific configuration
        if provider in self.config:
            # Use existing provider config and update with any kwargs
            provider_config = self.config[provider]
            provider_config.update(kwargs)
        else:
            # Create a new provider config from kwargs
            self.config[provider] = kwargs
            provider_config = kwargs

        # Handle region parameter properly
        if self.region:
            # If region is specified in constructor, it takes precedence
            logger.info(f"Using specified region: {self.region}")
            if provider == 'azure':
                self.config[provider]['location'] = self.region
            else:
                self.config[provider]['region'] = self.region
        elif 'region' in provider_config and provider != 'azure':
            # Use region from provider config if available
            self.region = provider_config['region']
            logger.info(f"Using region from config: {self.region}")
        elif 'location' in provider_config and provider == 'azure':
            # Azure uses 'location' instead of 'region'
            self.region = provider_config['location']
            logger.info(f"Using location from config: {self.region}")
        else:
            logger.warning("No region specified, will identify and use cheapest region")

        # Will be initialized in start()
        self.instance_manager = None
        self.task_queue = None
        self.running_instances: Set[str] = set()
        self.optimal_instance_type = None

        # Empty queue tracking for scale-down
        self.empty_queue_since = None
        self.instance_termination_delay_seconds = 300  # 5 minutes
        self._scaling_task = None

        # Initialize lock for instance creation
        self.instance_creation_lock = asyncio.Lock()

        # Initialize running state
        self.running = False

        # Initialize last scaling time
        self.last_scaling_time = None

        # Set check interval for scaling loop
        self.check_interval_seconds = 60  # Check scaling every minute

    def generate_worker_startup_script(self, provider: str, queue_name: str, config: Dict[str, Any]) -> str:
        """
        Generate a startup script for worker instances.

        Args:
            provider: Cloud provider name
            queue_name: Name of the queue to process
            config: Cloud provider configuration

        Returns:
            Base64-encoded startup script
        """
        # Create a simple script to set up the worker
        script = f"""#!/bin/bash
# Update system and install dependencies
apt-get update -y
apt-get install -y git python3 python3-pip

# Clone worker code
git clone {self.worker_repo_url} /opt/worker

# Set up virtual environment
cd /opt/worker
python3 -m pip install -r requirements.txt

# Create configuration file
cat > /opt/worker/config.json << EOF
{{
    "provider": "{provider}",
    "queue_name": "{queue_name}",
    "job_id": "{self.job_id}",
    "tasks_per_worker": {self.tasks_per_instance},
    "config": {json.dumps(config)}
}}
EOF

# Start worker process
cd /opt/worker
python3 worker.py --config=/opt/worker/config.json
"""
        return script

    async def start(self) -> None:
        """
        Start the orchestrator. This initializes the instance manager and begins monitoring.
        """
        logger.info(f"Starting InstanceOrchestrator for {self.provider} (job: {self.job_id})")

        # Initialize the instance manager
        if self.instance_manager is None:
            # Simply pass the full config and provider to create_instance_manager
            # It will extract the relevant provider config internally
            self.instance_manager = await create_instance_manager(self.provider, self.config)

        # Initialize the task queue if not set
        if self.task_queue is None:
            from cloud_tasks.queue_manager import create_queue

            logger.info(f"Initializing task queue: {self.queue_name}")

            try:
                self.task_queue = await create_queue(
                    provider=self.provider,
                    queue_name=self.queue_name,
                    config=self.config
                )
            except Exception as e:
                logger.error(f"Failed to initialize task queue: {e}", exc_info=True)
                raise RuntimeError(f"Task queue initialization failed. Please provide a task queue or check configuration: {e}")

        # Begin monitoring
        self.running = True
        await self.check_scaling()
        self._scaling_task = asyncio.create_task(self._scaling_loop())

    async def stop(self) -> None:
        """Stop the orchestrator and terminate all instances."""
        logger.info("Stopping orchestrator")
        self.running = False

        # Cancel scaling task if it exists
        if self._scaling_task is not None:
            self._scaling_task.cancel()
            try:
                await self._scaling_task
            except asyncio.CancelledError:
                pass

        # Terminate all instances
        await self.terminate_all_instances()

    async def check_scaling(self) -> None:
        """
        Check if we need to scale up or down based on queue depth.
        """
        logger.info("Checking if scaling is needed")

        # Get current queue depth
        try:
            queue_depth = await self.task_queue.get_queue_depth()
            logger.info(f"Current queue depth: {queue_depth}")
        except Exception as e:
            logger.error(f"Failed to get queue depth: {e}")
            logger.error("Cannot make scaling decisions without queue depth information")
            return

        # Get current instances
        try:
            current_instances = await self.list_job_instances()
            running_count = len([i for i in current_instances if i['state'] == 'running'])
            starting_count = len([i for i in current_instances if i['state'] == 'starting'])

            logger.info(f"Current instances: {running_count} running, {starting_count} starting")

            total_instances = running_count + starting_count
        except Exception as e:
            logger.error(f"Failed to get current instances: {e}")
            logger.error("Cannot make scaling decisions without instance information")
            return

        # Check if queue is empty
        if queue_depth == 0:
            if self.empty_queue_since is None:
                self.empty_queue_since = float(time.time())
                logger.info("Queue is empty, starting termination timer")
            else:
                empty_duration = float(time.time()) - self.empty_queue_since
                logger.info(f"Queue has been empty for {empty_duration:.1f} seconds")

            # If queue has been empty for a while and we have more than min_instances,
            # terminate excess instances
            if (self.empty_queue_since is not None and
                float(time.time()) - self.empty_queue_since > self.instance_termination_delay_seconds and
                total_instances > self.min_instances):

                # Calculate how many instances to terminate
                instances_to_terminate = total_instances - self.min_instances
                logger.info(f"Queue has been empty for {float(time.time()) - self.empty_queue_since}s, "
                           f"terminating {instances_to_terminate} instances")

                # Terminate instances
                terminate_count = 0
                for instance in current_instances:
                    if instance['state'] == 'running' and terminate_count < instances_to_terminate:
                        await self.instance_manager.terminate_instance(instance['id'])
                        logger.info(f"Terminated instance: {instance['id']}")
                        terminate_count += 1
        else:
            # Queue is not empty, reset timer
            if self.empty_queue_since is not None:
                logger.info("Queue is no longer empty, resetting termination timer")
            self.empty_queue_since = None

            # Calculate desired number of instances based on queue depth and tasks per instance
            desired_instances = min(
                self.max_instances,
                max(self.min_instances, (queue_depth + self.tasks_per_instance - 1) // self.tasks_per_instance)
            )

            logger.info(f"Calculated desired instances: {desired_instances} based on queue_depth={queue_depth} and tasks_per_instance={self.tasks_per_instance}")

            # Scale up if needed
            if total_instances < desired_instances:
                instances_to_add = desired_instances - total_instances
                logger.info(f"Scaling up: Adding {instances_to_add} instances (from {total_instances} to {desired_instances})")

                try:
                    new_instance_ids = await self.provision_instances(instances_to_add)
                    if new_instance_ids:
                        logger.info(f"Successfully provisioned {len(new_instance_ids)} new instances: {new_instance_ids}")
                    else:
                        logger.warning("No instances were provisioned despite scaling request")
                except Exception as e:
                    logger.error(f"Failed to provision instances: {e}")
            else:
                logger.info(f"No scaling needed. Current: {total_instances}, Desired: {desired_instances}")

        # Update last scaling time
        self.last_scaling_time = float(time.time())

    async def _scaling_loop(self) -> None:
        """Background task to periodically check scaling."""
        try:
            while self.running:
                try:
                    await self.check_scaling()
                except Exception as e:
                    logger.error(f"Error in scaling loop: {e}", exc_info=True)

                # Wait for next check
                await asyncio.sleep(self.check_interval_seconds)
        except asyncio.CancelledError:
            # Handle task cancellation gracefully
            logger.info("Scaling loop cancelled")
            raise  # Re-raise to properly handle cancellation

    async def list_job_instances(self) -> List[Dict[str, Any]]:
        """
        List instances for the current job.

        Returns:
            List of instance dictionaries
        """
        # Define tags to filter by
        tags = {'job_id': self.job_id}

        instances = await self.instance_manager.list_running_instances(tag_filter=tags)
        return instances

    async def provision_instances(self, count: int) -> List[str]:
        """
        Provision new instances for the job.

        Args:
            count: Number of instances to provision

        Returns:
            List of instance IDs
        """
        if count <= 0:
            return []

        async with self.instance_creation_lock:
            # Get optimal instance type based on requirements
            instance_type = await self.instance_manager.get_optimal_instance_type(
                self.cpu_required,
                self.memory_required_gb,
                self.disk_required_gb,
                use_spot=self.use_spot_instances
            )
            logger.info(f"Selected instance type: {instance_type}")

            # Generate startup script - for testing purposes use example values
            # In production, these would be real queue name and configuration
            startup_script = self.generate_worker_startup_script(
                provider=self.provider,
                queue_name="example-queue",
                config={}
            )

            # Define tags
            created_at = time.strftime('%Y-%m-%dT%H:%M:%S')
            tags = {
                'job_id': self.job_id,
                'created_at': created_at,
                'role': 'worker'
            }

            # Start instances
            instance_ids = []
            for _ in range(count):
                try:
                    instance_id = await self.instance_manager.start_instance(
                        instance_type,
                        startup_script,
                        tags,
                        use_spot=self.use_spot_instances
                    )
                    instance_ids.append(instance_id)
                    # Store instance with creation time
                    self.instances[instance_id] = {
                        'status': 'starting',
                        'created_at': datetime.datetime.now().timestamp(),
                        'instance_type': instance_type,
                        'is_spot': self.use_spot_instances
                    }
                    logger.info(f"Started {'spot' if self.use_spot_instances else 'on-demand'} instance {instance_id}")
                except Exception as e:
                    logger.error(f"Failed to start instance: {e}", exc_info=True)

            return instance_ids

    async def terminate_all_instances(self) -> None:
        """Terminate all instances associated with this job."""
        logger.info("Terminating all instances")

        instances = await self.list_job_instances()

        for instance in instances:
            try:
                await self.instance_manager.terminate_instance(instance['id'])
                logger.info(f"Terminated instance {instance['id']}")
            except Exception as e:
                logger.error(f"Error terminating instance {instance['id']}: {e}", exc_info=True)

    async def get_job_status(self) -> Dict[str, Any]:
        """
        Get the current status of the job.

        Returns:
            Dictionary with job status information
        """
        instances = await self.list_job_instances()
        queue_depth = await self.task_queue.get_queue_depth()

        running_count = len([i for i in instances if i['state'] == 'running'])
        starting_count = len([i for i in instances if i['state'] == 'starting'])

        return {
            'job_id': self.job_id,
            'queue_depth': queue_depth,
            'instances': {
                'total': len(instances),
                'running': running_count,
                'starting': starting_count,
                'details': instances
            },
            'settings': {
                'max_instances': self.max_instances,
                'min_instances': self.min_instances,
                'tasks_per_instance': self.tasks_per_instance,
                'worker_repo_url': self.worker_repo_url
            },
            'is_running': self.running
        }