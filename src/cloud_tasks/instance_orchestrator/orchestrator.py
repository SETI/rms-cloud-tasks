"""
Instance Orchestrator core module.
"""
import asyncio
import base64
import json
import logging
import time
from typing import Any, Dict, List, Optional, Set

from cloud_tasks.common.base import InstanceManager, TaskQueue

logger = logging.getLogger(__name__)


class InstanceOrchestrator:
    """
    Core orchestrator that manages instance scaling.

    This class coordinates the scaling of instances based on the queue depth.
    """

    def __init__(
        self,
        instance_manager: InstanceManager,
        task_queue: TaskQueue,
        worker_repo_url: str,
        max_instances: int,
        min_instances: int = 0,
        cpu_required: int = 1,
        memory_required_gb: int = 2,
        disk_required_gb: int = 10,
        tasks_per_instance: int = 10,
        check_interval_seconds: int = 30,
        instance_termination_delay_seconds: int = 300,
        job_id: Optional[str] = None,
    ):
        """
        Initialize the instance orchestrator.

        Args:
            instance_manager: InstanceManager implementation
            task_queue: TaskQueue implementation
            worker_repo_url: URL to the GitHub repo with worker code
            max_instances: Maximum number of instances to provision
            min_instances: Minimum number of instances to keep running
            cpu_required: Minimum CPU cores required per instance
            memory_required_gb: Minimum memory required per instance in GB
            disk_required_gb: Minimum disk space required per instance in GB
            tasks_per_instance: Number of tasks each instance should handle concurrently
            check_interval_seconds: Interval between scaling checks
            instance_termination_delay_seconds: Time to wait after queue empty before terminating
            job_id: Unique ID for this job run (to tag instances)
        """
        self.instance_manager = instance_manager
        self.task_queue = task_queue
        self.worker_repo_url = worker_repo_url
        self.max_instances = max_instances
        self.min_instances = min_instances
        self.cpu_required = cpu_required
        self.memory_required_gb = memory_required_gb
        self.disk_required_gb = disk_required_gb
        self.tasks_per_instance = tasks_per_instance
        self.check_interval_seconds = check_interval_seconds
        self.instance_termination_delay_seconds = instance_termination_delay_seconds
        self.job_id = job_id or f"job-{int(time.time())}"

        # State variables
        self.running = False
        self.instances: Dict[str, Dict[str, Any]] = {}  # instance_id -> instance_data
        self.last_scaling_time: float = 0.0
        self.empty_queue_since: Optional[float] = None

        # Background task
        self._scaling_task = None

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
        """Start the orchestrator."""
        if self.running:
            logger.warning("Orchestrator is already running")
            return

        self.running = True

        # Initial scaling
        await self.check_scaling()

        # Start background task to periodically check scaling
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
        queue_depth = await self.task_queue.get_queue_depth()
        logger.info(f"Current queue depth: {queue_depth}")

        # Get current instances
        current_instances = await self.list_job_instances()
        running_count = len([i for i in current_instances if i['state'] == 'running'])
        starting_count = len([i for i in current_instances if i['state'] == 'starting'])

        logger.info(f"Current instances: {running_count} running, {starting_count} starting")

        total_instances = running_count + starting_count

        # Check if queue is empty
        if queue_depth == 0:
            if self.empty_queue_since is None:
                self.empty_queue_since = time.time()
                logger.info("Queue is empty, starting termination timer")

            # If queue has been empty for a while and we have more than min_instances,
            # terminate excess instances
            if (self.empty_queue_since is not None and
                time.time() - self.empty_queue_since > self.instance_termination_delay_seconds and
                total_instances > self.min_instances):

                # Calculate how many instances to terminate
                instances_to_terminate = total_instances - self.min_instances
                logger.info(f"Queue has been empty for {time.time() - self.empty_queue_since}s, "
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
            self.empty_queue_since = None

            # Calculate desired number of instances based on queue depth and tasks per instance
            desired_instances = min(
                self.max_instances,
                max(self.min_instances, (queue_depth + self.tasks_per_instance - 1) // self.tasks_per_instance)
            )

            # Scale up if needed
            if total_instances < desired_instances:
                instances_to_add = desired_instances - total_instances
                logger.info(f"Scaling up: Adding {instances_to_add} instances")

                await self.provision_instances(instances_to_add)

        # Update last scaling time
        self.last_scaling_time = time.time()

    async def _scaling_loop(self) -> None:
        """Background task to periodically check scaling."""
        try:
            while self.running:
                try:
                    await self.check_scaling()
                except Exception as e:
                    logger.error(f"Error in scaling loop: {e}")

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

        logger.info(f"Provisioning {count} instances")

        # Get the optimal instance type
        instance_type = await self.instance_manager.get_optimal_instance_type(
            self.cpu_required, self.memory_required_gb, self.disk_required_gb
        )

        logger.info(f"Selected instance type: {instance_type}")

        # Define tags for the instances
        tags = {
            'job_id': self.job_id,
            'created_at': str(int(time.time())),
            'role': 'worker'
        }

        # Generate startup script
        # In a real implementation, this would include provider-specific config
        startup_script = self.generate_worker_startup_script(
            provider="example",  # Would be determined from the instance_manager
            queue_name="example-queue",  # Would come from task_queue
            config={}  # Would be the provider config
        )

        # Provision instances
        instance_ids = []
        for i in range(count):
            try:
                instance_id = await self.instance_manager.start_instance(
                    instance_type=instance_type,
                    user_data=startup_script,
                    tags=tags
                )

                logger.info(f"Started instance: {instance_id}")
                instance_ids.append(instance_id)
            except Exception as e:
                logger.error(f"Error starting instance: {e}")

        return instance_ids

    async def terminate_all_instances(self) -> None:
        """Terminate all instances associated with this job."""
        logger.info("Terminating all instances")

        instances = await self.list_job_instances()

        for instance in instances:
            try:
                await self.instance_manager.terminate_instance(instance['id'])
                logger.info(f"Terminated instance: {instance['id']}")
            except Exception as e:
                logger.error(f"Error terminating instance {instance['id']}: {e}")

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