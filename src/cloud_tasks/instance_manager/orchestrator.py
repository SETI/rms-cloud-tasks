"""
Instance Orchestrator core module.
"""

import asyncio
import base64
import json
import logging
import time
import traceback
from typing import Any, Dict, List, Optional, Set, Union
import datetime

from cloud_tasks.instance_manager.instance_manager import InstanceManager
from cloud_tasks.queue_manager.taskqueue import TaskQueue
from cloud_tasks.instance_manager import create_instance_manager
from cloud_tasks.common.logging_config import configure_logging
from cloud_tasks.common.config import Config, ProviderConfig


class InstanceOrchestrator:
    """
    Class that manages a pool of worker instances based on queue status.
    Determines when to scale up (start new instances) and down (terminate instances).
    """

    def __init__(self, config: Config):
        """
        Initialize the instance orchestrator.

        Args:
            config: Configuration object containing all settings.
        """
        self._config = config

        self._logger = logging.getLogger(__name__)

        self._logger.info(f"Initializing InstanceOrchestrator")

        self._provider = self._config.provider

        provider_config = self._config.get_provider_config()
        if not provider_config.job_id:
            raise ValueError("job_id must be specified")
        self._job_id = provider_config.job_id

        if not provider_config.queue_name:
            # This should have been derived in get_provider_config if job_id was present
            raise ValueError("queue_name is missing - this should not happen")
        self._queue_name = provider_config.queue_name

        # Extract run configuration (assuming config.run is populated)
        run_config = self._config.run
        if not run_config:
            raise ValueError("Run configuration section ('run:') is missing.")

        # Instance requirements
        # TODO: Define defaults more formally, perhaps in RunConfig itself
        self._min_cpu = run_config.min_cpu
        self._max_cpu = run_config.max_cpu
        self._min_total_memory = run_config.min_total_memory
        self._max_total_memory = run_config.max_total_memory
        self._min_memory_per_cpu = run_config.min_memory_per_cpu
        self._max_memory_per_cpu = run_config.max_memory_per_cpu
        self._min_disk = run_config.min_disk
        self._max_disk = run_config.max_disk
        self._min_disk_per_cpu = run_config.min_disk_per_cpu
        self._max_disk_per_cpu = run_config.max_disk_per_cpu
        self._instance_types = run_config.instance_types

        # Scaling parameters
        self._min_instances = run_config.min_instances
        self._max_instances = run_config.max_instances
        self._use_spot_instances = run_config.use_spot
        self._cpus_per_task = run_config.cpus_per_task or 1
        self._min_tasks_per_instance = run_config.min_tasks_per_instance
        self._max_tasks_per_instance = run_config.max_tasks_per_instance

        # Startup parameters
        self._image = run_config.image
        self._startup_script = run_config.startup_script

        # TODO: Add scale_up/down_thresholds to RunConfig?
        # self._scale_up_threshold = 10  # Default or fetch from config if added
        # self._scale_down_threshold = 2  # Default or fetch from config if added

        # Region/Location
        self._region = provider_config.region
        self._zone = provider_config.zone

        # Will be initialized in start()
        self._instance_manager: Optional[InstanceManager] = None
        self._task_queue: Optional[TaskQueue] = None
        self._running_instances: Set[str] = set()
        self._optimal_instance_type = None
        self._instances: Dict[str, Dict[str, Any]] = {}  # Dictionary to track instance status

        # Empty queue tracking for scale-down
        self._empty_queue_since = None
        self._instance_termination_delay_seconds = 300  # 5 minutes
        self._scaling_task = None

        # Initialize lock for instance creation
        self._instance_creation_lock = asyncio.Lock()

        # Initialize running state
        self._running = False

        # Initialize last scaling time
        self._last_scaling_time = None

        # Set check interval for scaling loop
        self._check_interval_seconds = 60  # Check scaling every minute

        self._logger.info("Orchestrator configured for:")
        self._logger.info(f" Provider: {self._provider}")
        self._logger.info(f" Region: {self._region}")
        self._logger.info(f" Zone: {self._zone}")
        self._logger.info(f" Job ID: {self._job_id}")
        self._logger.info(f" Queue: {self._queue_name}")
        self._logger.info(f" Instance scaling: {self._min_instances} to {self._max_instances}")
        if self._use_spot_instances:
            self._logger.info(f" Pricing: Spot instances")
        else:
            self._logger.info(f" Pricing: On-demand instances")
        self._logger.info("  Requirements:")
        self._logger.info(f"    CPUs: {self._min_cpu} to {self._max_cpu}")
        self._logger.info(f"    Memory: {self._min_total_memory} to {self._max_total_memory} GB")
        self._logger.info(
            f"    Memory per CPU: {self._min_memory_per_cpu} to {self._max_memory_per_cpu} GB"
        )
        self._logger.info(f"    Disk: {self._min_disk} to {self._max_disk} GB")
        self._logger.info(
            f"    Disk per CPU: {self._min_disk_per_cpu} to {self._max_disk_per_cpu} GB"
        )
        self._logger.info(f"    CPUs per task: {self._cpus_per_task}")
        self._logger.info(
            f"    Tasks per instance: {self._min_tasks_per_instance} to {self._max_tasks_per_instance}"
        )
        self._logger.info(f"    Instance types: {self._instance_types}")
        self._logger.info(f"  Image: {self._image}")
        self._logger.info(f"  Check interval: {self._check_interval_seconds} seconds")
        self._logger.info(
            f"  Instance termination delay: {self._instance_termination_delay_seconds} seconds"
        )

        self._logger.info(f"  Startup script: {self._startup_script}")

    @property
    def task_queue(self) -> TaskQueue:
        return self._task_queue

    @property
    def num_running_instances(self) -> int:
        return len(self._running_instances)

    @property
    def running(self) -> bool:
        return self._running

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def queue_name(self) -> str:
        return self._queue_name

    def generate_worker_startup_script(self) -> str:
        """
        Generate a startup script for worker instances.

        Returns:
            Shell script for instance startup
        """
        # TODO: Implement creation of startup script
        # If a custom startup script is provided, use it
        if self.startup_script:
            return self.startup_script

        self._logger.error("No startup script provided")
        raise RuntimeError("No startup script provided")

    async def start(self) -> None:
        """Start the orchestrator.

        This initializes the instance manager and begins monitoring.
        """
        self._logger.info(
            f"Starting InstanceOrchestrator for {self._provider} (Job ID: {self._job_id})"
        )

        # Initialize the instance manager
        if self._instance_manager is None:
            # Simply pass the full config and provider to create_instance_manager
            # It will extract the relevant provider config internally
            self._instance_manager = await create_instance_manager(self._config)

        # Initialize the task queue if not set
        if self._task_queue is None:
            from cloud_tasks.queue_manager import create_queue

            self._logger.info(f"Initializing task queue: {self._queue_name}")

            try:
                self._task_queue = await create_queue(self._config)
            except Exception as e:
                self._logger.error(f"Failed to initialize task queue: {e}", exc_info=True)
                raise RuntimeError(
                    f"Task queue initialization failed. Please provide a task queue or check configuration: {e}"
                )

        # Get optimal instance type based on requirements from config
        instance_type, instance_zone, instance_price = (
            await self._instance_manager.get_optimal_instance_type(
                min_cpu=self._min_cpu,
                max_cpu=self._max_cpu,
                min_total_memory=self._min_total_memory,
                max_total_memory=self._max_total_memory,
                min_memory_per_cpu=self._min_memory_per_cpu,
                max_memory_per_cpu=self._max_memory_per_cpu,
                min_disk=self._min_disk,
                max_disk=self._max_disk,
                min_disk_per_cpu=self._min_disk_per_cpu,
                max_disk_per_cpu=self._max_disk_per_cpu,
                instance_types=self._instance_types,
                use_spot=self._use_spot_instances,
            )
        )
        self._logger.info(
            f"Selected instance type: {instance_type} in {instance_zone} "
            f"at ${instance_price:.6f}/hour"
        )

        # Begin monitoring
        self._running = True
        await self.check_scaling()
        self._scaling_task = asyncio.create_task(self._scaling_loop())

    async def stop(self) -> None:
        """Stop the orchestrator and terminate all instances."""
        self._logger.info("Stopping orchestrator")
        self._running = False

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
        self._logger.info("Checking if scaling is needed")

        # Get current queue depth
        try:
            queue_depth = await self.task_queue.get_queue_depth()
            self._logger.info(f"Current queue depth: {queue_depth}")
        except Exception as e:
            self._logger.error(f"Failed to get queue depth: {e}", exc_info=True)
            self._logger.error("Cannot make scaling decisions without queue depth information")
            return

        # Get current instances
        try:
            current_instances = await self.list_job_instances()
            running_count = len([i for i in current_instances if i["state"] == "running"])
            starting_count = len([i for i in current_instances if i["state"] == "starting"])

            self._logger.info(
                f"Current instances: {running_count} running, {starting_count} starting"
            )

            total_instances = running_count + starting_count
        except Exception as e:
            self._logger.error(f"Failed to get current instances: {e}", exc_info=True)
            self._logger.error("Cannot make scaling decisions without instance information")
            return

        # Check if queue is empty
        if queue_depth == 0:
            if self._empty_queue_since is None:
                self._empty_queue_since = float(time.time())
                self._logger.info("Queue is empty, starting termination timer")
            else:
                empty_duration = float(time.time()) - self._empty_queue_since
                self._logger.info(f"Queue has been empty for {empty_duration:.1f} seconds")

            # If queue has been empty for a while and we have more than min_instances,
            # terminate excess instances
            if (
                self._empty_queue_since is not None
                and float(time.time()) - self._empty_queue_since
                > self._instance_termination_delay_seconds
                and total_instances > self._min_instances
            ):
                # Calculate how many instances to terminate
                instances_to_terminate = total_instances - self._min_instances
                self._logger.info(
                    f"Queue has been empty for {float(time.time()) - self._empty_queue_since}s, "
                    f"terminating {instances_to_terminate} instances"
                )

                # Terminate instances
                terminate_count = 0
                for instance in current_instances:
                    if instance["state"] == "running" and terminate_count < instances_to_terminate:
                        await self._instance_manager.terminate_instance(instance["id"])
                        self._logger.info(f"Terminated instance: {instance['id']}")
                        terminate_count += 1
        else:
            # Queue is not empty, reset timer
            if self._empty_queue_since is not None:
                self._logger.info("Queue is no longer empty, resetting termination timer")
            self._empty_queue_since = None

            # Calculate desired number of instances based on queue depth and tasks per instance
            desired_instances = min(
                self._max_instances,
                max(
                    self._min_instances,
                    (queue_depth + self._cpus_per_task - 1) // self._cpus_per_task,
                ),
            )

            self._logger.info(
                f"Calculated desired instances: {desired_instances} based on queue_depth={queue_depth} and tasks_per_instance={self._cpus_per_task}"
            )

            # Scale up if needed
            if total_instances < desired_instances:
                instances_to_add = desired_instances - total_instances
                self._logger.info(
                    f"Scaling up: Adding {instances_to_add} instances (from {total_instances} to {desired_instances})"
                )

                try:
                    new_instance_ids = await self.provision_instances(instances_to_add)
                    if new_instance_ids:
                        self._logger.info(
                            f"Successfully provisioned {len(new_instance_ids)} new instances: {new_instance_ids}"
                        )
                    else:
                        self._logger.warning(
                            "No instances were provisioned despite scaling request"
                        )
                except Exception as e:
                    self._logger.error(f"Failed to provision instances: {e}", exc_info=True)
            else:
                self._logger.info(
                    f"No scaling needed. Current: {total_instances}, Desired: {desired_instances}"
                )

        # Update last scaling time
        self._last_scaling_time = float(time.time())

    async def _scaling_loop(self) -> None:
        """Background task to periodically check scaling."""
        try:
            while self._running:
                try:
                    await self.check_scaling()
                except Exception as e:
                    self._logger.error(f"Error in scaling loop: {e}", exc_info=True)

                # Wait for next check
                await asyncio.sleep(self._check_interval_seconds)
        except asyncio.CancelledError:
            # Handle task cancellation gracefully
            self._logger.info("Scaling loop cancelled")
            raise  # Re-raise to properly handle cancellation

    async def list_job_instances(self) -> List[Dict[str, Any]]:
        """
        List instances for the current job.

        Returns:
            List of instance dictionaries
        """
        if not self._instance_manager:
            raise RuntimeError("Instance manager not initialized. Call start() first.")

        # Use job_id and include_non_job=False (or True if needed)
        instances = await self._instance_manager.list_running_instances(
            job_id=self._job_id,
            include_non_job=False,  # Assuming we only want instances for this specific job
        )
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

        if not self._instance_manager:
            raise RuntimeError("Instance manager not initialized. Call start() first.")

        async with self._instance_creation_lock:
            # Start instances
            instance_ids = []
            for _ in range(count):
                try:
                    instance_id = await self._instance_manager.start_instance(
                        instance_type=instance_type,
                        startup_script=self.startup_script or "",  # Pass empty string if None
                        labels=labels,  # Pass labels/tags consistently
                        use_spot=self.use_spot_instances,
                        custom_image=self.custom_image,
                    )
                    instance_ids.append(instance_id)
                    # Store instance with creation time
                    self.instances[instance_id] = {
                        "status": "starting",
                        "created_at": datetime.datetime.now().timestamp(),
                        "instance_type": instance_type,
                        "is_spot": self.use_spot_instances,
                    }
                    self._logger.info(
                        f"Started {'spot' if self.use_spot_instances else 'on-demand'} instance {instance_id}"
                    )
                except Exception as e:
                    self._logger.error(f"Failed to start instance: {e}", exc_info=True)

            return instance_ids

    async def terminate_all_instances(self) -> None:
        """Terminate all instances associated with this job."""
        self._logger.info("Terminating all instances")

        instances = await self.list_job_instances()

        for instance in instances:
            try:
                await self._instance_manager.terminate_instance(instance["id"])
                self._logger.info(f"Terminated instance {instance['id']}")
            except Exception as e:
                self._logger.error(
                    f"Error terminating instance {instance['id']}: {e}", exc_info=True
                )

    async def get_job_status(self) -> Dict[str, Any]:
        """
        Get the current status of the job.

        Returns:
            Dictionary with job status information
        """
        instances = await self.list_job_instances()
        queue_depth = await self.task_queue.get_queue_depth()

        running_count = len([i for i in instances if i["state"] == "running"])
        starting_count = len([i for i in instances if i["state"] == "starting"])

        return {
            "job_id": self._job_id,
            "queue_depth": queue_depth,
            "instances": {
                "total": len(instances),
                "running": running_count,
                "starting": starting_count,
                "details": instances,
            },
            "settings": {
                "max_instances": self._max_instances,
                "min_instances": self._min_instances,
                "cpus_per_task": self._cpus_per_task,
            },
            "is_running": self.running,
        }
