"""
Instance Orchestrator core module.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Set

from cloud_tasks.instance_manager.instance_manager import InstanceManager
from cloud_tasks.queue_manager import create_queue
from cloud_tasks.queue_manager.taskqueue import TaskQueue
from cloud_tasks.instance_manager import create_instance_manager
from cloud_tasks.common.config import Config


class InstanceOrchestrator:
    """
    Class that manages a pool of worker instances based on queue status.
    Determines when to scale up (start new instances) and down (terminate instances).
    """

    _DEFAULT_BOOT_DISK_SIZE_PER_CPU_GB = 10

    def __init__(self, config: Config):
        """
        Initialize the instance orchestrator.

        Args:
            config: Configuration object containing all settings.
        """
        self._logger = logging.getLogger(__name__)
        self._logger.debug(f"Initializing InstanceOrchestrator")

        self._config = config
        self._provider = self._config.provider

        provider_config = self._config.get_provider_config()
        self._provider_config = provider_config
        if not provider_config.job_id:
            raise ValueError("job_id must be specified")
        self._job_id = provider_config.job_id

        if not provider_config.queue_name:
            # This should have been derived in get_provider_config if job_id was present
            raise ValueError("queue_name is missing - this should not happen")
        self._queue_name = provider_config.queue_name

        # Extract run configuration (assuming config.run is populated)
        self._run_config = self._config.run
        if not self._run_config:
            raise ValueError("Run configuration section is missing - this should not happen")

        # TODO: Add scale_up/down_thresholds to RunConfig?
        # self._scale_up_threshold = 10  # Default or fetch from config if added
        # self._scale_down_threshold = 2  # Default or fetch from config if added

        # Region/Location
        self._region = provider_config.region
        self._zone = provider_config.zone

        if not provider_config.startup_script:
            raise RuntimeError("startup_script is required")

        # Will be initialized in start()
        self._instance_manager: Optional[InstanceManager] = None
        self._task_queue: Optional[TaskQueue] = None
        self._optimal_instance_info = None
        self._optimal_instance_boot_disk_size = None
        self._optimal_instance_num_tasks = None

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

        # Maximum number of instances to create in parallel
        self._min_instances = self._run_config.min_instances
        self._start_instance_max_threads = 10

        self._logger.info("Orchestrator configured for:")
        self._logger.info(f"  Provider: {self._provider}")
        self._logger.info(f"  Region: {self._region}")
        self._logger.info(f"  Zone: {self._zone}")
        self._logger.info(f"  Job ID: {self._job_id}")
        self._logger.info(f"  Queue: {self._queue_name}")
        self._logger.info(
            f"  Instance scaling: {self._run_config.min_instances} to {self._run_config.max_instances}"
        )
        if self._run_config.use_spot:
            self._logger.info(f"  Pricing: Spot instances")
        else:
            self._logger.info(f"  Pricing: On-demand instances")
        self._logger.info("  Requirements:")
        self._logger.info(f"    CPUs: {self._run_config.min_cpu} to {self._run_config.max_cpu}")
        self._logger.info(
            f"    Memory: {self._run_config.min_total_memory} to "
            f"{self._run_config.max_total_memory} GB"
        )
        self._logger.info(
            f"    Memory per CPU: {self._run_config.min_memory_per_cpu} to "
            f"{self._run_config.max_memory_per_cpu} GB"
        )
        self._logger.info(
            f"    Local SSD: {self._run_config.min_local_ssd} to "
            f"{self._run_config.max_local_ssd} GB"
        )
        self._logger.info(
            f"    Local SSD per CPU: {self._run_config.min_local_ssd_per_cpu} to "
            f"{self._run_config.max_local_ssd_per_cpu} GB"
        )
        self._logger.info(
            f"    Boot disk: {self._run_config.min_boot_disk} to "
            f"{self._run_config.max_boot_disk} GB"
        )
        self._logger.info(
            f"    Boot disk per CPU: {self._run_config.min_boot_disk_per_cpu} to "
            f"{self._run_config.max_boot_disk_per_cpu} GB"
        )
        self._logger.info(f"    CPUs per task: {self._run_config.cpus_per_task}")
        self._logger.info(
            f"    Tasks per instance: {self._run_config.min_tasks_per_instance} to "
            f"{self._run_config.max_tasks_per_instance}"
        )
        self._logger.info(f"    Instance types: {self._run_config.instance_types}")
        self._logger.info(f"  Image: {self._run_config.image}")
        self._logger.info(f"  Check interval: {self._check_interval_seconds} seconds")
        self._logger.info(
            f"  Instance termination delay: {self._instance_termination_delay_seconds} seconds"
        )
        self._logger.info(f"  Max parallel instance creations: {self._start_instance_max_threads}")

        self._logger.info(f"  Startup script:")
        for line in self._run_config.startup_script.replace("\r", "").strip().split("\n"):
            self._logger.info(f"    {line}")

    @property
    def task_queue(self) -> TaskQueue:
        return self._task_queue

    @property
    def running(self) -> bool:
        return self._running

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def queue_name(self) -> str:
        return self._queue_name

    def _generate_worker_startup_script(self) -> str:
        """
        Generate a startup script for worker instances.

        Returns:
            Shell script for instance startup
        """
        if self._provider == "GCP":
            gcp_supplement = f"""\
export RMS_CLOUD_RUN_PROJECT_ID={self._provider_config.project_id}
"""

        supplement = f"""\
export RMS_CLOUD_RUN_PROVIDER={self._provider}
{gcp_supplement}
export RMS_CLOUD_RUN_JOB_ID={self._job_id}
export RMS_CLOUD_RUN_QUEUE_NAME={self._queue_name}
export RMS_CLOUD_RUN_INSTANCE_TYPE={self._optimal_instance_info["name"]}
export RMS_CLOUD_RUN_INSTANCE_NUM_VCPUS={self._optimal_instance_info["vcpu"]}
export RMS_CLOUD_RUN_INSTANCE_MEM_GB={self._optimal_instance_info["mem_gb"]}
export RMS_CLOUD_RUN_INSTANCE_SSD_GB={self._optimal_instance_info["local_ssd_gb"]}
export RMS_CLOUD_RUN_INSTANCE_BOOT_DISK_GB={self._optimal_instance_boot_disk_size}
export RMS_CLOUD_RUN_INSTANCE_IS_SPOT={self._run_config.use_spot}
export RMS_CLOUD_RUN_INSTANCE_PRICE={self._optimal_instance_info["total_price"]}
export RMS_CLOUD_RUN_NUM_TASKS_PER_INSTANCE={self._optimal_instance_num_tasks}
export RMS_CLOUD_RUN_SHUTDOWN_GRACE_PERIOD=120
"""
        if not self._run_config.startup_script:
            raise RuntimeError("No startup script provided")

        ss = self._run_config.startup_script.strip()
        ss = ss.replace("\r", "")  # Remove any Windows line endings
        if ss.startswith("#!"):
            # Insert supplement after the shebang line
            ss_lines = ss.split("\n", 1)[1]
            if not ss_lines[0].endswith("/bash"):
                msg = "Startup script uses shell other than bash; this is not supported"
                self._logger.error(msg)
                raise RuntimeError(msg)
            ss = f"{ss_lines[0]}\n{supplement}\n{'\n'.join(ss_lines[1:])}"
        else:
            ss = f"{supplement}\n{ss}"
        return ss

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
            try:
                self._task_queue = await create_queue(self._config)
            except Exception as e:
                self._logger.error(f"Failed to initialize task queue: {e}", exc_info=True)
                raise RuntimeError(
                    f"Task queue initialization failed. Please provide a task queue or check configuration: {e}"
                )

        # Get optimal instance type based on requirements from config
        instance_info = await self._instance_manager.get_optimal_instance_type(
            vars(self._run_config)
        )
        self._logger.info(
            f"Selected instance type: {instance_info['name']} in {instance_info['zone']} "
            f"at ${instance_info['total_price']:.6f}/hour"
        )

        self._optimal_instance_info = instance_info

        # Derive the boot disk size from the constraints and the number of vCPUs in the
        # optimal instance
        boot_disk_size = self._run_config.min_boot_disk
        if boot_disk_size is None:
            boot_disk_size = self._run_config.max_boot_disk
        if self._run_config.min_boot_disk_per_cpu is not None:
            if boot_disk_size is None:
                boot_disk_size = self._run_config.min_boot_disk_per_cpu * instance_info["vcpu"]
            else:
                boot_disk_size = max(
                    self._run_config.min_boot_disk_per_cpu * instance_info["vcpu"], boot_disk_size
                )
        if self._run_config.max_boot_disk_per_cpu is not None:
            if boot_disk_size is None:
                boot_disk_size = self._run_config.max_boot_disk_per_cpu * instance_info["vcpu"]
            else:
                boot_disk_size = min(
                    self._run_config.max_boot_disk_per_cpu * instance_info["vcpu"], boot_disk_size
                )
        if boot_disk_size is None:
            self._logger.warning(
                f"No boot disk size constraints provided; using default of {self._DEFAULT_BOOT_DISK_SIZE_PER_CPU_GB} GB per CPU",
            )
            boot_disk_size = self._DEFAULT_BOOT_DISK_SIZE_PER_CPU_GB * instance_info["vcpu"]
        else:
            self._logger.info(f"Derived boot disk size: {boot_disk_size} GB")
        self._optimal_instance_boot_disk_size = boot_disk_size

        # Derive the number of tasks per instance from the constraints and the number of vCPUs in the
        # optimal instance
        if self._run_config.cpus_per_task is None:
            num_tasks = instance_info["vcpu"]  # Default to one task per vCPU
        else:
            num_tasks = int(instance_info["vcpu"] // self._run_config.cpus_per_task)
        # Enforce min/max constraints
        if self._run_config.min_tasks_per_instance is not None:
            num_tasks = max(num_tasks, self._run_config.min_tasks_per_instance)
        if self._run_config.max_tasks_per_instance is not None:
            num_tasks = min(num_tasks, self._run_config.max_tasks_per_instance)
        self._logger.info(f"Derived number of tasks per instance: {num_tasks}")
        self._optimal_instance_num_tasks = num_tasks

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
                and total_instances > self._run_config.min_instances
            ):
                # Calculate how many instances to terminate
                instances_to_terminate = total_instances - self._run_config.min_instances
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
            desired_instances = int(
                min(
                    self._run_config.max_instances,
                    max(
                        self._run_config.min_instances,
                        (queue_depth + self._run_config.cpus_per_task - 1)
                        // self._run_config.cpus_per_task,
                    ),
                )
            )

            self._logger.info(
                f"Calculated desired instances: {desired_instances} based on queue_depth={queue_depth} and tasks_per_instance={self._run_config.cpus_per_task}"
            )

            # Scale up if needed
            if total_instances < desired_instances:
                instances_to_add = desired_instances - total_instances
                self._logger.info(
                    f"Scaling up: Adding {instances_to_add} instances (from {total_instances} to {desired_instances})"
                )
                new_price = self._optimal_instance_info["total_price"] * desired_instances
                total_cpus = self._optimal_instance_info["vcpu"] * desired_instances
                simultaneous_tasks = int(total_cpus / self._run_config.cpus_per_task)
                price_str = f"*** ESTIMATED PRICE: ${new_price:.2f}/hour ***"
                hdr_str = "*" * len(price_str)
                self._logger.info(hdr_str)
                self._logger.info(price_str)
                self._logger.info(hdr_str)
                self._logger.info(
                    f"{total_cpus} vCPUs running {simultaneous_tasks} " "simultaneous tasks"
                )

                try:
                    new_instance_ids = await self.provision_instances(instances_to_add)
                    if new_instance_ids:
                        self._logger.info(
                            f"Successfully provisioned {len(new_instance_ids)} new instances"
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

        startup_script = self._generate_worker_startup_script()

        async with self._instance_creation_lock:
            # Create a list to store instance IDs
            instance_ids = []

            # Create a semaphore to limit concurrent instance creations
            semaphore = asyncio.Semaphore(self._start_instance_max_threads)

            # Define the function to start a single instance with semaphore control
            async def start_single_instance():
                async with semaphore:
                    try:
                        instance_id = await self._instance_manager.start_instance(
                            instance_type=self._optimal_instance_info["name"],
                            boot_disk_size=self._optimal_instance_boot_disk_size,
                            startup_script=startup_script,
                            job_id=self._job_id,
                            use_spot=self._run_config.use_spot,
                            image=self._run_config.image,
                            zone=self._optimal_instance_info["zone"],
                        )
                        self._logger.info(
                            f"Started {'spot' if self._run_config.use_spot else 'on-demand'} instance {instance_id}"
                        )
                        return instance_id
                    except Exception as e:
                        self._logger.error(f"Failed to start instance: {e}", exc_info=True)
                        return None

            # Create and gather all instance creation tasks
            tasks = [start_single_instance() for _ in range(count)]
            results = await asyncio.gather(*tasks)

            # Filter out None results (failed instance creations)
            instance_ids = [instance_id for instance_id in results if instance_id is not None]

            self._logger.info(
                f"Successfully provisioned {len(instance_ids)} of {count} requested instances"
            )
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
                "max_instances": self._run_config.max_instances,
                "min_instances": self._run_config.min_instances,
                "cpus_per_task": self._run_config.cpus_per_task,
            },
            "is_running": self.running,
        }
