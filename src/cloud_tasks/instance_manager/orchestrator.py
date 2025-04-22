"""
Instance Orchestrator core module.
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set, Tuple

from cloud_tasks.instance_manager.instance_manager import InstanceManager
from cloud_tasks.queue_manager import create_queue
from cloud_tasks.queue_manager.taskqueue import TaskQueue
from cloud_tasks.instance_manager import create_instance_manager
from cloud_tasks.common.config import Config

# Notes:
# - Instance selection constraints
# - # Instance constraints
# - Environment variables set in startup script


class InstanceOrchestrator:
    """
    Class that manages a pool of worker instances based on queue status.
    Determines when to scale up (start new instances) and down (terminate instances).
    """

    _DEFAULT_BOOT_DISK_SIZE_PER_CPU_GB = 10
    _DEFAULT_MIN_INSTANCES = 0
    _DEFAULT_MAX_INSTANCES = 10

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
        self._all_instance_info = None
        self._pricing_info = None

        # Empty queue tracking for scale-down
        self._empty_queue_since = None
        self._instance_termination_delay = self._run_config.instance_termination_delay
        self._scaling_task = None

        # Initialize lock for instance creation
        self._instance_creation_lock = asyncio.Lock()

        # Initialize running state
        self._running = False

        # Initialize last scaling time
        self._last_scaling_time = None
        self._scaling_task = None

        # Set check interval for scaling loop
        self._scaling_check_interval = self._run_config.scaling_check_interval

        # Maximum number of instances to create in parallel
        self._min_instances = self._run_config.min_instances or self._DEFAULT_MIN_INSTANCES
        self._max_instances = self._run_config.max_instances or self._DEFAULT_MAX_INSTANCES
        self._start_instance_max_threads = 10

        # Initialize thread pool for parallel instance creation
        self._thread_pool = ThreadPoolExecutor(max_workers=self._start_instance_max_threads)

        self._logger.info("Provider configuration:")
        self._logger.info(f"  Provider: {self._provider}")
        self._logger.info(f"  Region: {self._region}")
        self._logger.info(f"  Zone: {self._zone}")
        self._logger.info(f"  Job ID: {self._job_id}")
        self._logger.info(f"  Queue: {self._queue_name}")
        self._logger.info("Instance type selection constraints:")
        if self._run_config.instance_types is None:
            self._logger.info("  Instance types: None")
        else:
            inst_types_str = ", ".join(self._run_config.instance_types)
            self._logger.info(f"  Instance types: {inst_types_str}")
        self._logger.info(f"  CPUs: {self._run_config.min_cpu} to {self._run_config.max_cpu}")
        self._logger.info(
            f"  Memory: {self._run_config.min_total_memory} to "
            f"{self._run_config.max_total_memory} GB"
        )
        self._logger.info(
            f"  Memory per CPU: {self._run_config.min_memory_per_cpu} to "
            f"{self._run_config.max_memory_per_cpu} GB"
        )
        self._logger.info(
            f"  Local SSD: {self._run_config.min_local_ssd} to "
            f"{self._run_config.max_local_ssd} GB"
        )
        self._logger.info(
            f"  Local SSD per CPU: {self._run_config.min_local_ssd_per_cpu} to "
            f"{self._run_config.max_local_ssd_per_cpu} GB"
        )
        self._logger.info(
            f"  Boot disk: {self._run_config.min_boot_disk} to "
            f"{self._run_config.max_boot_disk} GB"
        )
        self._logger.info(
            f"  Boot disk per CPU: {self._run_config.min_boot_disk_per_cpu} to "
            f"{self._run_config.max_boot_disk_per_cpu} GB"
        )
        self._logger.info("Number of instances constraints:")
        self._logger.info(f"  # Instances: {self._min_instances} to {self._max_instances}")
        self._logger.info(
            f"  Total CPUs: {self._run_config.min_total_cpus} to "
            f"{self._run_config.max_total_cpus}"
        )
        self._logger.info(f"  CPUs per task: {self._run_config.cpus_per_task}")
        self._logger.info(
            f"    Tasks per instance: {self._run_config.min_tasks_per_instance} to "
            f"{self._run_config.max_tasks_per_instance}"
        )
        if self._run_config.min_total_price_per_hour is not None:
            min_price_str = f"${self._run_config.min_total_price_per_hour:.2f}"
        else:
            min_price_str = "None"
        if self._run_config.max_total_price_per_hour is not None:
            max_price_str = f"${self._run_config.max_total_price_per_hour:.2f}"
        else:
            max_price_str = "None"
        self._logger.info(f"  Total price per hour: {min_price_str} to {max_price_str}")
        if self._run_config.use_spot:
            self._logger.info(f"  Pricing: Spot instances")
        else:
            self._logger.info(f"  Pricing: On-demand instances")
        self._logger.info("Miscellaneous:")
        self._logger.info(f"  Scaling check interval: {self._scaling_check_interval} seconds")
        self._logger.info(
            f"  Instance termination delay: {self._instance_termination_delay} seconds"
        )
        self._logger.info(f"  Max runtime: {self._run_config.max_runtime} seconds")
        self._logger.info(f"  Worker use new process: {self._run_config.worker_use_new_process}")
        self._logger.info(f"  Max parallel instance creations: {self._start_instance_max_threads}")
        self._logger.info(f"  Image: {self._run_config.image}")
        self._logger.info(f"  Startup script:")
        for line in self._run_config.startup_script.replace("\r", "").strip().split("\n"):
            self._logger.info(f"    {line}")

    @property
    def task_queue(self) -> TaskQueue:
        return self._task_queue

    @property
    def is_running(self) -> bool:
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
export RMS_CLOUD_TASKS_PROJECT_ID={self._provider_config.project_id}
"""

        supplement = f"""\
export RMS_CLOUD_TASKS_PROVIDER={self._provider}
{gcp_supplement}
export RMS_CLOUD_TASKS_JOB_ID={self._job_id}
export RMS_CLOUD_TASKS_QUEUE_NAME={self._queue_name}
export RMS_CLOUD_TASKS_INSTANCE_TYPE={self._optimal_instance_info["name"]}
export RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS={self._optimal_instance_info["vcpu"]}
export RMS_CLOUD_TASKS_INSTANCE_MEM_GB={self._optimal_instance_info["mem_gb"]}
export RMS_CLOUD_TASKS_INSTANCE_SSD_GB={self._optimal_instance_info["local_ssd_gb"]}
export RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB={self._optimal_instance_boot_disk_size}
export RMS_CLOUD_TASKS_INSTANCE_IS_SPOT={self._run_config.use_spot}
export RMS_CLOUD_TASKS_INSTANCE_PRICE={self._optimal_instance_info["total_price"]}
export RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE={self._optimal_instance_num_tasks}
export RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD=120
export RMS_CLOUD_WORKER_USE_NEW_PROCESS={self._run_config.worker_use_new_process}
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

        self._logger.debug("New startup script using optimal instance type:")
        for line in ss.replace("\r", "").strip().split("\n"):
            self._logger.debug(f"    {line}")
        return ss

    async def initialize(self) -> None:
        """Initialize the orchestrator.

        This initializes the instance manager and task queue and loads the instance
        and pricing information.
        """
        # Initialize the instance manager
        if self._instance_manager is None:
            self._instance_manager = await create_instance_manager(self._config)

        # Initialize the task queue if not set
        if self._task_queue is None:
            try:
                self._task_queue = await create_queue(self._config)
            except Exception as e:
                self._logger.error(f"Failed to initialize task queue: {e}", exc_info=True)
                raise

    async def _initialize_pricing_info(self) -> None:
        """Initialize the pricing information."""
        if self._all_instance_info is None:
            self._all_instance_info = await self._instance_manager.get_available_instance_types()
        if self._pricing_info is None:
            self._pricing_info = await self._instance_manager.get_instance_pricing(
                self._all_instance_info
            )

    async def start(self) -> None:
        """Start the orchestrator.

        This initializes the instance manager and begins monitoring.
        """
        self._logger.debug(
            f"Starting InstanceOrchestrator for {self._provider} (Job ID: {self._job_id})"
        )

        await self.initialize()

        # Get optimal instance type based on requirements from config
        optimal_instance_info = await self._instance_manager.get_optimal_instance_type(
            vars(self._run_config)
        )

        self._optimal_instance_info = optimal_instance_info

        self._logger.info(
            f"|| Selected instance type: {optimal_instance_info['name']} in "
            f"{optimal_instance_info['zone']} "
            f"at ${optimal_instance_info['total_price']:.6f}/hour"
        )
        local_ssd_str = (
            f"{optimal_instance_info['local_ssd_gb']} GB local SSD"
            if optimal_instance_info["local_ssd_gb"]
            else "no local SSD"
        )
        self._logger.info(
            f"||  {optimal_instance_info['vcpu']} vCPUs, {optimal_instance_info['mem_gb']} GB RAM, "
            f"{local_ssd_str}"
        )

        # Derive the boot disk size from the constraints and the number of vCPUs in the
        # optimal instance
        min_boot_disk_size = self._run_config.min_boot_disk
        if self._run_config.min_boot_disk_per_cpu is not None:
            if min_boot_disk_size is None:
                min_boot_disk_size = (
                    self._run_config.min_boot_disk_per_cpu * optimal_instance_info["vcpu"]
                )
            else:
                min_boot_disk_size = max(
                    self._run_config.min_boot_disk,
                    self._run_config.min_boot_disk_per_cpu * optimal_instance_info["vcpu"],
                    min_boot_disk_size,
                )

        max_boot_disk_size = self._run_config.max_boot_disk
        if self._run_config.max_boot_disk_per_cpu is not None:
            if max_boot_disk_size is None:
                max_boot_disk_size = (
                    self._run_config.max_boot_disk_per_cpu * optimal_instance_info["vcpu"]
                )
            else:
                max_boot_disk_size = min(
                    self._run_config.max_boot_disk,
                    self._run_config.max_boot_disk_per_cpu * optimal_instance_info["vcpu"],
                    max_boot_disk_size,
                )

        if min_boot_disk_size is not None and max_boot_disk_size is not None:
            if min_boot_disk_size > max_boot_disk_size:
                raise ValueError("Calculated boot disk size constraints are inconsistent")

        boot_disk_size = min_boot_disk_size
        if boot_disk_size is None:
            boot_disk_size = max_boot_disk_size
        if boot_disk_size is None:
            self._logger.warning(
                "No boot disk size constraints provided; using default of "
                f"{self._DEFAULT_BOOT_DISK_SIZE_PER_CPU_GB} GB per CPU",
            )
            boot_disk_size = self._DEFAULT_BOOT_DISK_SIZE_PER_CPU_GB * optimal_instance_info["vcpu"]
        else:
            self._logger.info(f"|| Derived boot disk size: {boot_disk_size} GB")
        self._optimal_instance_boot_disk_size = boot_disk_size

        # Derive the number of tasks per instance from the constraints and the number of vCPUs in the
        # optimal instance
        if self._run_config.cpus_per_task is None:
            num_tasks = optimal_instance_info["vcpu"]  # Default to one task per vCPU
        else:
            num_tasks = int(optimal_instance_info["vcpu"] // self._run_config.cpus_per_task)
        # Enforce min/max constraints
        if self._run_config.min_tasks_per_instance is not None:
            num_tasks = max(num_tasks, self._run_config.min_tasks_per_instance)
        if self._run_config.max_tasks_per_instance is not None:
            num_tasks = min(num_tasks, self._run_config.max_tasks_per_instance)
        self._logger.info(f"|| Derived number of tasks per instance: {num_tasks}")
        self._optimal_instance_num_tasks = num_tasks

        self._running = True

        # Begin monitoring
        await self._check_scaling()  # Do it once right now
        self._scaling_task = asyncio.create_task(self._scaling_loop())

    async def _scaling_loop(self) -> None:
        """Background task to periodically check scaling."""
        last_check = time.time()
        try:
            while self._running:
                try:
                    now = time.time()
                    if now - last_check > self._scaling_check_interval:
                        await self._check_scaling()
                        last_check = now
                except Exception as e:
                    self._logger.error(f"Error in scaling loop: {e}", exc_info=True)

                # Wait for next check
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            # Handle task cancellation gracefully
            self._logger.info("Scaling loop cancelled")
            raise  # Re-raise to properly handle cancellation

    async def get_job_instances(self) -> Tuple[int, int, float, str]:
        await self._initialize_pricing_info()

        try:
            running_instances = await self.list_job_instances()
        except Exception as e:
            self._logger.error(f"Failed to get running instances: {e}", exc_info=True)
            self._logger.error("Cannot make scaling decisions without instance information")
            return

        # Count the number of instances of each type and running status
        # Also count by "state" and "zone" fields
        running_instances_by_type = {}
        for instance in running_instances:
            key = (instance["type"], instance["state"], instance["zone"])
            if key not in running_instances_by_type:
                running_instances_by_type[key] = 0
            running_instances_by_type[key] += 1

        num_running = 0
        running_cpus = 0
        running_price = 0
        if len(running_instances_by_type) == 0:
            summary = "No instances found"
            return num_running, running_cpus, running_price, summary

        summary = ""
        summary += f"Running instance summary:\n"
        summary += (
            f"  State       Instance Type             vCPUs  Zone             Count  Total Price\n"
        )
        summary += (
            f"  --------------------------------------------------------------------------------\n"
        )

        sorted_keys = sorted(running_instances_by_type.keys(), key=lambda x: (x[1], x[0], x[2]))
        for type_, state, zone in sorted_keys:
            count = running_instances_by_type[(type_, state, zone)]
            instance = self._all_instance_info[type_]
            cpus = instance["vcpu"]
            try:
                price = self._pricing_info[type_][zone]["total_price"] * count
            except KeyError:
                wildcard_zone = zone[:-1] + "*"
                try:
                    price = self._pricing_info[type_][wildcard_zone]["total_price"] * count
                except KeyError:
                    self._logger.warning(
                        f"No pricing info for instance type {type_} in zone {zone}"
                    )
                    price = 0
            price_str = "N/A"
            if state in ["running", "starting"]:
                price_str = f"${price:.2f}"
                num_running += count
                running_cpus += count * cpus
                running_price += price
            summary += f"  {state:<10}  {type_:<24}  "
            summary += f"{cpus:>5}  "
            summary += f"{zone:<15}  {count:>5}  "
            summary += f"{price_str:>11}\n"

        running_price_str = f"${running_price:.2f}"
        summary += (
            f"  --------------------------------------------------------------------------------\n"
        )
        summary += f"  Total running/starting:               {running_cpus:>5} (weighted)        "
        summary += f"{num_running:>5}  {running_price_str:>11}\n"

        return num_running, running_cpus, running_price, summary

    async def _check_scaling(self) -> None:
        """Check if we need to scale up based on number of running instances."""
        self._logger.info("Checking if scaling is needed...")

        # Get current queue depth
        try:
            queue_depth = await self.task_queue.get_queue_depth()
            self._logger.debug(f"Current queue depth: {queue_depth}")
        except Exception as e:
            self._logger.error(f"Failed to get queue depth: {e}", exc_info=True)

        # Check if queue is empty
        if queue_depth == 0:
            if self._empty_queue_since is None:
                self._empty_queue_since = float(time.time())
                self._logger.info("Queue is empty, starting termination timer")
            else:
                empty_duration = float(time.time()) - self._empty_queue_since
                self._logger.info(f"Queue has been empty for {empty_duration:.1f} seconds")
        else:
            # Queue is not empty, reset timer
            if self._empty_queue_since is not None:
                self._logger.info("Queue is no longer empty, resetting termination timer")
            self._empty_queue_since = None

            # Calculate desired number of instances based on queue depth and tasks per instance
            # cpus_per_task = self._run_config.cpus_per_task or 1
            # desired_instances = int((queue_depth + cpus_per_task - 1) // cpus_per_task)
            # if self._min_instances is not None:
            #     desired_instances = max(desired_instances, self._min_instances)
            # if self._max_instances is not None:
            #     desired_instances = min(desired_instances, self._max_instances)

            num_running, running_cpus, running_price, summary = await self.get_job_instances()
            for summary_line in summary.split("\n"):
                self._logger.info(summary_line)

            # Find our budget for new instances, cpus, and $$

            if num_running > self._max_instances:  # max_instances always has a value
                self._logger.warning(
                    f"  More instances running than max allowed: {num_running} > "
                    f"{self._max_instances}"
                )
            # How many instances we can start
            available_instances = max(self._max_instances - num_running, 0)

            if self._run_config.max_total_cpus is not None:
                if running_cpus > self._run_config.max_total_cpus:
                    self._logger.warning(
                        f"  More vCPUs running than max allowed: {running_cpus} > "
                        f"{self._run_config.max_total_cpus}"
                    )
                    available_cpus = 0
                else:
                    available_cpus = self._run_config.max_total_cpus - running_cpus
            else:
                available_cpus = None

            if self._run_config.max_total_price_per_hour is not None:
                if running_price > self._run_config.max_total_price_per_hour:
                    self._logger.warning(
                        f"  More money being spent than max allowed: ${running_price:.2f} > "
                        f"${self._run_config.max_total_price_per_hour:.2f}"
                    )
                    available_price = 0
                else:
                    available_price = self._run_config.max_total_price_per_hour - running_price
            else:
                available_price = None

            self._logger.debug(
                "Available instances "
                f"{'N/A' if available_instances is None else available_instances}, "
                f"cpus {'N/A' if available_cpus is None else available_cpus}, "
                f"price {'N/A' if available_price is None else available_price}"
            )

            # Find the minimum of the three budgets - this gives us the maximum number of instances
            # we can start
            instances_to_add = available_instances
            if available_cpus is not None:
                instances_to_add = min(
                    instances_to_add, available_cpus // self._optimal_instance_info["vcpu"]
                )
            if available_price is not None:
                instances_to_add = min(
                    instances_to_add, available_price // self._optimal_instance_info["total_price"]
                )

            if instances_to_add > 0:
                # Now see if we violated the minimum constraints
                if (
                    instances_to_add < self._min_instances
                    or (
                        self._run_config.min_total_cpus is not None
                        and instances_to_add * self._optimal_instance_info["vcpu"]
                        < self._run_config.min_total_cpus
                    )
                    or (
                        self._run_config.min_total_price_per_hour is not None
                        and instances_to_add * self._optimal_instance_info["total_price"]
                        < self._run_config.min_total_price_per_hour
                    )
                ):
                    self._logger.warning(
                        f"Violated minimum constraints: Max instances we can add is "
                        f"{instances_to_add} at "
                        f"${instances_to_add * self._optimal_instance_info['total_price']:.2f}/hour, "
                        f"but minimums are {self._min_instances} instances, "
                        f"{self._run_config.min_total_cpus} vCPUs, "
                        f"${self._run_config.min_total_price_per_hour:.2f}/hour"
                    )
                else:
                    self._logger.info(
                        f"Starting {instances_to_add} new instances for an incremental price of "
                        f"${instances_to_add * self._optimal_instance_info['total_price']:.2f}/hour"
                    )
                    await self._provision_instances(instances_to_add)

    async def stop(self, terminate_instances: bool = True) -> None:
        """Stop the orchestrator and optionally terminate all instances."""
        self._logger.debug("Stopping orchestrator")
        self._running = False

        # Cancel scaling task if it exists
        if self._scaling_task is not None:
            self._scaling_task.cancel()
            try:
                await self._scaling_task
            except asyncio.CancelledError:
                pass

        if terminate_instances:
            await self.terminate_all_instances()

        # Shutdown thread pool
        self._thread_pool.shutdown(wait=True)

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
            include_non_job=False,
        )
        return instances

    async def _provision_instances(self, count: int) -> List[str]:
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
            # Define the synchronous function to run in threads
            def start_single_instance_sync():
                try:
                    # Create a new event loop for this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        # Run the async operation in the new loop
                        instance_id, zone = loop.run_until_complete(
                            self._instance_manager.start_instance(
                                instance_type=self._optimal_instance_info["name"],
                                boot_disk_size=self._optimal_instance_boot_disk_size,
                                startup_script=startup_script,
                                job_id=self._job_id,
                                use_spot=self._run_config.use_spot,
                                image=self._run_config.image,
                                zone=self._optimal_instance_info["zone"],
                            )
                        )
                        self._logger.info(
                            f"Started {'spot' if self._run_config.use_spot else 'on-demand'} "
                            f"instance '{instance_id}' in zone '{zone}'"
                        )
                        return instance_id
                    finally:
                        # Clean up the loop
                        loop.close()
                except Exception as e:
                    self._logger.error(f"Failed to start instance: {e}", exc_info=True)
                    return None

            # Create futures for all instance creations
            loop = asyncio.get_running_loop()
            futures = [
                loop.run_in_executor(self._thread_pool, start_single_instance_sync)
                for _ in range(count)
            ]

            # Wait for all futures to complete
            results = await asyncio.gather(*futures)

            # Filter out None results (failed instance creations)
            instance_ids = [instance_id for instance_id in results if instance_id is not None]

            self._logger.info(
                f"Successfully provisioned {len(instance_ids)} of {count} requested instances"
            )
            return instance_ids

    async def terminate_all_instances(self) -> None:
        """Terminate all instances associated with this job."""
        self._logger.info("Terminating all instances")

        # Define the synchronous function to terminate a single instance
        def terminate_single_instance_sync(instance):
            try:
                # Create a new event loop for this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    # Run the async operation in the new loop
                    self._logger.info(f"Terminating instance: {instance['id']}")
                    loop.run_until_complete(
                        self._instance_manager.terminate_instance(instance["id"], instance["zone"])
                    )
                    self._logger.info(f"Terminated instance: {instance['id']}")
                    return True
                finally:
                    # Clean up the loop
                    loop.close()
            except Exception as e:
                self._logger.error(
                    f"Failed to terminate instance {instance['id']}: {e}", exc_info=True
                )
                return False

        current_instances = await self.list_job_instances()
        running_instances = [i for i in current_instances if i["state"] == "running"]

        # Create futures for all instance terminations
        loop = asyncio.get_running_loop()
        futures = [
            loop.run_in_executor(self._thread_pool, terminate_single_instance_sync, instance)
            for instance in running_instances
        ]

        # Wait for all futures to complete
        results = await asyncio.gather(*futures)

        # Count successful terminations
        terminate_count = sum(1 for result in results if result)
        self._logger.info(f"Successfully terminated {terminate_count} instances")

    async def report_job_status(self) -> None:
        """
        Report the current status of the job.
        """
        await self.get_job_instances()

        queue_depth = await self.task_queue.get_queue_depth()
        self._logger.info(f"Queue depth: {queue_depth}")

    # async def check_scaling(self) -> None:
    #     """
    #     Check if we need to scale up or down based on queue depth.
    #     """
    #     self._logger.debug("Checking if scaling is needed")

    #         # If queue has been empty for a while and we have more than min_instances,
    #         # terminate excess instances
    #         if (
    #             self._empty_queue_since is not None
    #             and float(time.time()) - self._empty_queue_since
    #             > self._instance_termination_delay_seconds
    #             and total_instances > self._min_instances
    #         ):
    #             # Calculate how many instances to terminate
    #             instances_to_terminate = total_instances - self._min_instances
    #             self._logger.info(
    #                 f"Queue has been empty for {float(time.time()) - self._empty_queue_since}s, "
    #                 f"terminating {instances_to_terminate} instances"
    #             )

    #             # Create a semaphore to limit concurrent terminations
    #             semaphore = asyncio.Semaphore(self._start_instance_max_threads)

    #             # Define the function to terminate a single instance with semaphore control
    #             async def terminate_single_instance(instance):
    #                 async with semaphore:
    #                     try:
    #                         self._logger.info(f"Terminating instance: {instance['id']}")
    #                         await self._instance_manager.terminate_instance(
    #                             instance["id"], instance["zone"]
    #                         )
    #                         self._logger.info(f"Terminated instance: {instance['id']}")
    #                         return True
    #                     except Exception as e:
    #                         self._logger.error(
    #                             f"Failed to terminate instance {instance['id']}: {e}", exc_info=True
    #                         )
    #                         return False

    #             # Filter running instances and create termination tasks
    #             running_instances = [i for i in current_instances if i["state"] == "running"]
    #             instances_to_terminate = running_instances[:instances_to_terminate]
    #             tasks = [terminate_single_instance(instance) for instance in instances_to_terminate]
    #             results = await asyncio.gather(*tasks)

    #             # Count successful terminations
    #             terminate_count = sum(1 for result in results if result)
    #             self._logger.info(f"Successfully terminated {terminate_count} instances")

    #         # Scale up if needed
    #         if total_instances < desired_instances:
    #             instances_to_add = desired_instances - total_instances
    #             self._logger.info(
    #                 f"Scaling up: Adding {instances_to_add} instances (from {total_instances} to {desired_instances})"
    #             )
    #             new_price = self._optimal_instance_info["total_price"] * desired_instances
    #             total_cpus = self._optimal_instance_info["vcpu"] * desired_instances
    #             simultaneous_tasks = int(total_cpus / cpus_per_task)
    #             price_str = f"*** ESTIMATED TOTAL PRICE: ${new_price:.2f}/hour ***"
    #             hdr_str = "*" * len(price_str)
    #             self._logger.info(hdr_str)
    #             self._logger.info(price_str)
    #             self._logger.info(hdr_str)
    #             self._logger.info(
    #                 f"{total_cpus} vCPUs running {simultaneous_tasks} " "simultaneous tasks"
    #             )

    #             try:
    #                 new_instance_ids = await self.provision_instances(instances_to_add)
    #             except Exception as e:
    #                 self._logger.error(f"Failed to provision instances: {e}", exc_info=True)
    #         else:
    #             self._logger.debug(
    #                 f"No scaling needed. Current: {total_instances}, Desired: {desired_instances}"
    #             )

    #     # Update last scaling time
    #     self._last_scaling_time = float(time.time())

    # queue_depth = await orchestrator.task_queue.get_queue_depth()
    # initial_queue_depth = queue_depth

    # if initial_queue_depth == 0:
    #     logger.warning(
    #         "Queue is empty. Add tasks using the 'load_tasks' command before starting "
    #         "the pool."
    #     )

    # # We might want a way to keep this running indefinitely even if queue is empty
    # # if min_instances > 0, or have a separate command just to maintain a pool.
    # # For now, it exits if the queue becomes empty.
    # with tqdm(total=initial_queue_depth, desc="Processing tasks") as pbar:
    #     last_depth = queue_depth

    #     while queue_depth > 0:  # TODO or orchestrator.num_running_instances > 0:
    #         # Check if the orchestrator is still running
    #         if not orchestrator.running:
    #             logger.info("Orchestrator stopped, exiting monitor loop.")
    #             break

    #         await asyncio.sleep(5)  # Shorter sleep for responsiveness

    #         # Check instance health/count (optional, orchestrator loop does this)
    #         status = await orchestrator.get_job_status()
    #         running_count = status["instances"]["running"]
    #         starting_count = status["instances"]["starting"]
    #         total_instances = running_count + starting_count

    #         try:
    #             queue_depth = await orchestrator.task_queue.get_queue_depth()
    #         except Exception as q_err:
    #             logger.error(f"Error getting queue depth in monitor loop: {q_err}")
    #             # Decide how to handle this - continue, break, etc.
    #             continue  # Continue for now

    #         # Update progress bar only if queue depth decreases
    #         processed = last_depth - queue_depth
    #         if processed > 0:
    #             pbar.update(processed)
    #             last_depth = queue_depth

    #         # Print job status if verbose
    #         if args.verbose >= 2:  # Use INFO level for status updates
    #             instances_info = f"{running_count} running, {starting_count} starting"
    #             logger.info(f"Queue depth: {queue_depth}, Instances: {instances_info}")

    #         # Exit condition if queue is empty and no minimum instances required
    #         if queue_depth == 0:  # TODO and orchestrator.min_instances == 0:
    #             logger.info("Queue is empty and min_instances is 0, finishing up.")
    #             # Wait briefly for any final processing or scaling down
    #             await asyncio.sleep(orchestrator.check_interval_seconds)
    #             break

    #     # Ensure progress bar reaches 100% if there were initial tasks
    #     if initial_queue_depth > 0:
    #         pbar.update(max(0, pbar.total - pbar.n))

    # logger.info("Monitoring complete or queue is empty.")

    # # Stop orchestrator gracefully
    # logger.info("Stopping orchestrator")
    # await orchestrator.stop()

    # logger.info(f"Job {orchestrator.job_id} management finished.")
