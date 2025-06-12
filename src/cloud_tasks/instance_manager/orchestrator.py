"""
Instance Orchestrator core module.
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

from .instance_manager import InstanceManager
from . import create_instance_manager
from ..common.config import Config
from ..queue_manager import create_queue

# Notes:
# - Instance selection constraints
# - # Instance constraints
# - Environment variables set in startup script
# - Startup script is required
# - Image is required (set default?)


class InstanceOrchestrator:
    """
    Class that manages a pool of worker instances based on queue status.
    Determines when to scale up (start new instances) and down (terminate instances).
    """

    _DEFAULT_BOOT_DISK_SIZE_PER_CPU_GB = 10

    def __init__(self, config: Config, dry_run: Optional[bool] = False):
        """
        Initialize the instance orchestrator.

        Args:
            config: Configuration object containing all settings.
        """
        self._logger = logging.getLogger(__name__)
        self._logger.debug("Initializing InstanceOrchestrator")

        self._config = config
        self._dry_run = dry_run

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

        self._task_queue = None

        # TODO: Add scale_up/down_thresholds to RunConfig?
        # self._scale_up_threshold = 10  # Default or fetch from config if added
        # self._scale_down_threshold = 2  # Default or fetch from config if added

        # Region/Location
        self._region = provider_config.region
        self._zone = provider_config.zone

        # Will be initialized in start()
        self._instance_manager: Optional[InstanceManager] = None
        self._optimal_instance_info = None
        self._optimal_instance_boot_disk_size = None
        self._optimal_instance_num_tasks = None
        self._image_uri = None
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
        self._min_instances = self._run_config.min_instances
        self._max_instances = self._run_config.max_instances
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
        if self._run_config.boot_disk_types is None:
            self._logger.info("  Boot disk types: None")
        else:
            self._logger.info(f"  Boot disk types: {', '.join(self._run_config.boot_disk_types)}")
        self._logger.info(f"  Boot disk total size: {self._run_config.total_boot_disk_size} GB")
        self._logger.info(f"  Boot disk base size: {self._run_config.boot_disk_base_size} GB")
        self._logger.info(f"  Boot disk per CPU: {self._run_config.boot_disk_per_cpu} GB")
        self._logger.info(f"  Boot disk per task: {self._run_config.boot_disk_per_task} GB")
        self._logger.info(
            f"  Local SSD: {self._run_config.min_local_ssd} to "
            f"{self._run_config.max_local_ssd} GB"
        )
        self._logger.info(
            f"  Local SSD per CPU: {self._run_config.min_local_ssd_per_cpu} to "
            f"{self._run_config.max_local_ssd_per_cpu} GB"
        )
        self._logger.info(
            f"  Local SSD per task: {self._run_config.min_local_ssd_per_task} to "
            f"{self._run_config.max_local_ssd_per_task} GB"
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
        self._logger.info(
            f"    Simultaneous tasks: {self._run_config.min_simultaneous_tasks} to "
            f"{self._run_config.max_simultaneous_tasks}"
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
            self._logger.info("  Pricing: Spot instances")
        else:
            self._logger.info("  Pricing: On-demand instances")
        self._logger.info("Miscellaneous:")
        self._logger.info(f"  Scaling check interval: {self._scaling_check_interval} seconds")
        self._logger.info(
            f"  Instance termination delay: {self._instance_termination_delay} seconds"
        )
        self._logger.info(f"  Max runtime: {self._run_config.max_runtime} seconds")
        self._logger.info(f"  Max parallel instance creations: {self._start_instance_max_threads}")
        self._logger.info(f"  Image: {self._run_config.image}")
        self._logger.info("  Startup script:")
        if self._run_config.startup_script is None:
            self._logger.info("    None")
        else:
            for line in self._run_config.startup_script.replace("\r", "").strip().split("\n"):
                self._logger.info(f"    {line}")

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
export RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE=1
export RMS_CLOUD_TASKS_INSTANCE_TYPE={self._optimal_instance_info["name"]}
export RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS={self._optimal_instance_info["vcpu"]}
export RMS_CLOUD_TASKS_INSTANCE_MEM_GB={self._optimal_instance_info["mem_gb"]}
export RMS_CLOUD_TASKS_INSTANCE_SSD_GB={self._optimal_instance_info["local_ssd_gb"]}
export RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB={self._optimal_instance_boot_disk_size}
export RMS_CLOUD_TASKS_INSTANCE_IS_SPOT={self._run_config.use_spot}
export RMS_CLOUD_TASKS_INSTANCE_PRICE={self._optimal_instance_info["total_price"]}
export RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE={self._optimal_instance_num_tasks}
export RMS_CLOUD_TASKS_MAX_RUNTIME={self._run_config.max_runtime}
export RMS_CLOUD_TASKS_RETRY_ON_EXIT={self._run_config.retry_on_exit}
export RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION={self._run_config.retry_on_exception}
"""
        if not self._run_config.startup_script:
            raise RuntimeError("No startup script provided")

        ss = self._run_config.startup_script.strip()
        ss = ss.replace("\r", "")  # Remove any Windows line endings
        if ss.startswith("#!"):
            # Insert supplement after the shebang line
            ss_lines = ss.split("\n", 1)
            if not ss_lines[0].endswith("/bash"):
                msg = "Startup script uses shell other than bash; this is not supported"
                self._logger.error(msg)
                raise RuntimeError(msg)
            ss = f"{ss_lines[0]}\n{supplement}\n{ss_lines}"
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
        if self._instance_manager is None:
            self._instance_manager = await create_instance_manager(self._config)
        if self._task_queue is None:
            self._task_queue = await create_queue(self._config)

    async def _initialize_pricing_info(self) -> None:
        """Initialize the pricing information."""
        if self._all_instance_info is None:
            # No constraints on the instance types - we want all of them so that we can analyze
            # any running instances that may already exist.
            self._all_instance_info = await self._instance_manager.get_available_instance_types()
        if self._pricing_info is None:
            # We don't want to include boot_disk_type when getting the instance prices because
            # we need the instance prices to include all possible boot disk types in case there
            # are existing running instances that uses types other than the ones currently
            # selected in the config.
            boot_disk_constraints = {
                "boot_disk_iops": self._run_config.boot_disk_iops,
                "boot_disk_throughput": self._run_config.boot_disk_throughput,
            }
            self._pricing_info = await self._instance_manager.get_instance_pricing(
                self._all_instance_info,
                use_spot=self._run_config.use_spot,
                boot_disk_constraints=boot_disk_constraints,
            )

    async def start(self) -> None:
        """Start the orchestrator.

        This initializes the instance manager and begins monitoring.
        """
        self._logger.debug(
            f"Starting InstanceOrchestrator for {self._provider} (Job ID: {self._job_id})"
        )

        await self.initialize()

        if self._run_config.startup_script is None:
            raise RuntimeError("startup_script is required")

        if self._region is None:
            raise RuntimeError("Region is required")

        # Get image - either custom or default
        image = self._run_config.image
        if image is not None:
            # If it's a full URI, use it directly
            if image.startswith("https://") or "/" in image:
                self._logger.info(f"Using image: {image}")
                self._image_uri = image
            else:
                # Assume it's a family name in one of the standard projects
                self._image_uri = await self._instance_manager.get_image_from_family(image)
                if self._image_uri is None:
                    raise RuntimeError(f'No image found for family "{image}"')
                self._logger.info(
                    f'Using most recent image from family "{image}": {self._image_uri}'
                )
        else:
            # Get default image
            self._image_uri = await self._instance_manager.get_default_image()
            if self._image_uri is None:
                raise RuntimeError("No default image found")
            self._logger.info(f"Using current default image: {self._image_uri}")

        # Get optimal instance type based on requirements from config
        optimal_instance_info = await self._instance_manager.get_optimal_instance_type(
            vars(self._run_config)
        )

        self._optimal_instance_info = optimal_instance_info

        boot_disk_type = optimal_instance_info["boot_disk_type"]

        self._logger.info(
            f"|| Selected instance type: {optimal_instance_info['name']} ({boot_disk_type}) "
            f"in {optimal_instance_info['zone']} "
            f"at ${optimal_instance_info['total_price']:.6f}/hour"
        )
        local_ssd_str = (
            f"{optimal_instance_info['local_ssd_gb']} GB local SSD"
            if optimal_instance_info["local_ssd_gb"]
            else "no local SSD"
        )
        self._logger.info(
            f"||   {optimal_instance_info['vcpu']} vCPUs, {optimal_instance_info['mem_gb']} GB RAM, "
            f"{local_ssd_str}"
        )

        # Derive the boot disk size from the constraints and the number of vCPUs in the
        # optimal instance

        boot_disk_size = optimal_instance_info["boot_disk_gb"]

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

        if self._dry_run:
            self._running = False
        else:
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
            boot_disk_type = instance["boot_disk_type"]
            if boot_disk_type is None:
                boot_disk_type = self._run_config.boot_disk_types[0]
            key = (
                instance["type"],
                boot_disk_type,
                instance["state"],
                instance["zone"],
            )
            if key not in running_instances_by_type:
                running_instances_by_type[key] = 0
            running_instances_by_type[key] += 1

        num_running = 0
        running_cpus = 0
        running_price = 0
        if len(running_instances_by_type) == 0:
            summary = "No running instances found"
            return num_running, running_cpus, running_price, summary

        summary = ""
        summary += "Running instance summary:\n"
        summary += "  State       Instance Type             Boot Disk    vCPUs  Zone             Count  Total Price\n"
        summary += "  ---------------------------------------------------------------------------------------------\n"

        sorted_keys = sorted(running_instances_by_type.keys(), key=lambda x: (x[1], x[0], x[2]))
        for type_, boot_disk_type, state, zone in sorted_keys:
            count = running_instances_by_type[(type_, boot_disk_type, state, zone)]
            instance = self._all_instance_info[type_]
            cpus = instance["vcpu"]
            try:
                price = self._pricing_info[type_][zone][boot_disk_type]["total_price"] * count
            except KeyError:
                wildcard_zone = zone[:-1] + "*"
                try:
                    price = (
                        self._pricing_info[type_][wildcard_zone][boot_disk_type]["total_price"]
                        * count
                    )
                except KeyError:
                    self._logger.warning(
                        f"No pricing info for instance type {type_} ({boot_disk_type}) in "
                        f"zone {zone}"
                    )
                    price = 0
            price_str = "N/A"
            if state in ["running", "starting"]:
                price_str = f"${price:.2f}"
                num_running += count
                running_cpus += count * cpus
                running_price += price
            summary += f"  {state:<10}  {type_:<24}  "
            summary += f"{str(boot_disk_type):<12} {cpus:>5}  "
            summary += f"{zone:<15}  {count:>5}  "
            summary += f"{price_str:>11}\n"

        running_price_str = f"${running_price:.2f}"
        summary += "  ---------------------------------------------------------------------------------------------\n"
        summary += f"  Total running/starting:                            {running_cpus:>5} "
        summary += f"(weighted)        {num_running:>5}  {running_price_str:>11}\n"

        return num_running, running_cpus, running_price, summary

    async def _check_scaling(self) -> None:
        """Check if we need to scale up based on number of running instances.

        The number of desired instances is determined from these config parameters:

        - min_instances
        - max_instances
        - min_total_cpus
        - max_total_cpus
        - cpus_per_task
        - min_simultaneous_tasks
        - max_simultaneous_tasks
        - min_total_price_per_hour
        - max_total_price_per_hour

        In each case the maximum value is used to compute the number of instances
        and then the minimum value is used to verify that other constraints did
        not limit the number of instances excessively.
        """
        if not self._running:
            return

        self._logger.info("Checking if scaling is needed...")

        if self._dry_run:
            queue_depth = 1
        else:
            queue_depth = await self._task_queue.get_queue_depth()
            if queue_depth is None:
                self._logger.error("Failed to get queue depth, assuming depth of 1")
                queue_depth = 1
            else:
                self._logger.info(f"Current queue depth: {queue_depth}")

            # Check if queue is empty
            if queue_depth == 0:
                if self._empty_queue_since is None:
                    self._empty_queue_since = float(time.time())
                    self._logger.info("Queue is empty, starting termination timer")
                else:
                    empty_duration = float(time.time()) - self._empty_queue_since
                    self._logger.info(f"Queue has been empty for {empty_duration:.1f} seconds")
                    if empty_duration > self._instance_termination_delay:
                        self._logger.info("TERMINATION TIMER EXPIRED - TERMINATING ALL INSTANCES")
                        await self.terminate_all_instances()
                        self._running = False
                return

        # Queue is not empty, reset timer
        if self._empty_queue_since is not None:
            self._logger.info("Queue is no longer empty, resetting termination timer")
        self._empty_queue_since = None

        # Calculate desired number of instances based on queue depth and tasks per instance
        cpus_per_task = self._run_config.cpus_per_task or 1
        desired_instances = int((queue_depth + cpus_per_task - 1) // cpus_per_task)
        if self._min_instances is not None:
            desired_instances = max(desired_instances, self._min_instances)
        if self._max_instances is not None:
            desired_instances = min(desired_instances, self._max_instances)

        num_running, running_cpus, running_price, summary = await self.get_job_instances()
        for summary_line in summary.split("\n"):
            self._logger.info(summary_line)

        # Find our budget for new instances, cpus, and $$

        if num_running > self._max_instances:  # max_instances always has a value
            self._logger.warning(
                f"  More instances running than max allowed: {num_running} > "
                f"{self._max_instances}"
            )

        # Find the maximum number of instances allowed by looking at
        # --max-instances and --max-simultaneous-tasks with --cpus-per-task
        max_allowed_instances = self._max_instances
        self._logger.debug(f"Initial constraint: max allowed instances: {max_allowed_instances}")

        # Derive the number of tasks per instance from the constraints and the number of vCPUs
        # in the optimal instance
        cpus_per_task = self._run_config.cpus_per_task or 1
        tasks_per_instance = self._optimal_instance_num_tasks
        tasks_running = int(running_cpus // cpus_per_task)
        if self._run_config.max_simultaneous_tasks is not None:
            if tasks_running > self._run_config.max_simultaneous_tasks:
                self._logger.warning(
                    f"  More tasks running than max allowed: {tasks_running} > "
                    f"{self._run_config.max_simultaneous_tasks}"
                )
                available_instances = 0
            else:
                # The maximum number of instances is equal to the current number of
                # instances already running plus the number of instances that can be
                # started based on the unused number of simultaneous tasks
                new_instances_from_tasks = int(
                    (self._run_config.max_simultaneous_tasks - tasks_running) / tasks_per_instance
                )
                new_max_allowed_instances = num_running + new_instances_from_tasks
                if new_max_allowed_instances < max_allowed_instances:
                    max_allowed_instances = new_max_allowed_instances
                    self._logger.debug(
                        "Reducing max allowed instances based on max_simultaneous_tasks: "
                        f"running tasks={tasks_running}, cpus_per_task={cpus_per_task}, "
                        f"new tasks_per_instance={tasks_per_instance}, "
                        f"new max allowed instances={max_allowed_instances}"
                    )

        # How many instances we can start
        available_instances = max(max_allowed_instances - num_running, 0)

        # Find the maximum number of vCPUs allowed by looking at
        # --max-total-cpus
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

        # Find the maximum amount of money allowed by looking at
        # --max-total-price-per-hour
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
            new_instances_to_add = available_cpus // self._optimal_instance_info["vcpu"]
            if new_instances_to_add < instances_to_add:
                instances_to_add = new_instances_to_add
                self._logger.debug(
                    f"Reducing instances to add based on max_total_cpus: "
                    f"available_cpus={available_cpus}, "
                    f"instances_to_add={instances_to_add}"
                )
        if available_price is not None:
            new_instances_to_add = available_price // self._optimal_instance_info["total_price"]
            if new_instances_to_add < instances_to_add:
                instances_to_add = new_instances_to_add
                self._logger.debug(
                    f"Reducing instances to add based on max_total_price_per_hour: "
                    f"available_price=${available_price:.2f}, "
                    f"instances_to_add={instances_to_add}"
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
                or (
                    self._run_config.min_simultaneous_tasks is not None
                    and tasks_running + instances_to_add * tasks_per_instance
                    < self._run_config.min_simultaneous_tasks
                )
            ):
                self._logger.warning(
                    f"Violated minimum constraints: Max instances we can add is "
                    f"{instances_to_add} at "
                    f"${instances_to_add * self._optimal_instance_info['total_price']:.2f}/hour "
                    f"giving a total of {tasks_running + instances_to_add * tasks_per_instance} "
                    f"simultaneous tasks, but minimums are {self._min_instances} instances, "
                    f"{self._run_config.min_total_cpus} vCPUs, "
                    f"${self._run_config.min_total_price_per_hour:.2f}/hour, "
                    f"{self._run_config.min_simultaneous_tasks} simultaneous tasks"
                )
            else:
                if self._dry_run:
                    self._logger.info(
                        f"Dry run mode - would start {instances_to_add} new instances for an "
                        "incremental price of "
                        f"${instances_to_add * self._optimal_instance_info['total_price']:.2f}/hour"
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

        if self._region is None:
            raise RuntimeError("Region is required")

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
            # Create a semaphore to limit concurrent instance creations
            semaphore = asyncio.Semaphore(self._start_instance_max_threads)

            # Define the synchronous function to start a single instance
            async def start_single_instance():
                async with semaphore:
                    try:
                        # Run the async operation
                        instance_id, zone = await self._instance_manager.start_instance(
                            instance_type=self._optimal_instance_info["name"],
                            boot_disk_size=self._optimal_instance_boot_disk_size,
                            boot_disk_type=self._optimal_instance_info["boot_disk_type"],
                            boot_disk_iops=self._optimal_instance_info["boot_disk_iops"],
                            boot_disk_throughput=self._optimal_instance_info[
                                "boot_disk_throughput"
                            ],
                            startup_script=startup_script,
                            job_id=self._job_id,
                            use_spot=self._run_config.use_spot,
                            image_uri=self._image_uri,
                            zone=self._optimal_instance_info["zone"],
                        )
                        self._logger.info(
                            f"Started {'spot' if self._run_config.use_spot else 'on-demand'} "
                            f"instance '{instance_id}' in zone '{zone}'"
                        )
                        return instance_id
                    except Exception as e:
                        self._logger.error(f"Failed to start instance: {e}", exc_info=True)
                        return None

            # Create tasks for all instance creations
            tasks = [start_single_instance() for _ in range(count)]

            # Wait for all tasks to complete
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

        async with self._instance_creation_lock:
            # Create a semaphore to limit concurrent terminations
            semaphore = asyncio.Semaphore(self._start_instance_max_threads)

            # Define the synchronous function to terminate a single instance
            async def terminate_single_instance(instance):
                async with semaphore:
                    try:
                        self._logger.info(f"Terminating instance: {instance['id']}")
                        await self._instance_manager.terminate_instance(
                            instance["id"], instance["zone"]
                        )
                        self._logger.info(f"Terminated instance: {instance['id']}")
                        return True
                    except Exception as e:
                        self._logger.error(
                            f"Failed to terminate instance {instance['id']}: {e}", exc_info=True
                        )
                        return False

            current_instances = await self.list_job_instances()
            running_instances = [i for i in current_instances if i["state"] == "running"]

            # Create tasks for all instance terminations
            tasks = [terminate_single_instance(instance) for instance in running_instances]

            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks)

            # Count successful terminations
            terminate_count = sum(1 for result in results if result)
            self._logger.info(f"Successfully terminated {terminate_count} instances")
