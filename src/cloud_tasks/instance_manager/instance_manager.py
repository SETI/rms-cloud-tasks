from abc import ABC, abstractmethod
from typing import Any

from ..common.config import ProviderConfig

# Type aliases for get_instance_pricing return structure (instance_type -> region -> zone -> pricing_info)
# PricingInfo may contain mixed metadata (prices, strings, etc.) so values are Any.
PricingInfo = dict[str, Any]
ZonePricing = dict[str, PricingInfo | None]
RegionPricing = dict[str, ZonePricing]
InstancePricingResult = dict[str, RegionPricing]


class InstanceManager(ABC):
    """Base interface for instance management operations."""

    # These rankings are valid across all providers
    _PROCESSOR_FAMILY_TO_PERFORMANCE_RANKING = {
        # Unknown/Other
        "Unknown": 0,
        "Intel": 1,  # Generic/legacy Intel, very low performance
        # Legacy/Oldest
        "Intel Nehalem": 2,  # Xeon 5500, ~2009
        "Intel Westmere": 3,  # Xeon 5600, ~2010
        "Intel Sandy Bridge": 4,  # Xeon E5-2600, ~2012
        "Intel Ivy Bridge": 5,  # Xeon E5 v2, ~2013
        "Intel Haswell": 6,  # Xeon E5 v3, ~2014
        "Intel Broadwell": 7,  # Xeon E5 v4, ~2016
        "Intel Core i7": 8,  # Mac1, ~2017
        # Early cloud ARM
        "AWS Graviton": 9,  # A1, ~2018
        # Early AMD EPYC
        "AMD Naples": 10,  # EPYC 7001, Zen 1, ~2017
        # 1st Gen Xeon Scalable
        "Intel Skylake": 11,  # Xeon Scalable 1st Gen, ~2017
        # 2nd Gen Xeon Scalable
        "Intel Cascade Lake": 12,  # Xeon Scalable 2nd Gen, ~2019
        # 2nd Gen AMD EPYC
        "AMD Rome": 13,  # EPYC 7002, Zen 2, ~2019
        # Early ARM/Apple
        "Apple M1": 14,  # Mac2, ~2020
        "Ampere Altra": 15,  # Arm Neoverse N1, ~2020
        # 3rd Gen Xeon Scalable
        "Intel Ice Lake": 16,  # Xeon Scalable 3rd Gen, ~2021
        # 3rd Gen AMD EPYC
        "AMD Milan": 17,  # EPYC 7003, Zen 3, ~2021
        # AWS Graviton2
        "AWS Graviton2": 18,  # M6g, ~2020
        # AWS Graviton3
        "AWS Graviton3": 19,  # M7g, ~2022
        # AWS Graviton3E
        "AWS Graviton3E": 20,  # HPC, ~2023
        # 4th Gen Xeon Scalable
        "Intel Sapphire Rapids": 21,  # Xeon Scalable 4th Gen, ~2023
        # 4th Gen AMD EPYC
        "AMD Genoa": 22,  # EPYC 9004, Zen 4, ~2022
        # AWS Graviton4
        "AWS Graviton4": 23,  # M8g, ~2024
        # AWS Inferentia2
        "AWS Inferentia2": 24,  # Modern AWS accelerator
        # 5th Gen Xeon Scalable
        "Intel Emerald Rapids": 25,  # Xeon Scalable 5th Gen, ~2024
        # 5th Gen AMD EPYC
        "AMD Turin": 26,  # EPYC 9005, Zen 5, ~2024 (expected)
        # Google Custom ARM
        "Google Axion": 27,  # Custom ARM, 2024 (early results)
    }

    def __init__(self, config: ProviderConfig) -> None:
        """Initialize the instance manager with configuration."""
        self.config = config

    def _instance_matches_constraints(
        self, instance_info: dict[str, Any], constraints: dict[str, Any] | None = None
    ) -> bool:
        """Check whether instance_info satisfies all provided constraints.

        Matching uses exact equality for scalar constraints (e.g. architecture)
        and numeric comparisons for min/max constraints. instance_info must
        contain keys such as "vcpu", "mem_gb", "local_ssd_gb", "architecture",
        "cpu_rank", "supports_spot". constraints may define optional keys
        (cpus_per_task, min_cpu, max_cpu, architecture, use_spot, etc.); missing
        keys in constraints are treated as "no constraint" (any value matches).

        Parameters:
            instance_info: Dict mapping instance attribute names to values.
                Required keys used by the implementation: "vcpu", "mem_gb",
                "local_ssd_gb", "architecture", "cpu_rank", "supports_spot".
                Constraint-derived values use cpus_per_task, min_cpu, max_cpu
                (from constraints) for comparisons.
            constraints: Optional dict defining expected key->value pairs or
                min/max predicates. None means match all instances.

        Returns:
            True if instance_info satisfies all constraints; False otherwise.
            If constraints is None, returns True.

        Raises:
            TypeError: If instance_info or constraints have wrong types (e.g.
                non-dict). KeyError may be raised if instance_info is missing
                required keys used in the implementation.
        """
        if constraints is None:
            return True

        cpus_per_task = constraints.get("cpus_per_task")
        if cpus_per_task is None:
            cpus_per_task = 1
        min_tasks_per_instance = constraints.get("min_tasks_per_instance")
        max_tasks_per_instance = constraints.get("max_tasks_per_instance")

        # Derive min/max_cpu from cpus_per_task and min/max_tasks_per_instance
        # if needed
        min_cpu = constraints.get("min_cpu")
        max_cpu = constraints.get("max_cpu")

        if min_tasks_per_instance is not None:
            min_cpu_from_tasks = cpus_per_task * min_tasks_per_instance
            if min_cpu is None:
                min_cpu = min_cpu_from_tasks
            else:
                min_cpu = max(min_cpu, min_cpu_from_tasks)
        if max_tasks_per_instance is not None:
            max_cpu_from_tasks = cpus_per_task * max_tasks_per_instance
            if max_cpu is None:
                max_cpu = max_cpu_from_tasks
            else:
                max_cpu = min(max_cpu, max_cpu_from_tasks)

        num_cpus = instance_info["vcpu"]
        memory_per_cpu = instance_info["mem_gb"] / num_cpus
        memory_per_task = memory_per_cpu * cpus_per_task

        local_ssd_base_size = constraints.get("local_ssd_base_size")
        if local_ssd_base_size is None:
            local_ssd_base_size = 0
        local_ssd_per_cpu = (instance_info["local_ssd_gb"] - local_ssd_base_size) / num_cpus
        local_ssd_per_task = local_ssd_per_cpu * cpus_per_task

        return (
            (
                constraints.get("architecture") is None
                or instance_info["architecture"] == constraints["architecture"]
            )
            and (
                constraints.get("min_cpu_rank") is None
                or instance_info["cpu_rank"] >= constraints["min_cpu_rank"]
            )
            and (
                constraints.get("max_cpu_rank") is None
                or instance_info["cpu_rank"] <= constraints["max_cpu_rank"]
            )
            and (min_cpu is None or instance_info["vcpu"] >= min_cpu)
            and (max_cpu is None or instance_info["vcpu"] <= max_cpu)
            and (
                constraints.get("min_total_memory") is None
                or instance_info["mem_gb"] >= constraints["min_total_memory"]
            )
            and (
                constraints.get("max_total_memory") is None
                or instance_info["mem_gb"] <= constraints["max_total_memory"]
            )
            and (
                constraints.get("min_memory_per_cpu") is None
                or memory_per_cpu >= constraints["min_memory_per_cpu"]
            )
            and (
                constraints.get("max_memory_per_cpu") is None
                or memory_per_cpu <= constraints["max_memory_per_cpu"]
            )
            and (
                constraints.get("min_memory_per_task") is None
                or memory_per_task >= constraints["min_memory_per_task"]
            )
            and (
                constraints.get("max_memory_per_task") is None
                or memory_per_task <= constraints["max_memory_per_task"]
            )
            and (
                constraints.get("min_local_ssd") is None
                or instance_info["local_ssd_gb"] >= constraints["min_local_ssd"]
            )
            and (
                constraints.get("max_local_ssd") is None
                or instance_info["local_ssd_gb"] <= constraints["max_local_ssd"]
            )
            and (
                constraints.get("min_local_ssd_per_cpu") is None
                or local_ssd_per_cpu >= constraints["min_local_ssd_per_cpu"]
            )
            and (
                constraints.get("max_local_ssd_per_cpu") is None
                or local_ssd_per_cpu <= constraints["max_local_ssd_per_cpu"]
            )
            and (
                constraints.get("min_local_ssd_per_task") is None
                or local_ssd_per_task >= constraints["min_local_ssd_per_task"]
            )
            and (
                constraints.get("max_local_ssd_per_task") is None
                or local_ssd_per_task <= constraints["max_local_ssd_per_task"]
            )
            and (
                "use_spot" not in constraints
                or constraints["use_spot"] is None
                or instance_info["supports_spot"]
            )
        )

    def _get_boot_disk_size(
        self, instance_info: dict[str, Any], boot_disk_constraints: dict[str, Any]
    ) -> float:
        """Compute boot disk size in GB from instance and constraint settings.

        Missing constraint keys boot_disk_base_size, boot_disk_per_cpu, and
        boot_disk_per_task are treated as 0. total_boot_disk_size (minimum
        floor) defaults to 10 GB if missing. cpus_per_task defaults to 1.
        Formula: boot_disk_from_cpus = boot_disk_base_size + boot_disk_per_cpu
        * num_cpus; boot_disk_from_tasks = boot_disk_base_size +
        boot_disk_per_task * tasks_per_instance. The returned size is
        max(total_boot_disk_size, boot_disk_from_cpus, boot_disk_from_tasks),
        so the effective minimum is 10 GB. No exception is raised for missing
        keys.

        Parameters:
            instance_info: dict[str, Any] – instance attributes; "vcpu" is
                used for per-cpu/per-task sizing.
            boot_disk_constraints: dict[str, Any] – keys read: boot_disk_base_size
                (numeric, default 0), boot_disk_per_cpu (numeric, default 0),
                boot_disk_per_task (numeric, default 0), total_boot_disk_size
                (numeric, default 10), cpus_per_task (int, default 1).

        Returns:
            float: Computed boot disk size in gigabytes (minimum 10 GB). No
            exception is raised for missing keys.
        """
        boot_disk_base_size = boot_disk_constraints.get("boot_disk_base_size")
        if boot_disk_base_size is None:
            boot_disk_base_size = 0
        boot_disk_per_cpu = boot_disk_constraints.get("boot_disk_per_cpu")
        if boot_disk_per_cpu is None:
            boot_disk_per_cpu = 0
        boot_disk_per_task = boot_disk_constraints.get("boot_disk_per_task")
        if boot_disk_per_task is None:
            boot_disk_per_task = 0
        num_cpus = instance_info["vcpu"]
        cpus_per_task = boot_disk_constraints.get("cpus_per_task")
        if cpus_per_task is None:
            cpus_per_task = 1
        tasks_per_instance = num_cpus // cpus_per_task

        boot_disk = boot_disk_constraints.get("total_boot_disk_size")
        if boot_disk is None:
            boot_disk = 10  # TODO Default is for GCP
        boot_disk_from_cpus = boot_disk_base_size + boot_disk_per_cpu * num_cpus
        boot_disk_from_tasks = boot_disk_base_size + boot_disk_per_task * tasks_per_instance

        return max(boot_disk, boot_disk_from_cpus, boot_disk_from_tasks)

    @abstractmethod
    async def get_available_instance_types(
        self, constraints: dict[str, Any] | None = None
    ) -> dict[str, dict[str, Any]]:
        """Get available instance types with their specifications.


        Args:
            constraints: Dictionary of constraints to filter instance types by. Constraints
                include::
                    "instance_types": List of regex patterns to filter instance types by name
                    "architecture": Architecture (X86_64 or ARM64)
                    "min_cpu": Minimum number of vCPUs
                    "max_cpu": Maximum number of vCPUs
                    "min_total_memory": Minimum total memory in GB
                    "max_total_memory": Maximum total memory in GB
                    "min_memory_per_cpu": Minimum memory per vCPU in GB
                    "max_memory_per_cpu": Maximum memory per vCPU in GB
                    "min_local_ssd": Minimum amount of local SSD storage in GB
                    "max_local_ssd": Maximum amount of local SSD storage in GB
                    "min_local_ssd_per_cpu": Minimum amount of local SSD storage per vCPU
                    "max_local_ssd_per_cpu": Maximum amount of local SSD storage per vCPU
                    "min_boot_disk": Minimum amount of boot disk storage in GB
                    "max_boot_disk": Maximum amount of boot disk storage in GB
                    "min_boot_disk_per_cpu": Minimum amount of boot disk storage per vCPU
                    "max_boot_disk_per_cpu": Maximum amount of boot disk storage per vCPU
                    "use_spot": Whether to filter for spot-capable instance types

        Returns:
            Dictionary mapping instance type to a dictionary of instance type specifications::
                "name": instance type name
                "vcpu": number of vCPUs
                "mem_gb": amount of RAM in GB
                "local_ssd_gb": amount of local SSD storage in GB
                "boot_disk_gb": amount of boot disk storage in GB
                "architecture": architecture of the instance type
                "supports_spot": whether the instance type supports spot pricing
                "description": description of the instance type
                "url": URL to the instance type details
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_instance_pricing(
        self,
        instance_types: dict[str, dict[str, Any]],
        *,
        use_spot: bool = False,
        boot_disk_constraints: dict[str, Any] | None = None,
    ) -> InstancePricingResult:
        """
        Get the hourly price for one or more specific instance types.

        Parameters:
            instance_types: A dictionary mapping instance type to a dictionary of instance type
                specifications as returned by get_available_instance_types().
            use_spot: Whether to use spot pricing.
            boot_disk_constraints: Dictionary of constraints used to determine the boot disk type
                and size. These are from the same config as the instance type constraints but are
                not used to filter instances.

        Returns:
            InstancePricingResult: A mapping of instance_type -> region -> zone -> PricingInfo.
            Each instance_type maps to regions; each region maps to zones; each zone maps to a
            PricingInfo dict with keys such as "cpu_price", "per_cpu_price", "mem_price",
            "mem_per_gb_price", "boot_disk_price", "boot_disk_per_gb_price", "local_ssd_price",
            "local_ssd_per_gb_price", "total_price", "total_price_per_cpu", "zone" (and possibly
            instance type info). All numeric values are in USD per hour (or per GB/hour where
            applicable). If any price is not available, it is set to None.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_optimal_instance_type(
        self, constraints: dict[str, Any] | None = None
    ) -> dict[str, float | str | None]:
        """
        Get the most cost-effective instance type that meets the constraints.

        Args:
            constraints: Dictionary of constraints to filter instance types by. Constraints
                include::
                    "instance_types": List of regex patterns to filter instance types by name
                    "architecture": Architecture (X86_64 or ARM64)
                    "min_cpu": Minimum number of vCPUs
                    "max_cpu": Maximum number of vCPUs
                    "min_total_memory": Minimum total memory in GB
                    "max_total_memory": Maximum total memory in GB
                    "min_memory_per_cpu": Minimum memory per vCPU in GB
                    "max_memory_per_cpu": Maximum memory per vCPU in GB
                    "min_local_ssd": Minimum amount of local SSD storage in GB
                    "max_local_ssd": Maximum amount of local SSD storage in GB
                    "min_local_ssd_per_cpu": Minimum amount of local SSD storage per vCPU
                    "max_local_ssd_per_cpu": Maximum amount of local SSD storage per vCPU
                    "min_storage": Minimum amount of other storage in GB
                    "max_storage": Maximum amount of other storage in GB
                    "min_storage_per_cpu": Minimum amount of other storage per vCPU
                    "max_storage_per_cpu": Maximum amount of other storage per vCPU
                    "use_spot": Whether to use spot instances

        Returns:
            Tuple of:
                - GCP instance type name (e.g., 'n1-standard-2')
                - Zone in which the instance type is cheapest
                - Price of the instance type in USD/hour
        """
        pass  # pragma: no cover

    @abstractmethod
    async def start_instance(
        self,
        *,
        instance_type: str,
        startup_script: str,
        job_id: str,
        use_spot: bool,
        image_uri: str,
        boot_disk_type: str,
        boot_disk_size: int,  # GB
        boot_disk_iops: int | None = None,
        boot_disk_throughput: int | None = None,  # MB/s
        zone: str | None = None,
    ) -> tuple[str, str]:
        """
        Start a new instance and return its ID.

        Args:
            instance_type: Type of instance to start
            startup_script: The startup script
            job_id: Job ID to use for the instance
            use_spot: Whether to use a spot instance
            image_uri: Image URI to use
            zone: Zone to use for the instance; if not specified use the default zone,
                or if none choose a random zone

        Returns:
            A tuple containing the ID of the started instance and the zone it was started
            in
        """
        pass  # pragma: no cover

    @abstractmethod
    async def terminate_instance(self, instance_id: str, zone: str | None = None) -> None:
        """Terminate an instance by ID.

        Args:
            instance_id: Instance name
            zone: The zone the instance is in; if not specified use the default zone
        """
        pass  # pragma: no cover

    @abstractmethod
    async def list_running_instances(
        self, job_id: str | None = None, include_non_job: bool = False
    ) -> list[dict[str, Any]]:
        """List currently running instances, optionally filtered by job.

        "Running" means instances that are in a runnable state (e.g. RUNNING
        or equivalent provider status). Filtering: if job_id is set, only
        instances associated with that job are included unless include_non_job
        is True, in which case instances not tied to any job may also be
        included. Ordering of the list is implementation-defined.

        Parameters:
            job_id: Optional str used to filter instances by associated job.
            include_non_job: If True, include instances not tied to any job.

        Returns:
            list[dict[str, Any]]: Each dict has at least: id (str), state (str),
            tags (list[str]), creation_time (str | datetime), zone (str), type
            (str). Optional key job_id (str | None). Additional provider-specific
            metadata keys (e.g. boot_disk_type, private_ip) may be present.

        Raises:
            Provider-specific exceptions on API or credential errors.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def list_available_images(self) -> list[dict[str, Any]]:
        """
        List available VM images.
        Returns common public OS images and the user's own custom images.

        Returns:
            List of dictionaries with image information
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_image_from_family(self, family_name: str) -> str | None:
        """
        Get the latest image from a specific family.

        Args:
            family_name: Image family name

        Returns:
            Image URI
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_default_image(self) -> str | None:
        """
        Get the latest Ubuntu 24.04 LTS image for Compute Engine.

        Returns:
            Image URI
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_available_regions(self, prefix: str | None = None) -> dict[str, Any]:
        """Get all available regions and their attributes.

        Parameters:
            prefix: Optional filter; if given, return only regions whose name or
                identifier starts with this string. None means no filtering.

        Returns:
            dict[str, Any]: Mapping of region identifiers (or names) to their
            attribute dicts (e.g. availability, zones, metadata). Value types
            depend on the provider.
        """
        pass  # pragma: no cover
