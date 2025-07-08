from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from ..common.config import ProviderConfig


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
        self, instance_info: Dict[str, Any], constraints: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Check if an instance matches the constraints."""
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
        self, instance_info: Dict[str, Any], boot_disk_constraints: Dict[str, Any]
    ) -> float:
        """Get the boot disk size for an instance."""
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
        self, constraints: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Dict[str, Any]]:
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
        instance_types: Dict[str, Dict[str, Any]],
        *,
        use_spot: bool = False,
        boot_disk_constraints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Dict[str, float | str | None]]]:
        """
        Get the hourly price for one or more specific instance types.

        Args:
            instance_types: A dictionary mapping instance type to a dictionary of instance type
                specifications as returned by get_available_instance_types().
            use_spot: Whether to use spot pricing
            boot_disk_constraints: Dictionary of constraints used to determine the boot disk type and
                size. These are from the same config as the instance type constraints but are not
                used to filter instances.

        Returns:
            A dictionary mapping instance type to a dictionary of hourly price in USD::
                "cpu_price": Total price of CPU in USD/hour
                "per_cpu_price": Price of CPU in USD/vCPU/hour
                "mem_price": Total price of RAM in USD/hour
                "mem_per_gb_price": Price of RAM in USD/GB/hour
                "boot_disk_price": Total price of boot disk in USD/hour
                "boot_disk_per_gb_price": Price of boot disk in USD/GB/hour
                "local_ssd_price": Total price of local SSD in USD/hour
                "local_ssd_per_gb_price": Price of local SSD in USD/GB/hour
                "total_price": Total price of instance in USD/hour
                "total_price_per_cpu": Total price of instance in USD/vCPU/hour
                "zone": availability zone
            Plus the original instance type info keyed by availability zone. If any price is not
            available, it is set to None.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_optimal_instance_type(
        self, constraints: Optional[Dict[str, Any]] = None
    ) -> Dict[str, float | str | None]:
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
        boot_disk_iops: Optional[int] = None,
        boot_disk_throughput: Optional[int] = None,  # MB/s
        zone: Optional[str] = None,
    ) -> Tuple[str, str]:
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
    async def terminate_instance(self, instance_id: str, zone: Optional[str] = None) -> None:
        """Terminate an instance by ID.

        Args:
            instance_id: Instance name
            zone: The zone the instance is in; if not specified use the default zone
        """
        pass  # pragma: no cover

    @abstractmethod
    async def list_running_instances(
        self, job_id: Optional[str] = None, include_non_job: bool = False
    ) -> List[Dict[str, Any]]:
        """List currently running instances, optionally filtered by tags."""
        pass  # pragma: no cover

    @abstractmethod
    async def list_available_images(self) -> List[Dict[str, Any]]:
        """
        List available VM images.
        Returns common public OS images and the user's own custom images.

        Returns:
            List of dictionaries with image information
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_image_from_family(self, family_name: str) -> str:
        """
        Get the latest image from a specific family.

        Args:
            family_name: Image family name

        Returns:
            Image URI
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_default_image(self) -> str:
        """
        Get the latest Ubuntu 24.04 LTS image for Compute Engine.

        Returns:
            Image URI
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_available_regions(self) -> Dict[str, Any]:
        """Get all available regions and their attributes."""
        pass  # pragma: no cover
