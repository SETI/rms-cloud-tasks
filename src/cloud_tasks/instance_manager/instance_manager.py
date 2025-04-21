from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from cloud_tasks.common.config import ProviderConfig


class InstanceManager(ABC):
    """Base interface for instance management operations."""

    def __init__(self, config: ProviderConfig) -> None:
        """Initialize the instance manager with configuration."""
        self.config = config

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
        pass

    @abstractmethod
    async def get_instance_pricing(
        self, instance_types: Dict[str, Dict[str, Any]], *, use_spot: bool = False
    ) -> Dict[str, Dict[str, Dict[str, float | str | None]]]:
        """
        Get the hourly price for one or more specific instance types.

        Args:
            instance_types: A dictionary mapping instance type to a dictionary of instance type
                specifications as returned by get_available_instance_types().
            use_spot: Whether to use spot pricing

        Returns:
            A dictionary mapping instance type to a dictionary of hourly price in USD::
                "cpu_price": Total price of CPU in USD/hour
                "per_cpu_price": Price of CPU in USD/vCPU/hour
                "mem_price": Total price of RAM in USD/hour
                "mem_per_gb_price": Price of RAM in USD/GB/hour
                "total_price": Total price of instance in USD/hour
                "total_price_per_cpu": Total price of instance in USD/vCPU/hour
                "zone": availability zone
            Plus the original instance type info keyed by availability zone. If any price is not
            available, it is set to None.
        """
        pass

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
        pass

    @abstractmethod
    async def start_instance(
        self,
        *,
        instance_type: str,
        boot_disk_size: int,  # GB
        startup_script: str,
        job_id: str,
        use_spot: bool,
        image: str,
        zone: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        Start a new instance and return its ID.

        Args:
            instance_type: Type of instance to start
            startup_script: The startup script
            job_id: Job ID to use for the instance
            use_spot: Whether to use a spot instance
            image: Image to use
            zone: Zone to use for the instance; if not specified use the default zone,
                or if none choose a random zone

        Returns:
            A tuple containing the ID of the started instance and the zone it was started
            in
        """
        pass

    @abstractmethod
    async def terminate_instance(self, instance_id: str, zone: Optional[str] = None) -> None:
        """Terminate an instance by ID.

        Args:
            instance_id: Instance name
            zone: The zone the instance is in; if not specified use the default zone
        """
        pass

    @abstractmethod
    async def list_running_instances(
        self, job_id: Optional[str] = None, include_non_job: bool = False
    ) -> List[Dict[str, Any]]:
        """List currently running instances, optionally filtered by tags."""
        pass

    @abstractmethod
    async def list_available_images(self) -> List[Dict[str, Any]]:
        """
        List available VM images.
        Returns common public OS images and the user's own custom images.

        Returns:
            List of dictionaries with image information
        """
        pass

    @abstractmethod
    async def get_available_regions(self) -> Dict[str, Any]:
        """Get all available regions and their attributes."""
        pass
