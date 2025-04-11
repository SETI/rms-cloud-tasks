from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

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
                    "use_spot": Whether to filter for spot-capable instance types

        Returns:
            Dictionary mapping instance type to a dictionary of instance type specifications::
                "name": instance type name
                "vcpu": number of vCPUs
                "mem_gb": amount of RAM in GB
                "local_ssd_gb": amount of local SSD storage in GB
                "storage_gb": amount of other storage in GB
                "architecture": architecture of the instance type
                "supports_spot": whether the instance type supports spot pricing
                "description": description of the instance type
                "url": URL to the instance type details
        """
        pass

    @abstractmethod
    async def start_instance(
        self,
        instance_type: str,
        user_data: str,
        tags: Dict[str, str],
        use_spot: bool = False,
        custom_image: Optional[str] = None,
    ) -> str:
        """
        Start a new instance and return its ID.

        Args:
            instance_type: Type of instance to start
            user_data: Startup script or user data to pass to the instance
            tags: Dictionary of tags to apply to the instance
            use_spot: Whether to use spot/preemptible instances (cheaper but can be terminated)
            custom_image: Custom image to use instead of default Ubuntu 24.04 LTS

        Returns:
            ID of the started instance
        """
        pass

    @abstractmethod
    async def terminate_instance(self, instance_id: str) -> None:
        """Terminate an instance by ID."""
        pass

    @abstractmethod
    async def list_running_instances(
        self, tag_filter: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
        """List currently running instances, optionally filtered by tags."""
        pass

    @abstractmethod
    async def get_instance_status(self, instance_id: str) -> str:
        """Get the current status of an instance."""
        pass

    @abstractmethod
    async def get_optimal_instance_type(self, constraints: Optional[Dict[str, Any]] = None) -> str:
        """
        Get the most cost-effective instance type that meets requirements.

        Args:
            constraints: Dictionary of constraints to filter instance types by. Constraints
                include::
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
            Instance type identifier for the most cost-effective option
        """
        pass

    @abstractmethod
    async def get_available_regions(self) -> Dict[str, Any]:
        """Get all available regions and their attributes."""
        pass
