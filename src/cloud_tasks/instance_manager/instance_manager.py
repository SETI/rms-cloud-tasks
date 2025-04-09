from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from cloud_tasks.common.config import ProviderConfig


class InstanceManager(ABC):
    """Base interface for instance management operations."""

    def __init__(self, config: ProviderConfig) -> None:
        """Initialize the instance manager with configuration."""
        self.config = config

    @abstractmethod
    async def get_available_instance_types(self) -> Dict[str, Dict[str, Any]]:
        """Get available instance types with their specifications."""
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
    async def get_optimal_instance_type(
        self,
        cpu_required: int,
        memory_required_gb: int,
        disk_required_gb: int,
        use_spot: bool = False,
    ) -> str:
        """
        Get the most cost-effective instance type that meets requirements.

        Args:
            cpu_required: Minimum number of vCPUs
            memory_required_gb: Minimum memory in GB
            disk_required_gb: Minimum disk space in GB
            use_spot: Whether to use spot/preemptible instances (cheaper but can be terminated)

        Returns:
            Instance type identifier for the most cost-effective option
        """
        pass

    @abstractmethod
    async def get_available_regions(self) -> Dict[str, Any]:
        """Get all available regions and their attributes."""
        pass
