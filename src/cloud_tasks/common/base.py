"""
Base interfaces for the multi-cloud task processing system.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class CloudProvider(ABC):
    """Base interface for cloud provider operations."""

    @abstractmethod
    async def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the cloud provider with configuration."""
        pass

    @abstractmethod
    async def validate_credentials(self) -> bool:
        """Validate that the provided credentials are valid."""
        pass


class TaskQueue(ABC):
    """Base interface for task queue operations."""

    @abstractmethod
    async def initialize(self, queue_name: str, config: Dict[str, Any]) -> None:
        """Initialize the task queue with configuration."""
        pass

    @abstractmethod
    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """Send a task to the queue."""
        pass

    @abstractmethod
    async def receive_tasks(self, max_count: int = 1, visibility_timeout_seconds: int = 30) -> List[Dict[str, Any]]:
        """Receive tasks from the queue with a visibility timeout."""
        pass

    @abstractmethod
    async def complete_task(self, task_handle: Any) -> None:
        """Mark a task as completed and remove from the queue."""
        pass

    @abstractmethod
    async def fail_task(self, task_handle: Any) -> None:
        """Mark a task as failed, allowing it to be retried."""
        pass

    @abstractmethod
    async def get_queue_depth(self) -> int:
        """Get the current depth (number of messages) in the queue."""
        pass

    @abstractmethod
    async def purge_queue(self) -> None:
        """Remove all messages from the queue."""
        pass


class InstanceManager(ABC):
    """Base interface for instance management operations."""

    @abstractmethod
    async def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the instance manager with configuration."""
        pass

    @abstractmethod
    async def list_available_instance_types(self) -> List[Dict[str, Any]]:
        """List available instance types with their specifications."""
        pass

    @abstractmethod
    async def start_instance(
        self, instance_type: str, user_data: str, tags: Dict[str, str]
    ) -> str:
        """Start a new instance and return its ID."""
        pass

    @abstractmethod
    async def terminate_instance(self, instance_id: str) -> None:
        """Terminate an instance by ID."""
        pass

    @abstractmethod
    async def list_running_instances(self, tag_filter: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """List currently running instances, optionally filtered by tags."""
        pass

    @abstractmethod
    async def get_instance_status(self, instance_id: str) -> str:
        """Get the current status of an instance."""
        pass

    @abstractmethod
    async def get_optimal_instance_type(
        self, cpu_required: int, memory_required_gb: int, disk_required_gb: int
    ) -> str:
        """Get the most cost-effective instance type that meets requirements."""
        pass