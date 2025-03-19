from abc import ABC, abstractmethod
from typing import Any, Dict, List

from cloud_tasks.common.config import ProviderConfig


class TaskQueue(ABC):
    """Base interface for task queue operations."""

    @abstractmethod
    async def __init__(self, queue_name: str, config: ProviderConfig) -> None:
        """Initialize the task queue with configuration."""
        pass

    @abstractmethod
    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """Send a task to the queue."""
        pass

    @abstractmethod
    async def receive_tasks(
        self, max_count: int = 1, visibility_timeout_seconds: int = 30
    ) -> List[Dict[str, Any]]:
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
