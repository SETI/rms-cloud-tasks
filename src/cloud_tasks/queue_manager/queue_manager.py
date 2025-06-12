from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..common.config import ProviderConfig


class QueueManager(ABC):
    """Base interface for task queue operations."""

    def __init__(
        self,
        config: Optional[ProviderConfig] = None,
        *,
        queue_name: Optional[str] = None,
        visibility_timeout: Optional[int] = None,
        exactly_once: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize the task queue with configuration."""
        pass  # pragma: no cover

    @abstractmethod
    async def send_message(self, message: Dict[str, Any], _quiet: bool = False) -> None:
        """Send a message to the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def send_task(self, task_id: str, task_data: Dict[str, Any]) -> None:
        """Send a task to the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def receive_messages(
        self, max_count: int = 1, acknowledge: bool = True
    ) -> List[Dict[str, Any]]:
        """Receive messages from the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def receive_tasks(
        self, max_count: int = 1, acknowledge: bool = True
    ) -> List[Dict[str, Any]]:
        """Receive tasks from the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def acknowledge_message(self, message_handle: Any) -> None:
        """Acknowledge a message and remove it from the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def acknowledge_task(self, task_handle: Any) -> None:
        """Acknowledge a task and remove it from the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def retry_message(self, message_handle: Any) -> None:
        """Retry a message."""
        pass  # pragma: no cover

    @abstractmethod
    async def retry_task(self, task_handle: Any) -> None:
        """Retry a task."""
        pass  # pragma: no cover

    @abstractmethod
    async def get_queue_depth(self) -> int | None:
        """Get the current depth (number of messages) in the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def purge_queue(self) -> None:
        """Remove all messages from the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def delete_queue(self) -> None:
        """Delete the queue and all associated resources."""
        pass  # pragma: no cover
