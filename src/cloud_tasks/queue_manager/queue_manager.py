from abc import ABC, abstractmethod
from typing import Any

from ..common.config import ProviderConfig


class QueueManager(ABC):
    """Base interface for task queue operations."""

    def __init__(
        self,
        config: ProviderConfig | None = None,
        *,
        queue_name: str | None = None,
        visibility_timeout: int | None = None,
        exactly_once: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize the task queue with configuration."""
        pass  # pragma: no cover

    @abstractmethod
    async def send_message(self, message: dict[str, Any], _quiet: bool = False) -> None:
        """Send a message to the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def send_task(self, task_id: str, task_data: dict[str, Any]) -> None:
        """Send a task to the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def receive_messages(
        self, max_count: int = 1, acknowledge: bool = True
    ) -> list[dict[str, Any]]:
        """Receive messages from the queue."""
        pass  # pragma: no cover

    @abstractmethod
    async def receive_tasks(
        self, max_count: int = 1, acknowledge: bool = True
    ) -> list[dict[str, Any]]:
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

    async def ensure_queue_ready(self) -> None:
        """
        Ensure the queue (and topic for GCP) exists before concurrent operations.
        Call this once before starting a multi-threaded or concurrent task enqueue loop.
        No-op by default; GCP overrides to create topic and subscription.
        """
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

    @abstractmethod
    def get_max_visibility_timeout(self) -> int:
        """Get the maximum visibility timeout allowed by this queue provider.

        Returns:
            Maximum visibility timeout in seconds
        """
        pass  # pragma: no cover

    @abstractmethod
    async def extend_message_visibility(
        self, message_handle: Any, timeout: int | None = None
    ) -> None:
        """Extend the visibility timeout for a message.

        Args:
            message_handle: Message object from receive_messages or "ack_id" from receive_tasks
            timeout: New visibility timeout in seconds. If None, extends by the original timeout.
        """
        pass  # pragma: no cover
