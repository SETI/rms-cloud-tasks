"""
Base interfaces for the multi-cloud task processing system.
"""

from abc import ABC, abstractmethod
from typing import Any


class CloudProvider(ABC):
    """Base interface for cloud provider operations."""

    @abstractmethod
    async def initialize(self, config: dict[str, Any]) -> None:
        """
        Initialize the cloud provider with configuration.

        Parameters:
            config: Provider configuration options (e.g. credentials, region).

        Returns:
            None.
        """
        pass

    @abstractmethod
    async def validate_credentials(self) -> bool:
        """Validate that the provided credentials are valid."""
        pass
