"""
Base interfaces for the multi-cloud task processing system.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


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
