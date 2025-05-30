"""
Task Queue Manager module and factory function
"""

from typing import Any, cast, Optional

from .queue_manager import QueueManager

from cloud_tasks.common.config import Config, AWSConfig, GCPConfig, AzureConfig


async def create_queue(
    config: Optional[Config] = None, visibility_timeout: Optional[int] = 30, **kwargs: Any
) -> QueueManager:
    """
    Create a TaskQueue implementation for the specified cloud provider.

    Args:
        config: Configuration

    Returns:
        A TaskQueue implementation for the specified provider

    Raises:
        ValueError: If the provider is not supported
    """
    if config is not None:
        provider = config.provider
        provider_config = config.get_provider_config(provider)

        match provider:
            case "AWS":
                # We import these here to avoid requiring the dependencies for unused providers
                from .aws import AWSSQSQueue

                queue: QueueManager = AWSSQSQueue(
                    cast(AWSConfig, provider_config),
                    visibility_timeout=visibility_timeout,
                    **kwargs,
                )
            case "GCP":
                from .gcp import GCPPubSubQueue

                queue = GCPPubSubQueue(
                    cast(GCPConfig, provider_config),
                    visibility_timeout=visibility_timeout,
                    **kwargs,
                )
            case "AZURE":  # pragma: no cover
                # TODO Implement Azure Service Bus queue
                from .azure import AzureServiceBusQueue

                queue = AzureServiceBusQueue(
                    cast(AzureConfig, provider_config),
                    visibility_timeout=visibility_timeout,
                    **kwargs,
                )
            case _:  # pragma: no cover
                # Can't get here because get_provider_config() raises an error
                raise ValueError(f"Unsupported queue provider: {provider}")

    else:
        provider = kwargs.get("provider")
        if provider is None:
            raise ValueError("provider argument is required when config is not given")

        match provider.upper():
            case "AWS":
                from .aws import AWSSQSQueue

                queue: QueueManager = AWSSQSQueue(visibility_timeout=visibility_timeout, **kwargs)
            case "GCP":
                from .gcp import GCPPubSubQueue

                queue = GCPPubSubQueue(visibility_timeout=visibility_timeout, **kwargs)
            case "AZURE":  # pragma: no cover
                # TODO Implement Azure Service Bus queue
                from .azure import AzureServiceBusQueue

                queue = AzureServiceBusQueue(visibility_timeout=visibility_timeout, **kwargs)
            case _:
                raise ValueError(f"Unsupported queue provider: {provider}")

    return queue
