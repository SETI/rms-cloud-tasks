"""
Task Queue Manager module and factory function
"""

from typing import Any, cast, Optional

from .queue_manager import QueueManager

from ..common.config import Config, AWSConfig, GCPConfig, AzureConfig


async def create_queue(
    config: Optional[Config] = None,
    visibility_timeout: Optional[int] = None,
    exactly_once: Optional[bool] = None,
    **kwargs: Any,
) -> QueueManager:
    """
    Create a TaskQueue implementation for the specified cloud provider.

    Args:
        config: Configuration
        visibility_timeout: Visibility timeout (in seconds) for messages pulled from the queue;
            if a message is not completed within this time, it will be made available again for
            processing.
        exactly_once: If True, messages are guaranteed to be delivered exactly once to any
            recipient. If False, messages will be delivered at least once, but could be
            delivered multiple times. If None, use the value in the configuration.
            The exact implications of this flag vary amount providers.

    Returns:
        A TaskQueue implementation for the specified provider

    Raises:
        ValueError: If the provider is not supported
    """
    provider_config = None
    if config is None:
        provider = kwargs.get("provider")
        if provider is None:
            raise ValueError("provider argument is required when config is not given")
    else:
        provider = config.provider
        provider_config = config.get_provider_config(provider)

    match provider:
        case "AWS":
            # We import these here to avoid requiring the dependencies for unused providers
            from .aws import AWSSQSQueue

            queue: QueueManager = AWSSQSQueue(
                cast(AWSConfig, provider_config),
                visibility_timeout=visibility_timeout,
                exactly_once=exactly_once,
                **kwargs,
            )
        case "GCP":
            from .gcp import GCPPubSubQueue

            queue = GCPPubSubQueue(
                cast(GCPConfig, provider_config),
                visibility_timeout=visibility_timeout,
                exactly_once=exactly_once,
                **kwargs,
            )
        case "AZURE":  # pragma: no cover
            # TODO Implement Azure Service Bus queue
            from .azure import AzureServiceBusQueue

            queue = AzureServiceBusQueue(
                cast(AzureConfig, provider_config),
                visibility_timeout=visibility_timeout,
                exactly_once=exactly_once,
                **kwargs,
            )
        case _:  # pragma: no cover
            # Can't get here because get_provider_config() raises an error
            raise ValueError(f"Unsupported queue provider: {provider}")

    return queue
