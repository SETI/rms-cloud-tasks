"""
Task Queue Manager module and factory function
"""
from typing import cast

from .taskqueue import TaskQueue

from cloud_tasks.common.config import Config, AWSConfig, GCPConfig, AzureConfig


async def create_queue(config: Config) -> TaskQueue:
    """
    Create a TaskQueue implementation for the specified cloud provider.

    Args:
        config: Configuration

    Returns:
        A TaskQueue implementation for the specified provider

    Raises:
        ValueError: If the provider is not supported
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)

    match provider:
        case "AWS":
            # We import these here to avoid requiring the dependencies for unused providers
            from .aws import AWSSQSQueue
            queue: TaskQueue = AWSSQSQueue(cast(AWSConfig, provider_config))
        case "GCP":
            from .gcp import GCPPubSubQueue
            queue = GCPPubSubQueue(cast(GCPConfig, provider_config))
        case "AZURE":
            from .azure import AzureServiceBusQueue
            queue = AzureServiceBusQueue(cast(AzureConfig, provider_config))
        case _:
            raise ValueError(f"Unsupported queue provider: {provider}")

    return queue
