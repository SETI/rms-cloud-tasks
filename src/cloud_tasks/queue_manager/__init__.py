"""
Task Queue Manager factory module.
"""

from .aws import AWSSQSQueue
from .azure import AzureServiceBusQueue
from .gcp import GCPPubSubQueue
from .taskqueue import TaskQueue

from cloud_tasks.common.config import Config, ProviderConfig, get_provider_config


async def create_queue(queue_name: str, config: Config) -> TaskQueue:
    """
    Create a TaskQueue implementation for the specified cloud provider.

    Args:
        provider: Cloud provider name ('aws', 'gcp', or 'azure')
        queue_name: Name of the queue to create/connect to
        config: Configuration dictionary

    Returns:
        A TaskQueue implementation for the specified provider

    Raises:
        ValueError: If the provider is not supported
    """
    provider = config.provider
    provider_config = get_provider_config(config, provider)

    match provider:
        case "aws":
            queue: TaskQueue = AWSSQSQueue(queue_name, provider_config)
        case "gcp":
            queue: TaskQueue = GCPPubSubQueue(queue_name, provider_config)
        case "azure":
            queue: TaskQueue = AzureServiceBusQueue(queue_name, provider_config)
        case _:
            raise ValueError(f"Unsupported queue provider: {provider}")

    return queue
