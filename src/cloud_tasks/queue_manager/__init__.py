"""
Task Queue Manager factory module.
"""
from typing import Any, Dict, Union

from cloud_tasks.common.base import TaskQueue
from cloud_tasks.common.config import get_provider_config


async def create_queue(provider: str, queue_name: str, config: Dict[str, Any]) -> TaskQueue:
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
    provider_config = get_provider_config(config, provider)

    if provider == 'aws':
        from cloud_tasks.queue_manager.aws import AWSSQSQueue
        queue: TaskQueue = AWSSQSQueue()
    elif provider == 'gcp':
        from cloud_tasks.queue_manager.gcp import GCPPubSubQueue
        queue = GCPPubSubQueue()
    elif provider == 'azure':
        from cloud_tasks.queue_manager.azure import AzureServiceBusQueue
        queue = AzureServiceBusQueue()
    else:
        raise ValueError(f"Unsupported queue provider: {provider}")

    await queue.initialize(queue_name, provider_config)
    return queue
