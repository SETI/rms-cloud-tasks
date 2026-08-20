"""
Task Queue Manager module and factory function
"""

from typing import Any, cast

from ..common.config import AWSConfig, AzureConfig, Config, GCPConfig
from .queue_manager import QueueManager

# Seconds added to max_runtime to derive a queue's visibility timeout. The extra time lets
# the worker notice that a task has overrun, kill it, and acknowledge or retry the message
# itself; without it the queue could redeliver the message at the same moment the worker is
# dealing with the overrun.
_VISIBILITY_TIMEOUT_MARGIN = 10


async def create_queue(
    config: Config | None = None,
    exactly_once: bool | None = None,
    **kwargs: Any,
) -> QueueManager:
    """
    Create a TaskQueue implementation for the specified cloud provider.

    The queue's visibility timeout is not settable by the caller; it is derived from
    ``config.run.max_runtime`` so that a message always stays invisible for at least as long
    as its task is allowed to run. A caller that passes no config, or a config with no
    max_runtime, gets the provider's default visibility timeout, which only matters if this
    call has to create the queue.

    Parameters:
        config: Configuration.
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
    max_runtime = None
    if config is None:
        provider = kwargs.get("provider")
        if provider is None:
            raise ValueError("provider argument is required when config is not given")
    else:
        provider = config.provider
        provider_config = config.get_provider_config(provider)
        max_runtime = config.run.max_runtime

    # The visibility timeout is never specified independently of max_runtime, because a
    # message that becomes visible again while its task is still running is handed to a
    # second worker and the task runs twice. Providers clip this to the maximum they allow
    # (GCP Pub/Sub, for example, permits no more than 600 seconds), and the worker renews the
    # visibility timeout of a running task, so a max_runtime beyond a provider's maximum is
    # still handled correctly.
    visibility_timeout = None if max_runtime is None else max_runtime + _VISIBILITY_TIMEOUT_MARGIN

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
