"""
Instance Orchestrator module and factory function
"""

from typing import cast

from .instance_manager import InstanceManager
from cloud_tasks.common.config import Config, AWSConfig, GCPConfig, AzureConfig


async def create_instance_manager(config: Config) -> InstanceManager:
    """
    Create an InstanceManager implementation for the specified cloud provider.

    Args:
        config: Configuration


    Returns:
        An InstanceManager implementation for the specified provider

    Raises:
        ValueError: If the provider is not supported
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)

    match provider:
        case "aws":
            # We import these here to avoid requiring the dependencies for unused providers
            from .aws import AWSEC2InstanceManager
            instance_manager: InstanceManager = AWSEC2InstanceManager(cast(AWSConfig, provider_config))
        case "gcp":
            from .gcp import GCPComputeInstanceManager
            instance_manager = GCPComputeInstanceManager(cast(GCPConfig, provider_config))
        case "azure":
            from .azure import AzureVMInstanceManager
            instance_manager = AzureVMInstanceManager(cast(AzureConfig, provider_config))
        case _:
            raise ValueError(f"Unsupported instance provider: {provider}")

    return instance_manager
