"""
Instance Orchestrator module and factory function
"""

from typing import cast

from ..common.config import AWSConfig, Config, GCPConfig
from .instance_manager import InstanceManager


async def create_instance_manager(config: Config) -> InstanceManager:
    """
    Create an InstanceManager implementation for the specified cloud provider.

    Parameters:
        config: Configuration.

    Returns:
        An InstanceManager implementation for the specified provider

    Raises:
        ValueError: If the provider is not supported
    """
    provider = config.provider
    provider_config = config.get_provider_config(provider)

    match provider:
        case "AWS":
            # We import these here to avoid requiring the dependencies for unused providers
            from .aws import AWSEC2InstanceManager

            instance_manager: InstanceManager = AWSEC2InstanceManager(
                cast(AWSConfig, provider_config)
            )
        case "GCP":
            from .gcp import GCPComputeInstanceManager

            instance_manager = GCPComputeInstanceManager(cast(GCPConfig, provider_config))
        # case "AZURE": TODO
        #     from .azure import AzureVMInstanceManager
        #     instance_manager = AzureVMInstanceManager(cast(AzureConfig, provider_config))
        case _:  # pragma: no cover
            # Can't get here because get_provider_config() raises an error
            raise ValueError(f"Unsupported instance provider: {provider}")

    return instance_manager
