"""
Instance Orchestrator factory module.
"""

from typing import Any, Dict

from cloud_tasks.common.base import InstanceManager
from cloud_tasks.common.config import get_provider_config


async def create_instance_manager(provider: str, config: Dict[str, Any]) -> InstanceManager:
    """
    Create an InstanceManager implementation for the specified cloud provider.

    Args:
        provider: Cloud provider name ('aws', 'gcp', or 'azure')
        config: Configuration dictionary

    Returns:
        An InstanceManager implementation for the specified provider

    Raises:
        ValueError: If the provider is not supported
    """
    provider_config = get_provider_config(config, provider)

    if provider == "aws":
        from cloud_tasks.instance_orchestrator.aws import AWSEC2InstanceManager

        instance_manager: InstanceManager = AWSEC2InstanceManager()
    elif provider == "gcp":
        from cloud_tasks.instance_orchestrator.gcp import GCPComputeInstanceManager

        instance_manager = GCPComputeInstanceManager()
    elif provider == "azure":
        from cloud_tasks.instance_orchestrator.azure import AzureVMInstanceManager

        instance_manager = AzureVMInstanceManager()
    else:
        raise ValueError(f"Unsupported instance provider: {provider}")

    await instance_manager.initialize(provider_config)
    return instance_manager
