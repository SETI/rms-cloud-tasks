"""
Configuration handling for the multi-cloud task processing system.
"""
import os
from typing import Any, Dict, Optional

import yaml  # type: ignore


class ConfigError(Exception):
    """Exception raised for configuration errors."""
    pass


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from a YAML file.

    Args:
        config_file: Path to the configuration file

    Returns:
        Dictionary containing the configuration

    Raises:
        ConfigError: If the file cannot be loaded or is invalid
    """
    try:
        if not os.path.exists(config_file):
            raise ConfigError(f"Configuration file not found: {config_file}")

        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

        if not isinstance(config, dict):
            raise ConfigError("Configuration file must contain a YAML dictionary")

        return config
    except yaml.YAMLError as e:
        raise ConfigError(f"Error parsing configuration file: {e}")
    except Exception as e:
        raise ConfigError(f"Error loading configuration: {e}")


def validate_cloud_config(config: Dict[str, Any], provider: str) -> None:
    """
    Validate that a cloud provider configuration has all required fields.

    Args:
        config: Configuration dictionary
        provider: Cloud provider name ('aws', 'gcp', or 'azure')

    Raises:
        ConfigError: If required fields are missing
    """
    if provider not in config:
        raise ConfigError(f"Missing configuration for cloud provider: {provider}")

    provider_config = config[provider]

    if provider == 'aws':
        required_fields = ['access_key', 'secret_key', 'region']
    elif provider == 'gcp':
        required_fields = ['project_id']
    elif provider == 'azure':
        required_fields = ['subscription_id', 'tenant_id', 'client_id', 'client_secret']
    else:
        raise ConfigError(f"Unsupported cloud provider: {provider}")

    for field in required_fields:
        if field not in provider_config:
            raise ConfigError(f"Missing required configuration field for {provider}: {field}")


def get_provider_config(config: Dict[str, Any], provider: str) -> Dict[str, Any]:
    """
    Get configuration for a specific cloud provider.

    Args:
        config: Full configuration dictionary
        provider: Cloud provider name ('aws', 'gcp', or 'azure')

    Returns:
        Configuration dictionary for the specified provider

    Raises:
        ConfigError: If provider configuration is missing
    """
    validate_cloud_config(config, provider)
    return config[provider]