"""
Configuration handling for the multi-cloud task processing system.
"""
import os
from typing import Any, Dict, Optional, Union, List

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


def get_run_config(config: Dict[str, Any], provider: str, cli_args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Get the run configuration with proper override hierarchy:
    CLI args > Provider-specific config > Global run config > Defaults

    Args:
        config: Full configuration dictionary
        provider: Cloud provider name ('aws', 'gcp', or 'azure')
        cli_args: Command-line arguments as a dictionary (optional)

    Returns:
        Dictionary with merged configuration values
    """
    # Default values
    run_config = {
        'cpu': 1,
        'memory_gb': 2,
        'disk_gb': 10,
        'image': 'ubuntu-2404-lts',
        'startup_script': '',
    }

    # Override with global run config if present
    if 'run' in config and isinstance(config['run'], dict):
        for key in run_config:
            if key in config['run']:
                run_config[key] = config['run'][key]

    # Override with provider-specific config if present
    if provider in config and isinstance(config[provider], dict):
        provider_config = config[provider]
        for key in run_config:
            if key in provider_config:
                run_config[key] = provider_config[key]

    # Override with CLI args if provided
    if cli_args:
        # Map CLI arg names to config names
        cli_map = {
            'cpu': 'cpu',
            'memory': 'memory_gb',
            'disk': 'disk_gb',
            'image': 'image',
            'startup_script_file': 'startup_script',
        }

        for cli_key, config_key in cli_map.items():
            if cli_key in cli_args and cli_args[cli_key] is not None:
                value = cli_args[cli_key]

                # Special handling for startup script file
                if cli_key == 'startup_script_file' and value:
                    try:
                        with open(value, 'r') as f:
                            run_config[config_key] = f.read()
                    except Exception as e:
                        raise ConfigError(f"Error reading startup script file {value}: {e}")
                else:
                    run_config[config_key] = value

    return run_config


def load_startup_script(file_path: str) -> str:
    """
    Load a startup script from a file.

    Args:
        file_path: Path to the startup script file

    Returns:
        Contents of the startup script file as a string

    Raises:
        ConfigError: If the file cannot be loaded
    """
    try:
        if not os.path.exists(file_path):
            raise ConfigError(f"Startup script file not found: {file_path}")

        with open(file_path, 'r') as f:
            script = f.read()

        return script
    except Exception as e:
        raise ConfigError(f"Error loading startup script: {e}")