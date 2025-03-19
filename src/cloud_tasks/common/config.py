"""
Configuration handling for the multi-cloud task processing system.
"""

import os
from typing import Any, Dict, Optional, List, Union
import yaml

from pydantic import BaseModel


class ProviderConfig(BaseModel):
    pass


class AWSConfig(ProviderConfig):
    queue_name: Optional[str] = None
    instance_types: Optional[List[str]] = None
    startup_script: Optional[str] = None
    image: Optional[str] = None
    cpu: Optional[int] = None
    memory_gb: Optional[float] = None
    disk_gb: Optional[float] = None

    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    region: Optional[str] = None


class GCPConfig(ProviderConfig):
    queue_name: Optional[str] = None
    instance_types: Optional[List[str]] = None
    startup_script: Optional[str] = None
    image: Optional[str] = None
    cpu: Optional[int] = None
    memory_gb: Optional[float] = None
    disk_gb: Optional[float] = None

    project_id: Optional[str] = None
    credentials_file: Optional[str] = None


class AzureConfig(ProviderConfig):
    queue_name: Optional[str] = None
    instance_types: Optional[List[str]] = None
    startup_script: Optional[str] = None
    image: Optional[str] = None
    cpu: Optional[int] = None
    memory_gb: Optional[float] = None
    disk_gb: Optional[float] = None

    subscription_id: Optional[str] = None
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


class Config(BaseModel):
    provider: Optional[str] = None
    aws: Optional[AWSConfig] = None
    gcp: Optional[GCPConfig] = None
    azure: Optional[AzureConfig] = None

    def get_provider_config(self, provider_name: Optional[str] = None) -> ProviderConfig:
        """Get configuration for a specific cloud provider.

        Args:
            provider_name: Cloud provider name ('aws', 'gcp', or 'azure')

        Returns:
            ProviderConfig object for the specified provider

        Raises:
            ValueError: If provider configuration is missing
        """
        if provider_name is None:
            provider_name = self.provider
        if provider_name is None:
            raise ValueError("Provider name not provided or detected in config")

        match provider_name:
            case "aws":
                provider_config = self.aws
            case "gcp":
                provider_config = self.gcp
            case "azure":
                provider_config = self.azure
            case _:
                raise ValueError(f"Unsupported provider: {provider_name}")

        if provider_config is None:
            raise ValueError(f"Provider configuration not found for {provider_name}")

        return provider_config


def load_config(config_file: str, cli_args: Optional[Dict[str, Any]] = None) -> Config:
    """
    Load configuration from a YAML file.

    Args:
        config_file: Path to the configuration file

    Returns:
        Config object containing the configuration

    Raises:
        FileNotFoundError: If the file cannot be found
        VlueError: If the file cannot be loaded or is invalid
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    with open(config_file, "r") as f:
        config_dict = yaml.safe_load(f)

    if not isinstance(config_dict, dict):
        raise ValueError("Configuration file must contain a YAML dictionary")

    # Convert to Config object
    config = Config(**config_dict)

    if cli_args is not None and cli_args.get("provider") is not None:
        config.provider = cli_args["provider"]
    if cli_args is not None and cli_args.get("queue_name"):
        if config.aws is not None:
            config.aws.queue_name = cli_args["queue_name"]
        if config.gcp is not None:
            config.gcp.queue_name = cli_args["queue_name"]
        if config.azure is not None:
            config.azure.queue_name = cli_args["queue_name"]

    return config



def get_run_config(
    config: Config, provider: str, cli_args: Optional[Dict[str, Any]] = None
) -> Config:
    """
    Get the run configuration with proper override hierarchy:
    CLI args > Provider-specific config > Global run config > Defaults

    Args:
        config: Full configuration object
        provider: Cloud provider name ('aws', 'gcp', or 'azure')
        cli_args: Command-line arguments as a dictionary (optional)

    Returns:
        Config object with merged configuration values
    """
    # Default values
    run_config = {
        "cpu": 1,
        "memory_gb": 2,
        "disk_gb": 10,
        "image": "ubuntu-2404-lts",
        "startup_script": "",
        "region": None,
    }

    # Override with CLI args if provided
    if cli_args:
        # Map CLI arg names to config names
        cli_map = {
            "cpu": "cpu",
            "memory": "memory_gb",
            "disk": "disk_gb",
            "image": "image",
            "startup_script_file": "startup_script",
            "instance_types": "instance_types",
            "region": "region",
        }

        for cli_key, config_key in cli_map.items():
            if cli_key in cli_args and cli_args[cli_key] is not None:
                value = cli_args[cli_key]

                # Special handling for startup script file
                if cli_key == "startup_script_file" and value:
                    try:
                        with open(value, "r") as f:
                            run_config[config_key] = f.read()
                    except Exception as e:
                        raise ValueError(f"Error reading startup script file {value}: {e}")
                elif cli_key == "instance_types" and value:
                    new_instance_types = []
                    for str1 in value:
                        for str2 in str1.split(","):
                            for str3 in str2.split(" "):
                                new_instance_types.append(str3.strip())
                    run_config[config_key] = new_instance_types
                else:
                    run_config[config_key] = value

    # Create Config object from the run configuration
    return Config(run_config)


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
            raise FileNotFoundError(f"Startup script file not found: {file_path}")

        with open(file_path, "r") as f:
            script = f.read()

        return script
    except Exception as e:
        raise RuntimeError(f"Error loading startup script: {e}")
