"""
Configuration handling for the multi-cloud task processing system.
"""
import os
from typing import Any, Dict, Optional, List, Union
import yaml


class ConfigError(Exception):
    """Exception raised for configuration errors."""
    pass


class AttrDict(dict[str, Any]):
    """Implements a dictionary that allows attribute-style access to its key-value pairs.

    A dictionary subclass that exposes its keys as attributes, allowing dict items to be
    accessed using attribute notation (dict.key) in addition to the normal dictionary
    lookup (dict[key]).

    Nested dictionaries are also converted to AttrDict objects automatically.

    Parameters:
        *args: Variable length argument list passed to dict constructor.
        **kwargs: Arbitrary keyword arguments passed to dict constructor.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(AttrDict, self).__init__(*args, **kwargs)

        # Recursively convert nested dictionaries to AttrDict
        for key, value in self.items():
            if isinstance(value, dict) and not isinstance(value, AttrDict):
                self[key] = AttrDict(value)

        # Set up attribute access
        self.__dict__ = self

    def __setitem__(self, key: str, value: Any) -> None:
        """Override to handle nested dictionaries when setting items."""
        if isinstance(value, dict) and not isinstance(value, AttrDict):
            value = AttrDict(value)
        super(AttrDict, self).__setitem__(key, value)

        # Update __dict__ since self[key] may have been modified
        self.__dict__ = self


class ProviderConfig(AttrDict):
    """Provider-specific configuration with attribute-style access.

    A specialized subclass of AttrDict specifically for cloud provider configuration data.
    Contains provider-specific settings and credentials.
    """

    def __init__(self, provider_name: str, *args: Any, **kwargs: Any) -> None:
        """Initialize a provider configuration.

        Args:
            provider_name: The name of the cloud provider ('aws', 'gcp', or 'azure')
            *args: Variable length argument list passed to AttrDict constructor
            **kwargs: Arbitrary keyword arguments passed to AttrDict constructor
        """
        super(ProviderConfig, self).__init__(*args, **kwargs)
        self.provider_name = provider_name

    def validate(self) -> None:
        """Validate that the provider configuration has all required fields.

        Raises:
            ConfigError: If required fields are missing
        """
        if self.provider_name == 'aws':
            required_fields = ['access_key', 'secret_key', 'region']
        elif self.provider_name == 'gcp':
            required_fields = ['project_id']
        elif self.provider_name == 'azure':
            required_fields = ['subscription_id', 'tenant_id', 'client_id', 'client_secret']
        else:
            raise ConfigError(f"Unsupported cloud provider: {self.provider_name}")

        for field in required_fields:
            if field not in self:
                raise ConfigError(f"Missing required configuration field for {self.provider_name}: {field}")


class Config(AttrDict):
    """Configuration container with attribute-style access.

    A specialized subclass of AttrDict specifically for configuration data.
    Provides methods to access provider-specific configurations.
    """

    def get_provider(self, provider_name: str) -> ProviderConfig:
        """Get configuration for a specific cloud provider.

        Args:
            provider_name: Cloud provider name ('aws', 'gcp', or 'azure')

        Returns:
            ProviderConfig object for the specified provider

        Raises:
            ConfigError: If provider configuration is missing
        """
        if provider_name not in self:
            raise ConfigError(f"Missing configuration for cloud provider: {provider_name}")

        provider_config = self[provider_name]

        # Provider config should already be a ProviderConfig
        if not isinstance(provider_config, ProviderConfig):
            raise ConfigError(f"Provider configuration for {provider_name} is not a ProviderConfig instance")

        return provider_config


def load_config(config_file: str) -> Config:
    """
    Load configuration from a YAML file.

    Args:
        config_file: Path to the configuration file

    Returns:
        Config object containing the configuration

    Raises:
        ConfigError: If the file cannot be loaded or is invalid
    """
    try:
        if not os.path.exists(config_file):
            raise ConfigError(f"Configuration file not found: {config_file}")

        with open(config_file, 'r') as f:
            config_dict = yaml.safe_load(f)

        if not isinstance(config_dict, dict):
            raise ConfigError("Configuration file must contain a YAML dictionary")

        # Convert to Config object
        config = Config(config_dict)

        # Process provider configurations
        known_providers = ['aws', 'gcp', 'azure']
        for provider in known_providers:
            if provider in config:
                # Convert provider config to ProviderConfig object
                provider_data = config[provider]
                provider_config = ProviderConfig(provider, provider_data)

                try:
                    provider_config.validate()
                except ConfigError as e:
                    raise ConfigError(f"Error in {provider} configuration: {str(e)}")

                config[provider] = provider_config

        return config

    except yaml.YAMLError as e:
        raise ConfigError(f"Error parsing configuration file: {e}")
    except Exception as e:
        if isinstance(e, ConfigError):
            # Re-raise ConfigError with its original message
            raise
        raise ConfigError(f"Error loading configuration: {e}")


def validate_cloud_config(config: Config, provider: str) -> None:
    """
    Validate that a cloud provider configuration has all required fields.

    Args:
        config: Configuration object
        provider: Cloud provider name ('aws', 'gcp', or 'azure')

    Raises:
        ConfigError: If required fields are missing
    """
    if provider not in config:
        raise ConfigError(f"Missing configuration for cloud provider: {provider}")

    # Config should already be a validated ProviderConfig instance
    if not isinstance(config[provider], ProviderConfig):
        raise ConfigError(f"Provider configuration for {provider} is not a ProviderConfig instance")


def get_provider_config(config: Config, provider: str) -> ProviderConfig:
    """
    Get configuration for a specific cloud provider.

    Args:
        config: Full configuration object
        provider: Cloud provider name ('aws', 'gcp', or 'azure')

    Returns:
        ProviderConfig object for the specified provider

    Raises:
        ConfigError: If provider configuration is missing
    """
    if provider not in config:
        raise ConfigError(f"Missing configuration for cloud provider: {provider}")

    provider_config = config[provider]
    if not isinstance(provider_config, ProviderConfig):
        raise ConfigError(f"Provider configuration for {provider} is not a ProviderConfig instance")

    return provider_config


def get_run_config(config: Config, provider: str, cli_args: Optional[Dict[str, Any]] = None) -> Config:
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
        'cpu': 1,
        'memory_gb': 2,
        'disk_gb': 10,
        'image': 'ubuntu-2404-lts',
        'startup_script': '',
        'region': None,
    }

    # Override with global run config if present
    if 'run' in config and isinstance(config['run'], dict):
        for key in run_config:
            if key in config['run']:
                run_config[key] = config['run'][key]

    # Override with provider-specific config if present
    if provider in config:
        # Get provider config without validation since we only need run config values
        # which are not required fields
        provider_data = config[provider]
        if isinstance(provider_data, ProviderConfig):
            provider_config = provider_data
        else:
            # Create a ProviderConfig but don't validate it
            provider_config = ProviderConfig(provider, provider_data)

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
            'instance_types': 'instance_types',
            'region': 'region',
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
                elif cli_key == 'instance_types' and value:
                    new_instance_types = []
                    for str1 in value:
                        for str2 in str1.split(','):
                            for str3 in str2.split(' '):
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
            raise ConfigError(f"Startup script file not found: {file_path}")

        with open(file_path, 'r') as f:
            script = f.read()

        return script
    except Exception as e:
        raise ConfigError(f"Error loading startup script: {e}")