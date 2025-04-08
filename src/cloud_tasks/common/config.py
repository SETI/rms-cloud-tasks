"""
Configuration handling for the multi-cloud task processing system.
"""

import os
from typing import Any, Dict, Optional, List, Literal, Union
import yaml

from pydantic import BaseModel, NonNegativeFloat, NonNegativeInt,constr, Field


class ProviderConfig(BaseModel):
    pass


class RunConfig(BaseModel, validate_assignment = True):
    # Memory and disk are in GB
    min_cpu: Optional[NonNegativeInt] = None
    max_cpu: Optional[NonNegativeInt] = None
    min_total_memory: Optional[NonNegativeFloat] = None
    max_total_memory: Optional[NonNegativeFloat] = None
    min_memory_per_cpu: Optional[NonNegativeFloat] = None
    max_memory_per_cpu: Optional[NonNegativeFloat] = None
    min_disk: Optional[NonNegativeFloat] = None
    max_disk: Optional[NonNegativeFloat] = None
    min_disk_per_cpu: Optional[NonNegativeFloat] = None
    max_disk_per_cpu: Optional[NonNegativeFloat] = None
    instance_types: Optional[List[str] | str] = None  # Overriden by provider config
    use_spot: Optional[bool] = None
    startup_script: Optional[str] = None
    startup_script_file: Optional[str] = None
    image: Optional[str] = None
    cpus_per_task: Optional[NonNegativeFloat] = None


class AWSConfig(ProviderConfig, validate_assignment = True):
    job_id: Optional[constr(min_length=1)] = None
    queue_name: Optional[constr(min_length=1)] = None
    instance_types: Optional[List[str] | str] = None
    startup_script: Optional[str] = None
    startup_script_file: Optional[constr(min_length=1)] = None
    image: Optional[constr(min_length=1)] = None

    access_key: Optional[constr(min_length=1)] = None
    secret_key: Optional[constr(min_length=1)] = None
    region: Optional[constr(min_length=1)] = None


class GCPConfig(ProviderConfig, validate_assignment = True):
    job_id: Optional[constr(min_length=1)] = None
    queue_name: Optional[constr(min_length=1)] = None
    instance_types: Optional[List[str] | str] = None
    startup_script: Optional[constr(min_length=1)] = None
    startup_script_file: Optional[constr(min_length=1)] = None
    image: Optional[constr(min_length=1)] = None

    project_id: Optional[constr(min_length=1)] = None
    region: Optional[constr(min_length=1)] = None
    zone: Optional[constr(min_length=1)] = None
    credentials_file: Optional[constr(min_length=1)] = None


class AzureConfig(ProviderConfig, validate_assignment = True):
    job_id: Optional[constr(min_length=1)] = None
    queue_name: Optional[constr(min_length=1)] = None
    instance_types: Optional[List[str] | str] = None
    startup_script: Optional[constr(min_length=1)] = None
    startup_script_file: Optional[constr(min_length=1)] = None
    image: Optional[constr(min_length=1)] = None

    subscription_id: Optional[str] = None
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


class Config(BaseModel, validate_assignment = True):
    provider: Optional[Literal["aws", "gcp", "azure"]] = None
    aws: Optional[AWSConfig] = None
    gcp: Optional[GCPConfig] = None
    azure: Optional[AzureConfig] = None
    run: Optional[RunConfig] = None

    def overload_from_cli(self, cli_args: Optional[Dict[str, Any]] = None) -> None:
        """Overload Config object with command line arguments.

        Args:
            cli_args: Command line arguments as a dictionary
        """
        # Override loaded file and/or defaults with command line arguments
        if cli_args is not None:
            for attr_name in vars(self):
                if attr_name in cli_args and cli_args[attr_name] is not None:
                    setattr(self, attr_name, cli_args[attr_name])
            for attr_name in vars(self.run):
                if attr_name in cli_args and cli_args[attr_name] is not None:
                    setattr(self.run, attr_name, cli_args[attr_name])
            if self.aws is not None:
                for attr_name in vars(self.aws):
                    if attr_name in cli_args and cli_args[attr_name] is not None:
                        setattr(self.aws, attr_name, cli_args[attr_name])
            if self.gcp is not None:
                for attr_name in vars(self.gcp):
                    if attr_name in cli_args and cli_args[attr_name] is not None:
                        setattr(self.gcp, attr_name, cli_args[attr_name])
            if self.azure is not None:
                for attr_name in vars(self.azure):
                    if attr_name in cli_args and cli_args[attr_name] is not None:
                        setattr(self.azure, attr_name, cli_args[attr_name])

    def update_run_config_from_provider_config(self) -> None:
        """Update run config with provider-specific config values."""
        match self.provider:
            case "aws":
                if self.aws.instance_types is not None:
                    self.run.instance_types = self.aws.instance_types
                if self.aws.startup_script is not None:
                    self.run.startup_script = self.aws.startup_script
                if self.aws.startup_script_file is not None:
                    self.run.startup_script_file = self.aws.startup_script_file
                if self.aws.image is not None:
                    self.run.image = self.aws.image
            case "gcp":
                if self.gcp.instance_types is not None:
                    self.run.instance_types = self.gcp.instance_types
                if self.gcp.startup_script is not None:
                    self.run.startup_script = self.gcp.startup_script
                if self.gcp.startup_script_file is not None:
                    self.run.startup_script_file = self.gcp.startup_script_file
                if self.gcp.image is not None:
                    self.run.image = self.gcp.image
            case "azure":
                if self.azure.instance_types is not None:
                    self.run.instance_types = self.azure.instance_types
                if self.azure.startup_script is not None:
                    self.run.startup_script = self.azure.startup_script
                if self.azure.startup_script_file is not None:
                    self.run.startup_script_file = self.azure.startup_script_file
                if self.azure.image is not None:
                    self.run.image = self.azure.image
            case None:
                raise ValueError("Provider must be specified")
            case _:
                raise ValueError(f"Unsupported provider: {self.provider}")

        if self.run.startup_script is not None and self.run.startup_script_file is not None:
            raise ValueError("Startup script and startup script file cannot both be provided")
        if self.run.startup_script_file is not None:
            if not os.path.exists(self.run.startup_script_file):
                raise FileNotFoundError(f"Startup script file not found: {self.run.startup_script_file}")

            with open(self.run.startup_script_file, "r") as f:
                self.run.startup_script = f.read()

    def validate_config(self) -> None:
        """Perform final validation of the configuration."""
        if self.provider is None:
            raise ValueError("Provider must be specified")

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

        if provider_config.queue_name is None:
            job_id = provider_config.job_id
            if job_id is not None:
                provider_config.queue_name = f"rms-cloud-run-{job_id}"

        return provider_config


def load_config(config_file: str) -> Config:
    """Load configuration from a YAML file.

    Args:
        config_file: Path to the configuration file

    Returns:
        Config object containing the configuration

    Raises:
        FileNotFoundError: If the file cannot be found
        ValueError: If the file cannot be loaded or is invalid
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    with open(config_file, "r") as f:
        config_dict = yaml.safe_load(f)

    if not isinstance(config_dict, dict):
        raise ValueError("Configuration file must contain a YAML dictionary")

    # This is annoying, but we do it so that the user doesn't have to specify all the sections
    # in the config file but later we actually have objects to manipulate.
    if "aws" not in config_dict:
        config_dict["aws"] = {}
    if "gcp" not in config_dict:
        config_dict["gcp"] = {}
    if "azure" not in config_dict:
        config_dict["azure"] = {}
    if "run" not in config_dict:
        config_dict["run"] = {}

    # Convert to Config object
    config = Config(**config_dict)

    return config
