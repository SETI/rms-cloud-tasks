"""
Configuration handling for the multi-cloud task processing system.
"""

import os
from typing import Any, Dict, Optional, List, Literal, Union
import yaml

from pydantic import (
    BaseModel,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    constr,
    model_validator,
)


class ProviderConfig(BaseModel):
    """Config options valid for all cloud providers"""

    # regex up to 23 repetitions of [-a-z0-9]
    job_id: Optional[constr(min_length=1, max_length=24, pattern="^[a-z][-a-z0-9]{0,23}$")] = None

    queue_name: Optional[constr(min_length=1, max_length=24, pattern="^[a-z][-a-z0-9]{0,23}$")] = (
        None
    )
    instance_types: Optional[List[str] | str] = None
    startup_script: Optional[str] = None
    startup_script_file: Optional[constr(min_length=1)] = None
    image: Optional[constr(min_length=1)] = None

    region: Optional[constr(min_length=1)] = None
    zone: Optional[constr(min_length=1)] = None


class RunConfig(BaseModel, validate_assignment=True):
    """Config options for selecting instances and running tasks"""

    #
    # Constraints on number of instances
    #
    min_instances: Optional[NonNegativeInt] = None
    max_instances: Optional[PositiveInt] = None

    @model_validator(mode="after")
    def validate_min_max_instances(self) -> "RunConfig":
        if self.min_instances is not None and self.max_instances is not None:
            if self.min_instances > self.max_instances:
                raise ValueError("min_instances must be less than max_instances")
        return self

    min_total_cpus: Optional[NonNegativeInt] = None
    max_total_cpus: Optional[PositiveInt] = None

    @model_validator(mode="after")
    def validate_min_max_total_cpus(self) -> "RunConfig":
        if self.min_total_cpus is not None and self.max_total_cpus is not None:
            if self.min_total_cpus > self.max_total_cpus:
                raise ValueError("min_total_cpus must be less than max_total_cpus")
        return self

    cpus_per_task: Optional[NonNegativeFloat] = None
    min_tasks_per_instance: Optional[PositiveInt] = None
    max_tasks_per_instance: Optional[PositiveInt] = None

    @model_validator(mode="after")
    def validate_min_max_tasks_per_instance(self) -> "RunConfig":
        if self.min_tasks_per_instance is not None and self.max_tasks_per_instance is not None:
            if self.min_tasks_per_instance > self.max_tasks_per_instance:
                raise ValueError("min_tasks_per_instance must be less than max_tasks_per_instance")
        return self

    min_total_price_per_hour: Optional[NonNegativeFloat] = None
    max_total_price_per_hour: Optional[NonNegativeFloat] = None

    @model_validator(mode="after")
    def validate_min_max_total_price_per_hour(self) -> "RunConfig":
        if self.min_total_price_per_hour is not None and self.max_total_price_per_hour is not None:
            if self.min_total_price_per_hour > self.max_total_price_per_hour:
                raise ValueError(
                    "min_total_price_per_hour must be less than max_total_price_per_hour"
                )
        if self.max_total_price_per_hour is not None and self.max_total_price_per_hour <= 0:
            raise ValueError("max_total_price_per_hour must be greater than 0")
        return self

    #
    # Constraints on instance attributes
    #
    # Memory and disk are in GB
    architecture: Optional[Literal["x86_64", "arm64", "X86_64", "ARM64"]] = "X86_64"
    min_cpu: Optional[NonNegativeInt] = None
    max_cpu: Optional[PositiveInt] = None

    @model_validator(mode="after")
    def validate_min_max_cpu(self) -> "RunConfig":
        if self.min_cpu is not None and self.max_cpu is not None:
            if self.min_cpu > self.max_cpu:
                raise ValueError("min_cpu must be less than max_cpu")
        return self

    min_total_memory: Optional[NonNegativeFloat] = None
    max_total_memory: Optional[NonNegativeFloat] = None

    @model_validator(mode="after")
    def validate_min_max_total_memory(self) -> "RunConfig":
        if self.min_total_memory is not None and self.max_total_memory is not None:
            if self.min_total_memory > self.max_total_memory:
                raise ValueError("min_total_memory must be less than max_total_memory")
        if self.max_total_memory is not None and self.max_total_memory <= 0:
            raise ValueError("max_total_memory must be greater than 0")
        return self

    min_memory_per_cpu: Optional[NonNegativeFloat] = None
    max_memory_per_cpu: Optional[NonNegativeFloat] = None

    @model_validator(mode="after")
    def validate_min_max_memory_per_cpu(self) -> "RunConfig":
        if self.min_memory_per_cpu is not None and self.max_memory_per_cpu is not None:
            if self.min_memory_per_cpu > self.max_memory_per_cpu:
                raise ValueError("min_memory_per_cpu must be less than max_memory_per_cpu")
        if self.max_memory_per_cpu is not None and self.max_memory_per_cpu <= 0:
            raise ValueError("max_memory_per_cpu must be greater than 0")
        return self

    min_local_ssd: Optional[NonNegativeFloat] = None
    max_local_ssd: Optional[NonNegativeFloat] = None

    @model_validator(mode="after")
    def validate_min_max_local_ssd(self) -> "RunConfig":
        if self.min_local_ssd is not None and self.max_local_ssd is not None:
            if self.min_local_ssd > self.max_local_ssd:
                raise ValueError("min_local_ssd must be less than max_local_ssd")
        if self.max_local_ssd is not None and self.max_local_ssd <= 0:
            raise ValueError("max_local_ssd must be greater than 0")
        return self

    min_local_ssd_per_cpu: Optional[NonNegativeFloat] = None
    max_local_ssd_per_cpu: Optional[NonNegativeFloat] = None

    @model_validator(mode="after")
    def validate_min_max_local_ssd_per_cpu(self) -> "RunConfig":
        if self.min_local_ssd_per_cpu is not None and self.max_local_ssd_per_cpu is not None:
            if self.min_local_ssd_per_cpu > self.max_local_ssd_per_cpu:
                raise ValueError("min_local_ssd_per_cpu must be less than max_local_ssd_per_cpu")
        if self.max_local_ssd_per_cpu is not None and self.max_local_ssd_per_cpu <= 0:
            raise ValueError("max_local_ssd_per_cpu must be greater than 0")
        return self

    min_boot_disk: Optional[PositiveFloat] = None
    max_boot_disk: Optional[PositiveFloat] = None

    @model_validator(mode="after")
    def validate_min_max_boot_disk(self) -> "RunConfig":
        if self.min_boot_disk is not None and self.max_boot_disk is not None:
            if self.min_boot_disk > self.max_boot_disk:
                raise ValueError("min_boot_disk must be less than max_boot_disk")
        return self

    min_boot_disk_per_cpu: Optional[PositiveFloat] = None
    max_boot_disk_per_cpu: Optional[PositiveFloat] = None

    @model_validator(mode="after")
    def validate_min_max_boot_disk_per_cpu(self) -> "RunConfig":
        if self.min_boot_disk_per_cpu is not None and self.max_boot_disk_per_cpu is not None:
            if self.min_boot_disk_per_cpu > self.max_boot_disk_per_cpu:
                raise ValueError("min_boot_disk_per_cpu must be less than max_boot_disk_per_cpu")
        return self

    instance_types: Optional[List[str] | str] = None  # Overriden by provider config

    #
    # Pricing options
    #
    use_spot: Optional[bool] = None

    #
    # Boot options
    #
    startup_script: Optional[str] = None
    startup_script_file: Optional[str] = None
    image: Optional[str] = None

    #
    # Worker and manage_pool options
    #
    scaling_check_interval: Optional[PositiveInt] = 60
    instance_termination_delay: Optional[PositiveInt] = 60
    max_runtime: Optional[PositiveInt] = 60  # Use for queue timeout and workout task kill
    worker_use_new_process: Optional[bool] = False


class AWSConfig(ProviderConfig, validate_assignment=True):
    """Config options specific to AWS"""

    access_key: Optional[constr(min_length=1)] = None
    secret_key: Optional[constr(min_length=1)] = None


class GCPConfig(ProviderConfig, validate_assignment=True):
    """Config options specific to GCP"""

    project_id: Optional[constr(min_length=1)] = None
    credentials_file: Optional[constr(min_length=1)] = None
    service_account: Optional[constr(min_length=1)] = None


class AzureConfig(ProviderConfig, validate_assignment=True):
    """Config options specific to Azure"""

    subscription_id: Optional[str] = None
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


class Config(BaseModel, validate_assignment=True):
    """Main configuration object.

    Must be created and populated like::

        config = load_config(args.config)
        config.overload_from_cli(vars(args))
        config.update_run_config_from_provider_config()
        config.validate_config()
    """

    provider: Optional[Literal["aws", "gcp", "azure", "AWS", "GCP", "AZURE"]] = None
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

        if self.provider is not None:
            self.provider = self.provider.upper()
        if self.run.architecture is not None:
            self.run.architecture = self.run.architecture.upper()

    def update_run_config_from_provider_config(self) -> None:
        """Update run config with provider-specific config values."""
        match self.provider:
            case "AWS":
                if self.aws.instance_types is not None:
                    self.run.instance_types = self.aws.instance_types
                if self.aws.startup_script is not None:
                    self.run.startup_script = self.aws.startup_script
                if self.aws.startup_script_file is not None:
                    self.run.startup_script_file = self.aws.startup_script_file
                if self.aws.image is not None:
                    self.run.image = self.aws.image
            case "GCP":
                if self.gcp.instance_types is not None:
                    self.run.instance_types = self.gcp.instance_types
                if self.gcp.startup_script is not None:
                    self.run.startup_script = self.gcp.startup_script
                if self.gcp.startup_script_file is not None:
                    self.run.startup_script_file = self.gcp.startup_script_file
                if self.gcp.image is not None:
                    self.run.image = self.gcp.image
            case "AZURE":
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
                raise FileNotFoundError(
                    f"Startup script file not found: {self.run.startup_script_file}"
                )

            with open(self.run.startup_script_file, "r") as f:
                self.run.startup_script = f.read()

    def validate_config(self) -> None:
        """Perform final validation of the configuration."""
        if self.provider is None:
            raise ValueError("Provider must be specified")

    def get_provider_config(self, provider_name: Optional[str] = None) -> ProviderConfig:
        """Get configuration for a specific cloud provider.

        Args:
            provider_name: Cloud provider name ('AWS', 'GCP', or 'AZURE')

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
            case "AWS":
                provider_config = self.aws
            case "GCP":
                provider_config = self.gcp
            case "AZURE":
                provider_config = self.azure
            case _:
                raise ValueError(f"Unsupported provider: {provider_name}")

        if provider_config is None:
            raise ValueError(f"Provider configuration not found for {provider_name}")

        if provider_config.queue_name is None:
            job_id = provider_config.job_id
            if job_id is not None:
                provider_config.queue_name = f"rms-cloud-tasks-{job_id}"

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
    if "aws" not in config_dict or config_dict["aws"] is None:
        config_dict["aws"] = {}
    if "gcp" not in config_dict or config_dict["gcp"] is None:
        config_dict["gcp"] = {}
    if "azure" not in config_dict or config_dict["azure"] is None:
        config_dict["azure"] = {}
    if "run" not in config_dict or config_dict["run"] is None:
        config_dict["run"] = {}

    # Convert to Config object
    config = Config(**config_dict)

    if config.provider is not None:
        config.provider = config.provider.upper()

    return config
