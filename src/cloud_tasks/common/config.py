"""
Configuration handling for the multi-cloud task processing system.
"""

import logging
import os
from typing import Annotated, Any, Dict, Optional, List, Literal, cast
import yaml

from filecache import FCPath
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    model_validator,
)

LOGGER = logging.getLogger(__name__)


class RunConfig(BaseModel, validate_assignment=True):
    """Config options for selecting instances and running tasks"""

    model_config = ConfigDict(extra="forbid")

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

    min_simultaneous_tasks: Optional[PositiveInt] = None
    max_simultaneous_tasks: Optional[PositiveInt] = None

    @model_validator(mode="after")
    def validate_min_max_simultaneous_tasks(self) -> "RunConfig":
        if self.min_simultaneous_tasks is not None and self.max_simultaneous_tasks is not None:
            if self.min_simultaneous_tasks > self.max_simultaneous_tasks:
                raise ValueError("min_simultaneous_tasks must be less than max_simultaneous_tasks")
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
    architecture: Optional[Literal["x86_64", "arm64", "X86_64", "ARM64"]] = None
    cpu_family: Optional[Annotated[str, Field(min_length=1)]] = None

    min_cpu_rank: Optional[NonNegativeInt] = None
    max_cpu_rank: Optional[NonNegativeInt] = None

    @model_validator(mode="after")
    def validate_min_max_cpu_rank(self) -> "RunConfig":
        if self.min_cpu_rank is not None and self.max_cpu_rank is not None:
            if self.min_cpu_rank > self.max_cpu_rank:
                raise ValueError("min_cpu_rank must be less than max_cpu_rank")
        return self

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

    min_memory_per_task: Optional[NonNegativeFloat] = None
    max_memory_per_task: Optional[NonNegativeFloat] = None

    @model_validator(mode="after")
    def validate_min_max_memory_per_task(self) -> "RunConfig":
        if self.min_memory_per_task is not None and self.max_memory_per_task is not None:
            if self.min_memory_per_task > self.max_memory_per_task:
                raise ValueError("min_memory_per_task must be less than max_memory_per_task")
        if self.max_memory_per_task is not None and self.max_memory_per_task <= 0:
            raise ValueError("max_memory_per_task must be greater than 0")
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

    local_ssd_base_size: Optional[NonNegativeFloat] = None
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

    min_local_ssd_per_task: Optional[NonNegativeFloat] = None
    max_local_ssd_per_task: Optional[NonNegativeFloat] = None

    @model_validator(mode="after")
    def validate_min_max_local_ssd_per_task(self) -> "RunConfig":
        if self.min_local_ssd_per_task is not None and self.max_local_ssd_per_task is not None:
            if self.min_local_ssd_per_task > self.max_local_ssd_per_task:
                raise ValueError("min_local_ssd_per_task must be less than max_local_ssd_per_task")
        if self.max_local_ssd_per_task is not None and self.max_local_ssd_per_task <= 0:
            raise ValueError("max_local_ssd_per_task must be greater than 0")
        return self

    #
    # Boot disk specifications
    #

    boot_disk_types: Optional[List[str] | str] = None
    boot_disk_iops: Optional[PositiveInt] = None  # GCP only
    boot_disk_throughput: Optional[PositiveInt] = None  # GCP only
    total_boot_disk_size: Optional[PositiveFloat] = None
    boot_disk_base_size: Optional[NonNegativeFloat] = None
    boot_disk_per_cpu: Optional[NonNegativeFloat] = None
    boot_disk_per_task: Optional[NonNegativeFloat] = None

    instance_types: Optional[List[str] | str] = None

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
    scaling_check_interval: Optional[PositiveInt] = None
    instance_termination_delay: Optional[PositiveInt] = None
    max_runtime: Optional[PositiveInt] = None  # Use for queue timeout and workout task kill
    retry_on_exit: Optional[bool] = None
    retry_on_exception: Optional[bool] = None
    retry_on_timeout: Optional[bool] = None


class ProviderConfig(RunConfig, validate_assignment=True):
    """Config options valid for all cloud providers"""

    model_config = ConfigDict(extra="forbid")

    job_id: Optional[
        Annotated[str, Field(min_length=1, max_length=24, pattern=r"^[a-z][-a-z0-9]{0,23}$")]
    ] = None
    queue_name: Optional[
        Annotated[str, Field(min_length=1, max_length=24, pattern=r"^[a-z][-a-z0-9]{0,23}$")]
    ] = None
    region: Optional[Annotated[str, Field(min_length=1)]] = None
    zone: Optional[Annotated[str, Field(min_length=1)]] = None
    exactly_once_queue: Optional[bool] = None


class AWSConfig(ProviderConfig, validate_assignment=True):
    """Config options specific to AWS"""

    model_config = ConfigDict(extra="forbid")

    access_key: Optional[Annotated[str, Field(min_length=1)]] = None
    secret_key: Optional[Annotated[str, Field(min_length=1)]] = None


class GCPConfig(ProviderConfig, validate_assignment=True):
    """Config options specific to GCP"""

    model_config = ConfigDict(extra="forbid")

    project_id: Optional[Annotated[str, Field(min_length=1)]] = None
    credentials_file: Optional[Annotated[str, Field(min_length=1)]] = None
    service_account: Optional[Annotated[str, Field(min_length=1)]] = None


class AzureConfig(ProviderConfig, validate_assignment=True):
    """Config options specific to Azure"""

    model_config = ConfigDict(extra="forbid")

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

    model_config = ConfigDict(extra="forbid")

    provider: Optional[Literal["aws", "gcp", "azure", "AWS", "GCP", "AZURE"]] = None
    aws: AWSConfig = AWSConfig()
    gcp: GCPConfig = GCPConfig()
    azure: AzureConfig = AzureConfig()
    run: RunConfig = RunConfig()

    def overload_from_cli(self, cli_args: Optional[Dict[str, Any]] = None) -> None:
        """Overload Config object with command line arguments.

        Args:
            cli_args: Command line arguments as a dictionary
        """
        if self.provider is not None:
            self.provider = cast(
                Literal["aws", "gcp", "azure", "AWS", "GCP", "AZURE"], self.provider.upper()
            )

        # Override loaded file and/or defaults with command line arguments
        if cli_args is not None:
            if "architecture" in cli_args and cli_args["architecture"] is not None:
                cli_args["architecture"] = cli_args["architecture"].upper()
            if "cpu_family" in cli_args and cli_args["cpu_family"] is not None:
                cli_args["cpu_family"] = cli_args["cpu_family"].upper()
            if "provider" in cli_args and cli_args["provider"] is not None:
                cli_args["provider"] = cli_args["provider"].upper()

            for attr_name in vars(self):
                if attr_name in cli_args and cli_args[attr_name] is not None:
                    val = getattr(self, attr_name)
                    if val is not None and val != cli_args[attr_name]:
                        LOGGER.warning(
                            f"Overloading {attr_name}={val} with CLI={cli_args[attr_name]}"
                        )
                    setattr(self, attr_name, cli_args[attr_name])
            if self.provider is not None:
                self.provider = cast(
                    Literal["aws", "gcp", "azure", "AWS", "GCP", "AZURE"], self.provider.upper()
                )
            for attr_name in vars(self.run):
                if attr_name in cli_args and cli_args[attr_name] is not None:
                    val = getattr(self.run, attr_name)
                    if val is not None and val != cli_args[attr_name]:
                        LOGGER.warning(
                            f"Overloading run.{attr_name}={val} with CLI={cli_args[attr_name]}"
                        )
                    setattr(self.run, attr_name, cli_args[attr_name])
            if self.provider == "AWS" and self.aws is not None:
                for attr_name in vars(self.aws):
                    if attr_name in cli_args and cli_args[attr_name] is not None:
                        val = getattr(self.aws, attr_name)
                        if val is not None and val != cli_args[attr_name]:
                            LOGGER.warning(
                                f"Overloading aws.{attr_name}={val} with CLI={cli_args[attr_name]}"
                            )
                        setattr(self.aws, attr_name, cli_args[attr_name])
            if self.provider == "GCP" and self.gcp is not None:
                for attr_name in vars(self.gcp):
                    if attr_name in cli_args and cli_args[attr_name] is not None:
                        val = getattr(self.gcp, attr_name)
                        if val is not None and val != cli_args[attr_name]:
                            LOGGER.warning(
                                f"Overloading gcp.{attr_name}={val} with CLI={cli_args[attr_name]}"
                            )
                        setattr(self.gcp, attr_name, cli_args[attr_name])
            if self.provider == "AZURE" and self.azure is not None:
                for attr_name in vars(self.azure):
                    if attr_name in cli_args and cli_args[attr_name] is not None:
                        val = getattr(self.azure, attr_name)
                        if val is not None and val != cli_args[attr_name]:
                            LOGGER.warning(
                                f"Overloading azure.{attr_name}={val} with "
                                f"CLI={cli_args[attr_name]}"
                            )
                        setattr(self.azure, attr_name, cli_args[attr_name])

    def update_run_config_from_provider_config(self) -> None:
        """Update run config with provider-specific config values."""
        match self.provider:
            case "AWS":
                run_vars = vars(self.run)
                aws_vars = vars(self.aws)
                for attr_name in run_vars:
                    if (
                        attr_name in aws_vars
                        and attr_name in run_vars
                        and aws_vars[attr_name] is not None
                    ):
                        if (
                            run_vars[attr_name] is not None
                            and run_vars[attr_name] != aws_vars[attr_name]
                        ):
                            LOGGER.warning(
                                f"Overriding run.{attr_name}={run_vars[attr_name]} with "
                                f"aws.{attr_name}={aws_vars[attr_name]}"
                            )
                        setattr(self.run, attr_name, aws_vars[attr_name])
            case "GCP":
                run_vars = vars(self.run)
                gcp_vars = vars(self.gcp)
                for attr_name in run_vars:
                    if (
                        attr_name in gcp_vars
                        and attr_name in run_vars
                        and gcp_vars[attr_name] is not None
                    ):
                        if (
                            run_vars[attr_name] is not None
                            and run_vars[attr_name] != gcp_vars[attr_name]
                        ):
                            LOGGER.warning(
                                f"Overriding run.{attr_name}={run_vars[attr_name]} with "
                                f"gcp.{attr_name}={gcp_vars[attr_name]}"
                            )
                        setattr(self.run, attr_name, gcp_vars[attr_name])
            case "AZURE":
                run_vars = vars(self.run)
                azure_vars = vars(self.azure)
                for attr_name in run_vars:
                    if (
                        attr_name in azure_vars
                        and attr_name in run_vars
                        and azure_vars[attr_name] is not None
                    ):
                        if (
                            run_vars[attr_name] is not None
                            and run_vars[attr_name] != azure_vars[attr_name]
                        ):
                            LOGGER.warning(
                                f"Overriding run.{attr_name}={run_vars[attr_name]} with "
                                f"azure.{attr_name}={azure_vars[attr_name]}"
                            )
                        setattr(self.run, attr_name, azure_vars[attr_name])
            case None:
                raise ValueError("Provider must be specified")
            case _:
                raise ValueError(f"Unsupported provider: {self.provider}")

        # Fix up the run config with the startup script and various defaults
        if self.run.startup_script is not None and self.run.startup_script_file is not None:
            raise ValueError("Startup script and startup script file cannot both be provided")
        if self.run.startup_script_file is not None:
            self.run.startup_script = FCPath(self.run.startup_script_file).read_text()

        # Set defaults for missing values
        if self.run.cpus_per_task is None:
            self.run.cpus_per_task = 1
        if self.run.min_instances is None:
            self.run.min_instances = 1
        if self.run.max_instances is None:
            self.run.max_instances = 10
        if self.run.max_total_price_per_hour is None:
            self.run.max_total_price_per_hour = 10
        if self.run.scaling_check_interval is None:
            self.run.scaling_check_interval = 60
        if self.run.instance_termination_delay is None:
            self.run.instance_termination_delay = 60
        if self.run.max_runtime is None:
            self.run.max_runtime = 60
        if self.run.architecture is None:
            self.run.architecture = "X86_64"
        if self.run.local_ssd_base_size is None:
            self.run.local_ssd_base_size = 0
        if self.run.total_boot_disk_size is None:
            self.run.total_boot_disk_size = 10
        if self.run.boot_disk_base_size is None:
            self.run.boot_disk_base_size = 0

        # Fix case
        if self.run.architecture is not None:
            self.run.architecture = cast(
                Literal["x86_64", "arm64", "X86_64", "ARM64"], self.run.architecture.upper()
            )
        if self.run.cpu_family is not None:
            self.run.cpu_family = self.run.cpu_family.upper()
        if self.run.boot_disk_types is not None:
            if isinstance(self.run.boot_disk_types, str):
                self.run.boot_disk_types = [self.run.boot_disk_types]
            self.run.boot_disk_types = [t.lower() for t in self.run.boot_disk_types]

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

        provider_config: ProviderConfig | None = None
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
                provider_config.queue_name = job_id

        return provider_config


def load_config(config_file: Optional[str] = None) -> Config:
    """Load configuration from a YAML file.

    Args:
        config_file: Path to the configuration file

    Returns:
        Config object containing the configuration

    Raises:
        FileNotFoundError: If the file cannot be found
        ValueError: If the file cannot be loaded or is invalid
    """
    if config_file:
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file not found: {config_file}")

        with FCPath(config_file).open(mode="r") as f:
            config_dict = yaml.safe_load(f)

        if not isinstance(config_dict, dict):
            raise ValueError("Configuration file must contain a YAML dictionary")
    else:
        config_dict = {}

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
        config.provider = cast(
            Literal["aws", "gcp", "azure", "AWS", "GCP", "AZURE"], config.provider.upper()
        )

    # If the startup script filename is provided in the config file, then any relative paths
    # are relative to the config file location.
    if config.run.startup_script_file is not None:
        config.run.startup_script_file = FCPath(
            FCPath(config_file).parent, config.run.startup_script_file
        ).as_posix()
    if config.aws.startup_script_file is not None:
        config.aws.startup_script_file = FCPath(
            FCPath(config_file).parent, config.aws.startup_script_file
        ).as_posix()
    if config.gcp.startup_script_file is not None:
        config.gcp.startup_script_file = FCPath(
            FCPath(config_file).parent, config.gcp.startup_script_file
        ).as_posix()
    if config.azure.startup_script_file is not None:
        config.azure.startup_script_file = FCPath(
            FCPath(config_file).parent, config.azure.startup_script_file
        ).as_posix()

    # Update the instance_types to always be a list
    if config.aws.instance_types is not None and isinstance(config.aws.instance_types, str):
        config.aws.instance_types = [config.aws.instance_types]
    if config.gcp.instance_types is not None and isinstance(config.gcp.instance_types, str):
        config.gcp.instance_types = [config.gcp.instance_types]
    if config.azure.instance_types is not None and isinstance(config.azure.instance_types, str):
        config.azure.instance_types = [config.azure.instance_types]

    # Update the boot_disk_type to always be a list
    if config.aws.boot_disk_types is not None and isinstance(config.aws.boot_disk_types, str):
        config.aws.boot_disk_types = [config.aws.boot_disk_types]
    if config.gcp.boot_disk_types is not None and isinstance(config.gcp.boot_disk_types, str):
        config.gcp.boot_disk_types = [config.gcp.boot_disk_types]
    if config.azure.boot_disk_types is not None and isinstance(config.azure.boot_disk_types, str):
        config.azure.boot_disk_types = [config.azure.boot_disk_types]

    return config
