# Manually verified 5/7/2025

import yaml
import pytest
import pydantic
from unittest.mock import patch, MagicMock
from src.cloud_tasks.common import config as config_mod
from src.cloud_tasks.common.config import (
    RunConfig,
    ProviderConfig,
    AWSConfig,
    GCPConfig,
    AzureConfig,
    Config,
    load_config,
)


# --- RunConfig validation tests ---
def test_runconfig_min_max_instances():
    RunConfig(min_instances=1, max_instances=2)
    with pytest.raises(ValueError):
        RunConfig(min_instances=3, max_instances=2)


def test_runconfig_min_max_total_cpus():
    RunConfig(min_total_cpus=1, max_total_cpus=2)
    with pytest.raises(ValueError):
        RunConfig(min_total_cpus=3, max_total_cpus=2)


def test_runconfig_min_max_tasks_per_instance():
    RunConfig(min_tasks_per_instance=1, max_tasks_per_instance=2)
    with pytest.raises(ValueError):
        RunConfig(min_tasks_per_instance=3, max_tasks_per_instance=2)


def test_runconfig_min_max_simultaneous_tasks():
    RunConfig(min_simultaneous_tasks=1, max_simultaneous_tasks=2)
    with pytest.raises(ValueError):
        RunConfig(min_simultaneous_tasks=3, max_simultaneous_tasks=2)


def test_runconfig_min_max_total_price_per_hour():
    RunConfig(min_total_price_per_hour=1, max_total_price_per_hour=2)
    with pytest.raises(ValueError):
        RunConfig(min_total_price_per_hour=3, max_total_price_per_hour=2)
    with pytest.raises(ValueError):
        RunConfig(max_total_price_per_hour=0)


def test_runconfig_min_max_cpu_rank():
    RunConfig(min_cpu_rank=1, max_cpu_rank=2)
    with pytest.raises(ValueError):
        RunConfig(min_cpu_rank=3, max_cpu_rank=2)


def test_runconfig_min_max_cpu():
    RunConfig(min_cpu=1, max_cpu=2)
    with pytest.raises(ValueError):
        RunConfig(min_cpu=3, max_cpu=2)


def test_runconfig_min_max_total_memory():
    RunConfig(min_total_memory=1, max_total_memory=2)
    with pytest.raises(ValueError):
        RunConfig(min_total_memory=3, max_total_memory=2)
    with pytest.raises(ValueError):
        RunConfig(max_total_memory=0)


def test_runconfig_min_max_memory_per_cpu():
    RunConfig(min_memory_per_cpu=1, max_memory_per_cpu=2)
    with pytest.raises(ValueError):
        RunConfig(min_memory_per_cpu=3, max_memory_per_cpu=2)
    with pytest.raises(ValueError):
        RunConfig(max_memory_per_cpu=0)


def test_runconfig_min_max_memory_per_task():
    RunConfig(min_memory_per_task=1, max_memory_per_task=2)
    with pytest.raises(ValueError):
        RunConfig(min_memory_per_task=3, max_memory_per_task=2)
    with pytest.raises(ValueError):
        RunConfig(max_memory_per_task=0)


def test_runconfig_min_max_local_ssd():
    RunConfig(min_local_ssd=1, max_local_ssd=2)
    with pytest.raises(ValueError):
        RunConfig(min_local_ssd=3, max_local_ssd=2)
    with pytest.raises(ValueError):
        RunConfig(max_local_ssd=0)


def test_runconfig_min_max_local_ssd_per_cpu():
    RunConfig(min_local_ssd_per_cpu=1, max_local_ssd_per_cpu=2)
    with pytest.raises(ValueError):
        RunConfig(min_local_ssd_per_cpu=3, max_local_ssd_per_cpu=2)
    with pytest.raises(ValueError):
        RunConfig(max_local_ssd_per_cpu=0)


def test_runconfig_min_max_local_ssd_per_task():
    RunConfig(min_local_ssd_per_task=1, max_local_ssd_per_task=2)
    with pytest.raises(ValueError):
        RunConfig(min_local_ssd_per_task=3, max_local_ssd_per_task=2)
    with pytest.raises(ValueError):
        RunConfig(max_local_ssd_per_task=0)


def test_runconfig_instance_types_list_or_str():
    RunConfig(instance_types=None)
    RunConfig(instance_types=["foo", "bar"])
    RunConfig(instance_types="foo")


def test_runconfig_architecture_case():
    rc = RunConfig(architecture="x86_64")
    assert rc.architecture == "x86_64"
    rc = RunConfig(architecture="X86_64")
    assert rc.architecture == "X86_64"
    rc = RunConfig(architecture="arm64")
    assert rc.architecture == "arm64"
    rc = RunConfig(architecture="ARM64")
    assert rc.architecture == "ARM64"


def test_update_run_config_from_provider_config_defaults():
    c = Config(
        provider="AWS",
        aws=AWSConfig(),
        gcp=GCPConfig(),
        azure=AzureConfig(),
        run=RunConfig(),
    )
    # Set all values to None to test defaults
    c.run.min_instances = None
    c.run.max_instances = None
    c.run.min_total_cpus = None
    c.run.max_total_cpus = None
    c.run.cpus_per_task = None
    c.run.min_tasks_per_instance = None
    c.run.max_tasks_per_instance = None
    c.run.min_simultaneous_tasks = None
    c.run.max_simultaneous_tasks = None
    c.run.min_total_price_per_hour = None
    c.run.max_total_price_per_hour = None

    c.run.architecture = None
    c.run.cpu_family = None
    c.run.min_cpu_rank = None
    c.run.max_cpu_rank = None
    c.run.min_cpu = None
    c.run.max_cpu = None
    c.run.min_total_memory = None
    c.run.max_total_memory = None
    c.run.min_memory_per_cpu = None
    c.run.max_memory_per_cpu = None
    c.run.min_memory_per_task = None
    c.run.max_memory_per_task = None
    c.run.min_local_ssd = None
    c.run.max_local_ssd = None
    c.run.local_ssd_base_size = None
    c.run.min_local_ssd_per_cpu = None
    c.run.max_local_ssd_per_cpu = None
    c.run.min_local_ssd_per_task = None
    c.run.max_local_ssd_per_task = None
    c.run.boot_disk_types = None
    c.run.boot_disk_iops = None
    c.run.boot_disk_throughput = None
    c.run.total_boot_disk_size = None
    c.run.boot_disk_base_size = None
    c.run.boot_disk_per_cpu = None
    c.run.boot_disk_per_task = None

    c.run.instance_types = None

    c.run.use_spot = None

    c.run.startup_script = None
    c.run.startup_script_file = None
    c.run.image = None

    c.run.scaling_check_interval = None
    c.run.instance_termination_delay = None
    c.run.max_runtime = None
    c.run.retry_on_exit = None
    c.run.retry_on_exception = None
    c.run.retry_on_timeout = None

    c.update_run_config_from_provider_config()

    # Verify all defaults are set correctly
    assert c.run.cpus_per_task == 1
    assert c.run.min_instances == 1
    assert c.run.max_instances == 10
    assert c.run.scaling_check_interval == 60
    assert c.run.instance_termination_delay == 60
    assert c.run.max_runtime == 60
    assert c.run.retry_on_exit is None
    assert c.run.retry_on_exception is None
    assert c.run.retry_on_timeout is None
    assert c.run.architecture == "X86_64"
    assert c.run.local_ssd_base_size == 0
    assert c.run.total_boot_disk_size == 10
    assert c.run.boot_disk_base_size == 0

    # Verify all fields that should be None are None
    assert c.run.min_total_cpus is None
    assert c.run.max_total_cpus is None
    assert c.run.min_tasks_per_instance is None
    assert c.run.max_tasks_per_instance is None
    assert c.run.min_simultaneous_tasks is None
    assert c.run.max_simultaneous_tasks is None
    assert c.run.min_total_price_per_hour is None
    assert c.run.max_total_price_per_hour == 10
    assert c.run.cpu_family is None
    assert c.run.min_cpu_rank is None
    assert c.run.max_cpu_rank is None
    assert c.run.min_cpu is None
    assert c.run.max_cpu is None
    assert c.run.min_total_memory is None
    assert c.run.max_total_memory is None
    assert c.run.min_memory_per_cpu is None
    assert c.run.max_memory_per_cpu is None
    assert c.run.min_memory_per_task is None
    assert c.run.max_memory_per_task is None
    assert c.run.min_local_ssd is None
    assert c.run.max_local_ssd is None
    assert c.run.min_local_ssd_per_cpu is None
    assert c.run.max_local_ssd_per_cpu is None
    assert c.run.min_local_ssd_per_task is None
    assert c.run.max_local_ssd_per_task is None
    assert c.run.boot_disk_types is None
    assert c.run.boot_disk_iops is None
    assert c.run.boot_disk_throughput is None
    assert c.run.boot_disk_per_cpu is None
    assert c.run.boot_disk_per_task is None
    assert c.run.instance_types is None
    assert c.run.use_spot is None
    assert c.run.startup_script is None
    assert c.run.startup_script_file is None
    assert c.run.image is None

    # Test that values are not overwritten if already set
    c.run.cpus_per_task = 2
    c.run.min_instances = 3
    c.run.max_instances = 5
    c.run.scaling_check_interval = 30
    c.run.instance_termination_delay = 45
    c.run.architecture = "arm64"
    c.run.local_ssd_base_size = 20
    c.run.total_boot_disk_size = 50
    c.run.boot_disk_base_size = 30
    c.run.cpu_family = "Intel Cascade Lake"
    c.run.min_cpu_rank = 1
    c.run.max_cpu_rank = 2
    c.run.boot_disk_types = ["SSD", "HDD"]
    c.run.boot_disk_iops = 1000
    c.run.boot_disk_throughput = 100
    c.run.max_runtime = 120
    c.run.retry_on_exit = False
    c.run.retry_on_exception = False
    c.run.retry_on_timeout = False

    c.update_run_config_from_provider_config()

    # Verify values are preserved
    assert c.run.cpus_per_task == 2
    assert c.run.min_instances == 3
    assert c.run.max_instances == 5
    assert c.run.scaling_check_interval == 30
    assert c.run.instance_termination_delay == 45
    assert c.run.architecture == "ARM64"  # Should be uppercased
    assert c.run.local_ssd_base_size == 20
    assert c.run.total_boot_disk_size == 50
    assert c.run.boot_disk_base_size == 30
    assert c.run.cpu_family == "INTEL CASCADE LAKE"  # Should be uppercased
    assert c.run.min_cpu_rank == 1
    assert c.run.max_cpu_rank == 2
    assert c.run.boot_disk_types == ["ssd", "hdd"]  # Should be lowercased
    assert c.run.boot_disk_iops == 1000
    assert c.run.boot_disk_throughput == 100
    assert c.run.max_runtime == 120
    assert c.run.retry_on_exit is False
    assert c.run.retry_on_exception is False
    assert c.run.retry_on_timeout is False


# --- ProviderConfig, AWSConfig, GCPConfig, AzureConfig ---
def test_provider_config_fields():
    ProviderConfig(job_id="a-job", queue_name="a-queue", region="r", zone="z")
    AWSConfig(access_key="a", secret_key="b")
    GCPConfig(project_id="pid", credentials_file="cf", service_account="sa")
    AzureConfig(subscription_id="sid", tenant_id="tid", client_id="cid", client_secret="cs")


@pytest.fixture
def config_obj():
    return Config(
        provider="GCP",
        aws=AWSConfig(),
        gcp=GCPConfig(project_id="pid", credentials_file="cf", service_account="sa"),
        azure=AzureConfig(),
        run=RunConfig(),
    )


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_config_overload_from_cli(config_obj, provider):
    c = config_obj
    c.aws.region = "us-east-1"
    c.gcp.region = "us-east-2"
    c.azure.region = "us-east-3"
    c.aws.architecture = "x86_64"
    c.gcp.architecture = "x86_64"
    c.azure.architecture = "x86_64"
    c.aws.cpu_family = "Intel Sapphire Rapids"
    c.gcp.cpu_family = "Intel Cascade Lake"
    c.azure.cpu_family = "Intel Emerald Rapids"
    cli_args = {
        "provider": provider,
        "region": "us-west-1",
        "architecture": "arm64",
        "cpu_family": "Intel Haswell",
    }
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.overload_from_cli(cli_args)
        mock_logger.warning.assert_called()
    assert c.provider == provider
    match provider:
        case "AWS":
            assert c.aws.region == "us-west-1"
            assert c.aws.architecture == "ARM64"
            assert c.aws.cpu_family == "INTEL HASWELL"
        case "GCP":
            assert c.gcp.region == "us-west-1"
            assert c.gcp.architecture == "ARM64"
            assert c.gcp.cpu_family == "INTEL HASWELL"
        case "AZURE":
            assert c.azure.region == "us-west-1"
            assert c.azure.architecture == "ARM64"
            assert c.azure.cpu_family == "INTEL HASWELL"
    assert c.run.architecture == "ARM64"
    assert c.run.cpu_family == "INTEL HASWELL"

    # Now test capitalization after update_run_config_from_provider_config
    c.update_run_config_from_provider_config()
    match provider:
        case "AWS":
            assert c.aws.architecture == "ARM64"
            assert c.aws.cpu_family == "INTEL HASWELL"
        case "GCP":
            assert c.gcp.architecture == "ARM64"
            assert c.gcp.cpu_family == "INTEL HASWELL"
        case "AZURE":
            assert c.azure.architecture == "ARM64"
            assert c.azure.cpu_family == "INTEL HASWELL"
    assert c.run.architecture == "ARM64"
    assert c.run.cpu_family == "INTEL HASWELL"

    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.overload_from_cli(cli_args)  # Repeat
        mock_logger.warning.assert_not_called()
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.overload_from_cli({})
        mock_logger.warning.assert_not_called()
        assert c.provider == provider
        match provider:
            case "AWS":
                assert c.aws.region == "us-west-1"
                assert c.aws.architecture == "ARM64"
                assert c.aws.cpu_family == "INTEL HASWELL"
            case "GCP":
                assert c.gcp.region == "us-west-1"
                assert c.gcp.architecture == "ARM64"
                assert c.gcp.cpu_family == "INTEL HASWELL"
            case "AZURE":
                assert c.azure.region == "us-west-1"
                assert c.azure.architecture == "ARM64"
                assert c.azure.cpu_family == "INTEL HASWELL"
        assert c.run.architecture == "ARM64"
        assert c.run.cpu_family == "INTEL HASWELL"


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_config_update_run_config_from_provider_config(config_obj, provider):
    c = config_obj
    c.provider = provider
    match provider:
        case "AWS":
            c.aws.min_cpu = 6
        case "GCP":
            c.gcp.min_cpu = 8
        case "AZURE":
            c.azure.min_cpu = 10
    # Overload when nothing was there before
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.update_run_config_from_provider_config()
        mock_logger.warning.assert_not_called()
    match provider:
        case "AWS":
            assert c.run.min_cpu == 6
        case "GCP":
            assert c.run.min_cpu == 8
        case "AZURE":
            assert c.run.min_cpu == 10
    match provider:
        case "AWS":
            c.aws.min_cpu = 1
        case "GCP":
            c.gcp.min_cpu = 2
        case "AZURE":
            c.azure.min_cpu = 3
    # Overload when a value was already set
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.update_run_config_from_provider_config()
        mock_logger.warning.assert_called()
    match provider:
        case "AWS":
            assert c.run.min_cpu == 1
        case "GCP":
            assert c.run.min_cpu == 2
        case "AZURE":
            assert c.run.min_cpu == 3
    # Test startup_script and startup_script_file conflict
    c.run.startup_script = "foo"
    match provider:
        case "AWS":
            c.aws.startup_script_file = "bar"
        case "GCP":
            c.gcp.startup_script_file = "bar"
        case "AZURE":
            c.azure.startup_script_file = "bar"
    with pytest.raises(ValueError):
        c.update_run_config_from_provider_config()
    # Test startup_script_file loads content
    c.run.startup_script = None
    c.run.startup_script_file = None
    match provider:
        case "AWS":
            c.aws.startup_script_file = "file.sh"
        case "GCP":
            c.gcp.startup_script_file = "file.sh"
        case "AZURE":
            c.azure.startup_script_file = "file.sh"
    with patch.object(config_mod, "FCPath", MagicMock()) as mfc:
        mfc.return_value.read_text.return_value = "script-content"
        c.update_run_config_from_provider_config()
        assert c.run.startup_script == "script-content"


def test_update_run_config_from_provider_config_none(config_obj):
    c = config_obj
    c.provider = None
    with pytest.raises(ValueError, match="Provider must be specified"):
        c.update_run_config_from_provider_config()


def test_update_run_config_from_provider_config_unsupported(config_obj):
    c = config_obj
    # Bypass pydantic validation to set an unsupported provider
    object.__setattr__(c, "provider", "FOOBAR")
    with pytest.raises(ValueError, match="Unsupported provider: FOOBAR"):
        c.update_run_config_from_provider_config()


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_config_validate_config(config_obj, provider):
    c = config_obj
    c.provider = None
    with pytest.raises(ValueError):
        c.validate_config()
    with pytest.raises(pydantic.ValidationError):
        c.provider = "BAD"
    c.provider = provider
    c.validate_config()  # Should not raise


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_config_get_provider_config(config_obj, provider):
    import pydantic

    c = config_obj
    c.provider = provider
    match provider:
        case "AWS":
            c.aws.job_id = "jid"
            c.aws.queue_name = None
        case "GCP":
            c.gcp.job_id = "jid"
            c.gcp.queue_name = None
        case "AZURE":
            c.azure.job_id = "jid"
            c.azure.queue_name = None
    pc = c.get_provider_config()
    match provider:
        case "AWS":
            assert isinstance(pc, AWSConfig)
        case "GCP":
            assert isinstance(pc, GCPConfig)
        case "AZURE":
            assert isinstance(pc, AzureConfig)
    assert pc.queue_name == "jid"
    # Test missing provider_name
    c.provider = None
    with pytest.raises(ValueError):
        c.get_provider_config()
    # Test unsupported provider
    with pytest.raises(pydantic.ValidationError):
        c.provider = "FOO"


def test_get_provider_config_unsupported(config_obj):
    c = config_obj
    # Bypass pydantic validation to set an unsupported provider
    object.__setattr__(c, "provider", "FOOBAR")
    with pytest.raises(ValueError, match="Unsupported provider: FOOBAR"):
        c.get_provider_config("FOOBAR")


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_config_get_provider_config_queue_name(config_obj, provider):
    c = config_obj
    c.provider = provider
    match provider:
        case "AWS":
            c.aws.job_id = "jid"
            c.aws.queue_name = None
        case "GCP":
            c.gcp.job_id = "jid"
            c.gcp.queue_name = None
        case "AZURE":
            c.azure.job_id = "jid"
            c.azure.queue_name = None
    pc = c.get_provider_config()
    assert pc.queue_name == "jid"
    # If queue_name is set, it should not be overwritten
    match provider:
        case "AWS":
            c.aws.queue_name = "qname"
        case "GCP":
            c.gcp.queue_name = "qname"
        case "AZURE":
            c.azure.queue_name = "qname"
    pc = c.get_provider_config()
    assert pc.queue_name == "qname"


# --- load_config ---
def test_load_config_file_gcp(tmp_path):
    config_dict = {
        "provider": "gcp",
        "gcp": {"project_id": "pid", "credentials_file": "cf", "service_account": "sa"},
        "aws": {},
        "azure": {},
        "run": {"architecture": "x86_64"},
    }
    file_path = tmp_path / "config.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(config_dict, f)
    with patch.object(config_mod, "FCPath", lambda *a, **kw: file_path):
        cfg = load_config(str(file_path))
        assert cfg.gcp.project_id == "pid"
        assert cfg.run.architecture == "x86_64"


def test_load_config_file_aws(tmp_path):
    config_dict = {
        "provider": "aws",
        "aws": {"access_key": "ak", "secret_key": "sk"},
        "gcp": {},
        "azure": {},
        "run": {"architecture": "x86_64"},
    }
    file_path = tmp_path / "config.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(config_dict, f)
    with patch.object(config_mod, "FCPath", lambda *a, **kw: file_path):
        cfg = load_config(str(file_path))
        assert cfg.aws.access_key == "ak"
        assert cfg.run.architecture == "x86_64"


def test_load_config_file_azure(tmp_path):
    config_dict = {
        "provider": "azure",
        "azure": {
            "subscription_id": "sid",
            "tenant_id": "tid",
            "client_id": "cid",
            "client_secret": "cs",
        },
        "gcp": {},
        "aws": {},
        "run": {"architecture": "x86_64"},
    }
    file_path = tmp_path / "config.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(config_dict, f)
    with patch.object(config_mod, "FCPath", lambda *a, **kw: file_path):
        cfg = load_config(str(file_path))
        assert cfg.azure.subscription_id == "sid"
        assert cfg.run.architecture == "x86_64"


def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/file.yaml")


def test_load_config_file_invalid_yaml(tmp_path):
    file_path = tmp_path / "bad.yaml"
    with open(file_path, "w") as f:
        f.write("- just\n- a\n- list\n")
    with patch.object(config_mod, "FCPath", lambda *a, **kw: file_path):
        with pytest.raises(ValueError):
            load_config(str(file_path))


def test_load_config_no_file():
    cfg = load_config(None)
    assert isinstance(cfg, Config)


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_load_config_relative_paths(tmp_path, provider):
    # Simulate config file with relative startup_script_file
    config_dict = {
        "provider": provider,
        "run": {
            "startup_script_file": "script-RUN.sh",
        },
        "gcp": {
            "project_id": "pid",
            "credentials_file": "cf",
            "service_account": "sa",
            "startup_script_file": "script-GCP.sh",
        },
        "aws": {
            "access_key": "ak",
            "secret_key": "sk",
            "startup_script_file": "script-AWS.sh",
        },
        "azure": {
            "subscription_id": "sid",
            "tenant_id": "tid",
            "client_id": "cid",
            "client_secret": "cs",
            "startup_script_file": "script-AZURE.sh",
        },
    }
    file_path = tmp_path / "config.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(config_dict, f)
    script_path = tmp_path / f"script-{provider}.sh"
    script_path.write_text("echo hi")
    print(file_path)
    print(script_path)
    cfg = load_config(str(file_path))
    print(cfg)
    cfg.update_run_config_from_provider_config()
    assert cfg.run.startup_script == "echo hi"
    assert cfg.aws.startup_script_file == str(tmp_path / "script-AWS.sh")
    assert cfg.gcp.startup_script_file == str(tmp_path / "script-GCP.sh")
    assert cfg.azure.startup_script_file == str(tmp_path / "script-AZURE.sh")


def test_load_config_instance_types_str_to_list(tmp_path):
    config_dict = {
        "provider": "gcp",
        "gcp": {"instance_types": "n1-standard-1"},
        "aws": {"instance_types": "t2.micro"},
        "azure": {"instance_types": "Standard_B1s"},
        "run": {},
    }
    file_path = tmp_path / "config.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(config_dict, f)
    cfg = load_config(str(file_path))
    assert cfg.gcp.instance_types == ["n1-standard-1"]
    assert cfg.aws.instance_types == ["t2.micro"]
    assert cfg.azure.instance_types == ["Standard_B1s"]


def test_load_config_instance_types_str_to_list_edge_cases(tmp_path):
    # Just a string
    config_dict = {
        "provider": "gcp",
        "gcp": {"instance_types": "n1-standard-1"},
        "aws": {"instance_types": "t2.micro"},
        "azure": {"instance_types": "Standard_B1s"},
        "run": {},
    }
    file_path = tmp_path / "config.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(config_dict, f)
    cfg = load_config(str(file_path))
    assert cfg.gcp.instance_types == ["n1-standard-1"]
    assert cfg.aws.instance_types == ["t2.micro"]
    assert cfg.azure.instance_types == ["Standard_B1s"]
    # Already a list
    config_dict = {
        "provider": "gcp",
        "gcp": {"instance_types": ["n1-standard-1"]},
        "aws": {"instance_types": ["t2.micro"]},
        "azure": {"instance_types": ["Standard_B1s"]},
        "run": {},
    }
    file_path = tmp_path / "config.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(config_dict, f)
    cfg = load_config(str(file_path))
    assert cfg.gcp.instance_types == ["n1-standard-1"]
    assert cfg.aws.instance_types == ["t2.micro"]
    assert cfg.azure.instance_types == ["Standard_B1s"]
    # Missing instance_types
    config_dict = {
        "provider": "gcp",
        "gcp": {},
        "aws": {},
        "azure": {},
        "run": {},
    }
    file_path2 = tmp_path / "config2.yaml"
    with open(file_path2, "w") as f:
        yaml.safe_dump(config_dict, f)
    cfg = load_config(str(file_path2))
    assert cfg.gcp.instance_types is None
    assert cfg.aws.instance_types is None
    assert cfg.azure.instance_types is None


def test_load_config_boot_disk_types_str_to_list_edge_cases(tmp_path):
    # Just a string
    config_dict = {
        "provider": "gcp",
        "gcp": {"boot_disk_types": "ssd"},
        "aws": {"boot_disk_types": "gp2"},
        "azure": {"boot_disk_types": "Premium_LRS"},
        "run": {},
    }
    file_path = tmp_path / "config.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(config_dict, f)
    cfg = load_config(str(file_path))
    assert cfg.gcp.boot_disk_types == ["ssd"]
    assert cfg.aws.boot_disk_types == ["gp2"]
    assert cfg.azure.boot_disk_types == ["Premium_LRS"]  # Already a list
    config_dict = {
        "provider": "gcp",
        "gcp": {"boot_disk_types": ["ssd"]},
        "aws": {"boot_disk_types": ["gp2"]},
        "azure": {"boot_disk_types": ["Premium_LRS"]},
        "run": {},
    }
    file_path = tmp_path / "config.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(config_dict, f)
    cfg = load_config(str(file_path))
    assert cfg.gcp.boot_disk_types == ["ssd"]
    assert cfg.aws.boot_disk_types == ["gp2"]
    assert cfg.azure.boot_disk_types == ["Premium_LRS"]
    # Missing boot_disk_types
    config_dict = {
        "provider": "gcp",
        "gcp": {},
        "aws": {},
        "azure": {},
        "run": {},
    }
    file_path2 = tmp_path / "config2.yaml"
    with open(file_path2, "w") as f:
        yaml.safe_dump(config_dict, f)
    cfg = load_config(str(file_path2))
    assert cfg.gcp.boot_disk_types is None
    assert cfg.aws.boot_disk_types is None
    assert cfg.azure.boot_disk_types is None


def test_config_overload_from_cli_aws_warning(config_obj):
    c = config_obj
    c.provider = "AWS"
    c.aws.region = "us-east-1"
    cli_args = {"region": "us-west-1"}
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.overload_from_cli(cli_args)
        mock_logger.warning.assert_called_with(
            "Overloading aws.region=us-east-1 with CLI=us-west-1"
        )
    assert c.aws.region == "us-west-1"


def test_config_overload_from_cli_gcp_warning(config_obj):
    c = config_obj
    c.provider = "GCP"
    c.gcp.region = "us-east1"
    cli_args = {"region": "us-west1"}
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.overload_from_cli(cli_args)
        mock_logger.warning.assert_called_with("Overloading gcp.region=us-east1 with CLI=us-west1")
    assert c.gcp.region == "us-west1"


def test_config_overload_from_cli_azure_warning(config_obj):
    c = config_obj
    c.provider = "AZURE"
    c.azure.region = "eastus"
    cli_args = {"region": "westus"}
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.overload_from_cli(cli_args)
        mock_logger.warning.assert_called_with("Overloading azure.region=eastus with CLI=westus")
    assert c.azure.region == "westus"


def test_config_overload_from_cli_no_warning(config_obj):
    c = config_obj
    c.provider = "AWS"
    c.aws.region = None
    cli_args = {"region": "us-west-1"}
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.overload_from_cli(cli_args)
        mock_logger.warning.assert_not_called()
    assert c.aws.region == "us-west-1"


def test_config_overload_from_cli_run_warning(config_obj):
    c = config_obj
    c.run.min_instances = 1
    cli_args = {"min_instances": 2}
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.overload_from_cli(cli_args)
        mock_logger.warning.assert_called_with("Overloading run.min_instances=1 with CLI=2")
    assert c.run.min_instances == 2


def test_boot_disk_types_capitalization(config_obj):
    c = config_obj
    c.run.boot_disk_types = "SSD"
    c.update_run_config_from_provider_config()
    assert c.run.boot_disk_types == ["ssd"]

    # Test single string - should be converted to a list with one element
    c.run.boot_disk_types = "SSD"
    # First convert to list like load_config does
    if isinstance(c.run.boot_disk_types, str):
        c.run.boot_disk_types = [c.run.boot_disk_types]
    c.update_run_config_from_provider_config()
    assert isinstance(c.run.boot_disk_types, list)
    assert c.run.boot_disk_types == ["ssd"]

    # Test list of strings
    c.run.boot_disk_types = ["SSD", "HDD", "NVMe"]
    c.update_run_config_from_provider_config()
    assert c.run.boot_disk_types == ["ssd", "hdd", "nvme"]

    # Test provider override
    c.gcp.boot_disk_types = ["SSD", "HDD"]
    c.update_run_config_from_provider_config()
    assert c.run.boot_disk_types == ["ssd", "hdd"]

    # Test None value
    c.run.boot_disk_types = None
    c.gcp.boot_disk_types = None  # Make sure provider config is also None
    c.update_run_config_from_provider_config()
    assert c.run.boot_disk_types is None

    # Test empty list
    c.run.boot_disk_types = []
    c.update_run_config_from_provider_config()
    assert c.run.boot_disk_types == []
