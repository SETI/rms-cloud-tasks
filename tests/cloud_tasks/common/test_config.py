# Manually verified 4/29/2025

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


def test_runconfig_min_max_boot_disk():
    RunConfig(min_boot_disk=1, max_boot_disk=2)
    with pytest.raises(ValueError):
        RunConfig(min_boot_disk=3, max_boot_disk=2)


def test_runconfig_min_max_boot_disk_per_cpu():
    RunConfig(min_boot_disk_per_cpu=1, max_boot_disk_per_cpu=2)
    with pytest.raises(ValueError):
        RunConfig(min_boot_disk_per_cpu=3, max_boot_disk_per_cpu=2)


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


# --- ProviderConfig, AWSConfig, GCPConfig, AzureConfig ---
def test_provider_config_fields():
    ProviderConfig(job_id="a-job", queue_name="a-queue", region="r", zone="z")
    AWSConfig(access_key="a", secret_key="b")
    GCPConfig(project_id="pid", credentials_file="cf", service_account="sa")
    AzureConfig(subscription_id="sid", tenant_id="tid", client_id="cid", client_secret="cs")


# --- Config class logic ---
def make_config_obj():
    return Config(
        provider="GCP",
        aws=AWSConfig(),
        gcp=GCPConfig(project_id="pid", credentials_file="cf", service_account="sa"),
        azure=AzureConfig(),
        run=RunConfig(),
    )


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_config_overload_from_cli(provider):
    c = make_config_obj()
    cli_args = {"provider": provider, "region": "us-west-1", "architecture": "arm64"}
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.overload_from_cli(cli_args)
        mock_logger.warning.assert_called()
    assert c.provider == provider
    match provider:
        case "AWS":
            assert c.aws.region == "us-west-1"
            assert c.aws.architecture == "ARM64"
        case "GCP":
            assert c.gcp.region == "us-west-1"
            assert c.gcp.architecture == "ARM64"
        case "AZURE":
            assert c.azure.region == "us-west-1"
            assert c.azure.architecture == "ARM64"
    assert c.run.architecture == "ARM64"
    with patch.object(config_mod, "LOGGER") as mock_logger:
        c.overload_from_cli({})
        mock_logger.warning.assert_not_called()
        assert c.provider == provider
        match provider:
            case "AWS":
                assert c.aws.region == "us-west-1"
                assert c.aws.architecture == "ARM64"
            case "GCP":
                assert c.gcp.region == "us-west-1"
                assert c.gcp.architecture == "ARM64"
            case "AZURE":
                assert c.azure.region == "us-west-1"
                assert c.azure.architecture == "ARM64"
        assert c.run.architecture == "ARM64"


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_config_update_run_config_from_provider_config(provider):
    c = make_config_obj()
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


def test_update_run_config_from_provider_config_none():
    c = make_config_obj()
    c.provider = None
    with pytest.raises(ValueError, match="Provider must be specified"):
        c.update_run_config_from_provider_config()


def test_update_run_config_from_provider_config_unsupported():
    c = make_config_obj()
    # Bypass pydantic validation to set an unsupported provider
    object.__setattr__(c, "provider", "FOOBAR")
    with pytest.raises(ValueError, match="Unsupported provider: FOOBAR"):
        c.update_run_config_from_provider_config()


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_config_validate_config(provider):
    c = make_config_obj()
    c.provider = None
    with pytest.raises(ValueError):
        c.validate_config()
    with pytest.raises(pydantic.ValidationError):
        c.provider = "BAD"
    c.provider = provider
    c.validate_config()  # Should not raise


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_config_get_provider_config(provider):
    import pydantic

    c = make_config_obj()
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
    # Test missing config
    c.provider = provider
    match provider:
        case "AWS":
            c.aws = None
        case "GCP":
            c.gcp = None
        case "AZURE":
            c.azure = None
    with pytest.raises(ValueError):
        c.get_provider_config()


def test_get_provider_config_unsupported():
    c = make_config_obj()
    # Bypass pydantic validation to set an unsupported provider
    object.__setattr__(c, "provider", "FOOBAR")
    with pytest.raises(ValueError, match="Unsupported provider: FOOBAR"):
        c.get_provider_config("FOOBAR")


@pytest.mark.parametrize("provider", ["AWS", "GCP", "AZURE"])
def test_config_get_provider_config_queue_name(provider):
    c = make_config_obj()
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
