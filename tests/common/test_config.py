"""
Unit tests for the configuration system.
"""

import os
import sys
import tempfile
import pytest
from pathlib import Path

# Add the src directory to the path so we can import cloud_tasks modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.cloud_tasks.common.config import (
    load_config,
    get_run_config,
    load_startup_script,
    Config,
    ProviderConfig,
    AWSConfig,
    GCPConfig,
    AzureConfig,
)


def test_load_config():
    """Test loading a configuration file."""
    # Create a temporary config file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        tmp.write(
            """
        # Test configuration
        aws:
          access_key: test-key
          secret_key: test-secret
          region: us-west-2
          cpu: 2
          memory_gb: 4
          disk_gb: 20

        gcp:
          project_id: test-project
          cpu: 8
          memory_gb: 16
          disk_gb: 32
        """
        )
        tmp_path = tmp.name

    try:
        # Load the config
        config = load_config(tmp_path)

        # Check that it's a Config instance
        assert isinstance(config, Config)

        # Check run config values
        assert config.aws.cpu == 2
        assert config.aws.memory_gb == 4
        assert config.aws.disk_gb == 20
        assert config.gcp.cpu == 8
        assert config.gcp.memory_gb == 16
        assert config.gcp.disk_gb == 32

        # Check that provider configs are ProviderConfig instances
        assert isinstance(config.aws, ProviderConfig)
        assert isinstance(config.gcp, ProviderConfig)

        # Check AWS config values
        assert config.aws.access_key == "test-key"
        assert config.aws.secret_key == "test-secret"
        assert config.aws.region == "us-west-2"

        # Check GCP config values
        assert config.gcp.project_id == "test-project"
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_get_provider_config():
    """Test the get_provider_config function."""
    # Create a Config instance
    config = Config(
        **{
            "aws": {"access_key": "test-key", "secret_key": "test-secret", "region": "us-west-2"},
            "gcp": {"project_id": "test-project"},
        }
    )

    # Get AWS config
    aws_config = config.get_provider_config("aws")
    assert isinstance(aws_config, AWSConfig)
    assert aws_config.access_key == "test-key"
    assert aws_config.secret_key == "test-secret"
    assert aws_config.region == "us-west-2"

    # Get GCP config
    gcp_config = config.get_provider_config("gcp")
    assert isinstance(gcp_config, ProviderConfig)
    assert gcp_config.project_id == "test-project"

    # Test getting non-existent provider
    with pytest.raises(ValueError) as excinfo:
        config.get_provider_config("azure")
    assert "Provider configuration not found for azure" in str(excinfo.value)


def test_config_get_provider_method():
    """Test the Config.get_provider method."""
    # Create a Config instance
    config = Config(
        **{
            "aws": {"access_key": "test-key", "secret_key": "test-secret", "region": "us-west-2"},
            "gcp": {"project_id": "test-project"},
        }
    )

    # Test get_provider with valid providers
    aws_config = config.get_provider_config("aws")
    assert isinstance(aws_config, AWSConfig)

    gcp_config = config.get_provider_config("gcp")
    assert isinstance(gcp_config, ProviderConfig)

    # Test get_provider with non-existent provider
    with pytest.raises(ValueError) as excinfo:
        config.get_provider_config("azure")
    assert "Provider configuration not found for azure" in str(excinfo.value)

    # Test get_provider with non-ProviderConfig section
    with pytest.raises(ValueError) as excinfo:
        config.get_provider_config("not_provider")
    assert "Unsupported provider: not_provider" in str(excinfo.value)


@pytest.mark.skip("skip")  # TODO: Fix this test
def test_get_run_config_cli_overrides():
    """Test that get_run_config uses CLI argument overrides."""
    # Create a Config with global and provider-specific settings
    config = Config(
        {
            "run": {
                "cpu": 2,
                "memory_gb": 4,
                "disk_gb": 20,
                "image": "global-image",
                "startup_script": 'echo "Global script"',
            },
            "aws": ProviderConfig(
                "aws",
                {
                    "access_key": "test-key",
                    "secret_key": "test-secret",
                    "region": "us-west-2",
                    "cpu": 8,
                    "memory_gb": 16,
                    "image": "aws-image",
                },
            ),
        }
    )

    # Create CLI args
    cli_args = {"cpu": 16, "memory": 32, "disk": 100, "image": "cli-image"}

    # Get run config with CLI overrides
    run_config = get_run_config(config, "aws", cli_args)

    # Check CLI values are used where specified
    assert run_config["cpu"] == 16  # Overridden by CLI
    assert run_config["memory_gb"] == 32  # Overridden by CLI
    assert run_config["disk_gb"] == 100  # Overridden by CLI
    assert run_config["image"] == "cli-image"  # Overridden by CLI
    assert run_config["startup_script"] == 'echo "Global script"'  # Not overridden

    # Test with startup script file
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
        tmp.write('echo "CLI script"')
        tmp_path = tmp.name

    try:
        # Add startup script file to CLI args
        cli_args["startup_script_file"] = tmp_path

        # Get run config with startup script file
        run_config = get_run_config(config, "aws", cli_args)

        # Check startup script is loaded from file
        assert run_config["startup_script"] == 'echo "CLI script"'
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


@pytest.mark.skip("skip")  # TODO: Fix this test
def test_load_startup_script():
    """Test loading a startup script from a file."""
    # Create a temporary script file
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
        tmp.write('#!/bin/bash\necho "Hello, World!"')
        tmp_path = tmp.name

    try:
        # Load the script
        script = load_startup_script(tmp_path)

        # Check the content
        assert script == '#!/bin/bash\necho "Hello, World!"'

        # Test with non-existent file
        with pytest.raises(ValueError) as excinfo:
            load_startup_script("/path/to/nonexistent/file")
        assert "Startup script file not found" in str(excinfo.value)
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_load_config_converts_providers():
    """Test that load_config converts provider sections to ProviderConfig instances."""
    # Create a temporary config file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        tmp.write(
            """
        aws:
          access_key: test-key
          secret_key: test-secret
          region: us-west-2

        gcp:
          project_id: test-project

        azure:
          subscription_id: test-sub
          tenant_id: test-tenant
          client_id: test-client
          client_secret: test-secret
        """
        )
        tmp_path = tmp.name

    try:
        # Load the config
        config = load_config(tmp_path)

        # Check that provider sections are converted to ProviderConfig instances
        assert isinstance(config.aws, AWSConfig)
        assert isinstance(config.gcp, GCPConfig)
        assert isinstance(config.azure, AzureConfig)
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


# TODO Need much better tests for the config system
