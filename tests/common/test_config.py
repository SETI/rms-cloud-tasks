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
    load_config, get_provider_config, get_run_config,
    ConfigError, load_startup_script, Config, ProviderConfig, AttrDict
)


def test_load_config():
    """Test loading a configuration file."""
    # Create a temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
        tmp.write("""
        # Test configuration
        run:
          cpu: 2
          memory_gb: 4
          disk_gb: 20

        aws:
          access_key: test-key
          secret_key: test-secret
          region: us-west-2

        gcp:
          project_id: test-project
        """)
        tmp_path = tmp.name

    try:
        # Load the config
        config = load_config(tmp_path)

        # Check that it's a Config instance
        assert isinstance(config, Config)

        # Check run config values
        assert config['run']['cpu'] == 2
        assert config['run']['memory_gb'] == 4
        assert config['run']['disk_gb'] == 20

        # Check that provider configs are ProviderConfig instances
        assert isinstance(config['aws'], ProviderConfig)
        assert isinstance(config['gcp'], ProviderConfig)

        # Check AWS config values
        assert config['aws']['access_key'] == 'test-key'
        assert config['aws']['secret_key'] == 'test-secret'
        assert config['aws']['region'] == 'us-west-2'

        # Check GCP config values
        assert config['gcp']['project_id'] == 'test-project'
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_config_functionality():
    """Test the Config class functionality."""
    # Create a Config instance
    config = Config({
        'key1': 'value1',
        'key2': {
            'nested_key': 'nested_value'
        },
        'aws': {
            'region': 'us-west-2'
        }
    })

    # Test dictionary-style access
    assert config['key1'] == 'value1'
    assert config['key2']['nested_key'] == 'nested_value'

    # Test attribute-style access
    assert config.key1 == 'value1'
    assert config.key2.nested_key == 'nested_value'

    # Test that __dict__ works
    assert 'key1' in config.__dict__
    assert 'key2' in config.__dict__

    # Test that nested dictionaries are converted to AttrDict objects
    assert isinstance(config.key2, AttrDict)

    # Test setting values
    config.key3 = 'value3'
    assert config.key3 == 'value3'
    assert config['key3'] == 'value3'

    # Test setting nested values using dictionary assignment to activate __setitem__
    config['key4'] = {'nested_key': 'new_value'}
    assert isinstance(config.key4, AttrDict)
    assert config.key4.nested_key == 'new_value'

    # Test overriding values
    config.key1 = 'new_value1'
    assert config.key1 == 'new_value1'
    assert config['key1'] == 'new_value1'


def test_get_provider_config():
    """Test the get_provider_config function."""
    # Create a Config instance
    config = Config({
        'aws': ProviderConfig('aws', {
            'access_key': 'test-key',
            'secret_key': 'test-secret',
            'region': 'us-west-2'
        }),
        'gcp': ProviderConfig('gcp', {
            'project_id': 'test-project'
        })
    })

    # Get AWS config
    aws_config = get_provider_config(config, 'aws')
    assert isinstance(aws_config, ProviderConfig)
    assert aws_config.provider_name == 'aws'
    assert aws_config['access_key'] == 'test-key'
    assert aws_config['secret_key'] == 'test-secret'
    assert aws_config['region'] == 'us-west-2'

    # Get GCP config
    gcp_config = get_provider_config(config, 'gcp')
    assert isinstance(gcp_config, ProviderConfig)
    assert gcp_config.provider_name == 'gcp'
    assert gcp_config['project_id'] == 'test-project'

    # Test getting non-existent provider
    with pytest.raises(ConfigError) as excinfo:
        get_provider_config(config, 'azure')
    assert "Missing configuration for cloud provider: azure" in str(excinfo.value)


def test_config_get_provider_method():
    """Test the Config.get_provider method."""
    # Create a Config instance
    config = Config({
        'aws': ProviderConfig('aws', {
            'access_key': 'test-key',
            'secret_key': 'test-secret',
            'region': 'us-west-2'
        }),
        'gcp': ProviderConfig('gcp', {
            'project_id': 'test-project'
        }),
        'not_provider': {
            'key': 'value'
        }
    })

    # Test get_provider with valid providers
    aws_config = config.get_provider('aws')
    assert isinstance(aws_config, ProviderConfig)
    assert aws_config.provider_name == 'aws'

    gcp_config = config.get_provider('gcp')
    assert isinstance(gcp_config, ProviderConfig)
    assert gcp_config.provider_name == 'gcp'

    # Test get_provider with non-existent provider
    with pytest.raises(ConfigError) as excinfo:
        config.get_provider('azure')
    assert "Missing configuration for cloud provider: azure" in str(excinfo.value)

    # Test get_provider with non-ProviderConfig section
    config['not_provider'] = {'key': 'value'}  # Not a ProviderConfig
    with pytest.raises(ConfigError) as excinfo:
        config.get_provider('not_provider')
    assert "Provider configuration for not_provider is not a ProviderConfig instance" in str(excinfo.value)


def test_provider_config_validation():
    """Test validation of ProviderConfig objects."""
    # Valid AWS config
    aws_config = ProviderConfig('aws', {
        'access_key': 'test-key',
        'secret_key': 'test-secret',
        'region': 'us-west-2'
    })
    aws_config.validate()  # Should not raise an error

    # Invalid AWS config (missing region)
    aws_config_invalid = ProviderConfig('aws', {
        'access_key': 'test-key',
        'secret_key': 'test-secret'
    })
    with pytest.raises(ConfigError) as excinfo:
        aws_config_invalid.validate()
    assert "Missing required configuration field for aws: region" in str(excinfo.value)

    # Valid GCP config
    gcp_config = ProviderConfig('gcp', {
        'project_id': 'test-project'
    })
    gcp_config.validate()  # Should not raise an error

    # Invalid GCP config (missing project_id)
    gcp_config_invalid = ProviderConfig('gcp', {})
    with pytest.raises(ConfigError) as excinfo:
        gcp_config_invalid.validate()
    assert "Missing required configuration field for gcp: project_id" in str(excinfo.value)

    # Valid Azure config
    azure_config = ProviderConfig('azure', {
        'subscription_id': 'test-sub',
        'tenant_id': 'test-tenant',
        'client_id': 'test-client',
        'client_secret': 'test-secret'
    })
    azure_config.validate()  # Should not raise an error

    # Invalid Azure config (missing client_secret)
    azure_config_invalid = ProviderConfig('azure', {
        'subscription_id': 'test-sub',
        'tenant_id': 'test-tenant',
        'client_id': 'test-client'
    })
    with pytest.raises(ConfigError) as excinfo:
        azure_config_invalid.validate()
    assert "Missing required configuration field for azure: client_secret" in str(excinfo.value)

    # Unsupported provider
    invalid_provider = ProviderConfig('invalid', {})
    with pytest.raises(ConfigError) as excinfo:
        invalid_provider.validate()
    assert "Unsupported cloud provider: invalid" in str(excinfo.value)


def test_get_run_config_defaults():
    """Test that get_run_config returns default values when not specified."""
    # Create a minimal Config
    config = Config({})

    # Get run config with defaults
    run_config = get_run_config(config, 'aws')

    # Check default values
    assert run_config['cpu'] == 1
    assert run_config['memory_gb'] == 2
    assert run_config['disk_gb'] == 10
    assert run_config['image'] == 'ubuntu-2404-lts'
    assert run_config['startup_script'] == ''


def test_get_run_config_global_overrides():
    """Test that get_run_config uses global run settings."""
    # Create a Config with global run settings
    config = Config({
        'run': {
            'cpu': 4,
            'memory_gb': 8,
            'disk_gb': 50,
            'image': 'custom-image',
            'startup_script': 'echo "Custom script"'
        }
    })

    # Get run config with global overrides
    run_config = get_run_config(config, 'aws')

    # Check global values are used
    assert run_config['cpu'] == 4
    assert run_config['memory_gb'] == 8
    assert run_config['disk_gb'] == 50
    assert run_config['image'] == 'custom-image'
    assert run_config['startup_script'] == 'echo "Custom script"'

    # Check that it's a Config instance
    assert isinstance(run_config, Config)


def test_get_run_config_provider_overrides():
    """Test that get_run_config uses provider-specific settings."""
    # Create a Config with global and provider-specific settings
    config = Config({
        'run': {
            'cpu': 2,
            'memory_gb': 4,
            'disk_gb': 20,
            'image': 'global-image',
            'startup_script': 'echo "Global script"'
        },
        'aws': ProviderConfig('aws', {
            'access_key': 'test-key',
            'secret_key': 'test-secret',
            'region': 'us-west-2',
            'cpu': 8,
            'memory_gb': 16,
            'image': 'aws-image'
        })
    })

    # Get run config with provider overrides
    run_config = get_run_config(config, 'aws')

    # Check provider values are used where specified
    assert run_config['cpu'] == 8  # Overridden by provider
    assert run_config['memory_gb'] == 16  # Overridden by provider
    assert run_config['disk_gb'] == 20  # From global
    assert run_config['image'] == 'aws-image'  # Overridden by provider
    assert run_config['startup_script'] == 'echo "Global script"'  # From global


def test_get_run_config_cli_overrides():
    """Test that get_run_config uses CLI argument overrides."""
    # Create a Config with global and provider-specific settings
    config = Config({
        'run': {
            'cpu': 2,
            'memory_gb': 4,
            'disk_gb': 20,
            'image': 'global-image',
            'startup_script': 'echo "Global script"'
        },
        'aws': ProviderConfig('aws', {
            'access_key': 'test-key',
            'secret_key': 'test-secret',
            'region': 'us-west-2',
            'cpu': 8,
            'memory_gb': 16,
            'image': 'aws-image'
        })
    })

    # Create CLI args
    cli_args = {
        'cpu': 16,
        'memory': 32,
        'disk': 100,
        'image': 'cli-image'
    }

    # Get run config with CLI overrides
    run_config = get_run_config(config, 'aws', cli_args)

    # Check CLI values are used where specified
    assert run_config['cpu'] == 16  # Overridden by CLI
    assert run_config['memory_gb'] == 32  # Overridden by CLI
    assert run_config['disk_gb'] == 100  # Overridden by CLI
    assert run_config['image'] == 'cli-image'  # Overridden by CLI
    assert run_config['startup_script'] == 'echo "Global script"'  # Not overridden

    # Test with startup script file
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
        tmp.write('echo "CLI script"')
        tmp_path = tmp.name

    try:
        # Add startup script file to CLI args
        cli_args['startup_script_file'] = tmp_path

        # Get run config with startup script file
        run_config = get_run_config(config, 'aws', cli_args)

        # Check startup script is loaded from file
        assert run_config['startup_script'] == 'echo "CLI script"'
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_load_startup_script():
    """Test loading a startup script from a file."""
    # Create a temporary script file
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
        tmp.write('#!/bin/bash\necho "Hello, World!"')
        tmp_path = tmp.name

    try:
        # Load the script
        script = load_startup_script(tmp_path)

        # Check the content
        assert script == '#!/bin/bash\necho "Hello, World!"'

        # Test with non-existent file
        with pytest.raises(ConfigError) as excinfo:
            load_startup_script('/path/to/nonexistent/file')
        assert "Startup script file not found" in str(excinfo.value)
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_load_config_converts_providers():
    """Test that load_config converts provider sections to ProviderConfig instances."""
    # Create a temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
        tmp.write("""
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
        """)
        tmp_path = tmp.name

    try:
        # Load the config
        config = load_config(tmp_path)

        # Check that provider sections are converted to ProviderConfig instances
        assert isinstance(config['aws'], ProviderConfig)
        assert config['aws'].provider_name == 'aws'

        assert isinstance(config['gcp'], ProviderConfig)
        assert config['gcp'].provider_name == 'gcp'

        assert isinstance(config['azure'], ProviderConfig)
        assert config['azure'].provider_name == 'azure'
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)