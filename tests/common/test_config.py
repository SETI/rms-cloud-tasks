"""
Unit tests for the configuration system.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Add the src directory to the path so we can import cloud_tasks modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.cloud_tasks.common.config import (
    load_config, get_provider_config, get_run_config,
    ConfigError, load_startup_script, Config, ProviderConfig
)


class TestConfigSystem(unittest.TestCase):
    """Test the configuration loading and processing."""

    def test_load_config(self):
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
            """)
            tmp_path = tmp.name

        try:
            config = load_config(tmp_path)
            # Test that config is a Config object
            self.assertIsInstance(config, Config)

            # Test attribute access
            self.assertIn('run', config)
            self.assertIn('aws', config)
            self.assertEqual(config.run.cpu, 2)
            self.assertEqual(config.aws.access_key, 'test-key')

            # Test dictionary access still works
            self.assertEqual(config['run']['cpu'], 2)
            self.assertEqual(config['aws']['access_key'], 'test-key')
        finally:
            # Clean up
            os.unlink(tmp_path)

    def test_config_functionality(self):
        """Test Config behavior specifically."""
        # Create a Config and test attribute access
        config = Config({
            'key1': 'value1',
            'key2': {
                'nested_key': 'nested_value'
            },
            'key3': [1, 2, 3]
        })

        # Test attribute access
        self.assertEqual(config.key1, 'value1')
        self.assertEqual(config['key1'], 'value1')

        # Test nested dictionaries are also converted
        self.assertIsInstance(config.key2, dict)
        self.assertEqual(config.key2['nested_key'], 'nested_value')

        # Test assignment
        config.key4 = 'new_value'
        self.assertEqual(config['key4'], 'new_value')
        self.assertEqual(config.key4, 'new_value')

        # Test regular dictionary operations
        self.assertIn('key1', config)
        self.assertEqual(len(config), 4)
        self.assertEqual(sorted(list(config.keys())),
                         sorted(['key1', 'key2', 'key3', 'key4']))

    def test_get_provider_config(self):
        """Test extracting provider-specific configuration."""
        # Create config with provider configs already converted to ProviderConfig objects
        aws_provider = ProviderConfig('aws', {
            'access_key': 'test-key',
            'secret_key': 'test-secret',
            'region': 'us-west-2'
        })

        gcp_provider = ProviderConfig('gcp', {
            'project_id': 'test-project'
        })

        config = Config({
            'aws': aws_provider,
            'gcp': gcp_provider
        })

        # Test getting AWS provider config
        aws_config = get_provider_config(config, 'aws')
        self.assertIsInstance(aws_config, ProviderConfig)
        self.assertEqual(aws_config.access_key, 'test-key')
        self.assertEqual(aws_config.provider_name, 'aws')

        # Test getting GCP provider config
        gcp_config = get_provider_config(config, 'gcp')
        self.assertIsInstance(gcp_config, ProviderConfig)
        self.assertEqual(gcp_config.project_id, 'test-project')
        self.assertEqual(gcp_config.provider_name, 'gcp')

    def test_config_get_provider_method(self):
        """Test the get_provider method on Config class."""
        # Create config with provider configs already converted to ProviderConfig objects
        aws_provider = ProviderConfig('aws', {
            'access_key': 'test-key',
            'secret_key': 'test-secret',
            'region': 'us-west-2'
        })

        gcp_provider = ProviderConfig('gcp', {
            'project_id': 'test-project'
        })

        config = Config({
            'aws': aws_provider,
            'gcp': gcp_provider
        })

        # Test getting AWS provider config
        aws_config = config.get_provider('aws')
        self.assertIsInstance(aws_config, ProviderConfig)
        self.assertEqual(aws_config.provider_name, 'aws')
        self.assertEqual(aws_config.access_key, 'test-key')

        # Test getting GCP provider config
        gcp_config = config.get_provider('gcp')
        self.assertIsInstance(gcp_config, ProviderConfig)
        self.assertEqual(gcp_config.provider_name, 'gcp')
        self.assertEqual(gcp_config.project_id, 'test-project')

        # Test invalid provider
        with self.assertRaises(ConfigError):
            config.get_provider('invalid_provider')

    def test_provider_config_validation(self):
        """Test validation of provider configurations."""
        # Valid AWS config
        aws_config = ProviderConfig('aws', {
            'access_key': 'test-key',
            'secret_key': 'test-secret',
            'region': 'us-west-2'
        })
        aws_config.validate()  # Should not raise an exception

        # Invalid AWS config (missing region)
        invalid_aws = ProviderConfig('aws', {
            'access_key': 'test-key',
            'secret_key': 'test-secret'
        })
        with self.assertRaises(ConfigError):
            invalid_aws.validate()

        # Valid GCP config
        gcp_config = ProviderConfig('gcp', {
            'project_id': 'test-project'
        })
        gcp_config.validate()  # Should not raise an exception

        # Invalid GCP config
        invalid_gcp = ProviderConfig('gcp', {})
        with self.assertRaises(ConfigError):
            invalid_gcp.validate()

        # Unsupported provider
        unknown = ProviderConfig('unknown', {})
        with self.assertRaises(ConfigError):
            unknown.validate()

    def test_get_run_config_defaults(self):
        """Test getting run configuration with defaults only."""
        config = Config({})
        run_config = get_run_config(config, 'aws')

        # Check default values and that it's a Config
        self.assertIsInstance(run_config, Config)
        self.assertEqual(run_config.cpu, 1)
        self.assertEqual(run_config.memory_gb, 2)
        self.assertEqual(run_config.disk_gb, 10)
        self.assertEqual(run_config.image, 'ubuntu-2404-lts')
        self.assertEqual(run_config.startup_script, '')

    def test_get_run_config_global_overrides(self):
        """Test global run config overrides defaults."""
        config = Config({
            'run': {
                'cpu': 4,
                'memory_gb': 8,
                'disk_gb': 100,
                'image': 'custom-image',
                'startup_script': '#!/bin/bash\necho test'
            }
        })

        run_config = get_run_config(config, 'aws')

        # Check overridden values and that it's a Config
        self.assertIsInstance(run_config, Config)
        self.assertEqual(run_config.cpu, 4)
        self.assertEqual(run_config.memory_gb, 8)
        self.assertEqual(run_config.disk_gb, 100)
        self.assertEqual(run_config.image, 'custom-image')
        self.assertEqual(run_config.startup_script, '#!/bin/bash\necho test')

    def test_get_run_config_provider_overrides(self):
        """Test provider-specific config overrides global config."""
        config = Config({
            'run': {
                'cpu': 4,
                'memory_gb': 8,
                'disk_gb': 100,
                'image': 'global-image',
                'startup_script': '#!/bin/bash\necho global'
            },
            'aws': {
                'access_key': 'test-key',
                'secret_key': 'test-secret',
                'region': 'us-west-2',
                'cpu': 8,
                'image': 'aws-image',
                'startup_script': '#!/bin/bash\necho aws'
            }
        })

        run_config = get_run_config(config, 'aws')

        # Check that AWS overrides apply and that it's a Config
        self.assertIsInstance(run_config, Config)
        self.assertEqual(run_config.cpu, 8)
        self.assertEqual(run_config.memory_gb, 8)  # From global
        self.assertEqual(run_config.disk_gb, 100)  # From global
        self.assertEqual(run_config.image, 'aws-image')
        self.assertEqual(run_config.startup_script, '#!/bin/bash\necho aws')

    def test_get_run_config_cli_overrides(self):
        """Test CLI args override all other settings."""
        config = Config({
            'run': {
                'cpu': 4,
                'memory_gb': 8,
                'disk_gb': 100,
                'image': 'global-image',
                'startup_script': '#!/bin/bash\necho global'
            },
            'aws': {
                'cpu': 8,
                'image': 'aws-image',
                'startup_script': '#!/bin/bash\necho aws'
            }
        })

        cli_args = {
            'cpu': 16,
            'memory': 32,
            'disk': 500,
            'image': 'cli-image',
        }

        run_config = get_run_config(config, 'aws', cli_args)

        # Check that CLI args override everything and that it's a Config
        self.assertIsInstance(run_config, Config)
        self.assertEqual(run_config.cpu, 16)
        self.assertEqual(run_config.memory_gb, 32)
        self.assertEqual(run_config.disk_gb, 500)
        self.assertEqual(run_config.image, 'cli-image')
        self.assertEqual(run_config.startup_script, '#!/bin/bash\necho aws')  # From provider

    def test_load_startup_script(self):
        """Test loading a startup script from a file."""
        script_content = '#!/bin/bash\necho "This is a test script"'

        # Create a temporary script file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as tmp:
            tmp.write(script_content)
            tmp_path = tmp.name

        try:
            # Test loading the script
            loaded_script = load_startup_script(tmp_path)
            self.assertEqual(loaded_script, script_content)

            # Test error on nonexistent file
            with self.assertRaises(ConfigError):
                load_startup_script('/nonexistent/path/to/script.sh')
        finally:
            # Clean up
            os.unlink(tmp_path)

    def test_load_config_converts_providers(self):
        """Test that load_config automatically converts provider configs to ProviderConfig objects."""
        # Create a temporary config file with provider configurations
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            tmp.write("""
            # Test configuration with provider configs
            aws:
              access_key: test-key
              secret_key: test-secret
              region: us-west-2

            gcp:
              project_id: test-project

            azure:
              subscription_id: test-subscription
              tenant_id: test-tenant
              client_id: test-client
              client_secret: test-secret
              location: eastus
            """)
            tmp_path = tmp.name

        try:
            # Load the configuration
            config = load_config(tmp_path)

            # Verify that all provider configs were converted to ProviderConfig objects
            self.assertIsInstance(config.aws, ProviderConfig)
            self.assertEqual(config.aws.provider_name, 'aws')
            self.assertEqual(config.aws.access_key, 'test-key')

            self.assertIsInstance(config.gcp, ProviderConfig)
            self.assertEqual(config.gcp.provider_name, 'gcp')
            self.assertEqual(config.gcp.project_id, 'test-project')

            self.assertIsInstance(config.azure, ProviderConfig)
            self.assertEqual(config.azure.provider_name, 'azure')
            self.assertEqual(config.azure.location, 'eastus')
        finally:
            # Clean up
            os.unlink(tmp_path)


if __name__ == '__main__':
    unittest.main()