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
    ConfigError, load_startup_script
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
            self.assertIn('run', config)
            self.assertIn('aws', config)
            self.assertEqual(config['run']['cpu'], 2)
            self.assertEqual(config['aws']['access_key'], 'test-key')
        finally:
            # Clean up
            os.unlink(tmp_path)

    def test_get_provider_config(self):
        """Test extracting provider-specific configuration."""
        config = {
            'aws': {
                'access_key': 'test-key',
                'secret_key': 'test-secret',
                'region': 'us-west-2'
            },
            'gcp': {
                'project_id': 'test-project'
            }
        }

        aws_config = get_provider_config(config, 'aws')
        self.assertEqual(aws_config['access_key'], 'test-key')

        gcp_config = get_provider_config(config, 'gcp')
        self.assertEqual(gcp_config['project_id'], 'test-project')

    def test_get_run_config_defaults(self):
        """Test getting run configuration with defaults only."""
        config = {}
        run_config = get_run_config(config, 'aws')

        # Check default values
        self.assertEqual(run_config['cpu'], 1)
        self.assertEqual(run_config['memory_gb'], 2)
        self.assertEqual(run_config['disk_gb'], 10)
        self.assertEqual(run_config['image'], 'ubuntu-2404-lts')
        self.assertEqual(run_config['startup_script'], '')

    def test_get_run_config_global_overrides(self):
        """Test global run config overrides defaults."""
        config = {
            'run': {
                'cpu': 4,
                'memory_gb': 8,
                'disk_gb': 100,
                'image': 'custom-image',
                'startup_script': '#!/bin/bash\necho test'
            }
        }

        run_config = get_run_config(config, 'aws')

        # Check overridden values
        self.assertEqual(run_config['cpu'], 4)
        self.assertEqual(run_config['memory_gb'], 8)
        self.assertEqual(run_config['disk_gb'], 100)
        self.assertEqual(run_config['image'], 'custom-image')
        self.assertEqual(run_config['startup_script'], '#!/bin/bash\necho test')

    def test_get_run_config_provider_overrides(self):
        """Test provider-specific config overrides global config."""
        config = {
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
        }

        run_config = get_run_config(config, 'aws')

        # Check that AWS overrides apply
        self.assertEqual(run_config['cpu'], 8)
        self.assertEqual(run_config['memory_gb'], 8)  # From global
        self.assertEqual(run_config['disk_gb'], 100)  # From global
        self.assertEqual(run_config['image'], 'aws-image')
        self.assertEqual(run_config['startup_script'], '#!/bin/bash\necho aws')

    def test_get_run_config_cli_overrides(self):
        """Test CLI args override all other settings."""
        config = {
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
        }

        cli_args = {
            'cpu': 16,
            'memory': 32,
            'disk': 500,
            'image': 'cli-image',
        }

        run_config = get_run_config(config, 'aws', cli_args)

        # Check that CLI args override everything
        self.assertEqual(run_config['cpu'], 16)
        self.assertEqual(run_config['memory_gb'], 32)
        self.assertEqual(run_config['disk_gb'], 500)
        self.assertEqual(run_config['image'], 'cli-image')
        self.assertEqual(run_config['startup_script'], '#!/bin/bash\necho aws')  # From provider

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


if __name__ == '__main__':
    unittest.main()