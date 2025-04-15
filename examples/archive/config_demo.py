#!/usr/bin/env python3
"""
Example demonstrating the Config functionality in Cloud Tasks.

This script shows how to use attribute-style access for configuration values,
making your code more readable and intuitive.
"""
import sys
import os
from pathlib import Path

# Add the src directory to the path so we can import cloud_tasks modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.cloud_tasks.common.config import Config, load_config


def main():
    """Demonstrate Config functionality."""
    # Load the example configuration
    config_path = Path(__file__).parent / "config.yaml"

    print(f"Loading configuration from {config_path}")
    config = load_config(str(config_path))

    # Print configuration using attribute-style access
    print("\n=== Configuration Access Examples ===\n")

    # Global run configuration
    print("Global run configuration:")
    print(f"  CPU: {config.run.cpu}")
    print(f"  Memory: {config.run.memory_gb} GB")
    print(f"  Disk: {config.run.disk_gb} GB")
    print(f"  Image: {config.run.image}")

    # AWS provider-specific configuration
    print("\nAWS configuration:")
    print(f"  Region: {config.aws.region}")
    print(f"  CPU: {config.aws.cpu}")  # Overrides global setting
    print(f"  Image: {config.aws.image}")

    # Create a combination configuration with overrides
    run_config = Config({
        'cpu': config.aws.cpu,  # From AWS provider
        'memory_gb': config.run.memory_gb,  # From global run config
        'disk_gb': 100,  # Custom override
        'image': config.aws.image,  # From AWS provider
    })

    print("\nCombined configuration with overrides:")
    print(f"  CPU: {run_config.cpu}")
    print(f"  Memory: {run_config.memory_gb} GB")
    print(f"  Disk: {run_config.disk_gb} GB")
    print(f"  Image: {run_config.image}")

    # Demonstrate dynamic updates
    print("\n=== Dynamic Updates ===\n")

    # Add a new attribute
    run_config.max_instances = 10
    print(f"Added max_instances: {run_config.max_instances}")

    # Update an existing attribute
    run_config.cpu = 16
    print(f"Updated CPU: {run_config.cpu}")

    # Add a nested configuration
    run_config.network = Config({
        'vpc_id': 'vpc-12345',
        'subnet_id': 'subnet-67890',
        'security_groups': ['sg-abcdef']
    })

    print("\nNested configuration:")
    print(f"  VPC ID: {run_config.network.vpc_id}")
    print(f"  Subnet ID: {run_config.network.subnet_id}")
    print(f"  Security Groups: {run_config.network.security_groups}")

    # Dictionary-style access still works
    print("\n=== Dictionary-style access still works ===\n")
    print(f"  CPU (dict-style): {run_config['cpu']}")
    print(f"  VPC ID (dict-style): {run_config['network']['vpc_id']}")


if __name__ == "__main__":
    main()