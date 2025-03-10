#!/usr/bin/env python3
"""
Test script that demonstrates region selection for cloud instance orchestration.
"""

import asyncio
import argparse
import yaml
import logging

from cloud_tasks.instance_orchestrator.orchestrator import InstanceOrchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S.%f'  # Explicitly use period for fractions
)

logger = logging.getLogger(__name__)


async def test_region_selection(provider, config_file, specify_region=None, use_spot=False):
    """
    Test the region selection feature for a given cloud provider.

    Args:
        provider: Cloud provider to test (aws, gcp, or azure)
        config_file: Path to the config file with provider credentials
        specify_region: Optional region to specify (if None, automatic selection is used)
        use_spot: Whether to use spot/preemptible instances
    """
    # Load provider configuration
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    # If region is specified on command line, include it in the log but don't modify config
    # The InstanceOrchestrator will handle this correctly based on the region parameter
    if specify_region:
        logger.info(f"Using specified region from command line: {specify_region}")
    elif provider == 'aws' and 'region' in config.get(provider, {}):
        logger.info(f"Using region from config: {config[provider]['region']}")
    elif provider == 'gcp' and 'region' in config.get(provider, {}):
        logger.info(f"Using region from config: {config[provider]['region']}")
    elif provider == 'azure' and 'location' in config.get(provider, {}):
        logger.info(f"Using location from config: {config[provider]['location']}")
    else:
        logger.info("No region specified - will determine cheapest region")

    # Create orchestrator with the full config
    orchestrator = InstanceOrchestrator(
        provider=provider,
        job_id=f"region-test-{provider}",
        cpu_required=2,
        memory_required_gb=4,
        disk_required_gb=20,
        min_instances=0,  # Don't actually start instances
        max_instances=0,  # Don't actually start instances
        use_spot_instances=use_spot,
        region=specify_region,  # Pass the region from command line if specified
        queue_name=f"region-test-{provider}-queue",  # Add queue name
        config=config  # Pass the full config dictionary
    )

    # Just run instance type selection - this will trigger region selection if needed
    await orchestrator.start()

    # Get optimal instance type - this will trigger region selection if needed
    instance_type = await orchestrator.instance_manager.get_optimal_instance_type(
        cpu_required=2,
        memory_required_gb=4,
        disk_required_gb=20,
        use_spot=use_spot
    )

    # Print selected region and instance type
    if provider == 'azure':
        logger.info(f"Selected location: {orchestrator.instance_manager.location}")
    else:
        logger.info(f"Selected region: {orchestrator.instance_manager.region}")

    logger.info(f"Selected instance type: {instance_type}")

    # Clean up
    await orchestrator.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test region selection feature')
    parser.add_argument('--provider', required=True, choices=['aws', 'gcp', 'azure'],
                        help='Cloud provider to test')
    parser.add_argument('--config', required=True, help='Path to config file')
    parser.add_argument('--region', help='Specific region to use (optional)')
    parser.add_argument('--use-spot', action='store_true',
                        help='Use spot/preemptible instances')

    args = parser.parse_args()

    # Run the test
    asyncio.run(test_region_selection(
        args.provider,
        args.config,
        args.region,
        args.use_spot
    ))