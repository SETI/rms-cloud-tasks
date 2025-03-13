import asyncio
import sys
import logging
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent))

# Set up logging with debug level to see detailed pricing information
def configure_debug_logging():
    """Configure logging to show detailed debug messages about instance pricing."""
    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add console handler with formatting
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Set specific loggers to DEBUG level
    logging.getLogger('src.cloud_tasks.instance_orchestrator.gcp').setLevel(logging.DEBUG)
    logging.getLogger('src.cloud_tasks.instance_orchestrator.aws').setLevel(logging.DEBUG)
    logging.getLogger('src.cloud_tasks.instance_orchestrator.azure').setLevel(logging.DEBUG)

    return root_logger

async def test_instance_pricing():
    """Test to demonstrate the debug logging for instance pricing information."""
    # Configure debug logging
    logger = configure_debug_logging()
    logger.info("Starting instance pricing debug logging test")

    # Print information about the changes made to all cloud providers
    logger.info("\nThe code has been updated to use Ubuntu 24.04 LTS across all providers:")
    logger.info("  - GCP: Ubuntu 24.04 LTS from ubuntu-os-cloud project")
    logger.info("  - AWS: Ubuntu 24.04 LTS AMI from Canonical (owner 099720109477)")
    logger.info("  - Azure: Ubuntu 24.04 LTS from Canonical (sku 24_04-lts)")

    logger.info("\nDetailed debug logging has been added for instance pricing:")
    logger.info("  - Lists all available instance types that meet requirements")
    logger.info("  - Shows detailed pricing API calls and responses")
    logger.info("  - Provides information about each pricing option considered")
    logger.info("  - Shows the final sorted list of instances by price")

    logger.info("\nExample debug log output for instance pricing (GCP example):")
    logger.info('  2025-04-01 12:34:56,789 - src.cloud_tasks.instance_orchestrator.gcp - DEBUG - Found 25 available machine types in zone us-central1-a')
    logger.info('  2025-04-01 12:34:56,790 - src.cloud_tasks.instance_orchestrator.gcp - DEBUG - Found 15 machine types that meet requirements:')
    logger.info('  2025-04-01 12:34:56,791 - src.cloud_tasks.instance_orchestrator.gcp - DEBUG -   [1] n1-standard-2: 2 vCPU, 7.50 GB memory')
    logger.info('  2025-04-01 12:34:56,792 - src.cloud_tasks.instance_orchestrator.gcp - DEBUG -   [2] n2-standard-2: 2 vCPU, 8.00 GB memory')
    logger.info('  2025-04-01 12:34:56,793 - src.cloud_tasks.instance_orchestrator.gcp - DEBUG - Getting pricing for machine type: n1-standard-2')
    logger.info('  2025-04-01 12:34:57,123 - src.cloud_tasks.instance_orchestrator.gcp - DEBUG - Matching SKU found: n1 standard core in region us-central1')
    logger.info('  2025-04-01 12:34:57,124 - src.cloud_tasks.instance_orchestrator.gcp - DEBUG -   Price: $0.047234 per hour')
    logger.info('  2025-04-01 12:34:58,234 - src.cloud_tasks.instance_orchestrator.gcp - DEBUG - Machine types sorted by price (cheapest first):')
    logger.info('  2025-04-01 12:34:58,235 - src.cloud_tasks.instance_orchestrator.gcp - DEBUG -   1. e2-standard-2: $0.033688/hour')
    logger.info('  2025-04-01 12:34:58,236 - src.cloud_tasks.instance_orchestrator.gcp - DEBUG -   2. n1-standard-2: $0.047234/hour')
    logger.info('  2025-04-01 12:34:58,237 - src.cloud_tasks.instance_orchestrator.gcp - INFO - Selected e2-standard-2 at $0.033688 per hour in us-central1 (us-central1-a)')

    logger.info("\nTo see the debug logs in your code, set the log level to DEBUG:")
    logger.info("  logging.getLogger('src.cloud_tasks.instance_orchestrator').setLevel(logging.DEBUG)")

    # Note about executing with real cloud credentials
    logger.info("\nWith proper cloud credentials, the following would happen:")
    logger.info("  1. Available instance types would be retrieved from each cloud provider")
    logger.info("  2. Instances would be filtered by CPU/memory requirements")
    logger.info("  3. API calls would be made to get current pricing")
    logger.info("  4. The debug logs would show all pricing details")
    logger.info("  5. The optimal instance would be selected based on price")

    # Note: We can't actually run this code without real cloud credentials,
    # but the updated code would show detailed logs when run with proper credentials

async def main():
    """Main entry point."""
    await test_instance_pricing()

if __name__ == "__main__":
    asyncio.run(main())