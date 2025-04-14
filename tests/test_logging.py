"""
A simple test script to verify the millisecond formatting in log timestamps.
"""
import logging
import sys
import os
from pathlib import Path

# Add the src directory to the path so we can import cloud_tasks modules
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.cloud_tasks.common.logging_config import configure_logging


def test_logging_format():
    """Test the logging configuration with millisecond precision."""
    # Configure logging
    configure_logging(level=logging.INFO)

    # Get a logger
    logger = logging.getLogger("test_logger")

    # Log some test messages
    logger.info("This is a test message with millisecond precision")
    logger.warning("This is a warning message")
    logger.error("This is an error message")

    # Log a message with extra data
    task_id = "task-123456"
    logger.info(f"Processing task {task_id}")

    print("\nLog testing complete. The timestamps above should show millisecond precision (3 digits).")


if __name__ == "__main__":
    test_logging_format()