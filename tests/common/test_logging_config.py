"""
Unit tests for the custom logging configuration.
"""
import logging
import re
import io
import sys
from pathlib import Path
from unittest.mock import patch

# Add the src directory to the path so we can import cloud_tasks modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.cloud_tasks.common.logging_config import configure_logging, MicrosecondFormatter


def test_microsecond_formatter():
    """Test that MicrosecondFormatter correctly formats timestamps with millisecond precision."""
    # Create a formatter with the microsecond format
    formatter = MicrosecondFormatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S.%f'
    )

    # Create a log record with a known timestamp
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test message",
        args=(),
        exc_info=None
    )

    # Format the record
    formatted = formatter.format(record)

    # Check that the timestamp has millisecond precision (3 digits after the dot)
    timestamp_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}'
    assert re.search(timestamp_pattern, formatted), f"Timestamp format incorrect: {formatted}"


def test_configure_logging():
    """Test that configure_logging properly sets up the logging system."""
    # Set up a handler that captures log output
    string_io = io.StringIO()
    handler = logging.StreamHandler(string_io)

    # Create the custom formatter
    formatter = MicrosecondFormatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S.%f'
    )
    handler.setFormatter(formatter)

    # Configure a test logger
    test_logger = logging.getLogger("test_logger")
    test_logger.setLevel(logging.INFO)
    test_logger.addHandler(handler)

    # Log a test message
    test_logger.info("Test message")

    # Get the captured output
    log_output = string_io.getvalue()

    # Check that the format is correct with millisecond precision
    timestamp_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}'
    assert re.search(timestamp_pattern, log_output), f"Log output format incorrect: {log_output}"