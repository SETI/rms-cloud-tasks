# Manually verified 4/29/2025

import logging
import re
import io

from cloud_tasks.common.logging_config import configure_logging, MicrosecondFormatter


def test_microsecond_formatter():
    """Test that MicrosecondFormatter correctly formats timestamps with millisecond precision."""
    # Create a formatter with the microsecond format
    formatter = MicrosecondFormatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S.%f"
    )

    # Create a log record with a known timestamp
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    # Format the record
    formatted = formatter.format(record)

    # Check that the timestamp has millisecond precision (3 digits after the dot)
    timestamp_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}"
    assert re.search(timestamp_pattern, formatted), f"Timestamp format incorrect: {formatted}"


def test_configure_logging():
    """Test that configure_logging properly sets up the logging system."""
    # Configure logging first to get the formatter
    root_logger = configure_logging(level=logging.INFO)
    original_formatter = root_logger.handlers[0].formatter

    # Now set up our capture handler with the same formatter
    string_io = io.StringIO()
    handler = logging.StreamHandler(string_io)
    handler.setFormatter(original_formatter)

    # Replace the default handler with our capture handler
    root_logger.handlers = [handler]

    # Test that library loggers are set to CRITICAL
    libraries = [
        "asyncio",
        "urllib3",
        "boto",
        "boto3",
        "boto3.resources",
        "botocore",
        "google",
        "google.auth",
        "google.cloud",
        "google.cloud.pubsub",
        "azure",
        "azure.servicebus",
    ]
    for lib in libraries:
        lib_logger = logging.getLogger(lib)
        assert lib_logger.level == logging.CRITICAL, f"{lib} logger not set to CRITICAL"

    # Test that root logger is set to INFO
    assert root_logger.level == logging.INFO, "Root logger not set to INFO"

    # Test that the formatter is correctly set
    formatter = handler.formatter
    assert isinstance(formatter, MicrosecondFormatter), "Handler not using MicrosecondFormatter"
    assert formatter._fmt == "%(asctime)s %(levelname)s - %(message)s"
    assert formatter.datefmt == "%Y-%m-%d %H:%M:%S.%f"

    # Test logging a message
    test_logger = logging.getLogger("test_logger")
    test_logger.info("Test message")
    log_output = string_io.getvalue()

    # Verify the log format
    timestamp_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}"
    message_pattern = f"{timestamp_pattern} INFO - Test message\n"
    assert re.match(message_pattern, log_output), f"Log output format incorrect: {log_output}"

    # Test that library messages at INFO level are not logged
    lib_logger = logging.getLogger("boto3")
    lib_logger.info("This should not appear")
    log_output = string_io.getvalue()
    assert "This should not appear" not in log_output

    # Test that library messages at CRITICAL level are logged
    lib_logger.critical("This should appear")
    log_output = string_io.getvalue()
    assert "This should appear" in log_output


def test_configure_logging_custom_levels():
    """Test configure_logging with custom log levels."""
    # Configure with DEBUG for main loggers and WARNING for libraries
    root_logger = configure_logging(level=logging.DEBUG, libraries_level=logging.WARNING)

    # Test root logger level
    assert root_logger.level == logging.DEBUG

    # Test library logger levels
    lib_logger = logging.getLogger("boto3")
    assert lib_logger.level == logging.WARNING

    # Clean up by resetting to default levels
    configure_logging()
