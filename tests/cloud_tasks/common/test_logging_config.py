"""
Tests for cloud_tasks.common.logging_config: MicrosecondFormatter and configure_logging.
"""

import io
import logging
import re
from collections.abc import Generator
from typing import TypedDict

import pytest

from cloud_tasks.common.logging_config import MicrosecondFormatter, configure_logging


class LoggingState(TypedDict):
    """Saved state for root and library loggers."""

    root_level: int
    root_handlers: list[logging.Handler]
    lib_levels: dict[str, int]


# Library loggers that configure_logging mutates; used to save/restore in tests.
_LOGGING_LIBRARIES = [
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


def _save_logging_state() -> LoggingState:
    """Save root logger and library logger state for later restore.

    Preserves root logger level, root handlers (and their formatter/level), and
    logger-specific levels for _LOGGING_LIBRARIES.

    Returns:
        LoggingState with keys: root_level (int), root_handlers (list of
        logging.Handler), lib_levels (mapping from _LOGGING_LIBRARIES names to int).
    """
    root = logging.getLogger()
    return LoggingState(
        root_level=root.level,
        root_handlers=list(root.handlers),
        lib_levels={lib: logging.getLogger(lib).level for lib in _LOGGING_LIBRARIES},
    )


def _restore_logging_state(state: LoggingState) -> None:
    """Restore root and library logger state from a previous _save_logging_state().

    Restores root level, root handlers, and per-library levels for
    _LOGGING_LIBRARIES. Used by preserve_logging_state and _save_logging_state.

    Parameters:
        state: LoggingState with keys "root_level" (int), "root_handlers" (list of
            logging.Handler), "lib_levels" (dict mapping _LOGGING_LIBRARIES names
            to int).

    Returns:
        None.

    Raises:
        KeyError: If a required key is missing from state.
    """
    root = logging.getLogger()
    root.setLevel(state["root_level"])
    for handler in list(root.handlers):
        root.removeHandler(handler)
    for handler in state["root_handlers"]:
        root.addHandler(handler)
    for lib, level in state["lib_levels"].items():
        logging.getLogger(lib).setLevel(level)


def test_microsecond_formatter() -> None:
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


@pytest.fixture
def preserve_logging_state() -> Generator[None, None, None]:
    """Save root logging state before the test and restore it after.

    Yields:
        None. While the test runs, logging state is unchanged; after the test,
        _restore_logging_state restores the root logger level and handlers (and
        handler formatter/level), and logger-specific levels for _LOGGING_LIBRARIES.
        Propagation flags are not explicitly saved; handlers are replaced on restore.
    Notes:
        Function-scoped so each test gets a clean restore. Uses _save_logging_state
        and _restore_logging_state to preserve and restore the saved state.
    """
    state = _save_logging_state()
    yield
    _restore_logging_state(state)


def test_configure_logging(preserve_logging_state: None) -> None:
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
    for lib in _LOGGING_LIBRARIES:
        lib_logger = logging.getLogger(lib)
        assert lib_logger.level == logging.CRITICAL, f"{lib} logger not set to CRITICAL"

    # Test that root logger is set to INFO
    assert root_logger.level == logging.INFO, "Root logger not set to INFO"

    # Test that the formatter is correctly set (Python 3.2+ uses _style._fmt)
    formatter = handler.formatter
    assert isinstance(formatter, MicrosecondFormatter), "Handler not using MicrosecondFormatter"
    fmt_val = getattr(formatter, "_fmt", getattr(formatter._style, "_fmt", None))
    assert fmt_val == "%(asctime)s %(levelname)s - %(message)s"
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


def test_configure_logging_custom_levels(preserve_logging_state: None) -> None:
    """Test configure_logging with custom log levels."""
    # Configure with DEBUG for main loggers and WARNING for libraries
    root_logger = configure_logging(level=logging.DEBUG, libraries_level=logging.WARNING)

    # Test root logger level
    assert root_logger.level == logging.DEBUG

    # Test library logger levels
    lib_logger = logging.getLogger("boto3")
    assert lib_logger.level == logging.WARNING
