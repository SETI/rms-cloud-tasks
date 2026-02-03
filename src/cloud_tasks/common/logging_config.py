"""
Custom logging configuration with proper microsecond support.
"""

import datetime
import logging

try:
    from typing import override
except ImportError:  # pragma: no cover
    from typing_extensions import override


class MicrosecondFormatter(logging.Formatter):
    """
    A custom formatter that correctly handles microseconds in log timestamps.
    The standard logging.Formatter doesn't properly support %f in datefmt.
    """

    @override
    def formatTime(  # noqa: N802
        self, record: logging.LogRecord, datefmt: str | None = None
    ) -> str:
        """
        Format the record's timestamp; supports microseconds in datefmt.

        When datefmt is None, the default format "%Y-%m-%d %H:%M:%S" is used
        (no fractional seconds). When datefmt contains ".%f", the formatted
        microseconds are truncated to the first 3 digits (millisecond precision),
        and any characters in the format string after ".%f" are ignored/discarded
        in the output.

        Parameters:
            record: The log record (logging.LogRecord) whose timestamp to format.
            datefmt: Optional strftime-style format string. If None, defaults to
                "%Y-%m-%d %H:%M:%S". If it contains ".%f", output uses millisecond
                precision and suffix characters after ".%f" are not included.

        Returns:
            str: The formatted timestamp. Example: datefmt None yields
            "2025-02-01 12:34:56"; datefmt "%Y-%m-%d %H:%M:%S.%f" yields
            "2025-02-01 12:34:56.123"; datefmt "%Y-%m-%d %H:%M:%S.%fZ" yields
            "2025-02-01 12:34:56.123" (suffix "Z" discarded).
        """
        ct = datetime.datetime.fromtimestamp(record.created)
        if datefmt is None:
            datefmt = "%Y-%m-%d %H:%M:%S"
        s = ct.strftime(datefmt)
        # Always truncate to 3 digits (millisecond precision) even when datefmt is provided
        if ".%f" in datefmt:
            # Find the position of microseconds in the formatted string
            parts = datefmt.split(".%f")
            if len(parts) > 1:
                # Get the length of the part before .%f
                prefix_len = len(ct.strftime(parts[0]))
                # Truncate the string to include only the first 3 digits of microseconds
                s = s[: prefix_len + 4]  # +4 accounts for the dot and 3 digits
        return s


def configure_logging(
    level: int = logging.INFO,
    libraries_level: int = logging.CRITICAL,
) -> logging.Logger:
    """
    Configure logging with proper microsecond support.

    Parameters:
        level: Logging level to use (default: INFO).
        libraries_level: Logging level for libraries (default: CRITICAL).

    Returns:
        The root logger.
    """

    logging.getLogger("asyncio").setLevel(libraries_level)
    logging.getLogger("urllib3").setLevel(libraries_level)

    # AWS
    logging.getLogger("boto").setLevel(libraries_level)
    logging.getLogger("boto3").setLevel(libraries_level)
    logging.getLogger("boto3.resources").setLevel(libraries_level)
    logging.getLogger("botocore").setLevel(libraries_level)

    # GCP
    logging.getLogger("google").setLevel(libraries_level)
    logging.getLogger("google.auth").setLevel(libraries_level)
    logging.getLogger("google.cloud").setLevel(libraries_level)
    logging.getLogger("google.cloud.pubsub").setLevel(libraries_level)

    # Azure
    logging.getLogger("azure").setLevel(libraries_level)
    logging.getLogger("azure.servicebus").setLevel(libraries_level)

    formatter = MicrosecondFormatter(
        # fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S.%f"
        fmt="%(asctime)s %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S.%f",
    )

    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear existing handlers to avoid duplicates if called multiple times
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add a console handler with the custom formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    return root_logger
