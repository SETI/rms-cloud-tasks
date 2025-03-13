"""
Custom logging configuration with proper microsecond support.
"""
import logging
import datetime

class MicrosecondFormatter(logging.Formatter):
    """
    A custom formatter that correctly handles microseconds in log timestamps.
    The standard logging.Formatter doesn't properly support %f in datefmt.
    """

    def formatTime(self, record, datefmt=None):
        """
        Override the standard formatTime to correctly handle microseconds.
        """
        ct = datetime.datetime.fromtimestamp(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
            # Always truncate to 3 digits (millisecond precision) even when datefmt is provided
            if '.%f' in datefmt:
                # Find the position of microseconds in the formatted string
                parts = datefmt.split('.%f')
                if len(parts) > 1:
                    # Get the length of the part before .%f
                    prefix_len = len(ct.strftime(parts[0]))
                    # Get the length of the microseconds (should be 6)
                    microsec_len = len(ct.strftime('.%f')) - 1  # subtract 1 for the dot
                    # Truncate the string to include only the first 3 digits of microseconds
                    s = s[:prefix_len + 4]  # +4 accounts for the dot and 3 digits
                    # If there's content after microseconds, append it
                    if len(parts) > 1 and parts[1]:
                        s += ct.strftime(parts[1])
        else:
            s = ct.strftime("%Y-%m-%d %H:%M:%S.%f")  # Include microseconds
            # Truncate to milliseconds (3 digits)
            s = s[:-3]
        return s


def configure_logging(level=logging.INFO):
    """
    Configure logging with proper microsecond support.

    Args:
        level: Logging level to use (default: INFO)
    """
    formatter = MicrosecondFormatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S.%f'
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