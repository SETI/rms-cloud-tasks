"""
UTC time utilities for consistent time handling across cloud_tasks.
All timestamps and event times should be normalized to UTC.
"""

import datetime
from typing import Optional


def utc_now() -> datetime.datetime:
    """Return current time in UTC (timezone-aware)."""
    return datetime.datetime.now(datetime.timezone.utc)


def utc_now_iso() -> str:
    """Return current time in UTC as ISO format string."""
    return utc_now().isoformat()


def parse_utc(s: Optional[str]) -> Optional[datetime.datetime]:
    """
    Parse ISO timestamp string and return as UTC timezone-aware datetime.
    If s is None or empty, return None.
    Naive timestamps are assumed to be UTC.
    """
    if s is None or (isinstance(s, str) and not s.strip()):
        return None
    s = s.replace("Z", "+00:00")
    dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)
