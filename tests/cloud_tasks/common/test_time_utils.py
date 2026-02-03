"""Tests for cloud_tasks.common.time_utils."""

import datetime

from cloud_tasks.common.time_utils import parse_utc, utc_now, utc_now_iso


def test_utc_now_returns_timezone_aware_datetime() -> None:
    """utc_now returns a timezone-aware datetime in UTC."""
    result = utc_now()
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    assert result.tzinfo.utcoffset(None) == datetime.timedelta(0)


def test_utc_now_iso_returns_iso_format_string() -> None:
    """utc_now_iso returns an ISO 8601 format string."""
    result = utc_now_iso()
    assert isinstance(result, str)
    assert "Z" in result or "+00:00" in result
    parsed = datetime.datetime.fromisoformat(result.replace("Z", "+00:00"))
    assert parsed.tzinfo is not None


def test_parse_utc_none_returns_none() -> None:
    """parse_utc(None) returns None."""
    assert parse_utc(None) is None


def test_parse_utc_empty_string_returns_none() -> None:
    """parse_utc('') and parse_utc('   ') return None."""
    assert parse_utc("") is None
    assert parse_utc("   ") is None


def test_parse_utc_with_z_suffix() -> None:
    """parse_utc accepts Z suffix and returns UTC datetime."""
    result = parse_utc("2025-01-15T12:30:00Z")
    assert result is not None
    assert result.tzinfo is not None
    assert result.year == 2025
    assert result.month == 1
    assert result.day == 15
    assert result.hour == 12
    assert result.minute == 30
    assert result.second == 0


def test_parse_utc_with_plus_00_00() -> None:
    """parse_utc accepts +00:00 and returns UTC datetime."""
    result = parse_utc("2025-06-01T00:00:00+00:00")
    assert result is not None
    assert result.tzinfo is not None
    assert result.year == 2025
    assert result.month == 6
    assert result.day == 1


def test_parse_utc_naive_treated_as_utc() -> None:
    """parse_utc with naive timestamp treats it as UTC."""
    result = parse_utc("2025-01-01T12:00:00")
    assert result is not None
    assert result.tzinfo is not None
    assert result.astimezone(datetime.timezone.utc).hour == 12
