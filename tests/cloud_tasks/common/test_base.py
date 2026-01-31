"""Tests for cloud_tasks.common.base."""

import pytest

from cloud_tasks.common.base import CloudProvider


def test_cloud_provider_is_abstract() -> None:
    """CloudProvider cannot be instantiated (abstract class)."""
    with pytest.raises(TypeError) as exc_info:
        CloudProvider()
    assert "abstract" in str(exc_info.value).lower() or "instantiate" in str(exc_info.value).lower()
