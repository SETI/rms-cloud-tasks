"""Tests for GCP pricing cache helpers: _get_pricing_cache_path, _load_pricing_cache_from_file, _save_pricing_cache_to_file."""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from cloud_tasks.common.config import GCPConfig
from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager

# Cache is considered stale after this many seconds (24 hours); tests use older mtime to skip load.
CACHE_MAX_AGE_SECONDS = 24 * 3600


def _make_manager_with_cache_helpers(gcp_config: GCPConfig) -> GCPComputeInstanceManager:
    """Create GCPComputeInstanceManager with mocked clients; cache helpers are real."""
    with (
        patch(
            "cloud_tasks.instance_manager.gcp.get_default_credentials",
            return_value=(MagicMock(), gcp_config.project_id or "test-project"),
        ),
        patch("google.cloud.compute_v1.InstancesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.ZonesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.RegionsClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.MachineTypesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.ImagesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.DisksClient", return_value=MagicMock()),
        patch("google.cloud.billing.CloudCatalogClient", return_value=MagicMock()),
    ):
        return GCPComputeInstanceManager(gcp_config)


def test_get_pricing_cache_path_contains_project_and_region(tmp_path: Path) -> None:
    """_get_pricing_cache_path returns path under temp dir with sanitized project and region."""
    config = GCPConfig(
        project_id="my-project",
        region="us-central1",
        zone=None,
        credentials_file=None,
        instance_types=None,
        service_account=None,
    )
    manager = _make_manager_with_cache_helpers(config)
    with patch("tempfile.gettempdir", return_value=str(tmp_path)):
        path = manager._get_pricing_cache_path()
    # Regex [^\\w\\-] replaces non-word chars (except hyphen); hyphens stay
    assert path == os.path.join(
        str(tmp_path),
        "cloud_tasks_gcp_pricing_my-project_us-central1.json",
    )
    manager._project_id = "project.with.dots"
    manager._region = "us-east1"
    with patch("tempfile.gettempdir", return_value=str(tmp_path)):
        path2 = manager._get_pricing_cache_path()
    assert "project_with_dots" in path2
    assert "us-east1" in path2
    assert path2.endswith(".json")


def test_save_pricing_cache_to_file_writes_json(tmp_path: Path) -> None:
    """_save_pricing_cache_to_file writes in-memory cache to file atomically."""
    config = GCPConfig(
        project_id="p",
        region="r",
        zone=None,
        credentials_file=None,
        instance_types=None,
        service_account=None,
    )
    manager = _make_manager_with_cache_helpers(config)
    manager._instance_pricing_cache[("n1-standard", False)] = {
        "r": {"z": {"some": "data"}},
    }
    manager._instance_pricing_cache[("n2-highmem", True)] = None

    with patch("tempfile.gettempdir", return_value=str(tmp_path)):
        manager._save_pricing_cache_to_file()
        path = manager._get_pricing_cache_path()
    assert os.path.isfile(path)
    with open(path) as f:
        data = json.load(f)
    assert data["n1-standard|False"] == {"r": {"z": {"some": "data"}}}
    assert data["n2-highmem|True"] is None


def test_load_pricing_cache_from_file_loads_valid_cache(tmp_path: Path) -> None:
    """_load_pricing_cache_from_file loads cache when file exists and is fresh."""
    config = GCPConfig(
        project_id="p",
        region="r",
        zone=None,
        credentials_file=None,
        instance_types=None,
        service_account=None,
    )
    manager = _make_manager_with_cache_helpers(config)
    cache_path = os.path.join(
        str(tmp_path),
        "cloud_tasks_gcp_pricing_p_r.json",
    )
    with open(cache_path, "w") as f:
        json.dump({"n1-standard|False": {"cached": True}, "n2|True": None}, f)

    with patch("tempfile.gettempdir", return_value=str(tmp_path)):
        manager._load_pricing_cache_from_file()

    assert manager._instance_pricing_cache[("n1-standard", False)] == {"cached": True}
    assert manager._instance_pricing_cache[("n2", True)] is None


def test_load_pricing_cache_from_file_skips_when_file_missing(tmp_path: Path) -> None:
    """_load_pricing_cache_from_file does nothing when cache file does not exist."""
    config = GCPConfig(
        project_id="nonexistent",
        region="nowhere",
        zone=None,
        credentials_file=None,
        instance_types=None,
        service_account=None,
    )
    manager = _make_manager_with_cache_helpers(config)
    with patch("tempfile.gettempdir", return_value=str(tmp_path)):
        manager._load_pricing_cache_from_file()
    assert manager._instance_pricing_cache == {}
    assert manager._pricing_cache_file_loaded is True


def test_load_pricing_cache_from_file_skips_when_already_loaded(tmp_path: Path) -> None:
    """_load_pricing_cache_from_file returns without reading if already loaded."""
    config = GCPConfig(
        project_id="p",
        region="r",
        zone=None,
        credentials_file=None,
        instance_types=None,
        service_account=None,
    )
    manager = _make_manager_with_cache_helpers(config)
    manager._pricing_cache_file_loaded = True
    manager._instance_pricing_cache[("existing", False)] = {"r": {"z": {}}}
    cache_path = os.path.join(str(tmp_path), "cloud_tasks_gcp_pricing_p_r.json")
    with open(cache_path, "w") as f:
        json.dump({"other|True": "from_file"}, f)

    with patch("tempfile.gettempdir", return_value=str(tmp_path)):
        manager._load_pricing_cache_from_file()

    assert manager._instance_pricing_cache == {("existing", False): {"r": {"z": {}}}}
    assert "other" not in str(manager._instance_pricing_cache)


def test_load_pricing_cache_from_file_skips_old_file(tmp_path: Path) -> None:
    """_load_pricing_cache_from_file does not load cache older than 24 hours."""
    import datetime
    from datetime import timezone

    config = GCPConfig(
        project_id="p",
        region="r",
        zone=None,
        credentials_file=None,
        instance_types=None,
        service_account=None,
    )
    manager = _make_manager_with_cache_helpers(config)
    cache_path = os.path.join(str(tmp_path), "cloud_tasks_gcp_pricing_p_r.json")
    with open(cache_path, "w") as f:
        json.dump({"old|False": "data"}, f)
    past = datetime.datetime.now(timezone.utc).timestamp() - (CACHE_MAX_AGE_SECONDS + 3600)
    os.utime(cache_path, (past, past))

    with patch("tempfile.gettempdir", return_value=str(tmp_path)):
        manager._load_pricing_cache_from_file()

    assert manager._instance_pricing_cache == {}


def test_load_pricing_cache_from_file_skips_invalid_key_format(tmp_path: Path) -> None:
    """_load_pricing_cache_from_file skips entries without '|' (key must have family|use_spot)."""
    config = GCPConfig(
        project_id="p",
        region="r",
        zone=None,
        credentials_file=None,
        instance_types=None,
        service_account=None,
    )
    manager = _make_manager_with_cache_helpers(config)
    cache_path = os.path.join(str(tmp_path), "cloud_tasks_gcp_pricing_p_r.json")
    with open(cache_path, "w") as f:
        json.dump(
            {
                "valid|true": {"x": 1},
                "no_pipe": {"y": 2},
            },
            f,
        )

    with patch("tempfile.gettempdir", return_value=str(tmp_path)):
        manager._load_pricing_cache_from_file()

    assert manager._instance_pricing_cache[("valid", True)] == {"x": 1}
    assert len(manager._instance_pricing_cache) == 1


def test_save_and_load_pricing_cache_roundtrip(tmp_path: Path) -> None:
    """_save_pricing_cache_to_file and _load_pricing_cache_from_file roundtrip."""
    config = GCPConfig(
        project_id="round",
        region="trip",
        zone=None,
        credentials_file=None,
        instance_types=None,
        service_account=None,
    )
    manager = _make_manager_with_cache_helpers(config)
    manager._instance_pricing_cache[("n1", False)] = {"r": {"z": {"cpu": 1.0}}}
    manager._instance_pricing_cache[("n2", True)] = None

    with patch("tempfile.gettempdir", return_value=str(tmp_path)):
        manager._save_pricing_cache_to_file()

    manager2 = _make_manager_with_cache_helpers(config)
    manager2._pricing_cache_file_loaded = False
    with patch("tempfile.gettempdir", return_value=str(tmp_path)):
        manager2._load_pricing_cache_from_file()

    assert manager2._instance_pricing_cache == manager._instance_pricing_cache
