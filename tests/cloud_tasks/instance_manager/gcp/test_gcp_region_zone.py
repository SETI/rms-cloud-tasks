"""Unit tests for the GCP Compute Engine instance manager."""

import copy
import random
from unittest.mock import MagicMock

import pytest

from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager

from .conftest import deepcopy_gcp_instance_manager


@pytest.mark.asyncio
async def test_get_available_regions_basic(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test basic functionality of get_available_regions."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Mock the regions response
    mock_region1 = MagicMock()
    mock_region1.name = "us-central1"
    mock_region1.description = "us-central1 region"
    mock_region1.status = "UP"
    mock_region1.zones = [
        "projects/test-project/zones/us-central1-a",
        "projects/test-project/zones/us-central1-b",
    ]

    mock_region2 = MagicMock()
    mock_region2.name = "europe-west1"
    mock_region2.description = "europe-west1 region"
    mock_region2.status = "UP"
    mock_region2.zones = [
        "projects/test-project/zones/europe-west1-a",
        "projects/test-project/zones/europe-west1-b",
    ]

    gcp_instance_manager_n1_n2._regions_client.list.return_value = [mock_region1, mock_region2]

    regions = await gcp_instance_manager_n1_n2.get_available_regions()

    assert len(regions) == 2
    assert "us-central1" in regions
    assert "europe-west1" in regions

    # Verify region details
    us_central = regions["us-central1"]
    assert us_central["name"] == "us-central1"
    assert us_central["description"] == "us-central1 region"
    assert us_central["status"] == "UP"
    assert us_central["endpoint"] == "https://us-central1-compute.googleapis.com"
    assert us_central["zones"] == ["us-central1-a", "us-central1-b"]


@pytest.mark.asyncio
async def test_get_available_regions_with_prefix(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test get_available_regions with prefix filtering."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Mock the regions response
    mock_region1 = MagicMock()
    mock_region1.name = "us-central1"
    mock_region1.description = "us-central1 region"
    mock_region1.status = "UP"
    mock_region1.zones = ["projects/test-project/zones/us-central1-a"]

    mock_region2 = MagicMock()
    mock_region2.name = "europe-west1"
    mock_region2.description = "europe-west1 region"
    mock_region2.status = "UP"
    mock_region2.zones = ["projects/test-project/zones/europe-west1-a"]

    gcp_instance_manager_n1_n2._regions_client.list.return_value = [mock_region1, mock_region2]

    regions = await gcp_instance_manager_n1_n2.get_available_regions(prefix="us-")

    assert len(regions) == 1
    assert "us-central1" in regions
    assert "europe-west1" not in regions


@pytest.mark.asyncio
async def test_get_available_regions_empty(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test get_available_regions when no regions are available."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._regions_client.list.return_value = []

    regions = await gcp_instance_manager_n1_n2.get_available_regions()

    assert len(regions) == 0


@pytest.mark.asyncio
async def test_get_default_zone_specified(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test _get_default_zone when zone is already specified."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._zone = "us-central1-a"

    zone = await gcp_instance_manager_n1_n2._get_default_zone()

    assert zone == "us-central1-a"
    # Verify no API calls were made
    assert not gcp_instance_manager_n1_n2._zones_client.list.called


@pytest.mark.asyncio
async def test_get_default_zone_from_region(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test _get_default_zone when getting first zone in region."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._zone = None
    gcp_instance_manager_n1_n2._region = "us-central1"

    # Mock the zones response
    mock_zone1 = MagicMock()
    mock_zone1.name = "us-central1-a"
    mock_zone2 = MagicMock()
    mock_zone2.name = "us-central1-b"

    gcp_instance_manager_n1_n2._zones_client.list.return_value = [mock_zone1, mock_zone2]

    zone = await gcp_instance_manager_n1_n2._get_default_zone()

    assert zone == "us-central1-a"
    # Verify correct filter was used
    gcp_instance_manager_n1_n2._zones_client.list.assert_called_once()
    request = gcp_instance_manager_n1_n2._zones_client.list.call_args[1]["request"]
    assert request.filter == "name eq us-central1-.*"


@pytest.mark.asyncio
async def test_get_default_zone_no_zones(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test _get_default_zone when no zones are found in region."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._zone = None
    gcp_instance_manager_n1_n2._region = "us-central1"

    gcp_instance_manager_n1_n2._zones_client.list.return_value = []

    with pytest.raises(ValueError, match="No zones found for region us-central1"):
        await gcp_instance_manager_n1_n2._get_default_zone()


@pytest.mark.asyncio
async def test_get_default_zone_no_region(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test _get_default_zone when neither zone nor region is specified."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._zone = None
    gcp_instance_manager_n1_n2._region = None

    with pytest.raises(RuntimeError, match="Region or zone must be specified"):
        await gcp_instance_manager_n1_n2._get_default_zone()


@pytest.mark.asyncio
async def test_get_random_zone_specified(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test _get_random_zone when zone is already specified."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._zone = "us-central1-a"

    zone = await gcp_instance_manager_n1_n2._get_random_zone()

    assert zone == "us-central1-a"
    # Verify no API calls were made
    assert not gcp_instance_manager_n1_n2._zones_client.list.called


@pytest.mark.asyncio
async def test_get_random_zone_from_region(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, monkeypatch
) -> None:
    """Test _get_random_zone when selecting random zone in region."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._zone = None
    gcp_instance_manager_n1_n2._region = "us-central1"

    # Mock the zones response
    mock_zone1 = MagicMock()
    mock_zone1.name = "us-central1-a"
    mock_zone2 = MagicMock()
    mock_zone2.name = "us-central1-b"
    mock_zone3 = MagicMock()
    mock_zone3.name = "us-central1-c"

    gcp_instance_manager_n1_n2._zones_client.list.return_value = [
        mock_zone1,
        mock_zone2,
        mock_zone3,
    ]

    # Mock random.randint to return predictable values
    monkeypatch.setattr(random, "randint", lambda x, y: 1)  # Always return index 1

    zone = await gcp_instance_manager_n1_n2._get_random_zone()

    assert zone == "us-central1-b"  # Should get the second zone due to mocked random
    # Verify correct filter was used
    gcp_instance_manager_n1_n2._zones_client.list.assert_called_once()
    request = gcp_instance_manager_n1_n2._zones_client.list.call_args[1]["request"]
    assert request.filter == "name eq us-central1-.*"


@pytest.mark.asyncio
async def test_get_random_zone_no_zones(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test _get_random_zone when no zones are found in region."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._zone = None
    gcp_instance_manager_n1_n2._region = "us-central1"

    gcp_instance_manager_n1_n2._zones_client.list.return_value = []

    with pytest.raises(ValueError, match="No zones found for region us-central1"):
        await gcp_instance_manager_n1_n2._get_random_zone()


@pytest.mark.asyncio
async def test_get_random_zone_different_region(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test _get_random_zone when specifying a different region."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._zone = None
    gcp_instance_manager_n1_n2._region = "us-central1"

    # Mock the zones response
    mock_zone1 = MagicMock()
    mock_zone1.name = "europe-west1-a"
    mock_zone2 = MagicMock()
    mock_zone2.name = "europe-west1-b"

    gcp_instance_manager_n1_n2._zones_client.list.return_value = [mock_zone1, mock_zone2]

    zone = await gcp_instance_manager_n1_n2._get_random_zone(region="europe-west1")
    assert zone.startswith("europe-west1-")

    # Verify correct filter was used for the specified region
    gcp_instance_manager_n1_n2._zones_client.list.assert_called_once()
    request = gcp_instance_manager_n1_n2._zones_client.list.call_args[1]["request"]
    assert request.filter == "name eq europe-west1-.*"
