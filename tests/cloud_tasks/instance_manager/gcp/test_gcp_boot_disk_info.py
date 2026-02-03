"""Unit tests for GCPComputeInstanceManager._get_boot_disk_info."""

from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound  # type: ignore

from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager

from .conftest import deepcopy_gcp_instance_manager


def test_get_boot_disk_info_no_disks(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """_get_boot_disk_info returns all None when instance has no disks.

    Parameters:
        gcp_instance_manager_n1_n2: GCPComputeInstanceManager fixture.

    Returns:
        None.
    """
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._disks_client = MagicMock()
    mock_instance = MagicMock()
    mock_instance.name = "instance-1"
    mock_instance.disks = []

    result = gcp_instance_manager_n1_n2._get_boot_disk_info(mock_instance)

    assert result == (None, None, None, None)
    gcp_instance_manager_n1_n2._disks_client.get.assert_not_called()


def test_get_boot_disk_info_no_boot_disk(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """_get_boot_disk_info returns all None when no disk has boot=True.

    Parameters:
        gcp_instance_manager_n1_n2: GCPComputeInstanceManager fixture.

    Returns:
        None.
    """
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._disks_client = MagicMock()
    mock_instance = MagicMock()
    mock_instance.name = "instance-1"
    data_disk = MagicMock()
    data_disk.boot = False
    data_disk.source = "https://www.googleapis.com/compute/v1/projects/p/zones/z/disks/data"
    mock_instance.disks = [data_disk]

    result = gcp_instance_manager_n1_n2._get_boot_disk_info(mock_instance)

    assert result == (None, None, None, None)
    gcp_instance_manager_n1_n2._disks_client.get.assert_not_called()


def test_get_boot_disk_info_returns_type_size_iops_throughput(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """_get_boot_disk_info returns disk type, size, IOPS, throughput from disk resource.

    Parameters:
        gcp_instance_manager_n1_n2: GCPComputeInstanceManager fixture.

    Returns:
        None.
    """
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_disks_client = MagicMock()
    mock_instance = MagicMock()
    mock_instance.name = "instance-1"
    boot_disk = MagicMock()
    boot_disk.boot = True
    boot_disk.source = "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/disks/my-boot-disk"
    mock_instance.disks = [boot_disk]

    mock_full_disk = MagicMock()
    mock_full_disk.type = "zones/us-central1-a/diskTypes/pd-balanced"
    mock_full_disk.size_gb = 100
    mock_full_disk.provisioned_iops = 10000
    mock_full_disk.provisioned_throughput = 600
    mock_disks_client.get.return_value = mock_full_disk
    gcp_instance_manager_n1_n2._disks_client = mock_disks_client

    result = gcp_instance_manager_n1_n2._get_boot_disk_info(mock_instance)

    assert result == ("pd-balanced", 100, 10000, 600)
    mock_disks_client.get.assert_called_once()
    call_request = mock_disks_client.get.call_args[1]["request"]
    assert call_request.project == gcp_instance_manager_n1_n2._project_id
    assert call_request.zone == "us-central1-a"
    assert call_request.disk == "my-boot-disk"


def test_get_boot_disk_info_pd_standard_no_iops_throughput(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """_get_boot_disk_info handles disk without provisioned_iops/throughput (e.g. pd-standard).

    Parameters:
        gcp_instance_manager_n1_n2: GCPComputeInstanceManager fixture.

    Returns:
        None.
    """
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_disks_client = MagicMock()
    mock_instance = MagicMock()
    mock_instance.name = "instance-2"
    boot_disk = MagicMock()
    boot_disk.boot = True
    boot_disk.source = (
        "https://www.googleapis.com/compute/v1/projects/p/zones/us-east1-b/disks/standard-disk"
    )
    mock_instance.disks = [boot_disk]

    mock_full_disk = MagicMock()
    mock_full_disk.type = "zones/us-east1-b/diskTypes/pd-standard"
    mock_full_disk.size_gb = 50
    mock_full_disk.provisioned_iops = None
    mock_full_disk.provisioned_throughput = None
    mock_disks_client.get.return_value = mock_full_disk
    gcp_instance_manager_n1_n2._disks_client = mock_disks_client

    result = gcp_instance_manager_n1_n2._get_boot_disk_info(mock_instance)

    assert result == ("pd-standard", 50, None, None)
    call_request = mock_disks_client.get.call_args[1]["request"]
    assert call_request.zone == "us-east1-b"
    assert call_request.disk == "standard-disk"


def test_get_boot_disk_info_uses_first_boot_disk_when_multiple_disks(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """_get_boot_disk_info uses the first disk with boot=True when multiple disks are attached.

    Parameters:
        gcp_instance_manager_n1_n2: GCPComputeInstanceManager fixture.

    Returns:
        None.
    """
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_disks_client = MagicMock()
    mock_instance = MagicMock()
    mock_instance.name = "instance-1"
    data_disk = MagicMock()
    data_disk.boot = False
    data_disk.source = (
        "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/disks/data-disk"
    )
    boot_disk = MagicMock()
    boot_disk.boot = True
    boot_disk.source = (
        "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/disks/main-boot"
    )
    mock_instance.disks = [data_disk, boot_disk]

    mock_full_disk = MagicMock()
    mock_full_disk.type = "zones/us-central1-a/diskTypes/pd-balanced"
    mock_full_disk.size_gb = 30
    mock_full_disk.provisioned_iops = None
    mock_full_disk.provisioned_throughput = None
    mock_disks_client.get.return_value = mock_full_disk
    gcp_instance_manager_n1_n2._disks_client = mock_disks_client

    result = gcp_instance_manager_n1_n2._get_boot_disk_info(mock_instance)

    assert result == ("pd-balanced", 30, None, None)
    mock_disks_client.get.assert_called_once()
    call_request = mock_disks_client.get.call_args[1]["request"]
    assert call_request.disk == "main-boot"


def test_get_boot_disk_info_propagates_disks_client_error(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """_get_boot_disk_info propagates exceptions from _disks_client.get.

    Parameters:
        gcp_instance_manager_n1_n2: GCPComputeInstanceManager fixture.

    Returns:
        None.
    """
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_disks_client = MagicMock()
    mock_disks_client.get.side_effect = NotFound("disk not found")
    gcp_instance_manager_n1_n2._disks_client = mock_disks_client
    mock_instance = MagicMock()
    mock_instance.name = "instance-1"
    boot_disk = MagicMock()
    boot_disk.boot = True
    boot_disk.source = (
        "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/disks/my-disk"
    )
    mock_instance.disks = [boot_disk]

    with pytest.raises(NotFound) as exc_info:
        gcp_instance_manager_n1_n2._get_boot_disk_info(mock_instance)

    assert "disk not found" in str(exc_info.value)
