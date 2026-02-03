"""Unit tests for GCPComputeInstanceManager._get_boot_disk_info."""

from typing import Any
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound  # type: ignore

from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager

from .conftest import deepcopy_gcp_instance_manager


def setup_mocked_instance_manager(
    orig: GCPComputeInstanceManager,
    *,
    name: str = "instance-1",
    disks: list[Any] | None = None,
) -> tuple[GCPComputeInstanceManager, MagicMock]:
    """Prepare a cloned manager with mocked _disks_client and a mock instance.

    Parameters:
        orig: GCPComputeInstanceManager fixture to clone.
        name: Optional instance name for the mock instance.
        disks: Optional list of disk objects; default is [].

    Returns:
        Tuple of (cloned manager with _disks_client set to MagicMock(),
        mock_instance with .name and .disks set).
    """
    manager = deepcopy_gcp_instance_manager(orig)
    manager._disks_client = MagicMock()
    mock_instance = MagicMock()
    mock_instance.name = name
    mock_instance.disks = [] if disks is None else disks
    return (manager, mock_instance)


def test_get_boot_disk_info_no_disks(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """_get_boot_disk_info returns all None when instance has no disks.

    Parameters:
        gcp_instance_manager_n1_n2: GCPComputeInstanceManager fixture.

    Returns:
        None.
    """
    manager, mock_instance = setup_mocked_instance_manager(gcp_instance_manager_n1_n2)

    result = manager._get_boot_disk_info(mock_instance)

    assert result == (None, None, None, None)
    manager._disks_client.get.assert_not_called()


def test_get_boot_disk_info_no_boot_disk(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """_get_boot_disk_info returns all None when no disk has boot=True.

    Parameters:
        gcp_instance_manager_n1_n2: GCPComputeInstanceManager fixture.

    Returns:
        None.
    """
    data_disk = MagicMock()
    data_disk.boot = False
    data_disk.source = "https://www.googleapis.com/compute/v1/projects/p/zones/z/disks/data"
    manager, mock_instance = setup_mocked_instance_manager(
        gcp_instance_manager_n1_n2, disks=[data_disk]
    )

    result = manager._get_boot_disk_info(mock_instance)

    assert result == (None, None, None, None)
    manager._disks_client.get.assert_not_called()


def test_get_boot_disk_info_returns_type_size_iops_throughput(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """_get_boot_disk_info returns disk type, size, IOPS, throughput from disk resource.

    Parameters:
        gcp_instance_manager_n1_n2: GCPComputeInstanceManager fixture.

    Returns:
        None.
    """
    boot_disk = MagicMock()
    boot_disk.boot = True
    boot_disk.source = "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/disks/my-boot-disk"
    manager, mock_instance = setup_mocked_instance_manager(
        gcp_instance_manager_n1_n2, disks=[boot_disk]
    )
    mock_full_disk = MagicMock()
    mock_full_disk.type = "zones/us-central1-a/diskTypes/pd-balanced"
    mock_full_disk.size_gb = 100
    mock_full_disk.provisioned_iops = 10000
    mock_full_disk.provisioned_throughput = 600
    manager._disks_client.get.return_value = mock_full_disk

    result = manager._get_boot_disk_info(mock_instance)

    assert result == ("pd-balanced", 100, 10000, 600)
    manager._disks_client.get.assert_called_once()
    call_request = manager._disks_client.get.call_args[1]["request"]
    assert call_request.project == manager._project_id
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
    boot_disk = MagicMock()
    boot_disk.boot = True
    boot_disk.source = (
        "https://www.googleapis.com/compute/v1/projects/p/zones/us-east1-b/disks/standard-disk"
    )
    manager, mock_instance = setup_mocked_instance_manager(
        gcp_instance_manager_n1_n2, name="instance-2", disks=[boot_disk]
    )
    mock_full_disk = MagicMock()
    mock_full_disk.type = "zones/us-east1-b/diskTypes/pd-standard"
    mock_full_disk.size_gb = 50
    mock_full_disk.provisioned_iops = None
    mock_full_disk.provisioned_throughput = None
    manager._disks_client.get.return_value = mock_full_disk

    result = manager._get_boot_disk_info(mock_instance)

    assert result == ("pd-standard", 50, None, None)
    call_request = manager._disks_client.get.call_args[1]["request"]
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
    manager, mock_instance = setup_mocked_instance_manager(
        gcp_instance_manager_n1_n2, disks=[data_disk, boot_disk]
    )
    mock_full_disk = MagicMock()
    mock_full_disk.type = "zones/us-central1-a/diskTypes/pd-balanced"
    mock_full_disk.size_gb = 30
    mock_full_disk.provisioned_iops = None
    mock_full_disk.provisioned_throughput = None
    manager._disks_client.get.return_value = mock_full_disk

    result = manager._get_boot_disk_info(mock_instance)

    assert result == ("pd-balanced", 30, None, None)
    manager._disks_client.get.assert_called_once()
    call_request = manager._disks_client.get.call_args[1]["request"]
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
    boot_disk = MagicMock()
    boot_disk.boot = True
    boot_disk.source = (
        "https://www.googleapis.com/compute/v1/projects/p/zones/us-central1-a/disks/my-disk"
    )
    manager, mock_instance = setup_mocked_instance_manager(
        gcp_instance_manager_n1_n2, disks=[boot_disk]
    )
    manager._disks_client.get.side_effect = NotFound("disk not found")

    with pytest.raises(NotFound) as exc_info:
        manager._get_boot_disk_info(mock_instance)

    assert "disk not found" in str(exc_info.value)
