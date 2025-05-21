"""Unit tests for the GCP Compute Engine instance manager."""

import pytest

from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager

from .conftest import deepcopy_gcp_instance_manager


@pytest.mark.asyncio
async def test_get_available_instance_types_no_constraints(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting available instance types with no constraints."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    # Arrange
    constraints = {
        "instance_types": None,
        "architecture": None,
        "min_cpu_rank": None,
        "cpus_per_task": None,
        "min_tasks_per_instance": None,
        "max_tasks_per_instance": None,
        "min_cpu": None,
        "max_cpu": None,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_memory_per_task": None,
        "max_memory_per_task": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "local_ssd_base_size": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_local_ssd_per_task": None,
        "max_local_ssd_per_task": None,
        "boot_disk_types": None,
        "total_boot_disk_size": None,
        "boot_disk_base_size": None,
        "boot_disk_per_cpu": None,
        "boot_disk_per_task": None,
        "use_spot": False,
    }

    # Act
    result = await gcp_instance_manager_n1_n2.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 2
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" in result

    # Verify n1-standard-2 instance details
    n1_instance = result["n1-standard-2"]
    assert n1_instance["name"] == "n1-standard-2"
    assert n1_instance["cpu_family"] == "Intel Haswell"
    assert n1_instance["cpu_rank"] == 6
    assert n1_instance["vcpu"] == 2
    assert n1_instance["mem_gb"] == 7.5
    assert n1_instance["local_ssd_gb"] == 0
    assert n1_instance["supported_boot_disk_types"] == [
        "pd-standard",
        "pd-balanced",
        "pd-extreme",
        "pd-ssd",
    ]
    assert n1_instance["available_boot_disk_types"] == [
        "pd-standard",
        "pd-balanced",
        "pd-extreme",
        "pd-ssd",
    ]
    assert n1_instance["boot_disk_iops"] == 3120
    assert n1_instance["boot_disk_throughput"] == 170
    assert n1_instance["boot_disk_gb"] == 10
    assert n1_instance["architecture"] == "X86_64"
    assert n1_instance["supports_spot"] is True
    assert n1_instance["description"] == "2 vCPUs, 7.5 GB RAM"
    assert (
        n1_instance["url"]
        == "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n1-standard-2"
    )

    # Verify n2-standard-4-lssd instance details
    n2_instance = result["n2-standard-4-lssd"]
    assert n2_instance["name"] == "n2-standard-4-lssd"
    assert n2_instance["cpu_family"] == "Intel Cascade Lake"
    assert n2_instance["cpu_rank"] == 12
    assert n2_instance["vcpu"] == 4
    assert n2_instance["mem_gb"] == 16
    assert n2_instance["local_ssd_gb"] == GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2
    assert n2_instance["supported_boot_disk_types"] == [
        "pd-standard",
        "pd-balanced",
        "pd-extreme",
        "pd-ssd",
    ]
    assert n2_instance["available_boot_disk_types"] == [
        "pd-standard",
        "pd-balanced",
        "pd-extreme",
        "pd-ssd",
    ]
    assert n2_instance["boot_disk_iops"] == 3120
    assert n2_instance["boot_disk_throughput"] == 170
    assert n2_instance["boot_disk_gb"] == 10
    assert n2_instance["architecture"] == "X86_64"
    assert n2_instance["supports_spot"] is True
    assert (
        n2_instance["url"]
        == "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n2-standard-4-lssd"
    )

    constraints = {}
    result = await gcp_instance_manager_n1_n2.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 2
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" in result


@pytest.mark.asyncio
async def test_get_available_instance_types_with_cpu_constraints(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting available instance types with CPU constraints."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    # Arrange
    constraints = {
        "min_cpu": 3,
        "max_cpu": 4,
    }

    # Act
    result = await gcp_instance_manager_n1_n2.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 1
    assert "n2-standard-4-lssd" in result
    assert "n1-standard-2" not in result


@pytest.mark.asyncio
async def test_get_available_instance_types_with_memory_constraints(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting available instance types with memory constraints."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    # Arrange
    constraints = {
        "min_total_memory": 10,
        "max_total_memory": 20,
    }

    # Act
    result = await gcp_instance_manager_n1_n2.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 1
    assert "n2-standard-4-lssd" in result
    assert "n1-standard-2" not in result


@pytest.mark.asyncio
async def test_get_available_instance_types_with_boot_disk_constraints(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting available instance types with boot disk constraints."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    # Arrange
    constraints = {
        # n1 and n2 support pd-standard, pd-balanced, pd-extreme, pd-ssd
        "boot_disk_types": ["pd-standard", "pd-extreme"],
    }

    # Act
    result = await gcp_instance_manager_n1_n2.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 2
    assert "n1-standard-2" in result
    assert result["n1-standard-2"]["available_boot_disk_types"] == ["pd-standard", "pd-extreme"]
    assert result["n1-standard-2"]["supported_boot_disk_types"] == [
        "pd-standard",
        "pd-balanced",
        "pd-extreme",
        "pd-ssd",
    ]
    assert "n2-standard-4-lssd" in result
    assert result["n2-standard-4-lssd"]["available_boot_disk_types"] == [
        "pd-standard",
        "pd-extreme",
    ]
    assert result["n2-standard-4-lssd"]["supported_boot_disk_types"] == [
        "pd-standard",
        "pd-balanced",
        "pd-extreme",
        "pd-ssd",
    ]


@pytest.mark.asyncio
async def test_get_available_instance_types_with_instance_type_filter(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting available instance types with instance type pattern filter."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    # Arrange
    constraints = {
        "instance_types": ["n1-.*"],
    }

    # Act
    result = await gcp_instance_manager_n1_n2.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 1
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" not in result


@pytest.mark.asyncio
async def test_get_available_instance_types_with_no_matches(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting available instance types with constraints that match no instances."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    # Arrange
    constraints = {
        "min_cpu": 8,  # Higher than any available instance
    }

    # Act
    result = await gcp_instance_manager_n1_n2.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 0


@pytest.mark.asyncio
async def test_get_available_instance_types_with_memory_per_cpu_constraints(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting available instance types with memory per CPU constraints."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    # Arrange
    constraints = {
        "min_memory_per_cpu": 3.5,  # n1-standard-2 has 3.75 GB/CPU
        "max_memory_per_cpu": 3.8,  # Upper bound excludes n2-standard-4-lssd which has 4 GB/CPU
    }

    # Act
    result = await gcp_instance_manager_n1_n2.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 1
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" not in result  # Has 4 GB/CPU
