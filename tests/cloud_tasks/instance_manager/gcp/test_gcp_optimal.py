"""Unit tests for the GCP Compute Engine instance manager."""

import copy
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager

from .conftest import (
    deepcopy_gcp_instance_manager,
    N1_2_CPU_PRICE,
    N1_2_RAM_PRICE,
    N1_4_CPU_PRICE,
    N1_4_RAM_PRICE,
    N2_CPU_PRICE,
    N2_RAM_PRICE,
    N2_PREEMPTIBLE_CPU_PRICE,
    N2_PREEMPTIBLE_RAM_PRICE,
    PD_STANDARD_PRICE,
    PD_BALANCED_PRICE,
    LSSD_PRICE,
    LSSD_PREEMPTIBLE_PRICE,
)


@pytest.mark.asyncio
async def test_get_optimal_instance_type_basic(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test getting optimal instance type with minimal constraints."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    # Arrange
    constraints = {"boot_disk_types": ["pd-standard"]}

    # Set up pricing SKUs
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    selected_price_info = await gcp_instance_manager_n1_n2.get_optimal_instance_type(constraints)

    # Assert
    # n1-standard-2 should be chosen as it's cheaper
    assert selected_price_info["name"] == "n1-standard-2"
    assert selected_price_info["zone"] == f"{gcp_instance_manager_n1_n2._region}-*"
    assert selected_price_info["total_price"] == pytest.approx(
        N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10, rel=1e-6
    )


@pytest.mark.asyncio
async def test_get_optimal_instance_type_no_matches(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test getting optimal instance type when no instances match constraints."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)

    # Set up pricing SKUs
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    constraints = {
        "min_cpu": 8,  # Higher than any available instance
        "use_spot": False,
    }

    # Act & Assert
    with pytest.raises(ValueError, match="No instance type meets requirements"):
        await gcp_instance_manager_n1_n2.get_optimal_instance_type(constraints)


@pytest.mark.asyncio
async def test_get_optimal_instance_type_spot_instance(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_preemptible_sku: MagicMock,
    ram_pricing_n1_preemptible_sku: MagicMock,
    cpu_pricing_n2_preemptible_sku: MagicMock,
    ram_pricing_n2_preemptible_sku: MagicMock,
    local_ssd_pricing_n2_preemptible_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test getting optimal instance type with spot instance requirement."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    # Arrange
    constraints = {
        "boot_disk_types": ["pd-standard"],
        "use_spot": True,
    }

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_preemptible_sku,
        ram_pricing_n1_preemptible_sku,
        cpu_pricing_n2_preemptible_sku,
        ram_pricing_n2_preemptible_sku,
        local_ssd_pricing_n2_preemptible_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    selected_price_info = await gcp_instance_manager_n1_n2.get_optimal_instance_type(constraints)

    # Assert
    print(f"Selected price info: {selected_price_info}")
    assert selected_price_info["name"] == "n2-standard-4-lssd"
    assert selected_price_info["zone"] == f"{gcp_instance_manager_n1_n2._region}-*"
    assert selected_price_info["total_price"] == pytest.approx(
        N2_PREEMPTIBLE_CPU_PRICE * 4
        + N2_PREEMPTIBLE_RAM_PRICE * 16
        + LSSD_PREEMPTIBLE_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2
        + PD_STANDARD_PRICE * 10,
        rel=1e-6,
    )


@pytest.mark.asyncio
async def test_get_optimal_instance_type_with_memory_constraints(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test getting optimal instance type with memory constraints."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    # Arrange
    constraints = {
        "min_total_memory": 10,  # Only n2-standard-4-lssd meets this
        "boot_disk_types": ["pd-balanced"],
        "use_spot": False,
    }

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    selected_price_info = await gcp_instance_manager_n1_n2.get_optimal_instance_type(constraints)

    # Assert
    assert selected_price_info["name"] == "n2-standard-4-lssd"
    assert selected_price_info["zone"] == f"{gcp_instance_manager_n1_n2._region}-*"
    assert selected_price_info["total_price"] == pytest.approx(
        N2_CPU_PRICE * 4
        + N2_RAM_PRICE * 16
        + LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2
        + PD_BALANCED_PRICE * 10,
        rel=1e-6,
    )


@pytest.mark.asyncio
async def test_get_optimal_instance_type_with_local_ssd_constraint(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test getting optimal instance type with local SSD requirement."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    # Arrange
    constraints = {
        "min_local_ssd": 500,  # Only n2-standard-4-lssd meets this
        "boot_disk_types": ["pd-balanced"],
        "use_spot": False,
    }

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    selected_price_info = await gcp_instance_manager_n1_n2.get_optimal_instance_type(constraints)

    # Assert
    assert selected_price_info["name"] == "n2-standard-4-lssd"
    assert selected_price_info["zone"] == f"{gcp_instance_manager_n1_n2._region}-*"
    assert selected_price_info["total_price"] == pytest.approx(
        N2_CPU_PRICE * 4
        + N2_RAM_PRICE * 16
        + LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2
        + PD_BALANCED_PRICE * 10,
        rel=1e-6,
    )


@pytest.mark.asyncio
async def test_get_optimal_instance_type_prefer_more_cpus(
    gcp_instance_manager_n1_2_4: GCPComputeInstanceManager,
    mock_instance_types_n1_2_4: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test that among equally priced instances, one with more CPUs is preferred."""
    gcp_instance_manager_n1_2_4 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_2_4)
    mock_instance_types_n1_2_4 = copy.deepcopy(mock_instance_types_n1_2_4)
    # Arrange
    constraints = {
        "boot_disk_types": ["pd-standard"],
        "use_spot": False,
    }

    gcp_instance_manager_n1_2_4._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    selected_price_info = await gcp_instance_manager_n1_2_4.get_optimal_instance_type(constraints)

    # Assert
    # n1-standard-4 should be chosen because it has more CPUs (4 vs 2)
    # even though it has the same price
    assert selected_price_info["name"] == "n1-standard-4"
    assert selected_price_info["zone"] == f"{gcp_instance_manager_n1_2_4._region}-*"
    assert selected_price_info["total_price"] == pytest.approx(
        N1_4_CPU_PRICE * 4 + N1_4_RAM_PRICE * 15 + PD_STANDARD_PRICE * 10, rel=1e-6
    )


@pytest.mark.asyncio
async def test_get_optimal_instance_type_no_pricing_data(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mocker,
) -> None:
    """Test get_optimal_instance_type raises ValueError if no pricing data is found for any instance types (line 705)."""
    # Arrange: available instance types, but get_instance_pricing returns empty dict
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    constraints = {
        "use_spot": False,
    }
    mocker.patch.object(gcp_instance_manager_n1_n2, "get_instance_pricing", return_value={})
    # Act & Assert
    with pytest.raises(ValueError, match="No pricing data found for any instance types"):
        await gcp_instance_manager_n1_n2.get_optimal_instance_type(constraints)


@pytest.mark.asyncio
async def test_get_optimal_instance_type_logs_sorted_instances(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
    caplog,
) -> None:
    """Test get_optimal_instance_type logs sorted instance types by price (line 712)."""
    # Arrange: Use two instance types with different prices
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    constraints = {
        "boot_disk_types": ["pd-standard", "pd-balanced"],
        "use_spot": False,
    }

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    with caplog.at_level("DEBUG"):
        await gcp_instance_manager_n1_n2.get_optimal_instance_type(constraints)
    # Assert: log should contain the sorted instance types by price
    assert any(
        "Instance types sorted by price (cheapest and most vCPUs first):" in record.message
        for record in caplog.records
    )
    # Also check that the log contains the expected instance type names
    assert any(
        "[  1] n1-standard-2        (pd-standard )" in record.message for record in caplog.records
    )
    assert any(
        "[  2] n1-standard-2        (pd-balanced )" in record.message for record in caplog.records
    )
    assert any(
        "[  3] n2-standard-4-lssd   (pd-standard )" in record.message for record in caplog.records
    )
    assert any(
        "[  4] n2-standard-4-lssd   (pd-balanced )" in record.message for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_optimal_instance_type_partial_pricing(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_optimal_instance_type returns the valid instance if only one has pricing."""
    # Arrange: two instance types, one with valid pricing, one with None
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    constraints = {
        "boot_disk_types": ["pd-balanced"],
        "use_spot": False,
    }

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    selected_price_info = await gcp_instance_manager_n1_n2.get_optimal_instance_type(constraints)
    # Assert: should not raise, should return the valid instance
    assert isinstance(selected_price_info, dict)
    assert selected_price_info["name"] == "n2-standard-4-lssd"


@pytest.mark.asyncio
async def test_get_optimal_instance_type_all_missing_pricing(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    ram_pricing_n1_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_optimal_instance_type raises ValueError if all pricing data is missing."""
    # Arrange: two instance types, both with None pricing
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    constraints = {
        "boot_disk_types": ["pd-balanced"],
        "use_spot": False,
    }

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        ram_pricing_n1_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    # Act & Assert
    with pytest.raises(ValueError, match="No pricing data found for any instance types"):
        await gcp_instance_manager_n1_n2.get_optimal_instance_type(constraints)
