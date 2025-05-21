"""Unit tests for the GCP Compute Engine instance manager."""

import copy
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest
from google.cloud import billing

from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager

from .conftest import (
    deepcopy_gcp_instance_manager,
    N1_2_CPU_PRICE,
    N1_2_RAM_PRICE,
    N2_CPU_PRICE,
    N2_RAM_PRICE,
    N1_PREEMPTIBLE_CPU_PRICE,
    N1_PREEMPTIBLE_RAM_PRICE,
    PD_STANDARD_PRICE,
    PD_BALANCED_PRICE,
    LSSD_PRICE,
)


@pytest.mark.asyncio
async def test_get_billing_compute_skus_first_call(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting billing compute SKUs on first call."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange
    mock_sku1 = MagicMock()
    mock_sku1.description = "Compute Engine Instance Core"
    mock_sku2 = MagicMock()
    mock_sku2.description = "Compute Engine RAM"
    mock_skus = [mock_sku1, mock_sku2]

    mock_service = MagicMock()
    mock_service.display_name = "Compute Engine"
    mock_service.name = "services/compute"

    # Mock the billing client's list_services and list_skus methods
    gcp_instance_manager_n1_n2._billing_client.list_services.return_value = [
        mock_service,
        MagicMock(display_name="Other Service"),
    ]
    gcp_instance_manager_n1_n2._billing_client.list_skus.return_value = mock_skus

    # Act
    result = await gcp_instance_manager_n1_n2._get_billing_compute_skus()

    # Assert
    assert result == mock_skus
    assert gcp_instance_manager_n1_n2._billing_compute_skus == mock_skus
    gcp_instance_manager_n1_n2._billing_client.list_services.assert_called_once()
    gcp_instance_manager_n1_n2._billing_client.list_skus.assert_called_once_with(
        request=billing.ListSkusRequest(parent="services/compute")
    )


@pytest.mark.asyncio
async def test_get_billing_compute_skus_cached(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting billing compute SKUs when they are already cached."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange
    mock_skus = [MagicMock(), MagicMock()]
    gcp_instance_manager_n1_n2._billing_compute_skus = mock_skus

    # Act
    result = await gcp_instance_manager_n1_n2._get_billing_compute_skus()

    # Assert
    assert result == mock_skus
    gcp_instance_manager_n1_n2._billing_client.list_services.assert_not_called()
    gcp_instance_manager_n1_n2._billing_client.list_skus.assert_not_called()


@pytest.mark.asyncio
async def test_get_billing_compute_skus_service_not_found(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting billing compute SKUs when compute service is not found."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange
    gcp_instance_manager_n1_n2._billing_client.list_services.return_value = [
        MagicMock(display_name="Other Service"),
        MagicMock(display_name="Another Service"),
    ]

    # Act & Assert
    with pytest.raises(
        RuntimeError, match="Could not find compute service 'Compute Engine' in billing catalog"
    ):
        await gcp_instance_manager_n1_n2._get_billing_compute_skus()

    assert gcp_instance_manager_n1_n2._billing_compute_skus is None
    gcp_instance_manager_n1_n2._billing_client.list_services.assert_called_once()
    gcp_instance_manager_n1_n2._billing_client.list_skus.assert_not_called()


@pytest.mark.asyncio
async def test_get_billing_compute_skus_empty_skus(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test getting billing compute SKUs when no SKUs are returned."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange
    mock_service = MagicMock()
    mock_service.display_name = "Compute Engine"
    mock_service.name = "services/compute"

    gcp_instance_manager_n1_n2._billing_client.list_services.return_value = [mock_service]
    gcp_instance_manager_n1_n2._billing_client.list_skus.return_value = []

    # Act
    result = await gcp_instance_manager_n1_n2._get_billing_compute_skus()

    # Assert
    assert result == []
    assert gcp_instance_manager_n1_n2._billing_compute_skus == []
    gcp_instance_manager_n1_n2._billing_client.list_services.assert_called_once()
    gcp_instance_manager_n1_n2._billing_client.list_skus.assert_called_once_with(
        request=billing.ListSkusRequest(parent="services/compute")
    )


@pytest.mark.asyncio
async def test_extract_pricing_info_no_pricing_info(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    caplog,
) -> None:
    """Test _extract_pricing_info when SKU has no pricing info."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_sku = MagicMock()
    mock_sku.pricing_info = []
    mock_sku.description = "Test SKU"
    with caplog.at_level("WARNING"):
        result = gcp_instance_manager_n1_n2._extract_pricing_info(
            "n1-standard", mock_sku, "h", "core"
        )
    assert result is None
    assert any("No pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_extract_pricing_info_multiple_pricing_info(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    caplog,
) -> None:
    """Test _extract_pricing_info when SKU has multiple pricing info entries."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_sku = MagicMock()
    mock_pricing_info1 = MagicMock()
    mock_pricing_info2 = MagicMock()
    mock_sku.pricing_info = [mock_pricing_info1, mock_pricing_info2]
    mock_sku.description = "Test SKU"
    with caplog.at_level("WARNING"):
        result = gcp_instance_manager_n1_n2._extract_pricing_info(
            "n1-standard", mock_sku, "h", "core"
        )
    assert result is None
    assert any("Multiple pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_extract_pricing_info_unknown_unit(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    caplog,
) -> None:
    """Test _extract_pricing_info when SKU has unknown pricing unit."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_sku = MagicMock()
    mock_pricing_info = MagicMock()
    mock_pricing_info.pricing_expression.usage_unit = "unknown_unit"
    mock_sku.pricing_info = [mock_pricing_info]
    mock_sku.description = "Test SKU"
    with caplog.at_level("WARNING"):
        result = gcp_instance_manager_n1_n2._extract_pricing_info(
            "n1-standard", mock_sku, "h", "core"
        )
    assert result is None
    assert any("has unknown pricing unit" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_extract_pricing_info_no_tiered_rates(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    caplog,
) -> None:
    """Test _extract_pricing_info when SKU has no tiered rates."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_sku = MagicMock()
    mock_pricing_info = MagicMock()
    mock_pricing_info.pricing_expression.usage_unit = "h"
    mock_pricing_info.pricing_expression.tiered_rates = []
    mock_sku.pricing_info = [mock_pricing_info]
    mock_sku.description = "Test SKU"
    with caplog.at_level("WARNING"):
        result = gcp_instance_manager_n1_n2._extract_pricing_info(
            "n1-standard", mock_sku, "h", "core"
        )
    assert result is None
    assert any("No tiered rates found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_extract_pricing_info_multiple_tiered_rates(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    caplog,
) -> None:
    """Test _extract_pricing_info when SKU has multiple tiered rates."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_sku = MagicMock()
    mock_pricing_info = MagicMock()
    mock_pricing_info.pricing_expression.usage_unit = "h"
    mock_tier1 = MagicMock()
    mock_tier1.unit_price.nanos = 1000000000  # $1.00
    mock_tier2 = MagicMock()
    mock_tier2.unit_price.nanos = 1000000000  # $1.00
    mock_pricing_info.pricing_expression.tiered_rates = [mock_tier1, mock_tier2]
    mock_sku.pricing_info = [mock_pricing_info]
    mock_sku.description = "Test SKU"
    with caplog.at_level("WARNING"):
        result = gcp_instance_manager_n1_n2._extract_pricing_info(
            "n1-standard", mock_sku, "h", "core"
        )
    assert result is mock_tier1
    assert any("Multiple pricing tiers found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_extract_pricing_info_success(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test _extract_pricing_info successful case."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Create mock SKU with valid pricing info
    mock_sku = MagicMock()
    mock_pricing_info = MagicMock()
    mock_pricing_info.pricing_expression.usage_unit = "h"
    mock_tier = MagicMock()
    mock_tier.unit_price.nanos = 1000000000  # $1.00
    mock_pricing_info.pricing_expression.tiered_rates = [mock_tier]
    mock_sku.pricing_info = [mock_pricing_info]
    mock_sku.description = "Test SKU"

    # Call the method and verify result
    result = gcp_instance_manager_n1_n2._extract_pricing_info("n1-standard", mock_sku, "h", "core")

    assert result is not None
    assert result.unit_price.nanos == 1000000000


@pytest.mark.asyncio
async def test_get_instance_pricing_basic(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test getting instance pricing with basic successful case."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    # Mock the billing SKUs response
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + list(boot_disk_pricing_default_skus)

    # Act
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    print(result)
    pricing = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]

    # Verify all pricing fields
    assert pricing["cpu_price"] == N1_2_CPU_PRICE * 2
    assert pricing["per_cpu_price"] == N1_2_CPU_PRICE
    assert pricing["mem_price"] == N1_2_RAM_PRICE * 7.5
    assert pricing["mem_per_gb_price"] == N1_2_RAM_PRICE
    assert pricing["boot_disk_type"] == "pd-standard"
    assert pricing["boot_disk_price"] == pytest.approx(PD_STANDARD_PRICE * 10, rel=1e-6)
    assert pricing["boot_disk_per_gb_price"] == PD_STANDARD_PRICE
    assert pricing["boot_disk_per_iops_price"] == 0
    assert pricing["boot_disk_iops_price"] == 0
    assert pricing["boot_disk_per_throughput_price"] == 0
    assert pricing["boot_disk_throughput_price"] == 0
    assert pricing["local_ssd_price"] == 0
    assert pricing["local_ssd_per_gb_price"] == 0
    assert (
        pricing["total_price"] == N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10
    )
    assert (
        pricing["total_price_per_cpu"]
        == (N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10) / 2
    )
    assert pricing["zone"] == f"{gcp_instance_manager_n1_n2._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n1-standard-2"
    assert pricing["cpu_family"] == "Intel Haswell"
    assert pricing["cpu_rank"] == 5
    assert pricing["vcpu"] == 2
    assert pricing["mem_gb"] == 7.5
    assert pricing["local_ssd_gb"] == 0
    assert pricing["available_boot_disk_types"] == ["pd-standard"]
    assert pricing["boot_disk_iops"] == 0
    assert pricing["boot_disk_throughput"] == 0
    assert pricing["boot_disk_gb"] == 10
    assert pricing["architecture"] == "X86_64"
    assert pricing["supports_spot"] is True
    assert pricing["description"] == "2 vCPUs, 7.5 GB RAM"
    assert (
        pricing["url"]
        == "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n1-standard-2"
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_with_local_ssd(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test getting instance pricing for instance with local SSD."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        {"n2-standard-4-lssd": mock_instance_types_n1_n2["n2-standard-4-lssd"]}, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n2-standard-4-lssd" in result
    pricing = result["n2-standard-4-lssd"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-balanced"]

    # Verify all pricing fields
    assert pricing["cpu_price"] == N2_CPU_PRICE * 4
    assert pricing["per_cpu_price"] == N2_CPU_PRICE
    assert pricing["mem_price"] == N2_RAM_PRICE * 16
    assert pricing["mem_per_gb_price"] == N2_RAM_PRICE
    assert pricing["boot_disk_type"] == "pd-balanced"
    assert pricing["boot_disk_price"] == pytest.approx(PD_BALANCED_PRICE * 20, rel=1e-6)
    assert pricing["boot_disk_per_gb_price"] == PD_BALANCED_PRICE
    assert pricing["boot_disk_per_iops_price"] == 0
    assert pricing["boot_disk_iops_price"] == 0
    assert pricing["boot_disk_per_throughput_price"] == 0
    assert pricing["boot_disk_throughput_price"] == 0
    assert pricing["local_ssd_price"] == pytest.approx(
        LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2, rel=1e-6
    )
    assert pricing["local_ssd_per_gb_price"] == LSSD_PRICE
    assert pricing["total_price"] == pytest.approx(
        N2_CPU_PRICE * 4
        + N2_RAM_PRICE * 16
        + PD_BALANCED_PRICE * 20
        + LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2,
        rel=1e-6,
    )
    assert pricing["total_price_per_cpu"] == pytest.approx(
        (
            N2_CPU_PRICE * 4
            + N2_RAM_PRICE * 16
            + PD_BALANCED_PRICE * 20
            + LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2
        )
        / 4,
        rel=1e-6,
    )
    assert pricing["zone"] == f"{gcp_instance_manager_n1_n2._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n2-standard-4-lssd"
    assert pricing["cpu_family"] == "Intel Cascade Lake"
    assert pricing["cpu_rank"] == 13
    assert pricing["vcpu"] == 4
    assert pricing["mem_gb"] == 16
    assert pricing["local_ssd_gb"] == GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2
    assert pricing["available_boot_disk_types"] == ["pd-balanced"]
    assert pricing["boot_disk_iops"] == 0
    assert pricing["boot_disk_throughput"] == 0
    assert pricing["boot_disk_gb"] == 20
    assert pricing["architecture"] == "X86_64"
    assert pricing["supports_spot"] is True
    assert pricing["description"] == "4 vCPUs, 16 GB RAM, 2 local SSD"


@pytest.mark.asyncio
async def test_get_instance_pricing_spot_instance(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_preemptible_sku: MagicMock,
    ram_pricing_n1_preemptible_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test getting instance pricing for spot instances."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_preemptible_sku,
        ram_pricing_n1_preemptible_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=True
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    pricing = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]

    # Verify all pricing fields
    assert pricing["cpu_price"] == N1_PREEMPTIBLE_CPU_PRICE * 2
    assert pricing["per_cpu_price"] == N1_PREEMPTIBLE_CPU_PRICE
    assert pricing["mem_price"] == N1_PREEMPTIBLE_RAM_PRICE * 7.5
    assert pricing["mem_per_gb_price"] == N1_PREEMPTIBLE_RAM_PRICE
    assert pricing["boot_disk_type"] == "pd-standard"
    assert pricing["boot_disk_price"] == pytest.approx(PD_STANDARD_PRICE * 10, rel=1e-6)
    assert pricing["boot_disk_per_gb_price"] == PD_STANDARD_PRICE
    assert pricing["boot_disk_per_iops_price"] == 0
    assert pricing["boot_disk_iops_price"] == 0
    assert pricing["boot_disk_per_throughput_price"] == 0
    assert pricing["boot_disk_throughput_price"] == 0
    assert pricing["local_ssd_price"] == 0
    assert pricing["local_ssd_per_gb_price"] == 0
    assert (
        pricing["total_price"]
        == N1_PREEMPTIBLE_CPU_PRICE * 2 + N1_PREEMPTIBLE_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10
    )
    assert (
        pricing["total_price_per_cpu"]
        == (N1_PREEMPTIBLE_CPU_PRICE * 2 + N1_PREEMPTIBLE_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10)
        / 2
    )
    assert pricing["zone"] == f"{gcp_instance_manager_n1_n2._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n1-standard-2"
    assert pricing["cpu_family"] == "Intel Haswell"
    assert pricing["cpu_rank"] == 5
    assert pricing["vcpu"] == 2
    assert pricing["mem_gb"] == 7.5
    assert pricing["local_ssd_gb"] == 0
    assert pricing["available_boot_disk_types"] == ["pd-standard"]
    assert pricing["boot_disk_iops"] == 0
    assert pricing["boot_disk_throughput"] == 0
    assert pricing["boot_disk_gb"] == 10
    assert pricing["architecture"] == "X86_64"
    assert pricing["supports_spot"] is True
    assert pricing["description"] == "2 vCPUs, 7.5 GB RAM"


@pytest.mark.asyncio
async def test_get_instance_pricing_cache_hit(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
) -> None:
    """Test getting instance pricing with cache hit."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    # Arrange
    # Pre-populate the cache with all fields
    machine_family = "n1"
    use_spot = False
    cached_pricing = {
        f"{gcp_instance_manager_n1_n2._region}-*": {
            "pd-standard": {
                "cpu_price": N1_2_CPU_PRICE * 2,
                "per_cpu_price": N1_2_CPU_PRICE,
                "mem_price": N1_2_RAM_PRICE * 7.5,
                "mem_per_gb_price": N1_2_RAM_PRICE,
                "boot_disk_type": "pd-standard",
                "boot_disk_price": PD_STANDARD_PRICE * 10,
                "boot_disk_per_gb_price": PD_STANDARD_PRICE,
                "boot_disk_per_iops_price": 0,
                "boot_disk_iops_price": 0,
                "boot_disk_per_throughput_price": 0,
                "boot_disk_throughput_price": 0,
                "local_ssd_price": 0,
                "local_ssd_per_gb_price": 0,
                "total_price": N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10,
                "total_price_per_cpu": (
                    N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10
                )
                / 2,
                "zone": f"{gcp_instance_manager_n1_n2._region}-*",
            }
        }
    }
    gcp_instance_manager_n1_n2._instance_pricing_cache[(machine_family, use_spot)] = cached_pricing

    # Act
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    pricing = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]

    # Verify all cached pricing fields
    assert pricing["cpu_price"] == N1_2_CPU_PRICE * 2
    assert pricing["per_cpu_price"] == N1_2_CPU_PRICE
    assert pricing["mem_price"] == N1_2_RAM_PRICE * 7.5
    assert pricing["mem_per_gb_price"] == N1_2_RAM_PRICE
    assert pricing["boot_disk_type"] == "pd-standard"
    assert pricing["boot_disk_price"] == pytest.approx(PD_STANDARD_PRICE * 10, rel=1e-6)
    assert pricing["boot_disk_per_gb_price"] == PD_STANDARD_PRICE
    assert pricing["boot_disk_per_iops_price"] == 0
    assert pricing["boot_disk_iops_price"] == 0
    assert pricing["boot_disk_per_throughput_price"] == 0
    assert pricing["boot_disk_throughput_price"] == 0
    assert pricing["local_ssd_price"] == 0
    assert pricing["local_ssd_per_gb_price"] == 0
    assert pricing["boot_disk_price"] == pytest.approx(PD_STANDARD_PRICE * 10, rel=1e-6)
    assert pricing["boot_disk_per_gb_price"] == PD_STANDARD_PRICE
    assert (
        pricing["total_price"] == N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10
    )
    assert (
        pricing["total_price_per_cpu"]
        == (N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10) / 2
    )
    assert pricing["zone"] == f"{gcp_instance_manager_n1_n2._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n1-standard-2"
    assert pricing["cpu_family"] == "Intel Haswell"
    assert pricing["cpu_rank"] == 5
    assert pricing["vcpu"] == 2
    assert pricing["mem_gb"] == 7.5
    assert pricing["local_ssd_gb"] == 0
    assert pricing["available_boot_disk_types"] == ["pd-standard"]
    assert pricing["boot_disk_iops"] == 0
    assert pricing["boot_disk_throughput"] == 0
    assert pricing["boot_disk_gb"] == 10
    assert pricing["architecture"] == "X86_64"
    assert pricing["supports_spot"] is True
    assert pricing["description"] == "2 vCPUs, 7.5 GB RAM"
    assert (
        pricing["url"]
        == "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n1-standard-2"
    )

    # Verify the billing client was not called
    gcp_instance_manager_n1_n2._billing_client.list_services.assert_not_called()


@pytest.mark.asyncio
async def test_get_instance_pricing_no_family_skus(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing when no SKUs are found for a machine family."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        mock_instance_types_n1_n2, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" in result
    # Instance type should have an empty pricing dictionary since no matching SKUs were found
    assert result["n1-standard-2"] == {}
    assert result["n2-standard-4-lssd"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_component_skus(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing when SKUs for some components are missing."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        mock_instance_types_n1_n2, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" in result
    # Instance type should have an empty pricing dictionary since RAM SKU is missing
    assert result["n1-standard-2"] != {}
    assert result["n2-standard-4-lssd"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_region_mismatch(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing when SKUs don't match the expected region."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    cpu_pricing_n1_sku = copy.deepcopy(cpu_pricing_n1_sku)
    cpu_pricing_n2_sku = copy.deepcopy(cpu_pricing_n2_sku)
    cpu_pricing_n1_sku.service_regions = ["europe-west1"]  # Different region
    cpu_pricing_n2_sku.service_regions = ["europe-west1"]  # Different region

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    # Act
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        mock_instance_types_n1_n2, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" in result
    # Instance type should have an empty pricing dictionary since no SKUs match the region
    assert result["n1-standard-2"] == {}
    assert result["n2-standard-4-lssd"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_empty_instance_types(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
) -> None:
    """Test get_instance_pricing when an empty instance types dict is provided."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Act
    result = await gcp_instance_manager_n1_n2.get_instance_pricing({}, use_spot=False)
    # Assert
    assert result == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_no_billing_skus(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
) -> None:
    """Test get_instance_pricing when no billing SKUs are available."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._billing_compute_skus = []
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        mock_instance_types_n1_n2, use_spot=False
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" in result
    assert result["n1-standard-2"] == {}
    assert result["n2-standard-4-lssd"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_multiple_skus_for_cpu(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing when multiple SKUs exist for a component (ambiguous)."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    dup_cpu_pricing_n2_sku = MagicMock()
    dup_cpu_pricing_n2_sku.description = "N2 Instance Core running in Americas DUPLICATE * 2"
    dup_cpu_pricing_n2_sku.service_regions = ["us-central1"]
    dup_cpu_pricing_info = MagicMock()
    dup_cpu_pricing_info.pricing_expression.usage_unit = "h"
    dup_cpu_tier_rate = MagicMock()
    dup_cpu_tier_rate.unit_price.nanos = N2_CPU_PRICE * 1e9 * 2  # Double the price
    dup_cpu_pricing_info.pricing_expression.tiered_rates = [dup_cpu_tier_rate]
    dup_cpu_pricing_n2_sku.pricing_info = [dup_cpu_pricing_info]

    gcp_instance_manager_n1_n2._billing_compute_skus = [  # No duplicates
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            mock_instance_types_n1_n2, use_spot=False
        )
    assert not any(
        "Multiple core SKUs found for n2 in region us-central1" in record.message
        for record in caplog.records
    )

    pricing_n1 = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]
    pricing_n2 = result["n2-standard-4-lssd"][f"{gcp_instance_manager_n1_n2._region}-*"][
        "pd-balanced"
    ]
    assert pricing_n1["total_price"] == pytest.approx(
        N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10, rel=1e-6
    )
    assert pricing_n2["total_price"] == pytest.approx(
        N2_CPU_PRICE * 4
        + N2_RAM_PRICE * 16
        + PD_BALANCED_PRICE * 20
        + LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2,
        rel=1e-6,
    )

    gcp_instance_manager_n1_n2._instance_pricing_cache = {}
    gcp_instance_manager_n1_n2._billing_compute_skus = [  # Duplicate
        dup_cpu_pricing_n2_sku,
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            mock_instance_types_n1_n2, use_spot=False
        )
    assert any(
        "Multiple core SKUs found for n2 in region us-central1" in record.message
        for record in caplog.records
    )

    pricing_n1 = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]
    pricing_n2 = result["n2-standard-4-lssd"][f"{gcp_instance_manager_n1_n2._region}-*"][
        "pd-balanced"
    ]
    assert pricing_n1["total_price"] == pytest.approx(
        N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10, rel=1e-6
    )
    assert pricing_n2["total_price"] == pytest.approx(
        N2_CPU_PRICE * 4 * 2
        + N2_RAM_PRICE * 16
        + PD_BALANCED_PRICE * 20
        + LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2,
        rel=1e-6,
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_multiple_skus_for_ram(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing when multiple SKUs exist for a component (ambiguous)."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    dup_ram_pricing_n2_sku = MagicMock()
    dup_ram_pricing_n2_sku.description = "N2 Instance Ram running in Americas DUPLICATE * 2"
    dup_ram_pricing_n2_sku.service_regions = ["us-central1"]
    dup_ram_pricing_info = MagicMock()
    dup_ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    dup_ram_tier_rate = MagicMock()
    dup_ram_tier_rate.unit_price.nanos = N2_RAM_PRICE * 1e9 * 2  # Double the price
    dup_ram_pricing_info.pricing_expression.tiered_rates = [dup_ram_tier_rate]
    dup_ram_pricing_n2_sku.pricing_info = [dup_ram_pricing_info]

    gcp_instance_manager_n1_n2._billing_compute_skus = [  # No duplicates
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            mock_instance_types_n1_n2, use_spot=False
        )
    assert not any(
        "Multiple ram SKUs found for n2 in region us-central1" in record.message
        for record in caplog.records
    )

    pricing_n1 = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]
    pricing_n2 = result["n2-standard-4-lssd"][f"{gcp_instance_manager_n1_n2._region}-*"][
        "pd-balanced"
    ]
    assert pricing_n1["total_price"] == pytest.approx(
        N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10, rel=1e-6
    )
    assert pricing_n2["total_price"] == pytest.approx(
        N2_CPU_PRICE * 4
        + N2_RAM_PRICE * 16
        + PD_BALANCED_PRICE * 20
        + LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2,
        rel=1e-6,
    )

    gcp_instance_manager_n1_n2._instance_pricing_cache = {}
    gcp_instance_manager_n1_n2._billing_compute_skus = [  # Duplicate
        dup_ram_pricing_n2_sku,
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            mock_instance_types_n1_n2, use_spot=False
        )
    assert any(
        "Multiple ram SKUs found for n2 in region us-central1" in record.message
        for record in caplog.records
    )

    pricing_n1 = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]
    pricing_n2 = result["n2-standard-4-lssd"][f"{gcp_instance_manager_n1_n2._region}-*"][
        "pd-balanced"
    ]
    assert pricing_n1["total_price"] == pytest.approx(
        N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10, rel=1e-6
    )
    assert pricing_n2["total_price"] == pytest.approx(
        N2_CPU_PRICE * 4
        + N2_RAM_PRICE * 16 * 2
        + PD_BALANCED_PRICE * 20
        + LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2,
        rel=1e-6,
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_multiple_skus_for_ssd(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    local_ssd_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing when multiple SKUs exist for a component (ambiguous)."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    dup_local_ssd_pricing_n2_sku = MagicMock()
    dup_local_ssd_pricing_n2_sku.description = (
        "N2 Local SSD provisioned space running in Americas DUPLICATE * 2"
    )
    dup_local_ssd_pricing_n2_sku.service_regions = ["us-central1"]
    dup_local_ssd_pricing_info = MagicMock()
    dup_local_ssd_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    dup_local_ssd_tier_rate = MagicMock()
    dup_local_ssd_tier_rate.unit_price.nanos = (
        LSSD_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH * 2
    )  # Double the price
    dup_local_ssd_pricing_info.pricing_expression.tiered_rates = [dup_local_ssd_tier_rate]
    dup_local_ssd_pricing_n2_sku.pricing_info = [dup_local_ssd_pricing_info]

    gcp_instance_manager_n1_n2._billing_compute_skus = [  # No duplicates
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            mock_instance_types_n1_n2, use_spot=False
        )
    assert not any(
        "Multiple local SSD SKUs found for n2 in region us-central1" in record.message
        for record in caplog.records
    )

    pricing_n1 = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]
    pricing_n2 = result["n2-standard-4-lssd"][f"{gcp_instance_manager_n1_n2._region}-*"][
        "pd-balanced"
    ]
    assert pricing_n1["total_price"] == pytest.approx(
        N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10, rel=1e-6
    )
    print(pricing_n2)
    assert pricing_n2["total_price"] == pytest.approx(
        N2_CPU_PRICE * 4
        + N2_RAM_PRICE * 16
        + PD_BALANCED_PRICE * 20
        + LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2,
        rel=1e-6,
    )

    gcp_instance_manager_n1_n2._instance_pricing_cache = {}
    gcp_instance_manager_n1_n2._billing_compute_skus = [  # Duplicate
        dup_local_ssd_pricing_n2_sku,
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
        local_ssd_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            mock_instance_types_n1_n2, use_spot=False
        )
    assert any(
        "Multiple local SSD SKUs found for n2 in region us-central1" in record.message
        for record in caplog.records
    )

    pricing_n1 = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]
    pricing_n2 = result["n2-standard-4-lssd"][f"{gcp_instance_manager_n1_n2._region}-*"][
        "pd-balanced"
    ]
    assert pricing_n1["total_price"] == pytest.approx(
        N1_2_CPU_PRICE * 2 + N1_2_RAM_PRICE * 7.5 + PD_STANDARD_PRICE * 10, rel=1e-6
    )
    assert pricing_n2["total_price"] == pytest.approx(
        N2_CPU_PRICE * 4
        + N2_RAM_PRICE * 16
        + PD_BALANCED_PRICE * 20
        + LSSD_PRICE * GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2 * 2,
        rel=1e-6,
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_pricing_info_none(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing when pricing info extraction returns None for a component."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    ram_pricing_n1_sku = MagicMock()
    ram_pricing_n1_sku.description = "N1 Instance Ram running in Americas"
    ram_pricing_n1_sku.service_regions = ["us-central1"]
    ram_pricing_n1_sku.pricing_info = [MagicMock()]

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + boot_disk_pricing_default_skus

    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_spot_not_available(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing when spot pricing is requested but not available."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)

    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + boot_disk_pricing_default_skus
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=True
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_ignores_sole_tenancy_custom_commitment(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing ignores sole tenancy, custom, and commitment SKUs (continue)."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    for desc in [
        "N1 Instance Core running in Americas Sole Tenancy",
        "N1 Custom Instance Core running in Americas",
        "N1 Instance Core running in Americas Commitment",
    ]:
        sku = MagicMock()
        sku.description = desc
        sku.service_regions = ["us-central1"]
        pricing_info = MagicMock()
        pricing_info.pricing_expression.usage_unit = "h"
        tier_rate = MagicMock()
        tier_rate.unit_price.nanos = 1000000000
        pricing_info.pricing_expression.tiered_rates = [tier_rate]
        sku.pricing_info = [pricing_info]
        gcp_instance_manager_n1_n2._billing_compute_skus = [
            ram_pricing_n1_sku,
            sku,
        ] + boot_disk_pricing_default_skus
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
        assert result is not None
        assert "n1-standard-2" in result
        assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_local_ssd_sku_but_no_ssd(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing ignores local SSD SKUs if instance type has no SSD (continue)."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_instance_types_n1_n2 = copy.deepcopy(mock_instance_types_n1_n2)
    ssd_sku = MagicMock()
    ssd_sku.description = "N1 Local SSD provisioned space running in Americas"
    ssd_sku.service_regions = ["us-central1"]
    pricing_info = MagicMock()
    pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    tier_rate = MagicMock()
    tier_rate.unit_price.nanos = 1000000000
    pricing_info.pricing_expression.tiered_rates = [tier_rate]
    ssd_sku.pricing_info = [pricing_info]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        ssd_sku,
    ] + boot_disk_pricing_default_skus
    # n1-standard-2 has no local SSD
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert result["n1-standard-2"] != {}


@pytest.mark.asyncio
async def test_get_instance_pricing_lssd_instance_no_local_ssd_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n2_sku: MagicMock,
    ram_pricing_n2_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if -lssd instance has no local SSD SKU."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange: Instance type with -lssd, but no local SSD SKU is present
    # No local SSD SKU
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n2_sku,
        ram_pricing_n2_sku,
    ] + boot_disk_pricing_default_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n2-standard-4-lssd": mock_instance_types_n1_n2["n2-standard-4-lssd"]}, use_spot=False
        )
    assert result["n2-standard-4-lssd"] == {}
    assert any(
        "No local SSD SKU found for instance family n2 in region us-central1; ignoring these instance types"
        in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_preemptible_mismatch(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    cpu_pricing_n1_preemptible_sku: MagicMock,
    ram_pricing_n1_preemptible_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing ignores preemptible SKUs if use_spot is False, and vice versa (continue)."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        cpu_pricing_n1_preemptible_sku,
        ram_pricing_n1_preemptible_sku,
    ] + boot_disk_pricing_default_skus
    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
    )
    pricing = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]
    assert pricing["cpu_price"] == N1_2_CPU_PRICE * 2

    result = await gcp_instance_manager_n1_n2.get_instance_pricing(
        {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=True
    )
    pricing = result["n1-standard-2"][f"{gcp_instance_manager_n1_n2._region}-*"]["pd-standard"]
    assert pricing["cpu_price"] == N1_PREEMPTIBLE_CPU_PRICE * 2


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_core_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if core SKU is missing (line 556)."""
    # Arrange: Only RAM SKU present
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        ram_pricing_n1_sku,
    ] + boot_disk_pricing_default_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No core SKU found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_ram_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if RAM SKU is missing (line 572)."""
    # Arrange: Only core SKU present
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
    ] + boot_disk_pricing_default_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No ram SKU found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_pd_standard_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if pd-standard SKU is missing."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Filter out pd-standard from default skus
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "storage pd capacity" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + filtered_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any(
        f"No PD Standard boot disk SKU found for instance family n1 in region {gcp_instance_manager_n1_n2._region}"
        in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_pd_balanced_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if pd-balanced SKU is missing."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Filter out pd-balanced from default skus
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "balanced pd capacity" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + filtered_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any(
        f"No PD Balanced boot disk SKU found for instance family n1 in region {gcp_instance_manager_n1_n2._region}"
        in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_pd_ssd_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if pd-ssd SKU is missing."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Filter out pd-ssd from default skus
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "ssd backed pd capacity" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + filtered_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any(
        f"No PD SSD boot disk SKU found for instance family n1 in region {gcp_instance_manager_n1_n2._region}"
        in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_pd_extreme_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if pd-extreme SKU is missing."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Filter out pd-extreme from default skus
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "extreme pd capacity" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + filtered_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any(
        f"No PD Extreme boot disk SKU found for instance family n1 in region {gcp_instance_manager_n1_n2._region}"
        in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_hd_balanced_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if hd-balanced SKU is missing."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Filter out hd-balanced from default skus
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "hyperdisk balanced capacity" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + filtered_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any(
        f"No HD Balanced boot disk SKU found for instance family n1 in region {gcp_instance_manager_n1_n2._region}"
        in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_pd_extreme_iops_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if pd-extreme IOPS SKU is missing."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Filter out pd-extreme IOPS from default skus
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "extreme pd iops" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + filtered_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any(
        f"No PD Extreme IOPS SKU found for instance family n1 in region {gcp_instance_manager_n1_n2._region}"
        in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_hd_balanced_iops_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if hd-balanced IOPS SKU is missing."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Filter out hd-balanced IOPS from default skus
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "hyperdisk balanced iops" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + filtered_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any(
        f"No HD Balanced IOPS SKU found for instance family n1 in region {gcp_instance_manager_n1_n2._region}"
        in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_hd_balanced_throughput_sku(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if hd-balanced throughput SKU is missing."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Filter out hd-balanced throughput from default skus
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "hyperdisk balanced throughput" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
    ] + filtered_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any(
        f"No HD Balanced Throughput SKU found for instance family n1 in region {gcp_instance_manager_n1_n2._region}"
        in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_cpu_pricing_info_none(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if cpu_pricing_info is None."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange: Core SKU with no pricing info (so _extract_pricing_info returns None), valid RAM SKU
    cpu_sku = MagicMock()
    cpu_sku.description = "N1 Instance Core running in Americas"
    cpu_sku.service_regions = ["us-central1"]
    cpu_sku.pricing_info = []  # No pricing info triggers None
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_sku,
        ram_pricing_n1_sku,
    ] + boot_disk_pricing_default_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_ram_pricing_info_none(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if ram_pricing_info is None (line 606)."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange: RAM SKU with no pricing info (so _extract_pricing_info returns None), valid core SKU
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_sku.pricing_info = []  # No pricing info triggers None
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_sku,
    ] + boot_disk_pricing_default_skus
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_boot_disk_pricing_info_none_pd_standard(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if pd-standard boot disk pricing_info is None."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange: pd-standard SKU with no pricing info (so _extract_pricing_info returns None)
    pd_standard_sku = MagicMock()
    pd_standard_sku.description = "Storage PD Capacity in Iowa"
    pd_standard_sku.service_regions = ["us-central1"]
    pd_standard_sku.pricing_info = []  # No pricing info triggers None

    # Filter out pd-standard from default skus and add our mock
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "Storage PD Capacity" not in sku.description
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        pd_standard_sku,
    ] + filtered_skus

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    print(result)
    assert result["n1-standard-2"] == {}
    assert any("No pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_boot_disk_pricing_info_none_pd_extreme_iops(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if pd-extreme IOPS pricing_info is None."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange: pd-extreme IOPS SKU with no pricing info (so _extract_pricing_info returns None)
    pd_extreme_iops_sku = MagicMock()
    pd_extreme_iops_sku.description = "extreme pd iops in Iowa"
    pd_extreme_iops_sku.service_regions = ["us-central1"]
    pd_extreme_iops_sku.pricing_info = []  # No pricing info triggers None

    # Filter out pd-extreme IOPS from default skus and add our mock
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "extreme pd iops" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        pd_extreme_iops_sku,
    ] + filtered_skus

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_boot_disk_pricing_info_none_hd_balanced_iops(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if hd-balanced IOPS pricing_info is None."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange: hd-balanced IOPS SKU with no pricing info (so _extract_pricing_info returns None)
    hd_balanced_iops_sku = MagicMock()
    hd_balanced_iops_sku.description = "hyperdisk balanced iops in Iowa"
    hd_balanced_iops_sku.service_regions = ["us-central1"]
    hd_balanced_iops_sku.pricing_info = []  # No pricing info triggers None

    # Filter out hd-balanced IOPS from default skus and add our mock
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "hyperdisk balanced iops" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        hd_balanced_iops_sku,
    ] + filtered_skus

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_boot_disk_pricing_info_none_hd_balanced_throughput(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager,
    mock_instance_types_n1_n2: Dict[str, Dict[str, Any]],
    caplog,
    cpu_pricing_n1_sku: MagicMock,
    ram_pricing_n1_sku: MagicMock,
    boot_disk_pricing_default_skus: List[MagicMock],
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if hd-balanced throughput pricing_info is None."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    # Arrange: hd-balanced throughput SKU with no pricing info (so _extract_pricing_info returns None)
    hd_balanced_throughput_sku = MagicMock()
    hd_balanced_throughput_sku.description = "hyperdisk balanced throughput in Iowa"
    hd_balanced_throughput_sku.service_regions = ["us-central1"]
    hd_balanced_throughput_sku.pricing_info = []  # No pricing info triggers None

    # Filter out hd-balanced throughput from default skus and add our mock
    filtered_skus = [
        sku
        for sku in boot_disk_pricing_default_skus
        if "hyperdisk balanced throughput" not in sku.description.lower()
    ]
    gcp_instance_manager_n1_n2._billing_compute_skus = [
        cpu_pricing_n1_sku,
        ram_pricing_n1_sku,
        hd_balanced_throughput_sku,
    ] + filtered_skus

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager_n1_n2.get_instance_pricing(
            {"n1-standard-2": mock_instance_types_n1_n2["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No pricing info found" in record.message for record in caplog.records)
