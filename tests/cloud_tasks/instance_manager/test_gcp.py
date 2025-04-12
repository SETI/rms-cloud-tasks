"""Unit tests for the GCP Compute Engine instance manager."""

import asyncio
from typing import Any, Dict, List, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from google.cloud import compute_v1, billing
from google.oauth2.credentials import Credentials

from cloud_tasks.common.config import GCPConfig
from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager


@pytest.fixture
def mock_credentials() -> MagicMock:
    """Create mock credentials for testing."""
    credentials = MagicMock(spec=Credentials)
    credentials.token = "mock-token"
    credentials.valid = True
    credentials.expired = False
    return credentials


@pytest.fixture
def mock_default_credentials(mock_credentials: MagicMock) -> Tuple[MagicMock, str]:
    """Create mock default credentials tuple for testing."""
    return mock_credentials, "test-project"


@pytest.fixture
def mock_pricing_sku() -> MagicMock:
    """Create a mock pricing SKU with standard pricing info."""
    sku = MagicMock()
    pricing_info = MagicMock()
    pricing_info.pricing_expression.usage_unit = "h"
    tier_rate = MagicMock()
    tier_rate.unit_price.nanos = 1000000000  # $1.00
    pricing_info.pricing_expression.tiered_rates = [tier_rate]
    sku.pricing_info = [pricing_info]
    sku.description = "N1 Instance Core running in Americas"
    sku.service_regions = ["us-central1"]
    return sku


@pytest.fixture
def mock_instance_types() -> Dict[str, Dict[str, Any]]:
    """Create mock instance types dictionary."""
    return {
        "n1-standard-2": {
            "name": "n1-standard-2",
            "vcpu": 2,
            "mem_gb": 7.5,
            "local_ssd_gb": 0,
            "storage_gb": 0,
            "architecture": "x86_64",
            "supports_spot": True,
            "description": "2 vCPUs, 7.5 GB RAM",
        },
        "n2-standard-4-lssd": {
            "name": "n2-standard-4-lssd",
            "vcpu": 4,
            "mem_gb": 16,
            "local_ssd_gb": 750,  # 2 * 375 GB
            "storage_gb": 0,
            "architecture": "x86_64",
            "supports_spot": True,
            "description": "4 vCPUs, 16 GB RAM, 2 local SSD",
        },
    }


@pytest.fixture
def gcp_config() -> GCPConfig:
    """Create a mock GCP configuration for testing."""
    return GCPConfig(
        project_id="test-project",
        region="us-central1",
        zone="us-central1-a",
        credentials_file=None,
        instance_types=None,
        service_account=None,
    )


@pytest.fixture
def mock_machine_type() -> MagicMock:
    """Create a mock machine type for testing."""
    machine = MagicMock()
    machine.name = "n1-standard-2"
    machine.description = "2 vCPUs, 7.5 GB RAM"
    machine.guest_cpus = 2
    machine.memory_mb = 7680  # 7.5 GB in MB
    machine.architecture = "x86_64"
    machine.self_link = "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n1-standard-2"
    return machine


@pytest.fixture
def mock_machine_type_with_ssd() -> MagicMock:
    """Create a mock machine type with local SSD for testing."""
    machine = MagicMock()
    machine.name = "n2-standard-4-lssd"
    machine.description = "4 vCPUs, 16 GB RAM, 2 local SSD"
    machine.guest_cpus = 4
    machine.memory_mb = 16384  # 16 GB in MB
    machine.architecture = "x86_64"
    machine.self_link = "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n2-standard-4-lssd"
    return machine


@pytest.fixture
def mock_machine_types_client(
    mock_machine_type: MagicMock, mock_machine_type_with_ssd: MagicMock
) -> MagicMock:
    """Create a mock machine types client."""
    client = MagicMock()
    client.list.return_value = [mock_machine_type, mock_machine_type_with_ssd]
    return client


@pytest_asyncio.fixture
async def gcp_instance_manager(
    gcp_config: GCPConfig,
    mock_machine_types_client: MagicMock,
    mock_default_credentials: Tuple[MagicMock, str],
) -> GCPComputeInstanceManager:
    """Create a GCP instance manager with mocked dependencies."""
    with patch("google.auth.default", return_value=mock_default_credentials), patch(
        "google.cloud.compute_v1.InstancesClient", return_value=MagicMock()
    ), patch("google.cloud.compute_v1.ZonesClient", return_value=MagicMock()), patch(
        "google.cloud.compute_v1.RegionsClient", return_value=MagicMock()
    ), patch(
        "google.cloud.compute_v1.MachineTypesClient", return_value=mock_machine_types_client
    ), patch(
        "google.cloud.compute_v1.ImagesClient", return_value=MagicMock()
    ), patch(
        "google.cloud.billing.CloudCatalogClient", return_value=MagicMock()
    ):
        manager = GCPComputeInstanceManager(gcp_config)
        return manager


@pytest.mark.asyncio
async def test_get_available_instance_types_no_constraints(
    gcp_instance_manager: GCPComputeInstanceManager, mock_machine_types_client: MagicMock
) -> None:
    """Test getting available instance types with no constraints."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": None,
        "max_cpu": None,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Act
    result = await gcp_instance_manager.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 2
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" in result

    # Verify n1-standard-2 instance details
    n1_instance = result["n1-standard-2"]
    assert n1_instance["vcpu"] == 2
    assert n1_instance["mem_gb"] == 7.5
    assert n1_instance["local_ssd_gb"] == 0
    assert n1_instance["architecture"] == "x86_64"

    # Verify n2-standard-4-lssd instance details
    n2_instance = result["n2-standard-4-lssd"]
    assert n2_instance["vcpu"] == 4
    assert n2_instance["mem_gb"] == 16
    assert n2_instance["local_ssd_gb"] == 750  # 2 * 375 GB
    assert n2_instance["architecture"] == "x86_64"


@pytest.mark.asyncio
async def test_get_available_instance_types_with_cpu_constraints(
    gcp_instance_manager: GCPComputeInstanceManager, mock_machine_types_client: MagicMock
) -> None:
    """Test getting available instance types with CPU constraints."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": 3,
        "max_cpu": 4,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Act
    result = await gcp_instance_manager.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 1
    assert "n2-standard-4-lssd" in result
    assert "n1-standard-2" not in result


@pytest.mark.asyncio
async def test_get_available_instance_types_with_memory_constraints(
    gcp_instance_manager: GCPComputeInstanceManager, mock_machine_types_client: MagicMock
) -> None:
    """Test getting available instance types with memory constraints."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": None,
        "max_cpu": None,
        "min_total_memory": 10,
        "max_total_memory": 20,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Act
    result = await gcp_instance_manager.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 1
    assert "n2-standard-4-lssd" in result
    assert "n1-standard-2" not in result


@pytest.mark.asyncio
async def test_get_available_instance_types_with_instance_type_filter(
    gcp_instance_manager: GCPComputeInstanceManager, mock_machine_types_client: MagicMock
) -> None:
    """Test getting available instance types with instance type pattern filter."""
    # Arrange
    constraints = {
        "instance_types": ["n1-.*"],
        "min_cpu": None,
        "max_cpu": None,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Act
    result = await gcp_instance_manager.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 1
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" not in result


@pytest.mark.asyncio
async def test_get_available_instance_types_with_no_matches(
    gcp_instance_manager: GCPComputeInstanceManager, mock_machine_types_client: MagicMock
) -> None:
    """Test getting available instance types with constraints that match no instances."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": 8,  # Higher than any available instance
        "max_cpu": None,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Act
    result = await gcp_instance_manager.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 0


@pytest.mark.asyncio
async def test_get_available_instance_types_with_memory_per_cpu_constraints(
    gcp_instance_manager: GCPComputeInstanceManager, mock_machine_types_client: MagicMock
) -> None:
    """Test getting available instance types with memory per CPU constraints."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": None,
        "max_cpu": None,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": 3.5,  # n1-standard-2 has 3.75 GB/CPU
        "max_memory_per_cpu": 3.8,  # Upper bound excludes n2-standard-4-lssd which has 4 GB/CPU
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Act
    result = await gcp_instance_manager.get_available_instance_types(constraints)

    # Assert
    assert len(result) == 1
    assert "n1-standard-2" in result
    assert "n2-standard-4-lssd" not in result  # Has 4 GB/CPU


@pytest.mark.asyncio
async def test_get_billing_compute_skus_first_call(
    gcp_instance_manager: GCPComputeInstanceManager,
) -> None:
    """Test getting billing compute SKUs on first call."""
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
    gcp_instance_manager._billing_client.list_services.return_value = [
        mock_service,
        MagicMock(display_name="Other Service"),
    ]
    gcp_instance_manager._billing_client.list_skus.return_value = mock_skus

    # Act
    result = await gcp_instance_manager._get_billing_compute_skus()

    # Assert
    assert result == mock_skus
    assert gcp_instance_manager._billing_compute_skus == mock_skus
    gcp_instance_manager._billing_client.list_services.assert_called_once()
    gcp_instance_manager._billing_client.list_skus.assert_called_once_with(
        request=billing.ListSkusRequest(parent="services/compute")
    )


@pytest.mark.asyncio
async def test_get_billing_compute_skus_cached(
    gcp_instance_manager: GCPComputeInstanceManager,
) -> None:
    """Test getting billing compute SKUs when they are already cached."""
    # Arrange
    mock_skus = [MagicMock(), MagicMock()]
    gcp_instance_manager._billing_compute_skus = mock_skus

    # Act
    result = await gcp_instance_manager._get_billing_compute_skus()

    # Assert
    assert result == mock_skus
    gcp_instance_manager._billing_client.list_services.assert_not_called()
    gcp_instance_manager._billing_client.list_skus.assert_not_called()


@pytest.mark.asyncio
async def test_get_billing_compute_skus_service_not_found(
    gcp_instance_manager: GCPComputeInstanceManager,
) -> None:
    """Test getting billing compute SKUs when compute service is not found."""
    # Arrange
    gcp_instance_manager._billing_client.list_services.return_value = [
        MagicMock(display_name="Other Service"),
        MagicMock(display_name="Another Service"),
    ]

    # Act & Assert
    with pytest.raises(
        RuntimeError, match="Could not find compute service 'Compute Engine' in billing catalog"
    ):
        await gcp_instance_manager._get_billing_compute_skus()

    assert gcp_instance_manager._billing_compute_skus is None
    gcp_instance_manager._billing_client.list_services.assert_called_once()
    gcp_instance_manager._billing_client.list_skus.assert_not_called()


@pytest.mark.asyncio
async def test_get_billing_compute_skus_empty_skus(
    gcp_instance_manager: GCPComputeInstanceManager,
) -> None:
    """Test getting billing compute SKUs when no SKUs are returned."""
    # Arrange
    mock_service = MagicMock()
    mock_service.display_name = "Compute Engine"
    mock_service.name = "services/compute"

    gcp_instance_manager._billing_client.list_services.return_value = [mock_service]
    gcp_instance_manager._billing_client.list_skus.return_value = []

    # Act
    result = await gcp_instance_manager._get_billing_compute_skus()

    # Assert
    assert result == []
    assert gcp_instance_manager._billing_compute_skus == []
    gcp_instance_manager._billing_client.list_services.assert_called_once()
    gcp_instance_manager._billing_client.list_skus.assert_called_once_with(
        request=billing.ListSkusRequest(parent="services/compute")
    )


@pytest.mark.asyncio
async def test_get_instance_pricing_basic(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting instance pricing with basic successful case."""
    # Arrange
    core_sku = mock_pricing_sku
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 500000000  # $0.50
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]

    # Mock the billing SKUs response
    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku]

    # Act
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    pricing = result["n1-standard-2"][f"{gcp_instance_manager._region}-*"]

    # Verify all pricing fields
    assert pricing["cpu_price"] == 2.0  # $1.00 * 2 vCPUs
    assert pricing["per_cpu_price"] == 1.0
    assert pricing["mem_price"] == 3.75  # $0.50 * 7.5 GB
    assert pricing["mem_per_gb_price"] == 0.50
    assert pricing["local_ssd_price"] == 0  # No local SSD
    assert pricing["local_ssd_per_gb_price"] == 0
    assert pricing["storage_price"] == 0  # No additional storage
    assert pricing["storage_per_gb_price"] == 0
    assert pricing["total_price"] == 5.75  # $2.00 + $3.75
    assert pricing["zone"] == f"{gcp_instance_manager._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n1-standard-2"
    assert pricing["vcpu"] == 2
    assert pricing["mem_gb"] == 7.5
    assert pricing["local_ssd_gb"] == 0
    assert pricing["storage_gb"] == 0
    assert pricing["architecture"] == "x86_64"
    assert pricing["supports_spot"] == True


@pytest.mark.asyncio
async def test_get_instance_pricing_with_local_ssd(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting instance pricing for instance with local SSD."""
    # Arrange
    core_sku = mock_pricing_sku
    core_sku.description = "N2 Instance Core running in Americas"
    ram_sku = MagicMock()
    ram_sku.description = "N2 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 500000000  # $0.50
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]

    local_ssd_sku = MagicMock()
    local_ssd_sku.description = "N2 Local SSD provisioned space running in Americas"
    local_ssd_sku.service_regions = ["us-central1"]
    ssd_pricing_info = MagicMock()
    ssd_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    ssd_tier_rate = MagicMock()
    # $0.17/GB/hour * 730.5 hours/month = $124.185/GB/month
    # $124.185 * 1e9 nanos = 124185000000 nanos
    ssd_tier_rate.unit_price.nanos = 124185000000
    ssd_pricing_info.pricing_expression.tiered_rates = [ssd_tier_rate]
    local_ssd_sku.pricing_info = [ssd_pricing_info]

    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku, local_ssd_sku]

    # Act
    result = await gcp_instance_manager.get_instance_pricing(
        {"n2-standard-4-lssd": mock_instance_types["n2-standard-4-lssd"]}, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n2-standard-4-lssd" in result
    pricing = result["n2-standard-4-lssd"][f"{gcp_instance_manager._region}-*"]

    # Verify all pricing fields
    assert pricing["cpu_price"] == 4.0  # $1.00 * 4 vCPUs
    assert pricing["per_cpu_price"] == 1.0
    assert pricing["mem_price"] == 8.0  # $0.50 * 16 GB
    assert pricing["mem_per_gb_price"] == 0.50
    assert pricing["local_ssd_price"] == pytest.approx(127.5, rel=1e-6)  # $0.17 * 750 GB
    assert pricing["local_ssd_per_gb_price"] == pytest.approx(0.17, rel=1e-6)  # $0.17/GB/hour
    assert pricing["storage_price"] == 0  # No additional storage
    assert pricing["storage_per_gb_price"] == 0
    assert pricing["total_price"] == pytest.approx(139.5, rel=1e-6)  # $4.0 + $8.0 + $127.5
    assert pricing["zone"] == f"{gcp_instance_manager._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n2-standard-4-lssd"
    assert pricing["vcpu"] == 4
    assert pricing["mem_gb"] == 16
    assert pricing["local_ssd_gb"] == 750
    assert pricing["storage_gb"] == 0
    assert pricing["architecture"] == "x86_64"
    assert pricing["supports_spot"] == True


@pytest.mark.asyncio
async def test_get_instance_pricing_spot_instance(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting instance pricing for spot instances."""
    # Arrange
    core_sku = mock_pricing_sku
    core_sku.description = "N1 Preemptible Instance Core running in Americas"
    ram_sku = MagicMock()
    ram_sku.description = "N1 Preemptible Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 500000000  # $0.50
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]

    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku]

    # Act
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=True
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    pricing = result["n1-standard-2"][f"{gcp_instance_manager._region}-*"]

    # Verify all pricing fields
    assert pricing["cpu_price"] == 2.0
    assert pricing["per_cpu_price"] == 1.0
    assert pricing["mem_price"] == 3.75
    assert pricing["mem_per_gb_price"] == 0.50
    assert pricing["local_ssd_price"] == 0
    assert pricing["local_ssd_per_gb_price"] == 0
    assert pricing["storage_price"] == 0
    assert pricing["storage_per_gb_price"] == 0
    assert pricing["total_price"] == 5.75
    assert pricing["zone"] == f"{gcp_instance_manager._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n1-standard-2"
    assert pricing["vcpu"] == 2
    assert pricing["mem_gb"] == 7.5
    assert pricing["local_ssd_gb"] == 0
    assert pricing["storage_gb"] == 0
    assert pricing["architecture"] == "x86_64"
    assert pricing["supports_spot"] == True


@pytest.mark.asyncio
async def test_get_instance_pricing_cache_hit(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
) -> None:
    """Test getting instance pricing with cache hit."""
    # Arrange
    # Pre-populate the cache with all fields
    machine_family = "n1"
    use_spot = False
    cached_pricing = {
        f"{gcp_instance_manager._region}-*": {
            "cpu_price": 2.0,
            "per_cpu_price": 1.0,
            "mem_price": 3.75,
            "mem_per_gb_price": 0.50,
            "local_ssd_price": 0,
            "local_ssd_per_gb_price": 0,
            "storage_price": 0,
            "storage_per_gb_price": 0,
            "total_price": 5.75,
            "zone": f"{gcp_instance_manager._region}-*",
        }
    }
    gcp_instance_manager._instance_pricing_cache[(machine_family, use_spot)] = cached_pricing

    # Act
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    pricing = result["n1-standard-2"][f"{gcp_instance_manager._region}-*"]

    # Verify all cached pricing fields
    assert pricing["cpu_price"] == 2.0
    assert pricing["per_cpu_price"] == 1.0
    assert pricing["mem_price"] == 3.75
    assert pricing["mem_per_gb_price"] == 0.50
    assert pricing["local_ssd_price"] == 0
    assert pricing["local_ssd_per_gb_price"] == 0
    assert pricing["storage_price"] == 0
    assert pricing["storage_per_gb_price"] == 0
    assert pricing["total_price"] == 5.75
    assert pricing["zone"] == f"{gcp_instance_manager._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n1-standard-2"
    assert pricing["vcpu"] == 2
    assert pricing["mem_gb"] == 7.5
    assert pricing["local_ssd_gb"] == 0
    assert pricing["storage_gb"] == 0
    assert pricing["architecture"] == "x86_64"
    assert pricing["supports_spot"] == True

    # Verify the billing client was not called
    gcp_instance_manager._billing_client.list_services.assert_not_called()


@pytest.mark.asyncio
async def test_get_optimal_instance_type_basic(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting optimal instance type with minimal constraints."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": None,
        "max_cpu": None,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Set up pricing SKUs
    core_sku = mock_pricing_sku  # $1.00/core/hour
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 500000000  # $0.50/GB/hour
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]

    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku]

    # Act
    instance_type, zone, price = await gcp_instance_manager.get_optimal_instance_type(constraints)

    # Assert
    # n1-standard-2 should be chosen as it's cheaper
    assert instance_type == "n1-standard-2"
    assert zone == f"{gcp_instance_manager._region}-*"
    assert price == pytest.approx(5.75)  # 2 cores * $1.00 + 7.5GB * $0.50


@pytest.mark.asyncio
async def test_get_optimal_instance_type_no_matches(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting optimal instance type when no instances match constraints."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": 8,  # Higher than any available instance
        "max_cpu": None,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Act & Assert
    with pytest.raises(ValueError, match="No instance type meets requirements"):
        await gcp_instance_manager.get_optimal_instance_type(constraints)


@pytest.mark.asyncio
async def test_get_optimal_instance_type_spot_instance(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting optimal instance type with spot instance requirement."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": None,
        "max_cpu": None,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": True,
    }

    # Set up spot pricing SKUs
    core_sku = MagicMock()
    core_sku.description = "N1 Preemptible Instance Core running in Americas"
    core_sku.service_regions = ["us-central1"]
    core_pricing_info = MagicMock()
    core_pricing_info.pricing_expression.usage_unit = "h"
    core_tier_rate = MagicMock()
    core_tier_rate.unit_price.nanos = 300000000  # $0.30/core/hour (70% discount)
    core_pricing_info.pricing_expression.tiered_rates = [core_tier_rate]
    core_sku.pricing_info = [core_pricing_info]

    ram_sku = MagicMock()
    ram_sku.description = "N1 Preemptible Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 150000000  # $0.15/GB/hour (70% discount)
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]

    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku]

    # Act
    instance_type, zone, price = await gcp_instance_manager.get_optimal_instance_type(constraints)

    # Assert
    assert instance_type == "n1-standard-2"
    assert zone == f"{gcp_instance_manager._region}-*"
    assert price == pytest.approx(1.725)  # (2 cores * $0.30 + 7.5GB * $0.15)


@pytest.mark.asyncio
async def test_get_optimal_instance_type_with_memory_constraints(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting optimal instance type with memory constraints."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": None,
        "max_cpu": None,
        "min_total_memory": 10,  # Only n2-standard-4-lssd meets this
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Set up N2 pricing SKUs
    core_sku = MagicMock()
    core_sku.description = "N2 Instance Core running in Americas"
    core_sku.service_regions = ["us-central1"]
    core_pricing_info = MagicMock()
    core_pricing_info.pricing_expression.usage_unit = "h"
    core_tier_rate = MagicMock()
    core_tier_rate.unit_price.nanos = 400000000  # $0.40/core/hour
    core_pricing_info.pricing_expression.tiered_rates = [core_tier_rate]
    core_sku.pricing_info = [core_pricing_info]

    ram_sku = MagicMock()
    ram_sku.description = "N2 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 200000000  # $0.20/GB/hour
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]

    local_ssd_sku = MagicMock()
    local_ssd_sku.description = "N2 Local SSD provisioned space running in Americas"
    local_ssd_sku.service_regions = ["us-central1"]
    ssd_pricing_info = MagicMock()
    ssd_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    ssd_tier_rate = MagicMock()
    ssd_tier_rate.unit_price.nanos = 0  # Free for this test
    ssd_pricing_info.pricing_expression.tiered_rates = [ssd_tier_rate]
    local_ssd_sku.pricing_info = [ssd_pricing_info]

    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku, local_ssd_sku]

    # Act
    instance_type, zone, price = await gcp_instance_manager.get_optimal_instance_type(constraints)

    # Assert
    assert instance_type == "n2-standard-4-lssd"
    assert zone == f"{gcp_instance_manager._region}-*"
    assert price == pytest.approx(4.80)  # 4 cores * $0.40 + 16GB * $0.20


@pytest.mark.asyncio
async def test_get_optimal_instance_type_with_local_ssd_constraint(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting optimal instance type with local SSD requirement."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": None,
        "max_cpu": None,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": 500,  # Only n2-standard-4-lssd meets this
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Set up N2 pricing SKUs
    core_sku = MagicMock()
    core_sku.description = "N2 Instance Core running in Americas"
    core_sku.service_regions = ["us-central1"]
    core_pricing_info = MagicMock()
    core_pricing_info.pricing_expression.usage_unit = "h"
    core_tier_rate = MagicMock()
    core_tier_rate.unit_price.nanos = 400000000  # $0.40/core/hour
    core_pricing_info.pricing_expression.tiered_rates = [core_tier_rate]
    core_sku.pricing_info = [core_pricing_info]

    ram_sku = MagicMock()
    ram_sku.description = "N2 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 200000000  # $0.20/GB/hour
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]

    local_ssd_sku = MagicMock()
    local_ssd_sku.description = "N2 Local SSD provisioned space running in Americas"
    local_ssd_sku.service_regions = ["us-central1"]
    ssd_pricing_info = MagicMock()
    ssd_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    ssd_tier_rate = MagicMock()
    ssd_tier_rate.unit_price.nanos = 124185000000  # $0.17/GB/hour
    ssd_pricing_info.pricing_expression.tiered_rates = [ssd_tier_rate]
    local_ssd_sku.pricing_info = [ssd_pricing_info]

    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku, local_ssd_sku]

    # Act
    instance_type, zone, price = await gcp_instance_manager.get_optimal_instance_type(constraints)

    # Assert
    assert instance_type == "n2-standard-4-lssd"
    assert zone == f"{gcp_instance_manager._region}-*"
    assert price == pytest.approx(132.3)  # 4 cores * $0.40 + 16GB * $0.20 + 750GB * $0.17


@pytest.mark.asyncio
async def test_get_optimal_instance_type_prefer_more_cpus(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test that among equally priced instances, one with more CPUs is preferred."""
    # Arrange
    constraints = {
        "instance_types": None,
        "min_cpu": None,
        "max_cpu": None,
        "min_total_memory": None,
        "max_total_memory": None,
        "min_memory_per_cpu": None,
        "max_memory_per_cpu": None,
        "min_local_ssd": None,
        "max_local_ssd": None,
        "min_local_ssd_per_cpu": None,
        "max_local_ssd_per_cpu": None,
        "min_storage": None,
        "max_storage": None,
        "min_storage_per_cpu": None,
        "max_storage_per_cpu": None,
        "use_spot": False,
    }

    # Set up N1 pricing SKUs
    n1_core_sku = MagicMock()
    n1_core_sku.description = "N1 Instance Core running in Americas"
    n1_core_sku.service_regions = ["us-central1"]
    n1_core_pricing_info = MagicMock()
    n1_core_pricing_info.pricing_expression.usage_unit = "h"
    n1_core_tier_rate = MagicMock()
    n1_core_tier_rate.unit_price.nanos = 1000000000  # $1.00/core/hour
    n1_core_pricing_info.pricing_expression.tiered_rates = [n1_core_tier_rate]
    n1_core_sku.pricing_info = [n1_core_pricing_info]

    n1_ram_sku = MagicMock()
    n1_ram_sku.description = "N1 Instance Ram running in Americas"
    n1_ram_sku.service_regions = ["us-central1"]
    n1_ram_pricing_info = MagicMock()
    n1_ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    n1_ram_tier_rate = MagicMock()
    n1_ram_tier_rate.unit_price.nanos = 500000000  # $0.50/GB/hour
    n1_ram_pricing_info.pricing_expression.tiered_rates = [n1_ram_tier_rate]
    n1_ram_sku.pricing_info = [n1_ram_pricing_info]

    # Set up N2 pricing SKUs with adjusted prices to match N1 total
    n2_core_sku = MagicMock()
    n2_core_sku.description = "N2 Instance Core running in Americas"
    n2_core_sku.service_regions = ["us-central1"]
    n2_core_pricing_info = MagicMock()
    n2_core_pricing_info.pricing_expression.usage_unit = "h"
    n2_core_tier_rate = MagicMock()
    n2_core_tier_rate.unit_price.nanos = 500000000  # $0.50/core/hour (half price)
    n2_core_pricing_info.pricing_expression.tiered_rates = [n2_core_tier_rate]
    n2_core_sku.pricing_info = [n2_core_pricing_info]

    n2_ram_sku = MagicMock()
    n2_ram_sku.description = "N2 Instance Ram running in Americas"
    n2_ram_sku.service_regions = ["us-central1"]
    n2_ram_pricing_info = MagicMock()
    n2_ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    n2_ram_tier_rate = MagicMock()
    n2_ram_tier_rate.unit_price.nanos = 500000000 * 7.5 / 16  # (half price)
    n2_ram_pricing_info.pricing_expression.tiered_rates = [n2_ram_tier_rate]
    n2_ram_sku.pricing_info = [n2_ram_pricing_info]

    # Add local SSD SKU with very low price
    local_ssd_sku = MagicMock()
    local_ssd_sku.description = "N2 Local SSD provisioned space running in Americas"
    local_ssd_sku.service_regions = ["us-central1"]
    ssd_pricing_info = MagicMock()
    ssd_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    ssd_tier_rate = MagicMock()
    ssd_tier_rate.unit_price.nanos = 0  # So we don't mess up the total price
    ssd_pricing_info.pricing_expression.tiered_rates = [ssd_tier_rate]
    local_ssd_sku.pricing_info = [ssd_pricing_info]

    # N1: 2 cores * $1.00 + 7.5GB * $0.50 = $5.75
    # N2: 4 cores * $0.50 + 16GB * $0.25 + negligible SSD = $6.00
    gcp_instance_manager._billing_compute_skus = [
        n1_core_sku,
        n1_ram_sku,
        n2_core_sku,
        n2_ram_sku,
        local_ssd_sku,
    ]

    # Act
    instance_type, zone, price = await gcp_instance_manager.get_optimal_instance_type(constraints)

    # Assert
    # n2-standard-4-lssd should be chosen because it has more CPUs (4 vs 2)
    # even though it has the same price
    assert instance_type == "n2-standard-4-lssd"
    assert zone == f"{gcp_instance_manager._region}-*"
    assert price == pytest.approx(5.75, rel=1e-2)
