"""Unit tests for the GCP Compute Engine instance manager."""

import copy
from typing import Any, Dict, Tuple, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch
import random
import time

import pytest
import pytest_asyncio
from google.api_core.exceptions import NotFound  # type: ignore
from google.cloud import billing
from google.oauth2.credentials import Credentials
import uuid as _uuid  # Import uuid module with alias to avoid conflicts
import asyncio
from google.cloud import compute_v1
from google.api_core import exceptions as gcp_exceptions
import logging

from cloud_tasks.common.config import GCPConfig
from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager


# We go to a lot of effort to make sure these tests run fast. It turns out that the patches
# in gcp_instance_manager take 0.3 seconds for every test that is run. To avoid this, we
# make that fixture module scope. Unfortunatley this means that we also have to make all the
# fixtures it depends on module scope as well. Then, since many of the tests mustate the
# fixture return values, we have to deepcopy them at the top of each test. However, we can't
# just deepcopy the gcp_instance_manager object, because it contains a thread variable that
# can't be serialized. So, we have a special routine to handle that case.


@pytest.fixture(scope="module")
def event_loop():
    """Create an instance of the default event loop for module-scope async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def mock_credentials() -> MagicMock:
    """Create mock credentials for testing."""
    credentials = MagicMock(spec=Credentials)
    credentials.token = "mock-token"
    credentials.valid = True
    credentials.expired = False
    return credentials


@pytest.fixture(scope="module")
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
            "boot_disk_gb": 0,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "2 vCPUs, 7.5 GB RAM",
        },
        "n2-standard-4-lssd": {
            "name": "n2-standard-4-lssd",
            "vcpu": 4,
            "mem_gb": 16,
            "local_ssd_gb": 750,  # 2 * 375 GB
            "boot_disk_gb": 0,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "4 vCPUs, 16 GB RAM, 2 local SSD",
        },
    }


@pytest.fixture(scope="module")
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


@pytest.fixture(scope="module")
def mock_machine_type() -> MagicMock:
    """Create a mock machine type for testing."""
    machine = MagicMock()
    machine.name = "n1-standard-2"
    machine.description = "2 vCPUs, 7.5 GB RAM"
    machine.guest_cpus = 2
    machine.memory_mb = 7680  # 7.5 GB in MB
    machine.architecture = "X86_64"
    machine.self_link = "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n1-standard-2"
    return machine


@pytest.fixture(scope="module")
def mock_machine_type_with_ssd() -> MagicMock:
    """Create a mock machine type with local SSD for testing."""
    machine = MagicMock()
    machine.name = "n2-standard-4-lssd"
    machine.description = "4 vCPUs, 16 GB RAM, 2 local SSD"
    machine.guest_cpus = 4
    machine.memory_mb = 16384  # 16 GB in MB
    machine.architecture = "X86_64"
    machine.self_link = "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n2-standard-4-lssd"
    return machine


@pytest.fixture(scope="module")
def mock_machine_types_client(
    mock_machine_type: MagicMock, mock_machine_type_with_ssd: MagicMock
) -> MagicMock:
    """Create a mock machine types client."""
    client = MagicMock()
    client.list.return_value = [mock_machine_type, mock_machine_type_with_ssd]
    return client


def deepcopy_gcp_instance_manager(
    gcp_instance_manager: GCPComputeInstanceManager,
) -> GCPComputeInstanceManager:
    """Deepcopy a GCP instance manager."""
    if hasattr(gcp_instance_manager, "_thread_local"):
        old_thread = gcp_instance_manager._thread_local
        gcp_instance_manager._thread_local = None
        print(gcp_instance_manager._thread_local)
    new_gcp_instance_manager = copy.deepcopy(gcp_instance_manager)
    if hasattr(gcp_instance_manager, "_thread_local"):
        gcp_instance_manager._thread_local = old_thread
        new_gcp_instance_manager._thread_local = old_thread
    return new_gcp_instance_manager


@pytest_asyncio.fixture(scope="module")
async def gcp_instance_manager(
    gcp_config: GCPConfig,
    mock_machine_types_client: MagicMock,
    mock_default_credentials: Tuple[MagicMock, str],
) -> GCPComputeInstanceManager:
    """Create a GCP instance manager with mocked dependencies."""
    start = time.time()
    with (
        patch("google.auth.default", return_value=mock_default_credentials),
        patch("google.cloud.compute_v1.InstancesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.ZonesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.RegionsClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.MachineTypesClient", return_value=mock_machine_types_client),
        patch("google.cloud.compute_v1.ImagesClient", return_value=MagicMock()),
        patch("google.cloud.billing.CloudCatalogClient", return_value=MagicMock()),
    ):
        manager = GCPComputeInstanceManager(gcp_config)
        end = time.time()
        print(f"Time taken to create GCPComputeInstanceManager: {end - start} seconds")
        return manager


@pytest.mark.asyncio
async def test_get_available_instance_types_no_constraints(
    gcp_instance_manager: GCPComputeInstanceManager, mock_machine_types_client: MagicMock
) -> None:
    """Test getting available instance types with no constraints."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_machine_types_client = copy.deepcopy(mock_machine_types_client)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    assert n1_instance["architecture"] == "X86_64"

    # Verify n2-standard-4-lssd instance details
    n2_instance = result["n2-standard-4-lssd"]
    assert n2_instance["vcpu"] == 4
    assert n2_instance["mem_gb"] == 16
    assert n2_instance["local_ssd_gb"] == 750  # 2 * 375 GB
    assert n2_instance["architecture"] == "X86_64"


@pytest.mark.asyncio
async def test_get_available_instance_types_with_cpu_constraints(
    gcp_instance_manager: GCPComputeInstanceManager, mock_machine_types_client: MagicMock
) -> None:
    """Test getting available instance types with CPU constraints."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_machine_types_client = copy.deepcopy(mock_machine_types_client)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_machine_types_client = copy.deepcopy(mock_machine_types_client)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_machine_types_client = copy.deepcopy(mock_machine_types_client)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_machine_types_client = copy.deepcopy(mock_machine_types_client)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_machine_types_client = copy.deepcopy(mock_machine_types_client)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
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
    assert pricing["boot_disk_price"] == 0  # No additional storage
    assert pricing["boot_disk_per_gb_price"] == 0
    assert pricing["total_price"] == 5.75  # $2.00 + $3.75
    assert pricing["zone"] == f"{gcp_instance_manager._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n1-standard-2"
    assert pricing["vcpu"] == 2
    assert pricing["mem_gb"] == 7.5
    assert pricing["local_ssd_gb"] == 0
    assert pricing["boot_disk_gb"] == 0
    assert pricing["architecture"] == "X86_64"
    assert pricing["supports_spot"] == True


@pytest.mark.asyncio
async def test_get_instance_pricing_with_local_ssd(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting instance pricing for instance with local SSD."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
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
    assert pricing["boot_disk_price"] == 0  # No additional storage
    assert pricing["boot_disk_per_gb_price"] == 0
    assert pricing["total_price"] == pytest.approx(139.5, rel=1e-6)  # $4.0 + $8.0 + $127.5
    assert pricing["zone"] == f"{gcp_instance_manager._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n2-standard-4-lssd"
    assert pricing["vcpu"] == 4
    assert pricing["mem_gb"] == 16
    assert pricing["local_ssd_gb"] == 750
    assert pricing["boot_disk_gb"] == 0
    assert pricing["architecture"] == "X86_64"
    assert pricing["supports_spot"] == True


@pytest.mark.asyncio
async def test_get_instance_pricing_spot_instance(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting instance pricing for spot instances."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
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
    assert pricing["boot_disk_price"] == 0
    assert pricing["boot_disk_per_gb_price"] == 0
    assert pricing["total_price"] == 5.75
    assert pricing["zone"] == f"{gcp_instance_manager._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n1-standard-2"
    assert pricing["vcpu"] == 2
    assert pricing["mem_gb"] == 7.5
    assert pricing["local_ssd_gb"] == 0
    assert pricing["boot_disk_gb"] == 0
    assert pricing["architecture"] == "X86_64"
    assert pricing["supports_spot"] == True


@pytest.mark.asyncio
async def test_get_instance_pricing_cache_hit(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
) -> None:
    """Test getting instance pricing with cache hit."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
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
            "boot_disk_price": 0,
            "boot_disk_per_gb_price": 0,
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
    assert pricing["boot_disk_price"] == 0
    assert pricing["boot_disk_per_gb_price"] == 0
    assert pricing["total_price"] == 5.75
    assert pricing["zone"] == f"{gcp_instance_manager._region}-*"

    # Verify instance info is included
    assert pricing["name"] == "n1-standard-2"
    assert pricing["vcpu"] == 2
    assert pricing["mem_gb"] == 7.5
    assert pricing["local_ssd_gb"] == 0
    assert pricing["boot_disk_gb"] == 0
    assert pricing["architecture"] == "X86_64"
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    selected_price_info = await gcp_instance_manager.get_optimal_instance_type(constraints)

    # Assert
    # n1-standard-2 should be chosen as it's cheaper
    assert selected_price_info["name"] == "n1-standard-2"
    assert selected_price_info["zone"] == f"{gcp_instance_manager._region}-*"
    assert selected_price_info["total_price"] == pytest.approx(
        5.75
    )  # 2 cores * $1.00 + 7.5GB * $0.50


@pytest.mark.asyncio
async def test_get_optimal_instance_type_no_matches(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting optimal instance type when no instances match constraints."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    selected_price_info = await gcp_instance_manager.get_optimal_instance_type(constraints)

    # Assert
    assert selected_price_info["name"] == "n1-standard-2"
    assert selected_price_info["zone"] == f"{gcp_instance_manager._region}-*"
    assert selected_price_info["total_price"] == pytest.approx(
        1.725
    )  # (2 cores * $0.30 + 7.5GB * $0.15)


@pytest.mark.asyncio
async def test_get_optimal_instance_type_with_memory_constraints(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting optimal instance type with memory constraints."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    selected_price_info = await gcp_instance_manager.get_optimal_instance_type(constraints)

    # Assert
    assert selected_price_info["name"] == "n2-standard-4-lssd"
    assert selected_price_info["zone"] == f"{gcp_instance_manager._region}-*"
    assert selected_price_info["total_price"] == pytest.approx(
        4.80
    )  # 4 cores * $0.40 + 16GB * $0.20


@pytest.mark.asyncio
async def test_get_optimal_instance_type_with_local_ssd_constraint(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test getting optimal instance type with local SSD requirement."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    selected_price_info = await gcp_instance_manager.get_optimal_instance_type(constraints)

    # Assert
    assert selected_price_info["name"] == "n2-standard-4-lssd"
    assert selected_price_info["zone"] == f"{gcp_instance_manager._region}-*"
    assert selected_price_info["total_price"] == pytest.approx(
        132.3
    )  # 4 cores * $0.40 + 16GB * $0.20 + 750GB * $0.17


@pytest.mark.asyncio
async def test_get_optimal_instance_type_prefer_more_cpus(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test that among equally priced instances, one with more CPUs is preferred."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
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
    selected_price_info = await gcp_instance_manager.get_optimal_instance_type(constraints)

    # Assert
    # n2-standard-4-lssd should be chosen because it has more CPUs (4 vs 2)
    # even though it has the same price
    assert selected_price_info["name"] == "n2-standard-4-lssd"
    assert selected_price_info["zone"] == f"{gcp_instance_manager._region}-*"
    assert selected_price_info["total_price"] == pytest.approx(5.75, rel=1e-2)


# Tests for the start_instance method
# These tests verify that:
# 1. Basic instance creation works with minimal parameters
# 2. Spot instances are properly configured
# 3. Service accounts are correctly attached when specified
# 4. Different image specification methods work (custom URI, image family, default)
# 5. Zone selection works correctly with wildcards
# 6. Error handling works properly when instance creation fails


@pytest.mark.asyncio
async def test_start_instance_basic(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting a basic instance with minimal parameters."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Arrange
    instance_type = "n1-standard-2"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    image = "ubuntu-2404-lts"

    # Mock the UUID generation to have a predictable instance ID
    mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("uuid.uuid4", return_value=mock_uuid):
        # Mock _get_image_from_family to return a predictable image path
        with patch.object(
            gcp_instance_manager, "_get_image_from_family", new=AsyncMock()
        ) as mock_get_image:
            mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

            # Mock the insert operation and its result
            mock_operation = MagicMock()
            mock_operation.name = "mock-operation-name"
            mock_operation.error_code = None
            mock_operation.warnings = None
            mock_result = MagicMock()
            mock_operation.result.return_value = mock_result

            mock_compute_client = MagicMock()
            mock_compute_client.insert = MagicMock(return_value=mock_operation)

            with patch("google.cloud.compute_v1.InstancesClient", return_value=mock_compute_client):
                # Mock _wait_for_operation to return successfully
                with patch.object(
                    gcp_instance_manager, "_wait_for_operation", new=AsyncMock()
                ) as mock_wait:
                    mock_wait.return_value = mock_result

                    # Act
                    instance_id, zone = await gcp_instance_manager.start_instance(
                        instance_type=instance_type,
                        boot_disk_size=20,
                        startup_script=startup_script,
                        job_id=job_id,
                        use_spot=use_spot,
                        image=image,
                        zone=gcp_instance_manager._zone,
                    )

                    # Assert
                    assert instance_id.startswith(
                        f"{gcp_instance_manager._JOB_ID_TAG_PREFIX}{job_id}-"
                    )
                    assert zone == gcp_instance_manager._zone

                    # Check that the compute client was called with the correct parameters
                    mock_compute_client.insert.assert_called_once()
                    call_args = mock_compute_client.insert.call_args
                    assert call_args[1]["project"] == gcp_instance_manager._project_id
                    assert call_args[1]["zone"] == gcp_instance_manager._zone

                    # Check instance resource configuration
                    instance_config = call_args[1]["instance_resource"]
                    assert instance_config.name == instance_id
                    assert (
                        instance_config.machine_type
                        == f"zones/{gcp_instance_manager._zone}/machineTypes/{instance_type}"
                    )

                    # Check metadata (startup script)
                    assert instance_config.metadata.items[0].key == "startup-script"
                    assert instance_config.metadata.items[0].value == startup_script

                    # Check scheduling (not preemptible)
                    assert not instance_config.scheduling.preemptible

                    # Verify tags for job identification
                    assert instance_config.tags.items == [
                        gcp_instance_manager._job_id_to_tag(job_id)
                    ]

                    # Verify wait_for_operation was called
                    mock_wait.assert_called_once()


@pytest.mark.asyncio
async def test_start_instance_spot(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting a spot instance."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Arrange
    instance_type = "n1-standard-2"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = True
    image = "ubuntu-2404-lts"

    # Mock the UUID generation to have a predictable instance ID
    mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("uuid.uuid4", return_value=mock_uuid):
        # Mock _get_image_from_family to return a predictable image path
        with patch.object(
            gcp_instance_manager, "_get_image_from_family", new=AsyncMock()
        ) as mock_get_image:
            mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

            # Mock the insert operation and its result
            mock_operation = MagicMock()
            mock_operation.name = "mock-operation-name"
            mock_operation.error_code = None
            mock_operation.warnings = None
            mock_result = MagicMock()
            mock_operation.result.return_value = mock_result

            mock_compute_client = MagicMock()
            mock_compute_client.insert = MagicMock(return_value=mock_operation)
            gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

            # Mock _wait_for_operation to return successfully
            with patch.object(
                gcp_instance_manager, "_wait_for_operation", new=AsyncMock()
            ) as mock_wait:
                mock_wait.return_value = mock_result

                # Act
                instance_id, zone = await gcp_instance_manager.start_instance(
                    instance_type=instance_type,
                    boot_disk_size=20,
                    startup_script=startup_script,
                    job_id=job_id,
                    use_spot=use_spot,
                    image=image,
                    zone=gcp_instance_manager._zone,
                )

                # Assert
                assert instance_id.startswith(f"{gcp_instance_manager._JOB_ID_TAG_PREFIX}{job_id}-")
                assert zone == gcp_instance_manager._zone

                # Check that the compute client was called with the correct parameters
                mock_compute_client.insert.assert_called_once()
                call_args = mock_compute_client.insert.call_args

                # Check that spot scheduling was used
                instance_config = call_args[1]["instance_resource"]
                assert instance_config.scheduling.preemptible == True
                assert instance_config.scheduling.automatic_restart == False
                assert instance_config.scheduling.on_host_maintenance == "TERMINATE"


@pytest.mark.asyncio
async def test_start_instance_with_service_account(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting an instance with a service account."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_type = "n1-standard-2"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    image = "ubuntu-2404-lts"

    # Set a service account for the instance
    service_account = "test-service-account@test-project.iam.gserviceaccount.com"
    gcp_instance_manager._service_account = service_account

    # Mock the UUID generation to have a predictable instance ID
    mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("uuid.uuid4", return_value=mock_uuid):
        # Mock _get_image_from_family to return a predictable image path
        with patch.object(
            gcp_instance_manager, "_get_image_from_family", new=AsyncMock()
        ) as mock_get_image:
            mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

            # Mock the insert operation and its result
            mock_operation = MagicMock()
            mock_operation.name = "mock-operation-name"
            mock_operation.error_code = None
            mock_operation.warnings = None
            mock_result = MagicMock()
            mock_operation.result.return_value = mock_result

            mock_compute_client = MagicMock()
            mock_compute_client.insert = MagicMock(return_value=mock_operation)
            gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

            # Mock _wait_for_operation to return successfully
            with patch.object(
                gcp_instance_manager, "_wait_for_operation", new=AsyncMock()
            ) as mock_wait:
                mock_wait.return_value = mock_result

                # Act
                instance_id, zone = await gcp_instance_manager.start_instance(
                    instance_type=instance_type,
                    boot_disk_size=20,
                    startup_script=startup_script,
                    job_id=job_id,
                    use_spot=use_spot,
                    image=image,
                    zone=gcp_instance_manager._zone,
                )

                # Assert
                assert instance_id.startswith(f"{gcp_instance_manager._JOB_ID_TAG_PREFIX}{job_id}-")
                assert zone == gcp_instance_manager._zone

                # Check that the service account was included in the instance configuration
                call_args = mock_compute_client.insert.call_args
                instance_config = call_args[1]["instance_resource"]

                assert len(instance_config.service_accounts) == 1
                assert instance_config.service_accounts[0].email == service_account
                assert instance_config.service_accounts[0].scopes == [
                    "https://www.googleapis.com/auth/cloud-platform"
                ]


@pytest.mark.asyncio
async def test_start_instance_with_custom_image_uri(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting an instance with a custom image URI."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_type = "n1-standard-2"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    custom_image = "https://compute.googleapis.com/compute/v1/projects/my-project/global/images/my-custom-image"

    # Mock the UUID generation to have a predictable instance ID
    mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("uuid.uuid4", return_value=mock_uuid):
        # Mock the insert operation and its result
        mock_operation = MagicMock()
        mock_operation.name = "mock-operation-name"
        mock_operation.error_code = None
        mock_operation.warnings = None
        mock_result = MagicMock()
        mock_operation.result.return_value = mock_result

        mock_compute_client = MagicMock()
        mock_compute_client.insert = MagicMock(return_value=mock_operation)
        gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

        # Mock _wait_for_operation to return successfully
        with patch.object(
            gcp_instance_manager, "_wait_for_operation", new=AsyncMock()
        ) as mock_wait:
            mock_wait.return_value = mock_result

            # Act
            instance_id, zone = await gcp_instance_manager.start_instance(
                instance_type=instance_type,
                boot_disk_size=20,
                startup_script=startup_script,
                job_id=job_id,
                use_spot=use_spot,
                image=custom_image,
                zone=gcp_instance_manager._zone,
            )

            # Assert
            assert instance_id.startswith(f"{gcp_instance_manager._JOB_ID_TAG_PREFIX}{job_id}-")
            assert zone == gcp_instance_manager._zone

            # Check that the image was set correctly in the instance configuration
            call_args = mock_compute_client.insert.call_args
            instance_config = call_args[1]["instance_resource"]

            assert instance_config.disks[0].initialize_params.source_image == custom_image
            # _get_image_from_family should not have been called since we provided a full image URI


@pytest.mark.asyncio
async def test_start_instance_with_random_zone(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting an instance with a wildcard zone that needs to be randomly selected."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_type = "n1-standard-2"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    image = "ubuntu-2404-lts"
    wildcard_zone = "us-central1-*"  # Wildcard zone

    # Mock _get_random_zone to return a specific zone
    with patch.object(
        gcp_instance_manager, "_get_random_zone", new=AsyncMock()
    ) as mock_get_random_zone:
        random_zone = "us-central1-c"
        mock_get_random_zone.return_value = random_zone

        # Mock the UUID generation to have a predictable instance ID
        mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
        with patch("uuid.uuid4", return_value=mock_uuid):
            # Mock _get_image_from_family to return a predictable image path
            with patch.object(
                gcp_instance_manager, "_get_image_from_family", new=AsyncMock()
            ) as mock_get_image:
                mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

                # Mock the insert operation and its result
                mock_operation = MagicMock()
                mock_operation.name = "mock-operation-name"
                mock_operation.error_code = None
                mock_operation.warnings = None
                mock_result = MagicMock()
                mock_operation.result.return_value = mock_result

                mock_compute_client = MagicMock()
                mock_compute_client.insert = MagicMock(return_value=mock_operation)
                gcp_instance_manager._get_compute_client = MagicMock(
                    return_value=mock_compute_client
                )

                # Mock _wait_for_operation to return successfully
                with patch.object(
                    gcp_instance_manager, "_wait_for_operation", new=AsyncMock()
                ) as mock_wait:
                    mock_wait.return_value = mock_result

                    # Act
                    instance_id, zone = await gcp_instance_manager.start_instance(
                        instance_type=instance_type,
                        boot_disk_size=20,
                        startup_script=startup_script,
                        job_id=job_id,
                        use_spot=use_spot,
                        image=image,
                        zone=wildcard_zone,
                    )

                    # Assert
                    assert instance_id.startswith(
                        f"{gcp_instance_manager._JOB_ID_TAG_PREFIX}{job_id}-"
                    )
                    assert zone == random_zone

                    # Verify that _get_random_zone was called to resolve the wildcard
                    mock_get_random_zone.assert_called_once()

                    # Check that the resolved random zone was used
                    call_args = mock_compute_client.insert.call_args
                    assert call_args[1]["zone"] == random_zone


@pytest.mark.asyncio
async def test_start_instance_error_handling(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when starting an instance fails."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_type = "n1-standard-2"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    image = "ubuntu-2404-lts"

    # Mock the UUID generation to have a predictable instance ID
    mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("uuid.uuid4", return_value=mock_uuid):
        # Mock _get_image_from_family to return a predictable image path
        with patch.object(
            gcp_instance_manager, "_get_image_from_family", new=AsyncMock()
        ) as mock_get_image:
            mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

            # Mock the insert operation to raise an exception
            error_message = "Failed to create instance"
            mock_compute_client = MagicMock()
            mock_compute_client.insert = MagicMock(side_effect=RuntimeError(error_message))
            gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

            # Act & Assert
            with pytest.raises(RuntimeError, match=error_message):
                await gcp_instance_manager.start_instance(
                    instance_type=instance_type,
                    boot_disk_size=20,
                    startup_script=startup_script,
                    job_id=job_id,
                    use_spot=use_spot,
                    image=image,
                    zone=gcp_instance_manager._zone,
                )


@pytest.mark.asyncio
async def test_terminate_instance_basic(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test terminating an instance with successful operation."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_id = "test-instance-123"

    # Mock the delete operation and its result
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.error_code = None
    mock_operation.warnings = None
    mock_result = MagicMock()
    mock_operation.result.return_value = mock_result

    mock_compute_client = MagicMock()
    mock_compute_client.delete = MagicMock(return_value=mock_operation)
    gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Mock _wait_for_operation to return successfully
    with patch.object(gcp_instance_manager, "_wait_for_operation", new=AsyncMock()) as mock_wait:
        mock_wait.return_value = mock_result

        # Act
        await gcp_instance_manager.terminate_instance(instance_id)

        # Assert
        # Check that the compute client was called with the correct parameters
        mock_compute_client.delete.assert_called_once_with(
            project=gcp_instance_manager._project_id,
            zone=gcp_instance_manager._zone,
            instance=instance_id,
        )

        # Verify _wait_for_operation was called with the correct arguments
        mock_wait.assert_called_once_with(
            mock_operation,  # Pass the operation object, not just its name
            gcp_instance_manager._zone,
            f"Termination of instance {instance_id}",
        )


@pytest.mark.asyncio
async def test_terminate_instance_not_found(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test terminating an instance that doesn't exist."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_id = "nonexistent-instance"

    # Mock the delete method to raise NotFound exception
    mock_compute_client = MagicMock()
    mock_compute_client.delete = MagicMock(side_effect=NotFound("Instance not found"))
    gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    try:
        await gcp_instance_manager.terminate_instance(instance_id)
    except NotFound:
        pass
    except Exception as e:
        raise e

    # Assert
    # Check that the compute client was called with the correct parameters
    mock_compute_client.delete.assert_called_once_with(
        project=gcp_instance_manager._project_id,
        zone=gcp_instance_manager._zone,
        instance=instance_id,
    )
    # Note: No exception should be raised as the method handles NotFound gracefully


@pytest.mark.asyncio
async def test_terminate_instance_error_handling(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when terminating an instance fails."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_id = "test-instance-123"

    # Mock the delete method to raise an exception
    error_message = "Failed to terminate instance"
    mock_compute_client = MagicMock()
    mock_compute_client.delete = MagicMock(side_effect=RuntimeError(error_message))
    gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act & Assert
    with pytest.raises(RuntimeError, match=error_message):
        await gcp_instance_manager.terminate_instance(instance_id)

    # Verify the method was called with correct parameters
    mock_compute_client.delete.assert_called_once_with(
        project=gcp_instance_manager._project_id,
        zone=gcp_instance_manager._zone,
        instance=instance_id,
    )


@pytest.mark.asyncio
async def test_list_running_instances_basic(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test listing running instances with no filters."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_instance1 = MagicMock()
    mock_instance1.name = "instance-1"
    mock_instance1.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance1.status = "RUNNING"
    mock_instance1.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance1.zone = "zones/us-central1-a"
    mock_instance1.tags = MagicMock()
    mock_instance1.tags.items = ["rmscr-job-123"]
    mock_instance1.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    mock_instance2 = MagicMock()
    mock_instance2.name = "instance-2"
    mock_instance2.machine_type = "zones/us-central1-a/machineTypes/n2-standard-4"
    mock_instance2.status = "RUNNING"
    mock_instance2.creation_timestamp = "2024-03-20T11:00:00.000-07:00"
    mock_instance2.zone = "zones/us-central1-a"
    mock_instance2.tags = MagicMock()
    mock_instance2.tags.items = ["rmscr-job-456"]
    mock_instance2.network_interfaces = [
        MagicMock(network_i_p="10.0.0.3", access_configs=[MagicMock(nat_i_p="34.123.123.124")])
    ]

    # Mock the compute client's list method
    mock_compute_client = MagicMock()
    mock_compute_client.list.return_value = [mock_instance1, mock_instance2]
    gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager.list_running_instances()

    # Assert
    assert len(instances) == 2

    # Check first instance
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["type"] == "n1-standard-2"
    assert instances[0]["state"] == "running"
    assert instances[0]["zone"] == "us-central1-a"
    assert instances[0]["job_id"] == "job-123"
    assert instances[0]["private_ip"] == "10.0.0.2"
    assert instances[0]["public_ip"] == "34.123.123.123"

    # Check second instance
    assert instances[1]["id"] == "instance-2"
    assert instances[1]["type"] == "n2-standard-4"
    assert instances[1]["state"] == "running"
    assert instances[1]["zone"] == "us-central1-a"
    assert instances[1]["job_id"] == "job-456"
    assert instances[1]["private_ip"] == "10.0.0.3"
    assert instances[1]["public_ip"] == "34.123.123.124"


@pytest.mark.asyncio
async def test_list_running_instances_with_job_id(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test listing instances filtered by job ID."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_instance1 = MagicMock()
    mock_instance1.name = "instance-1"
    mock_instance1.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance1.status = "RUNNING"
    mock_instance1.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance1.zone = "zones/us-central1-a"
    mock_instance1.tags = MagicMock()
    mock_instance1.tags.items = ["rmscr-job-123"]
    mock_instance1.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    mock_instance2 = MagicMock()
    mock_instance2.name = "instance-2"
    mock_instance2.machine_type = "zones/us-central1-a/machineTypes/n2-standard-4"
    mock_instance2.status = "RUNNING"
    mock_instance2.creation_timestamp = "2024-03-20T11:00:00.000-07:00"
    mock_instance2.zone = "zones/us-central1-a"
    mock_instance2.tags = MagicMock()
    mock_instance2.tags.items = ["rmscr-job-456"]
    mock_instance2.network_interfaces = [
        MagicMock(network_i_p="10.0.0.3", access_configs=[MagicMock(nat_i_p="34.123.123.124")])
    ]

    # Mock the compute client's list method
    mock_compute_client = MagicMock()
    mock_compute_client.list.return_value = [mock_instance1, mock_instance2]
    gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager.list_running_instances(job_id="job-123")

    # Assert
    assert len(instances) == 1
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["job_id"] == "job-123"


@pytest.mark.asyncio
async def test_list_running_instances_include_non_job(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test listing instances including non-job instances."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_instance1 = MagicMock()
    mock_instance1.name = "instance-1"
    mock_instance1.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance1.status = "RUNNING"
    mock_instance1.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance1.zone = "zones/us-central1-a"
    mock_instance1.tags = MagicMock()
    mock_instance1.tags.items = ["rmscr-job-123"]
    mock_instance1.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    mock_instance2 = MagicMock()
    mock_instance2.name = "instance-2"
    mock_instance2.machine_type = "zones/us-central1-a/machineTypes/n2-standard-4"
    mock_instance2.status = "RUNNING"
    mock_instance2.creation_timestamp = "2024-03-20T11:00:00.000-07:00"
    mock_instance2.zone = "zones/us-central1-a"
    mock_instance2.tags = MagicMock()
    mock_instance2.tags.items = []  # No job tag
    mock_instance2.network_interfaces = [
        MagicMock(network_i_p="10.0.0.3", access_configs=[MagicMock(nat_i_p="34.123.123.124")])
    ]

    # Mock the compute client's list method
    mock_compute_client = MagicMock()
    mock_compute_client.list.return_value = [mock_instance1, mock_instance2]
    gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager.list_running_instances(include_non_job=True)

    # Assert
    assert len(instances) == 2
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["job_id"] == "job-123"
    assert "job_id" not in instances[1]
    assert instances[1]["id"] == "instance-2"


@pytest.mark.asyncio
async def test_list_running_instances_region_based(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test listing instances across all zones in a region when no specific zone is set."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Clear the zone to force region-based listing
    gcp_instance_manager._zone = None

    # Mock zones in the region
    mock_zone1 = MagicMock()
    mock_zone1.name = "us-central1-a"
    mock_zone2 = MagicMock()
    mock_zone2.name = "us-central1-b"

    # Mock the zones client to return our mock zones
    gcp_instance_manager._zones_client.list.return_value = [mock_zone1, mock_zone2]

    # Create mock instances for each zone
    mock_instance1 = MagicMock()
    mock_instance1.name = "instance-1"
    mock_instance1.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance1.status = "RUNNING"
    mock_instance1.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance1.zone = "zones/us-central1-a"
    mock_instance1.tags = MagicMock()
    mock_instance1.tags.items = ["rmscr-job-123"]
    mock_instance1.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    mock_instance2 = MagicMock()
    mock_instance2.name = "instance-2"
    mock_instance2.machine_type = "zones/us-central1-b/machineTypes/n2-standard-4"
    mock_instance2.status = "RUNNING"
    mock_instance2.creation_timestamp = "2024-03-20T11:00:00.000-07:00"
    mock_instance2.zone = "zones/us-central1-b"
    mock_instance2.tags = MagicMock()
    mock_instance2.tags.items = ["rmscr-job-456"]
    mock_instance2.network_interfaces = [
        MagicMock(network_i_p="10.0.0.3", access_configs=[MagicMock(nat_i_p="34.123.123.124")])
    ]

    # Mock the compute client to return different instances for each zone
    mock_compute_client = MagicMock()
    mock_compute_client.list = MagicMock(side_effect=[[mock_instance1], [mock_instance2]])
    gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager.list_running_instances()

    # Assert
    assert len(instances) == 2
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["zone"] == "us-central1-a"
    assert instances[1]["id"] == "instance-2"
    assert instances[1]["zone"] == "us-central1-b"

    # Verify that zones were listed
    gcp_instance_manager._zones_client.list.assert_called_once()
    # Verify that instances were listed in each zone
    assert mock_compute_client.list.call_count == 2


@pytest.mark.asyncio
async def test_list_running_instances_zone_listing_error(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when listing zones fails."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    gcp_instance_manager._zone = None  # Force region-based listing
    error_msg = "Permission denied"
    gcp_instance_manager._zones_client.list = MagicMock(side_effect=RuntimeError(error_msg))

    # Act & Assert
    with pytest.raises(ValueError, match=f"Error listing zones.*{error_msg}"):
        await gcp_instance_manager.list_running_instances()


@pytest.mark.asyncio
async def test_list_running_instances_instance_listing_error(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test handling when listing instances in a zone fails."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    gcp_instance_manager._zone = None  # Force region-based listing

    # Mock zones in the region
    mock_zone1 = MagicMock()
    mock_zone1.name = "us-central1-a"
    mock_zone2 = MagicMock()
    mock_zone2.name = "us-central1-b"
    gcp_instance_manager._zones_client.list.return_value = [mock_zone1, mock_zone2]

    # Mock instance in first zone
    mock_instance1 = MagicMock()
    mock_instance1.name = "instance-1"
    mock_instance1.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance1.status = "RUNNING"
    mock_instance1.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance1.zone = "zones/us-central1-a"
    mock_instance1.tags = MagicMock()
    mock_instance1.tags.items = ["rmscr-job-123"]
    mock_instance1.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    # Mock the compute client to succeed for first zone but fail for second
    mock_compute_client = MagicMock()
    mock_compute_client.list = MagicMock(
        side_effect=[[mock_instance1], RuntimeError("Failed to list instances")]
    )
    gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager.list_running_instances()

    # Assert
    # Should still get instances from the successful zone
    assert len(instances) == 1
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["zone"] == "us-central1-a"


@pytest.mark.asyncio
async def test_list_running_instances_unknown_status(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test handling instances with unknown status."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_instance = MagicMock()
    mock_instance.name = "instance-1"
    mock_instance.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance.status = "UNKNOWN_STATUS"  # Status not in _STATUS_MAP
    mock_instance.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance.zone = "zones/us-central1-a"
    mock_instance.tags = MagicMock()
    mock_instance.tags.items = ["rmscr-job-123"]
    mock_instance.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    # Mock the compute client
    mock_compute_client = MagicMock()
    mock_compute_client.list.return_value = [mock_instance]
    gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager.list_running_instances()

    # Assert
    assert len(instances) == 1
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["state"] == "unknown"


@pytest.mark.asyncio
async def test_list_running_instances_no_network_interfaces(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test handling instances with no network interfaces."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_instance = MagicMock()
    mock_instance.name = "instance-1"
    mock_instance.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance.status = "RUNNING"
    mock_instance.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance.zone = "zones/us-central1-a"
    mock_instance.tags = MagicMock()
    mock_instance.tags.items = ["rmscr-job-123"]
    mock_instance.network_interfaces = []  # No network interfaces

    # Mock the compute client
    mock_compute_client = MagicMock()
    mock_compute_client.list.return_value = [mock_instance]
    gcp_instance_manager._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager.list_running_instances()

    # Assert
    assert len(instances) == 1
    assert instances[0]["id"] == "instance-1"
    assert "private_ip" not in instances[0]
    assert "public_ip" not in instances[0]


@pytest.mark.asyncio
async def test_get_image_from_family(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test getting image from a family."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    family_name = "ubuntu-2404-lts"
    project = "ubuntu-os-cloud"

    # Create mock image
    mock_image = MagicMock()
    mock_image.name = "ubuntu-2404-20240401"
    mock_image.creation_timestamp = "2024-04-01T12:00:00.000-07:00"
    mock_image.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240401"

    # Mock the images client - this needs to be a non-async method that returns the mock image directly
    # because the implementation doesn't await the get_from_family call
    gcp_instance_manager._images_client.get_from_family = MagicMock(return_value=mock_image)

    # Act
    image_uri = await gcp_instance_manager._get_image_from_family(family_name, project)

    # Assert
    assert image_uri == mock_image.self_link
    gcp_instance_manager._images_client.get_from_family.assert_called_once()
    request = gcp_instance_manager._images_client.get_from_family.call_args[1]["request"]
    assert request.family == family_name
    assert request.project == project


@pytest.mark.asyncio
async def test_get_image_from_family_error(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when getting image from a family fails."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    family_name = "nonexistent-family"
    project = "ubuntu-os-cloud"
    error_message = "Family not found"

    # Mock the images client to raise an exception - use MagicMock instead of AsyncMock
    gcp_instance_manager._images_client.get_from_family = MagicMock(
        side_effect=ValueError(error_message)
    )

    # Act & Assert
    with pytest.raises(ValueError, match=f"Could not find image in family {family_name}"):
        await gcp_instance_manager._get_image_from_family(family_name, project)


@pytest.mark.asyncio
async def test_get_default_image(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test getting the default Ubuntu 24.04 LTS image."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Create mock images
    mock_image1 = MagicMock()
    mock_image1.name = "ubuntu-2404-20240401"
    mock_image1.creation_timestamp = "2024-04-01T12:00:00.000-07:00"
    mock_image1.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240401"

    mock_image2 = MagicMock()
    mock_image2.name = "ubuntu-2404-20240501"
    mock_image2.creation_timestamp = "2024-05-01T12:00:00.000-07:00"  # Newer image
    mock_image2.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240501"

    # Mock the images client to return both images
    gcp_instance_manager._images_client.list = MagicMock(return_value=[mock_image1, mock_image2])

    # Act
    image_uri = await gcp_instance_manager._get_default_image()

    # Assert
    assert image_uri == mock_image2.self_link  # Should return the newer image
    gcp_instance_manager._images_client.list.assert_called_once()
    request = gcp_instance_manager._images_client.list.call_args[1]["request"]
    assert request.project == "ubuntu-os-cloud"
    assert request.filter == "family = 'ubuntu-2404-lts'"


@pytest.mark.asyncio
async def test_get_default_image_no_images(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when no default images are found."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Mock the images client to return an empty list
    gcp_instance_manager._images_client.list = MagicMock(return_value=[])

    # Act & Assert
    with pytest.raises(ValueError, match="No Ubuntu 24.04 LTS image found"):
        await gcp_instance_manager._get_default_image()


@pytest.mark.asyncio
async def test_list_available_images(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test listing available images."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Create mock images for public projects
    mock_ubuntu_image = MagicMock()
    mock_ubuntu_image.id = "12345"
    mock_ubuntu_image.name = "ubuntu-2404-20240501"
    mock_ubuntu_image.description = "Ubuntu 24.04 LTS"
    mock_ubuntu_image.family = "ubuntu-2404-lts"
    mock_ubuntu_image.creation_timestamp = "2024-05-01T12:00:00.000-07:00"
    mock_ubuntu_image.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240501"
    mock_ubuntu_image.status = "READY"
    mock_ubuntu_image.deprecated = None

    # Create a mock deprecated image
    mock_deprecated_image = MagicMock()
    mock_deprecated_image.id = "23456"
    mock_deprecated_image.name = "ubuntu-2204-deprecated"
    mock_deprecated_image.family = "ubuntu-2204-lts"
    mock_deprecated_image.creation_timestamp = "2022-04-01T12:00:00.000-07:00"
    mock_deprecated_image.deprecated = MagicMock()
    mock_deprecated_image.deprecated.state = "DEPRECATED"

    # Create mock images for user project
    mock_custom_image = MagicMock()
    mock_custom_image.id = "34567"
    mock_custom_image.name = "custom-image"
    mock_custom_image.description = "Custom user image"
    mock_custom_image.family = "custom-family"
    mock_custom_image.creation_timestamp = "2024-05-15T12:00:00.000-07:00"
    mock_custom_image.self_link = (
        "https://compute.googleapis.com/compute/v1/projects/test-project/global/images/custom-image"
    )
    mock_custom_image.status = "READY"

    # Mock the images client to return different responses for different projects
    def mock_list_images(**kwargs):
        request = kwargs.get("request")
        if request.project == "ubuntu-os-cloud":
            return [mock_ubuntu_image, mock_deprecated_image]
        elif request.project == "test-project":
            return [mock_custom_image]
        else:
            return []

    gcp_instance_manager._images_client.list = MagicMock(side_effect=mock_list_images)

    # Act
    images = await gcp_instance_manager.list_available_images()

    # Assert
    # We should get at least the Ubuntu image and the custom image
    # The deprecated image should be filtered out
    assert len(images) >= 2

    # Find the Ubuntu image in the results
    ubuntu_result = next((img for img in images if img["name"] == "ubuntu-2404-20240501"), None)
    assert ubuntu_result is not None
    assert ubuntu_result["id"] == "12345"
    assert ubuntu_result["description"] == "Ubuntu 24.04 LTS"
    assert ubuntu_result["family"] == "ubuntu-2404-lts"
    assert ubuntu_result["source"] == "GCP"
    assert ubuntu_result["project"] == "ubuntu-os-cloud"
    assert ubuntu_result["status"] == "READY"

    # Find the custom image in the results
    custom_result = next((img for img in images if img["name"] == "custom-image"), None)
    assert custom_result is not None
    assert custom_result["id"] == "34567"
    assert custom_result["description"] == "Custom user image"
    assert custom_result["family"] == "custom-family"
    assert custom_result["source"] == "User"
    assert custom_result["project"] == "test-project"
    assert custom_result["status"] == "READY"


@pytest.mark.asyncio
async def test_list_available_images_error_handling(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when listing images."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)

    # Mock the images client to raise an exception for one project but succeed for another
    def mock_list_images(**kwargs):
        request = kwargs.get("request")
        if request.project == "ubuntu-os-cloud":
            # Create a mock image for ubuntu-os-cloud
            mock_image = MagicMock()
            mock_image.id = "12345"
            mock_image.name = "ubuntu-2404-20240501"
            mock_image.description = "Ubuntu 24.04 LTS"
            mock_image.family = "ubuntu-2404-lts"
            mock_image.creation_timestamp = "2024-05-01T12:00:00.000-07:00"
            mock_image.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240501"
            mock_image.status = "READY"
            mock_image.deprecated = None
            return [mock_image]
        else:
            # Raise an exception for all other projects
            raise RuntimeError(f"Error accessing project {request.project}")

    gcp_instance_manager._images_client.list = MagicMock(side_effect=mock_list_images)

    # Act
    images = await gcp_instance_manager.list_available_images()

    # Assert
    # We should still get the Ubuntu image, even though other projects failed
    assert len(images) == 1
    assert images[0]["name"] == "ubuntu-2404-20240501"
    assert images[0]["project"] == "ubuntu-os-cloud"


@pytest.mark.asyncio
async def test_wait_for_operation_success(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test successful operation completion."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.error_code = None
    mock_operation.warnings = None
    mock_result = MagicMock()
    mock_operation.result.return_value = mock_result

    # Act
    result = await gcp_instance_manager._wait_for_operation(
        mock_operation, "us-central1-a", "Test operation"
    )

    # Assert
    assert result == mock_result
    mock_operation.result.assert_called_once_with(timeout=120)


@pytest.mark.asyncio
async def test_wait_for_operation_with_error(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test operation that fails with an error."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.error_code = "RESOURCE_NOT_FOUND"
    mock_operation.error_message = "The resource was not found"
    mock_operation.exception = MagicMock(return_value=RuntimeError("Resource not found"))

    # Act & Assert
    with pytest.raises(RuntimeError, match="Resource not found"):
        await gcp_instance_manager._wait_for_operation(
            mock_operation, "us-central1-a", "Test operation"
        )

    mock_operation.result.assert_called_once_with(timeout=120)


@pytest.mark.asyncio
async def test_wait_for_operation_with_warnings(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test operation that completes with warnings."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.error_code = None

    # Create mock warnings
    mock_warning1 = MagicMock()
    mock_warning1.code = "QUOTA_WARNING"
    mock_warning1.message = "Approaching quota limit"

    mock_warning2 = MagicMock()
    mock_warning2.code = "PERFORMANCE_WARNING"
    mock_warning2.message = "Instance may experience degraded performance"

    mock_operation.warnings = [mock_warning1, mock_warning2]

    mock_result = MagicMock()
    mock_operation.result.return_value = mock_result

    # Act
    result = await gcp_instance_manager._wait_for_operation(
        mock_operation, "us-central1-a", "Test operation"
    )

    # Assert
    assert result == mock_result
    mock_operation.result.assert_called_once_with(timeout=120)


@pytest.mark.asyncio
async def test_wait_for_operation_timeout(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test operation that times out."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.result = MagicMock(side_effect=TimeoutError("Operation timed out"))

    # Act & Assert
    with pytest.raises(TimeoutError, match="Operation timed out"):
        await gcp_instance_manager._wait_for_operation(
            mock_operation, "us-central1-a", "Test operation"
        )

    mock_operation.result.assert_called_once_with(timeout=120)


@pytest.mark.asyncio
async def test_wait_for_operation_cancellation(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test operation that gets cancelled."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Arrange
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.result = MagicMock(side_effect=asyncio.CancelledError())

    # Act & Assert
    with pytest.raises(asyncio.CancelledError):
        await gcp_instance_manager._wait_for_operation(
            mock_operation, "us-central1-a", "Test operation"
        )

    mock_operation.result.assert_called_once_with(timeout=120)


@pytest.mark.asyncio
async def test_get_available_regions_basic(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test basic functionality of get_available_regions."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
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

    gcp_instance_manager._regions_client.list.return_value = [mock_region1, mock_region2]

    regions = await gcp_instance_manager.get_available_regions()

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
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test get_available_regions with prefix filtering."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
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

    gcp_instance_manager._regions_client.list.return_value = [mock_region1, mock_region2]

    regions = await gcp_instance_manager.get_available_regions(prefix="us-")

    assert len(regions) == 1
    assert "us-central1" in regions
    assert "europe-west1" not in regions


@pytest.mark.asyncio
async def test_get_available_regions_empty(gcp_instance_manager: GCPComputeInstanceManager) -> None:
    """Test get_available_regions when no regions are available."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    gcp_instance_manager._regions_client.list.return_value = []

    regions = await gcp_instance_manager.get_available_regions()

    assert len(regions) == 0


@pytest.mark.asyncio
async def test_get_default_zone_specified(gcp_instance_manager: GCPComputeInstanceManager) -> None:
    """Test _get_default_zone when zone is already specified."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    gcp_instance_manager._zone = "us-central1-a"

    zone = await gcp_instance_manager._get_default_zone()

    assert zone == "us-central1-a"
    # Verify no API calls were made
    assert not gcp_instance_manager._zones_client.list.called


@pytest.mark.asyncio
async def test_get_default_zone_from_region(
    gcp_instance_manager: GCPComputeInstanceManager,
) -> None:
    """Test _get_default_zone when getting first zone in region."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    gcp_instance_manager._zone = None
    gcp_instance_manager._region = "us-central1"

    # Mock the zones response
    mock_zone1 = MagicMock()
    mock_zone1.name = "us-central1-a"
    mock_zone2 = MagicMock()
    mock_zone2.name = "us-central1-b"

    gcp_instance_manager._zones_client.list.return_value = [mock_zone1, mock_zone2]

    zone = await gcp_instance_manager._get_default_zone()

    assert zone == "us-central1-a"
    # Verify correct filter was used
    gcp_instance_manager._zones_client.list.assert_called_once()
    request = gcp_instance_manager._zones_client.list.call_args[1]["request"]
    assert request.filter == "name eq us-central1-.*"


@pytest.mark.asyncio
async def test_get_default_zone_no_zones(gcp_instance_manager: GCPComputeInstanceManager) -> None:
    """Test _get_default_zone when no zones are found in region."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    gcp_instance_manager._zone = None
    gcp_instance_manager._region = "us-central1"

    gcp_instance_manager._zones_client.list.return_value = []

    with pytest.raises(ValueError, match="No zones found for region us-central1"):
        await gcp_instance_manager._get_default_zone()


@pytest.mark.asyncio
async def test_get_default_zone_no_region(gcp_instance_manager: GCPComputeInstanceManager) -> None:
    """Test _get_default_zone when neither zone nor region is specified."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    gcp_instance_manager._zone = None
    gcp_instance_manager._region = None

    with pytest.raises(RuntimeError, match="Region or zone must be specified"):
        await gcp_instance_manager._get_default_zone()


@pytest.mark.asyncio
async def test_get_random_zone_specified(gcp_instance_manager: GCPComputeInstanceManager) -> None:
    """Test _get_random_zone when zone is already specified."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    gcp_instance_manager._zone = "us-central1-a"

    zone = await gcp_instance_manager._get_random_zone()

    assert zone == "us-central1-a"
    # Verify no API calls were made
    assert not gcp_instance_manager._zones_client.list.called


@pytest.mark.asyncio
async def test_get_random_zone_from_region(
    gcp_instance_manager: GCPComputeInstanceManager, monkeypatch
) -> None:
    """Test _get_random_zone when selecting random zone in region."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    gcp_instance_manager._zone = None
    gcp_instance_manager._region = "us-central1"

    # Mock the zones response
    mock_zone1 = MagicMock()
    mock_zone1.name = "us-central1-a"
    mock_zone2 = MagicMock()
    mock_zone2.name = "us-central1-b"
    mock_zone3 = MagicMock()
    mock_zone3.name = "us-central1-c"

    gcp_instance_manager._zones_client.list.return_value = [mock_zone1, mock_zone2, mock_zone3]

    # Mock random.randint to return predictable values
    monkeypatch.setattr(random, "randint", lambda x, y: 1)  # Always return index 1

    zone = await gcp_instance_manager._get_random_zone()

    assert zone == "us-central1-b"  # Should get the second zone due to mocked random
    # Verify correct filter was used
    gcp_instance_manager._zones_client.list.assert_called_once()
    request = gcp_instance_manager._zones_client.list.call_args[1]["request"]
    assert request.filter == "name eq us-central1-.*"


@pytest.mark.asyncio
async def test_get_random_zone_no_zones(gcp_instance_manager: GCPComputeInstanceManager) -> None:
    """Test _get_random_zone when no zones are found in region."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    gcp_instance_manager._zone = None
    gcp_instance_manager._region = "us-central1"

    gcp_instance_manager._zones_client.list.return_value = []

    with pytest.raises(ValueError, match="No zones found for region us-central1"):
        await gcp_instance_manager._get_random_zone()


@pytest.mark.asyncio
async def test_get_random_zone_different_region(
    gcp_instance_manager: GCPComputeInstanceManager,
) -> None:
    """Test _get_random_zone when specifying a different region."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    gcp_instance_manager._zone = None
    gcp_instance_manager._region = "us-central1"

    # Mock the zones response
    mock_zone1 = MagicMock()
    mock_zone1.name = "europe-west1-a"
    mock_zone2 = MagicMock()
    mock_zone2.name = "europe-west1-b"

    gcp_instance_manager._zones_client.list.return_value = [mock_zone1, mock_zone2]

    zone = await gcp_instance_manager._get_random_zone(region="europe-west1")

    # Verify correct filter was used for the specified region
    gcp_instance_manager._zones_client.list.assert_called_once()
    request = gcp_instance_manager._zones_client.list.call_args[1]["request"]
    assert request.filter == "name eq europe-west1-.*"


@pytest.mark.asyncio
async def test_extract_pricing_info_no_pricing_info(
    gcp_instance_manager: GCPComputeInstanceManager,
    caplog,
) -> None:
    """Test _extract_pricing_info when SKU has no pricing info."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_sku = MagicMock()
    mock_sku.pricing_info = []
    mock_sku.description = "Test SKU"
    with caplog.at_level("WARNING"):
        result = gcp_instance_manager._extract_pricing_info("n1-standard", mock_sku, "h", "core")
    assert result is None
    assert any("No pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_extract_pricing_info_multiple_pricing_info(
    gcp_instance_manager: GCPComputeInstanceManager,
    caplog,
) -> None:
    """Test _extract_pricing_info when SKU has multiple pricing info entries."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_sku = MagicMock()
    mock_pricing_info1 = MagicMock()
    mock_pricing_info2 = MagicMock()
    mock_sku.pricing_info = [mock_pricing_info1, mock_pricing_info2]
    mock_sku.description = "Test SKU"
    with caplog.at_level("WARNING"):
        result = gcp_instance_manager._extract_pricing_info("n1-standard", mock_sku, "h", "core")
    assert result is None
    assert any("Multiple pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_extract_pricing_info_unknown_unit(
    gcp_instance_manager: GCPComputeInstanceManager,
    caplog,
) -> None:
    """Test _extract_pricing_info when SKU has unknown pricing unit."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_sku = MagicMock()
    mock_pricing_info = MagicMock()
    mock_pricing_info.pricing_expression.usage_unit = "unknown_unit"
    mock_sku.pricing_info = [mock_pricing_info]
    mock_sku.description = "Test SKU"
    with caplog.at_level("WARNING"):
        result = gcp_instance_manager._extract_pricing_info("n1-standard", mock_sku, "h", "core")
    assert result is None
    assert any("has unknown pricing unit" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_extract_pricing_info_no_tiered_rates(
    gcp_instance_manager: GCPComputeInstanceManager,
    caplog,
) -> None:
    """Test _extract_pricing_info when SKU has no tiered rates."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_sku = MagicMock()
    mock_pricing_info = MagicMock()
    mock_pricing_info.pricing_expression.usage_unit = "h"
    mock_pricing_info.pricing_expression.tiered_rates = []
    mock_sku.pricing_info = [mock_pricing_info]
    mock_sku.description = "Test SKU"
    with caplog.at_level("WARNING"):
        result = gcp_instance_manager._extract_pricing_info("n1-standard", mock_sku, "h", "core")
    assert result is None
    assert any("No tiered rates found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_extract_pricing_info_multiple_tiered_rates(
    gcp_instance_manager: GCPComputeInstanceManager,
    caplog,
) -> None:
    """Test _extract_pricing_info when SKU has multiple tiered rates."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_sku = MagicMock()
    mock_pricing_info = MagicMock()
    mock_pricing_info.pricing_expression.usage_unit = "h"
    mock_tier1 = MagicMock()
    mock_tier2 = MagicMock()
    mock_pricing_info.pricing_expression.tiered_rates = [mock_tier1, mock_tier2]
    mock_sku.pricing_info = [mock_pricing_info]
    mock_sku.description = "Test SKU"
    with caplog.at_level("WARNING"):
        result = gcp_instance_manager._extract_pricing_info("n1-standard", mock_sku, "h", "core")
    assert result is None
    assert any("Multiple tiered rates found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_extract_pricing_info_success(
    gcp_instance_manager: GCPComputeInstanceManager,
) -> None:
    """Test _extract_pricing_info successful case."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
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
    result = gcp_instance_manager._extract_pricing_info("n1-standard", mock_sku, "h", "core")

    assert result is not None
    assert result.unit_price.nanos == 1000000000


@pytest.mark.asyncio
async def test_get_instance_pricing_no_family_skus(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test get_instance_pricing when no SKUs are found for a machine family."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    # Mock the billing SKUs response with SKUs that don't match our machine family
    mock_sku = MagicMock()
    mock_sku.description = "E2 Instance Core running in Americas"  # Different family
    mock_sku.service_regions = ["us-central1"]
    mock_pricing_info = MagicMock()
    mock_pricing_info.pricing_expression.usage_unit = "h"
    mock_tier_rate = MagicMock()
    mock_tier_rate.unit_price.nanos = 1000000000  # $1.00
    mock_pricing_info.pricing_expression.tiered_rates = [mock_tier_rate]
    mock_sku.pricing_info = [mock_pricing_info]

    gcp_instance_manager._billing_compute_skus = [mock_sku]

    # Act
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    # Instance type should have an empty pricing dictionary since no matching SKUs were found
    assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_component_skus(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test get_instance_pricing when SKUs for some components are missing."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    # Only provide CPU SKU, missing RAM and local SSD SKUs
    core_sku = mock_pricing_sku
    core_sku.description = "N2 Instance Core running in Americas"
    core_sku.service_regions = ["us-central1"]
    core_pricing_info = MagicMock()
    core_pricing_info.pricing_expression.usage_unit = "h"
    core_tier_rate = MagicMock()
    core_tier_rate.unit_price.nanos = 1000000000  # $1.00
    core_pricing_info.pricing_expression.tiered_rates = [core_tier_rate]
    core_sku.pricing_info = [core_pricing_info]

    gcp_instance_manager._billing_compute_skus = [core_sku]

    # Act
    result = await gcp_instance_manager.get_instance_pricing(
        {"n2-standard-4-lssd": mock_instance_types["n2-standard-4-lssd"]}, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n2-standard-4-lssd" in result
    # Instance type should have an empty pricing dictionary since RAM SKU is missing
    assert result["n2-standard-4-lssd"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_region_mismatch(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test get_instance_pricing when SKUs don't match the expected region."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    core_sku = mock_pricing_sku
    core_sku.description = "N1 Instance Core running in Europe"
    core_sku.service_regions = ["europe-west1"]  # Different region
    core_pricing_info = MagicMock()
    core_pricing_info.pricing_expression.usage_unit = "h"
    core_tier_rate = MagicMock()
    core_tier_rate.unit_price.nanos = 1000000000  # $1.00
    core_pricing_info.pricing_expression.tiered_rates = [core_tier_rate]
    core_sku.pricing_info = [core_pricing_info]

    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Europe"
    ram_sku.service_regions = ["europe-west1"]  # Different region
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 500000000  # $0.50
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]

    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku]

    # Act
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
    )

    # Assert
    assert result is not None
    assert "n1-standard-2" in result
    # Instance type should have an empty pricing dictionary since no SKUs match the region
    assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_empty_instance_types(
    gcp_instance_manager: GCPComputeInstanceManager,
) -> None:
    """Test get_instance_pricing when an empty instance types dict is provided."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    # Act
    result = await gcp_instance_manager.get_instance_pricing({}, use_spot=False)
    # Assert
    assert result == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_no_billing_skus(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
) -> None:
    """Test get_instance_pricing when no billing SKUs are available."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    gcp_instance_manager._billing_compute_skus = []
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_multiple_skus_for_component(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
) -> None:
    """Test get_instance_pricing when multiple SKUs exist for a component (ambiguous)."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    core_sku1 = MagicMock()
    core_sku1.description = "N1 Instance Core running in Americas"
    core_sku1.service_regions = ["us-central1"]
    core_pricing_info1 = MagicMock()
    core_pricing_info1.pricing_expression.usage_unit = "h"
    core_tier_rate1 = MagicMock()
    core_tier_rate1.unit_price.nanos = 1000000000
    core_pricing_info1.pricing_expression.tiered_rates = [core_tier_rate1]
    core_sku1.pricing_info = [core_pricing_info1]

    core_sku2 = MagicMock()
    core_sku2.description = "N1 Instance Core running in Americas"
    core_sku2.service_regions = ["us-central1"]
    core_pricing_info2 = MagicMock()
    core_pricing_info2.pricing_expression.usage_unit = "h"
    core_tier_rate2 = MagicMock()
    core_tier_rate2.unit_price.nanos = 2000000000
    core_pricing_info2.pricing_expression.tiered_rates = [core_tier_rate2]
    core_sku2.pricing_info = [core_pricing_info2]

    gcp_instance_manager._billing_compute_skus = [core_sku1, core_sku2]
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_pricing_info_none(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test get_instance_pricing when pricing info extraction returns None for a component."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    # Patch _extract_pricing_info to return None for RAM
    core_sku = mock_pricing_sku
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_sku.pricing_info = [MagicMock()]
    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku]
    orig_extract = gcp_instance_manager._extract_pricing_info

    def fake_extract(family, sku, unit, component):
        if component == "ram":
            return None
        return orig_extract(family, sku, unit, component)

    gcp_instance_manager._extract_pricing_info = fake_extract
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert result["n1-standard-2"] == {}
    gcp_instance_manager._extract_pricing_info = orig_extract


@pytest.mark.asyncio
async def test_get_instance_pricing_spot_not_available(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test get_instance_pricing when spot pricing is requested but not available."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    # Only regular SKUs, no preemptible/spot SKUs
    core_sku = mock_pricing_sku
    core_sku.description = "N1 Instance Core running in Americas"
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 500000000
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]
    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku]
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=True
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_ignores_sole_tenancy_custom_commitment(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
) -> None:
    """Test get_instance_pricing ignores sole tenancy, custom, and commitment SKUs (continue)."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
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
        gcp_instance_manager._billing_compute_skus = [sku]
        result = await gcp_instance_manager.get_instance_pricing(
            {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
        )
        assert result is not None
        assert "n1-standard-2" in result
        assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_local_ssd_sku_but_no_ssd(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
) -> None:
    """Test get_instance_pricing ignores local SSD SKUs if instance type has no SSD (continue)."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    ssd_sku = MagicMock()
    ssd_sku.description = "N1 Local SSD provisioned space running in Americas"
    ssd_sku.service_regions = ["us-central1"]
    pricing_info = MagicMock()
    pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    tier_rate = MagicMock()
    tier_rate.unit_price.nanos = 1000000000
    pricing_info.pricing_expression.tiered_rates = [tier_rate]
    ssd_sku.pricing_info = [pricing_info]
    gcp_instance_manager._billing_compute_skus = [ssd_sku]
    # n1-standard-2 has no local SSD
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_preemptible_mismatch(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
) -> None:
    """Test get_instance_pricing ignores preemptible SKUs if use_spot is False, and vice versa (continue)."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    # Preemptible SKU, use_spot=False
    preemptible_sku = MagicMock()
    preemptible_sku.description = "N1 Preemptible Instance Core running in Americas"
    preemptible_sku.service_regions = ["us-central1"]
    pricing_info = MagicMock()
    pricing_info.pricing_expression.usage_unit = "h"
    tier_rate = MagicMock()
    tier_rate.unit_price.nanos = 1000000000
    pricing_info.pricing_expression.tiered_rates = [tier_rate]
    preemptible_sku.pricing_info = [pricing_info]
    gcp_instance_manager._billing_compute_skus = [preemptible_sku]
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert result["n1-standard-2"] == {}
    # Non-preemptible SKU, use_spot=True
    non_preemptible_sku = MagicMock()
    non_preemptible_sku.description = "N1 Instance Core running in Americas"
    non_preemptible_sku.service_regions = ["us-central1"]
    non_preemptible_sku.pricing_info = [pricing_info]
    gcp_instance_manager._billing_compute_skus = [non_preemptible_sku]
    result = await gcp_instance_manager.get_instance_pricing(
        {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=True
    )
    assert result is not None
    assert "n1-standard-2" in result
    assert result["n1-standard-2"] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_z3_storage_optimized_warning(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_pricing_sku: MagicMock,
    caplog,
) -> None:
    """Test get_instance_pricing for z3 storage-optimized instance triggers warning."""
    # Arrange: create a mock z3 instance type
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    z3_instance_type = "z3-highmem-8-lssd"
    mock_instance_types = {
        z3_instance_type: {
            "name": z3_instance_type,
            "vcpu": 8,
            "mem_gb": 64,
            "local_ssd_gb": 3000,  # Example value
            "boot_disk_gb": 0,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "8 vCPUs, 64 GB RAM, 8 local SSD",
        }
    }
    # Provide a minimal SKU list that will not match z3
    gcp_instance_manager._billing_compute_skus = [mock_pricing_sku]

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager.get_instance_pricing(
            mock_instance_types, use_spot=False
        )

    # Assert: warning about Z3 storage-optimized instances should be present
    assert any(
        "Z3 storage-optimized instances have no pricing information for local SSDs"
        in record.message
        for record in caplog.records
    )
    # The result should contain the z3 instance type with an empty pricing dict
    assert z3_instance_type in result
    assert result[z3_instance_type] == {}


@pytest.mark.asyncio
async def test_get_instance_pricing_multiple_ram_skus(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_pricing_sku: MagicMock,
    caplog,
) -> None:
    """Test get_instance_pricing warns and uses the first RAM SKU if multiple RAM SKUs exist."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    instance_type = "n1-standard-2"
    mock_instance_types = {
        instance_type: {
            "name": instance_type,
            "vcpu": 2,
            "mem_gb": 7.5,
            "local_ssd_gb": 0,
            "boot_disk_gb": 0,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "2 vCPUs, 7.5 GB RAM",
        }
    }
    # One core SKU, two RAM SKUs
    core_sku = mock_pricing_sku
    ram_sku1 = MagicMock()
    ram_sku1.description = "N1 Instance Ram running in Americas"
    ram_sku1.service_regions = ["us-central1"]
    ram_pricing_info1 = MagicMock()
    ram_pricing_info1.pricing_expression.usage_unit = "GiBy.h"
    ram_pricing_info1.pricing_expression.tiered_rates = [
        MagicMock(unit_price=MagicMock(nanos=500000000))
    ]
    ram_sku1.pricing_info = [ram_pricing_info1]
    ram_sku2 = MagicMock()
    ram_sku2.description = "N1 Instance Ram running in Americas"
    ram_sku2.service_regions = ["us-central1"]
    ram_pricing_info2 = MagicMock()
    ram_pricing_info2.pricing_expression.usage_unit = "GiBy.h"
    ram_pricing_info2.pricing_expression.tiered_rates = [
        MagicMock(unit_price=MagicMock(nanos=600000000))
    ]
    ram_sku2.pricing_info = [ram_pricing_info2]
    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku1, ram_sku2]

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager.get_instance_pricing(
            mock_instance_types, use_spot=False
        )

    assert any(
        "Multiple ram SKUs found for n1 in region us-central1" in record.message
        for record in caplog.records
    )
    assert instance_type in result
    pricing = result[instance_type][f"{gcp_instance_manager._region}-*"]
    # CPU pricing
    assert pricing["cpu_price"] == 2.0  # $1.00 * 2 vCPUs
    assert pricing["per_cpu_price"] == 1.0
    # RAM pricing should use the first SKU (0.5/GB)
    assert pricing["mem_price"] == 0.5 * 7.5
    assert pricing["mem_per_gb_price"] == 0.5
    # Other fields
    assert pricing["local_ssd_price"] == 0
    assert pricing["boot_disk_price"] == 0
    # Total price
    assert pricing["total_price"] == pricing["cpu_price"] + pricing["mem_price"]


@pytest.mark.asyncio
async def test_get_instance_pricing_multiple_ssd_skus(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_pricing_sku: MagicMock,
    caplog,
) -> None:
    """Test get_instance_pricing warns and uses the first SSD SKU if multiple SSD SKUs exist."""
    # Arrange
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    instance_type = "n1-standard-2-lssd"
    mock_instance_types = {
        instance_type: {
            "name": instance_type,
            "vcpu": 2,
            "mem_gb": 7.5,
            "local_ssd_gb": 375,
            "boot_disk_gb": 0,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "2 vCPUs, 7.5 GB RAM, 1 local SSD",
        }
    }
    # One core SKU, one RAM SKU, two SSD SKUs
    core_sku = mock_pricing_sku
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_pricing_info.pricing_expression.tiered_rates = [
        MagicMock(unit_price=MagicMock(nanos=500000000))
    ]
    ram_sku.pricing_info = [ram_pricing_info]
    ssd_sku1 = MagicMock()
    ssd_sku1.description = "N1 Local SSD provisioned space running in Americas"
    ssd_sku1.service_regions = ["us-central1"]
    ssd_pricing_info1 = MagicMock()
    ssd_pricing_info1.pricing_expression.usage_unit = "GiBy.mo"
    ssd_pricing_info1.pricing_expression.tiered_rates = [
        MagicMock(unit_price=MagicMock(nanos=1000000000))
    ]
    ssd_sku1.pricing_info = [ssd_pricing_info1]
    ssd_sku2 = MagicMock()
    ssd_sku2.description = "N1 Local SSD provisioned space running in Americas"
    ssd_sku2.service_regions = ["us-central1"]
    ssd_pricing_info2 = MagicMock()
    ssd_pricing_info2.pricing_expression.usage_unit = "GiBy.mo"
    ssd_pricing_info2.pricing_expression.tiered_rates = [
        MagicMock(unit_price=MagicMock(nanos=2000000000))
    ]
    ssd_sku2.pricing_info = [ssd_pricing_info2]
    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku, ssd_sku1, ssd_sku2]

    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager.get_instance_pricing(
            mock_instance_types, use_spot=False
        )

    assert any(
        "Multiple local SSD SKUs found for n1 in region us-central1" in record.message
        for record in caplog.records
    )
    assert instance_type in result
    pricing = result[instance_type][f"{gcp_instance_manager._region}-*"]
    # CPU and RAM should be present
    assert pricing["cpu_price"] == 2.0
    assert pricing["per_cpu_price"] == 1.0
    assert pricing["mem_price"] == 3.75
    assert pricing["mem_per_gb_price"] == 0.5
    # SSD fields should use the first SSD SKU
    ssd_per_gb_per_hour = 1000000000 / 1e9 / 730.5
    expected_ssd_price = ssd_per_gb_per_hour * 375
    assert abs(pricing["local_ssd_price"] - expected_ssd_price) < 1e-6
    assert abs(pricing["local_ssd_per_gb_price"] - ssd_per_gb_per_hour) < 1e-6
    # Total price should be cpu+ram+ssd
    assert (
        abs(
            pricing["total_price"]
            - (pricing["cpu_price"] + pricing["mem_price"] + pricing["local_ssd_price"])
        )
        < 1e-6
    )


@pytest.mark.asyncio
async def test_get_optimal_instance_type_no_pricing_data(
    gcp_instance_manager: GCPComputeInstanceManager,
    mocker,
) -> None:
    """Test get_optimal_instance_type raises ValueError if no pricing data is found for any instance types (line 705)."""
    # Arrange: available instance types, but get_instance_pricing returns empty dict
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
        "use_spot": False,
    }
    mocker.patch.object(gcp_instance_manager, "get_instance_pricing", return_value={})
    # Act & Assert
    with pytest.raises(ValueError, match="No pricing data found for any instance types"):
        await gcp_instance_manager.get_optimal_instance_type(constraints)


@pytest.mark.asyncio
async def test_get_optimal_instance_type_logs_sorted_instances(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_pricing_sku: MagicMock,
    caplog,
) -> None:
    """Test get_optimal_instance_type logs sorted instance types by price (line 712)."""
    # Arrange: Use two instance types with different prices
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
        "use_spot": False,
    }
    # Set up pricing SKUs for two instance types
    core_sku1 = mock_pricing_sku
    ram_sku1 = MagicMock()
    ram_sku1.description = "N1 Instance Ram running in Americas"
    ram_sku1.service_regions = ["us-central1"]
    ram_pricing_info1 = MagicMock()
    ram_pricing_info1.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate1 = MagicMock()
    ram_tier_rate1.unit_price.nanos = 500000000  # $0.50/GB/hour
    ram_pricing_info1.pricing_expression.tiered_rates = [ram_tier_rate1]
    ram_sku1.pricing_info = [ram_pricing_info1]

    core_sku2 = MagicMock()
    core_sku2.description = "N2 Instance Core running in Americas"
    core_sku2.service_regions = ["us-central1"]
    core_pricing_info2 = MagicMock()
    core_pricing_info2.pricing_expression.usage_unit = "h"
    core_tier_rate2 = MagicMock()
    core_tier_rate2.unit_price.nanos = 2000000000  # $2.00/core/hour
    core_pricing_info2.pricing_expression.tiered_rates = [core_tier_rate2]
    core_sku2.pricing_info = [core_pricing_info2]

    ram_sku2 = MagicMock()
    ram_sku2.description = "N2 Instance Ram running in Americas"
    ram_sku2.service_regions = ["us-central1"]
    ram_pricing_info2 = MagicMock()
    ram_pricing_info2.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate2 = MagicMock()
    ram_tier_rate2.unit_price.nanos = 1000000000  # $1.00/GB/hour
    ram_pricing_info2.pricing_expression.tiered_rates = [ram_tier_rate2]
    ram_sku2.pricing_info = [ram_pricing_info2]

    gcp_instance_manager._billing_compute_skus = [core_sku1, ram_sku1, core_sku2, ram_sku2]

    # Add a second instance type to mock_instance_types
    instance_types = {
        "n1-standard-2": {
            "name": "n1-standard-2",
            "vcpu": 2,
            "mem_gb": 7.5,
            "local_ssd_gb": 0,
            "boot_disk_gb": 0,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "2 vCPUs, 7.5 GB RAM",
        },
        "n2-standard-2": {
            "name": "n2-standard-2",
            "vcpu": 2,
            "mem_gb": 8,
            "local_ssd_gb": 0,
            "boot_disk_gb": 0,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "2 vCPUs, 8 GB RAM",
        },
    }

    # Patch get_available_instance_types to return our instance_types
    async def fake_get_available_instance_types(constraints):
        return instance_types

    gcp_instance_manager.get_available_instance_types = fake_get_available_instance_types

    with caplog.at_level("DEBUG"):
        selected_price_info = await gcp_instance_manager.get_optimal_instance_type(constraints)
    # Assert: log should contain the sorted instance types by price
    assert any(
        "Instance types sorted by price (cheapest and most vCPUs first):" in record.message
        for record in caplog.records
    )
    # Also check that the log contains the expected instance type names
    assert any("n1-standard-2" in record.message for record in caplog.records)
    assert any("n2-standard-2" in record.message for record in caplog.records)
    # The function should still return the selected price info
    assert selected_price_info["name"] in instance_types


@pytest.mark.asyncio
async def test_get_optimal_instance_type_partial_pricing(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mocker,
) -> None:
    """Test get_optimal_instance_type returns the valid instance if only one has pricing (line 701)."""
    # Arrange: two instance types, one with valid pricing, one with None
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
        "use_spot": False,
    }
    # Two instance types, one with valid pricing, one with None
    instance_types = {
        "n1-standard-2": mock_instance_types["n1-standard-2"],
        "n2-standard-4-lssd": mock_instance_types["n2-standard-4-lssd"],
    }
    # Only n1-standard-2 has valid pricing
    pricing_data = {
        "n1-standard-2": {
            "us-central1-*": {
                "total_price": 5.0,
                "vcpu": 2,
                "name": "n1-standard-2",
                "zone": "us-central1-*",
            }
        },
        "n2-standard-4-lssd": None,
    }
    mocker.patch.object(
        gcp_instance_manager, "get_available_instance_types", return_value=instance_types
    )
    mocker.patch.object(gcp_instance_manager, "get_instance_pricing", return_value=pricing_data)
    # Act
    selected = await gcp_instance_manager.get_optimal_instance_type(constraints)
    # Assert: should not raise, should return the valid instance
    assert isinstance(selected, dict)
    assert selected["name"] == "n1-standard-2"


@pytest.mark.asyncio
async def test_get_optimal_instance_type_all_missing_pricing(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mocker,
) -> None:
    """Test get_optimal_instance_type raises ValueError if all pricing data is missing (line 705)."""
    # Arrange: two instance types, both with None pricing
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
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
        "min_boot_disk": None,
        "max_boot_disk": None,
        "min_boot_disk_per_cpu": None,
        "max_boot_disk_per_cpu": None,
        "use_spot": False,
    }
    instance_types = {
        "n1-standard-2": mock_instance_types["n1-standard-2"],
        "n2-standard-4-lssd": mock_instance_types["n2-standard-4-lssd"],
    }
    pricing_data = {
        "n1-standard-2": None,
        "n2-standard-4-lssd": None,
    }
    mocker.patch.object(
        gcp_instance_manager, "get_available_instance_types", return_value=instance_types
    )
    mocker.patch.object(gcp_instance_manager, "get_instance_pricing", return_value=pricing_data)
    # Act & Assert
    with pytest.raises(ValueError, match="No pricing data found for any instance types"):
        await gcp_instance_manager.get_optimal_instance_type(constraints)


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_core_sku(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    caplog,
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if core SKU is missing (line 556)."""
    # Arrange: Only RAM SKU present
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 500000000
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]
    gcp_instance_manager._billing_compute_skus = [ram_sku]
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager.get_instance_pricing(
            {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No core SKU found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_missing_ram_sku(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
    caplog,
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if RAM SKU is missing (line 572)."""
    # Arrange: Only core SKU present
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    gcp_instance_manager._billing_compute_skus = [mock_pricing_sku]
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager.get_instance_pricing(
            {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No ram SKU found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_cpu_pricing_info_none(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    caplog,
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if cpu_pricing_info is None (line 591)."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    # Arrange: Core SKU with no pricing info (so _extract_pricing_info returns None), valid RAM SKU
    core_sku = MagicMock()
    core_sku.description = "N1 Instance Core running in Americas"
    core_sku.service_regions = ["us-central1"]
    core_sku.pricing_info = []  # No pricing info triggers None
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 500000000
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]
    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku]
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager.get_instance_pricing(
            {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_ram_pricing_info_none(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
    caplog,
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if ram_pricing_info is None (line 606)."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    # Arrange: RAM SKU with no pricing info (so _extract_pricing_info returns None), valid core SKU
    core_sku = mock_pricing_sku
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_sku.pricing_info = []  # No pricing info triggers None
    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku]
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager.get_instance_pricing(
            {"n1-standard-2": mock_instance_types["n1-standard-2"]}, use_spot=False
        )
    assert result["n1-standard-2"] == {}
    assert any("No pricing info found" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_get_instance_pricing_lssd_instance_no_local_ssd_sku(
    gcp_instance_manager: GCPComputeInstanceManager,
    mock_instance_types: Dict[str, Dict[str, Any]],
    mock_pricing_sku: MagicMock,
    caplog,
) -> None:
    """Test get_instance_pricing returns empty dict and logs warning if -lssd instance has no local SSD SKU (line 606)."""
    gcp_instance_manager = deepcopy_gcp_instance_manager(gcp_instance_manager)
    mock_instance_types = copy.deepcopy(mock_instance_types)
    mock_pricing_sku = copy.deepcopy(mock_pricing_sku)
    # Arrange: Instance type with -lssd, but no local SSD SKU is present
    instance_type = "n1-standard-2-lssd"
    mock_instance_types[instance_type] = {
        "name": instance_type,
        "vcpu": 2,
        "mem_gb": 7.5,
        "local_ssd_gb": 375,
        "boot_disk_gb": 0,
        "architecture": "X86_64",
        "supports_spot": True,
        "description": "2 vCPUs, 7.5 GB RAM, 1 local SSD",
    }
    core_sku = mock_pricing_sku
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = 500000000
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]
    # No local SSD SKU
    gcp_instance_manager._billing_compute_skus = [core_sku, ram_sku]
    with caplog.at_level("WARNING"):
        result = await gcp_instance_manager.get_instance_pricing(
            {instance_type: mock_instance_types[instance_type]}, use_spot=False
        )
    assert result[instance_type] == {}
    assert any(
        "No local SSD SKU found for LSSD instance type n1-standard-2-lssd" in record.message
        for record in caplog.records
    )
