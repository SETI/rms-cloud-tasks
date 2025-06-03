import asyncio
import copy
import time
from typing import Any, Dict, Tuple, List

import pytest
import pytest_asyncio
from unittest.mock import MagicMock, patch

from cloud_tasks.common.config import GCPConfig
from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager
from google.oauth2.credentials import Credentials


# We go to a lot of effort to make sure these tests run fast. It turns out that the patches
# in gcp_instance_manager_n1_n2 take 0.3 seconds for every test that is run. To avoid this, we
# make that fixture module scope. Unfortunatley this means that we also have to make all the
# fixtures it depends on module scope as well. Then, since many of the tests mustate the
# fixture return values, we have to deepcopy them at the top of each test. However, we can't
# just deepcopy the gcp_instance_manager_n1_n2 object, because it contains a thread variable that
# can't be serialized. So, we have a special routine to handle that case.

# N2 is more expensive than N1
# N2 preemptible is less expensive than N1 preemptible

N1_2_CPU_PRICE = 1.00  # /cpu/hour
N1_2_RAM_PRICE = 0.05  # /GB/hour

N1_4_CPU_PRICE = 1.00  # /cpu/hour
N1_4_RAM_PRICE = 0.05  # /GB/hour

N2_CPU_PRICE = 1.25  # /cpu/hour
N2_RAM_PRICE = 0.07  # /GB/hour

N1_PREEMPTIBLE_CPU_PRICE = 0.30  # /cpu/hour
N1_PREEMPTIBLE_RAM_PRICE = 0.02  # /GB/hour

N2_PREEMPTIBLE_CPU_PRICE = 0.20  # /cpu/hour
N2_PREEMPTIBLE_RAM_PRICE = 0.01  # /GB/hour

PD_STANDARD_PRICE = 0.00003  # /GB/hour
PD_BALANCED_PRICE = 0.00005  # /GB/hour
PD_SSD_PRICE = 0.00008  # /GB/hour
PD_EXTREME_PRICE = 0.000110  # /GB/hour
HD_BALANCED_PRICE = 0.000137  # /GB/hour

PD_EXTREME_IOPS_PRICE = 0.00000123  # /hour

HD_BALANCED_IOPS_PRICE = 0.00000274  # /hour
HD_BALANCED_THROUGHPUT_PRICE = 0.00000822  # /GB/hour

LSSD_PRICE = 0.000017  # /GB/hour
LSSD_PREEMPTIBLE_PRICE = 0.000008  # /GB/hour


@pytest_asyncio.fixture(scope="package")
async def event_loop():
    """Create an instance of the default event loop for module-scope async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="package")
def mock_credentials() -> MagicMock:
    """Create mock credentials for testing."""
    credentials = MagicMock(spec=Credentials)
    credentials.token = "mock-token"
    credentials.valid = True
    credentials.expired = False
    return credentials


@pytest.fixture(scope="package")
def mock_default_credentials(mock_credentials: MagicMock) -> Tuple[MagicMock, str]:
    """Create mock default credentials tuple for testing."""
    return mock_credentials, "test-project"


# CPU Pricing Fixtures


@pytest.fixture
def cpu_pricing_n1_sku() -> MagicMock:
    """Create a mock CPU pricing SKU with standard pricing info."""
    cpu_sku = MagicMock()
    cpu_sku.description = "N1 Instance Core running in Americas"
    cpu_sku.service_regions = ["us-central1"]
    pricing_info = MagicMock()
    pricing_info.pricing_expression.usage_unit = "h"
    cpu_tier_rate = MagicMock()
    cpu_tier_rate.unit_price.nanos = N1_2_CPU_PRICE * 1e9
    pricing_info.pricing_expression.tiered_rates = [cpu_tier_rate]
    cpu_sku.pricing_info = [pricing_info]
    return cpu_sku


@pytest.fixture
def cpu_pricing_n1_preemptible_sku() -> MagicMock:
    """Create a mock CPU pricing SKU with standard pricing info."""
    cpu_sku = MagicMock()
    cpu_sku.description = "N1 Preemptible Instance Core running in Americas"
    cpu_sku.service_regions = ["us-central1"]
    pricing_info = MagicMock()
    pricing_info.pricing_expression.usage_unit = "h"
    cpu_tier_rate = MagicMock()
    cpu_tier_rate.unit_price.nanos = N1_PREEMPTIBLE_CPU_PRICE * 1e9
    pricing_info.pricing_expression.tiered_rates = [cpu_tier_rate]
    cpu_sku.pricing_info = [pricing_info]
    return cpu_sku


@pytest.fixture
def cpu_pricing_n2_sku() -> MagicMock:
    """Create a mock CPU pricing SKU with standard pricing info."""
    cpu_sku = MagicMock()
    cpu_sku.description = "N2 Instance Core running in Americas"
    cpu_sku.service_regions = ["us-central1"]
    pricing_info = MagicMock()
    pricing_info.pricing_expression.usage_unit = "h"
    cpu_tier_rate = MagicMock()
    cpu_tier_rate.unit_price.nanos = N2_CPU_PRICE * 1e9
    pricing_info.pricing_expression.tiered_rates = [cpu_tier_rate]
    cpu_sku.pricing_info = [pricing_info]
    return cpu_sku


@pytest.fixture
def cpu_pricing_n2_preemptible_sku() -> MagicMock:
    """Create a mock CPU pricing SKU with standard pricing info."""
    cpu_sku = MagicMock()
    cpu_sku.description = "N2 Preemptible Instance Core running in Americas"
    cpu_sku.service_regions = ["us-central1"]
    pricing_info = MagicMock()
    pricing_info.pricing_expression.usage_unit = "h"
    cpu_tier_rate = MagicMock()
    cpu_tier_rate.unit_price.nanos = N2_PREEMPTIBLE_CPU_PRICE * 1e9
    pricing_info.pricing_expression.tiered_rates = [cpu_tier_rate]
    cpu_sku.pricing_info = [pricing_info]
    return cpu_sku


# RAM Pricing Fixtures


@pytest.fixture
def ram_pricing_n1_sku() -> MagicMock:
    """Create a mock RAM pricing SKU with standard pricing info."""
    ram_sku = MagicMock()
    ram_sku.description = "N1 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = N1_2_RAM_PRICE * 1e9
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]
    return ram_sku


@pytest.fixture
def ram_pricing_n1_preemptible_sku() -> MagicMock:
    """Create a mock RAM pricing SKU with standard pricing info."""
    ram_sku = MagicMock()
    ram_sku.description = "N1 Preemptible Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = N1_PREEMPTIBLE_RAM_PRICE * 1e9
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]
    return ram_sku


@pytest.fixture
def ram_pricing_n2_sku() -> MagicMock:
    """Create a mock RAM pricing SKU with standard pricing info."""
    ram_sku = MagicMock()
    ram_sku.description = "N2 Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = N2_RAM_PRICE * 1e9
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]
    return ram_sku


@pytest.fixture
def ram_pricing_n2_preemptible_sku() -> MagicMock:
    """Create a mock RAM pricing SKU with standard pricing info."""
    ram_sku = MagicMock()
    ram_sku.description = "N2 Preemptible Instance Ram running in Americas"
    ram_sku.service_regions = ["us-central1"]
    ram_pricing_info = MagicMock()
    ram_pricing_info.pricing_expression.usage_unit = "GiBy.h"
    ram_tier_rate = MagicMock()
    ram_tier_rate.unit_price.nanos = N2_PREEMPTIBLE_RAM_PRICE * 1e9
    ram_pricing_info.pricing_expression.tiered_rates = [ram_tier_rate]
    ram_sku.pricing_info = [ram_pricing_info]
    return ram_sku


# Local SSD Pricing Fixtures


@pytest.fixture
def local_ssd_pricing_n2_sku() -> MagicMock:
    """Create a mock local SSD pricing SKU with standard pricing info."""
    local_ssd_sku = MagicMock()
    local_ssd_sku.description = "N2 Local SSD provisioned space running in Americas"
    local_ssd_sku.service_regions = ["us-central1"]
    local_ssd_pricing_info = MagicMock()
    local_ssd_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    local_ssd_tier_rate = MagicMock()
    local_ssd_tier_rate.unit_price.nanos = (
        LSSD_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH
    )
    local_ssd_pricing_info.pricing_expression.tiered_rates = [local_ssd_tier_rate]
    local_ssd_sku.pricing_info = [local_ssd_pricing_info]
    return local_ssd_sku


@pytest.fixture
def local_ssd_pricing_n2_preemptible_sku() -> MagicMock:
    """Create a mock local SSD pricing SKU with standard pricing info."""
    local_ssd_sku = MagicMock()
    local_ssd_sku.description = "N2 Preemptible Local SSD provisioned space running in Americas"
    local_ssd_sku.service_regions = ["us-central1"]
    local_ssd_pricing_info = MagicMock()
    local_ssd_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    local_ssd_tier_rate = MagicMock()
    local_ssd_tier_rate.unit_price.nanos = (
        LSSD_PREEMPTIBLE_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH
    )
    local_ssd_pricing_info.pricing_expression.tiered_rates = [local_ssd_tier_rate]
    local_ssd_sku.pricing_info = [local_ssd_pricing_info]
    return local_ssd_sku


# Boot Disk Pricing Fixtures


@pytest.fixture
def boot_disk_pricing_default_skus() -> List[MagicMock]:
    """Create a mock boot disk pricing SKU with standard pricing info."""
    pd_standard_sku = MagicMock()
    pd_standard_sku.description = "Storage PD Capacity in Iowa"
    pd_standard_sku.service_regions = ["us-central1"]
    pd_standard_pricing_info = MagicMock()
    pd_standard_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    pd_standard_tier_rate = MagicMock()
    pd_standard_tier_rate.unit_price.nanos = (
        PD_STANDARD_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH
    )
    pd_standard_pricing_info.pricing_expression.tiered_rates = [pd_standard_tier_rate]
    pd_standard_sku.pricing_info = [pd_standard_pricing_info]

    pd_balanced_sku = MagicMock()
    pd_balanced_sku.description = "Balanced PD Capacity in Iowa"
    pd_balanced_sku.service_regions = ["us-central1"]
    pd_balanced_pricing_info = MagicMock()
    pd_balanced_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    pd_balanced_tier_rate = MagicMock()
    pd_balanced_tier_rate.unit_price.nanos = (
        PD_BALANCED_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH
    )
    pd_balanced_pricing_info.pricing_expression.tiered_rates = [pd_balanced_tier_rate]
    pd_balanced_sku.pricing_info = [pd_balanced_pricing_info]

    pd_ssd_sku = MagicMock()
    pd_ssd_sku.description = "SSD Backed PD Capacity in Iowa"
    pd_ssd_sku.service_regions = ["us-central1"]
    pd_ssd_pricing_info = MagicMock()
    pd_ssd_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    pd_ssd_tier_rate = MagicMock()
    pd_ssd_tier_rate.unit_price.nanos = (
        PD_SSD_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH
    )
    pd_ssd_pricing_info.pricing_expression.tiered_rates = [pd_ssd_tier_rate]
    pd_ssd_sku.pricing_info = [pd_ssd_pricing_info]

    pd_extreme_sku = MagicMock()
    pd_extreme_sku.description = "Extreme PD Capacity in Iowa"
    pd_extreme_sku.service_regions = ["us-central1"]
    pd_extreme_pricing_info = MagicMock()
    pd_extreme_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    pd_extreme_tier_rate = MagicMock()
    pd_extreme_tier_rate.unit_price.nanos = (
        PD_EXTREME_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH
    )
    pd_extreme_pricing_info.pricing_expression.tiered_rates = [pd_extreme_tier_rate]
    pd_extreme_sku.pricing_info = [pd_extreme_pricing_info]

    hd_balanced_sku = MagicMock()
    hd_balanced_sku.description = "Hyperdisk Balanced Capacity in Iowa"
    hd_balanced_sku.service_regions = ["us-central1"]
    hd_balanced_pricing_info = MagicMock()
    hd_balanced_pricing_info.pricing_expression.usage_unit = "GiBy.mo"
    hd_balanced_tier_rate = MagicMock()
    hd_balanced_tier_rate.unit_price.nanos = (
        HD_BALANCED_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH
    )
    hd_balanced_pricing_info.pricing_expression.tiered_rates = [hd_balanced_tier_rate]
    hd_balanced_sku.pricing_info = [hd_balanced_pricing_info]

    hd_balanced_iops_sku = MagicMock()
    hd_balanced_iops_sku.description = "Extreme PD IOPS in Iowa"
    hd_balanced_iops_sku.service_regions = ["us-central1"]
    hd_balanced_iops_pricing_info = MagicMock()
    hd_balanced_iops_pricing_info.pricing_expression.usage_unit = "mo"
    hd_balanced_iops_tier_rate = MagicMock()
    hd_balanced_iops_tier_rate.unit_price.nanos = (
        HD_BALANCED_IOPS_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH
    )
    hd_balanced_iops_pricing_info.pricing_expression.tiered_rates = [hd_balanced_iops_tier_rate]
    hd_balanced_iops_sku.pricing_info = [hd_balanced_iops_pricing_info]

    pd_extreme_iops_sku = MagicMock()
    pd_extreme_iops_sku.description = "Hyperdisk Balanced IOPS in Iowa"
    pd_extreme_iops_sku.service_regions = ["us-central1"]
    pd_extreme_iops_pricing_info = MagicMock()
    pd_extreme_iops_pricing_info.pricing_expression.usage_unit = "mo"
    pd_extreme_iops_tier_rate = MagicMock()
    pd_extreme_iops_tier_rate.unit_price.nanos = (
        PD_EXTREME_IOPS_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH
    )
    pd_extreme_iops_pricing_info.pricing_expression.tiered_rates = [pd_extreme_iops_tier_rate]
    pd_extreme_iops_sku.pricing_info = [pd_extreme_iops_pricing_info]

    hd_balanced_throughput_sku = MagicMock()
    hd_balanced_throughput_sku.description = "Hyperdisk Balanced Throughput in Iowa"
    hd_balanced_throughput_sku.service_regions = ["us-central1"]
    hd_balanced_throughput_pricing_info = MagicMock()
    hd_balanced_throughput_pricing_info.pricing_expression.usage_unit = "mo"
    hd_balanced_throughput_tier_rate = MagicMock()
    hd_balanced_throughput_tier_rate.unit_price.nanos = (
        HD_BALANCED_THROUGHPUT_PRICE * 1e9 * GCPComputeInstanceManager._HOURS_PER_MONTH
    )
    hd_balanced_throughput_pricing_info.pricing_expression.tiered_rates = [
        hd_balanced_throughput_tier_rate
    ]
    hd_balanced_throughput_sku.pricing_info = [hd_balanced_throughput_pricing_info]

    return [
        pd_standard_sku,
        pd_balanced_sku,
        pd_ssd_sku,
        pd_extreme_sku,
        hd_balanced_sku,
        hd_balanced_iops_sku,
        pd_extreme_iops_sku,
        hd_balanced_throughput_sku,
    ]


# Instance Type Fixtures


@pytest.fixture
def mock_instance_types_n1_n2() -> Dict[str, Dict[str, Any]]:
    """Create mock instance types dictionary."""
    return {
        "n1-standard-2": {
            "name": "n1-standard-2",
            "cpu_family": "Intel Haswell",
            "cpu_rank": 5,
            "vcpu": 2,
            "mem_gb": 7.5,
            "local_ssd_gb": 0,
            "supported_boot_disk_types": ["pd-standard"],
            "available_boot_disk_types": ["pd-standard"],
            "boot_disk_iops": 0,
            "boot_disk_throughput": 0,
            "boot_disk_gb": 10,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "2 vCPUs, 7.5 GB RAM",
            "url": "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n1-standard-2",
        },
        "n2-standard-4-lssd": {
            "name": "n2-standard-4-lssd",
            "cpu_family": "Intel Cascade Lake",
            "cpu_rank": 13,
            "vcpu": 4,
            "mem_gb": 16,
            "local_ssd_gb": GCPComputeInstanceManager._ONE_LOCAL_SSD_SIZE * 2,
            "supported_boot_disk_types": ["pd-balanced"],
            "available_boot_disk_types": ["pd-balanced"],
            "boot_disk_iops": 0,
            "boot_disk_throughput": 0,
            "boot_disk_gb": 20,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "4 vCPUs, 16 GB RAM, 2 local SSD",
            "url": "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n2-standard-4-lssd",
        },
    }


@pytest.fixture
def mock_instance_types_n1_2_4() -> Dict[str, Dict[str, Any]]:
    """Create mock instance types dictionary."""
    return {
        "n1-standard-2": {
            "name": "n1-standard-2",
            "cpu_family": "Intel Haswell",
            "cpu_rank": 5,
            "vcpu": 2,
            "mem_gb": 7.5,
            "local_ssd_gb": 0,
            "available_boot_disk_types": ["pd-standard"],
            "boot_disk_iops": 0,
            "boot_disk_throughput": 0,
            "boot_disk_gb": 10,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "2 vCPUs, 7.5 GB RAM",
            "url": "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n1-standard-2",
        },
        "n1-standard-4": {
            "name": "n1-standard-4",
            "cpu_family": "Intel Haswell",
            "cpu_rank": 5,
            "vcpu": 4,
            "mem_gb": 15,
            "local_ssd_gb": 0,
            "available_boot_disk_types": ["pd-standard"],
            "boot_disk_iops": 0,
            "boot_disk_throughput": 0,
            "boot_disk_gb": 10,
            "architecture": "X86_64",
            "supports_spot": True,
            "description": "2 vCPUs, 7.5 GB RAM",
            "url": "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n1-standard-4",
        },
    }


@pytest.fixture(scope="package")
def mock_machine_type_n1_2() -> MagicMock:
    """Create a mock machine type for testing."""
    machine = MagicMock()
    machine.name = "n1-standard-2"
    machine.description = "2 vCPUs, 7.5 GB RAM"
    machine.guest_cpus = 2
    machine.memory_mb = 7680  # 7.5 GB in MB
    machine.architecture = "X86_64"
    machine.self_link = "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n1-standard-2"
    return machine


@pytest.fixture(scope="package")
def mock_machine_type_n1_4() -> MagicMock:
    """Create a mock machine type for testing."""
    machine = MagicMock()
    machine.name = "n1-standard-4"
    machine.description = "4 vCPUs, 15 GB RAM"
    machine.guest_cpus = 4
    machine.memory_mb = 15360  # 15 GB in MB
    machine.architecture = "X86_64"
    machine.self_link = "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n1-standard-2"
    return machine


@pytest.fixture(scope="package")
def mock_machine_type_n2_4() -> MagicMock:
    """Create a mock machine type with local SSD for testing."""
    machine = MagicMock()
    machine.name = "n2-standard-4-lssd"
    machine.description = "4 vCPUs, 16 GB RAM, 2 local SSD"
    machine.guest_cpus = 4
    machine.memory_mb = 16384  # 16 GB in MB
    machine.architecture = "X86_64"
    machine.self_link = "https://compute.googleapis.com/compute/v1/projects/test-project/zones/us-central1-a/machineTypes/n2-standard-4-lssd"
    return machine


@pytest.fixture(scope="package")
def mock_machine_types_client_n1_n2(
    mock_machine_type_n1_2: MagicMock, mock_machine_type_n2_4: MagicMock
) -> MagicMock:
    """Create a mock machine types client."""
    client = MagicMock()
    client.list.return_value = [mock_machine_type_n1_2, mock_machine_type_n2_4]
    return client


@pytest.fixture(scope="package")
def mock_machine_types_client_n1_2_4(
    mock_machine_type_n1_2: MagicMock, mock_machine_type_n1_4: MagicMock
) -> MagicMock:
    """Create a mock machine types client."""
    client = MagicMock()
    client.list.return_value = [mock_machine_type_n1_2, mock_machine_type_n1_4]
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


@pytest.fixture(scope="package")
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


@pytest_asyncio.fixture
async def gcp_instance_manager_n1_n2(
    gcp_config: GCPConfig,
    mock_machine_types_client_n1_n2: MagicMock,
    mock_default_credentials: Tuple[MagicMock, str],
) -> GCPComputeInstanceManager:
    """Create a GCP instance manager with mocked dependencies."""
    start = time.time()
    with (
        patch(
            "cloud_tasks.instance_manager.gcp.get_default_credentials",
            return_value=mock_default_credentials,
        ),
        patch("google.cloud.compute_v1.InstancesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.ZonesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.RegionsClient", return_value=MagicMock()),
        patch(
            "google.cloud.compute_v1.MachineTypesClient",
            return_value=mock_machine_types_client_n1_n2,
        ),
        patch("google.cloud.compute_v1.ImagesClient", return_value=MagicMock()),
        patch("google.cloud.billing.CloudCatalogClient", return_value=MagicMock()),
    ):
        manager = GCPComputeInstanceManager(gcp_config)
        end = time.time()
        print(f"Time taken to create GCPComputeInstanceManager: {end - start} seconds")
        return manager


@pytest_asyncio.fixture
async def gcp_instance_manager_n1_2_4(
    gcp_config: GCPConfig,
    mock_machine_types_client_n1_2_4: MagicMock,
    mock_default_credentials: Tuple[MagicMock, str],
) -> GCPComputeInstanceManager:
    """Create a GCP instance manager with mocked dependencies."""
    start = time.time()
    with (
        patch(
            "cloud_tasks.instance_manager.gcp.get_default_credentials",
            return_value=mock_default_credentials,
        ),
        patch("google.cloud.compute_v1.InstancesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.ZonesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.RegionsClient", return_value=MagicMock()),
        patch(
            "google.cloud.compute_v1.MachineTypesClient",
            return_value=mock_machine_types_client_n1_2_4,
        ),
        patch("google.cloud.compute_v1.ImagesClient", return_value=MagicMock()),
        patch("google.cloud.billing.CloudCatalogClient", return_value=MagicMock()),
    ):
        manager = GCPComputeInstanceManager(gcp_config)
        end = time.time()
        print(f"Time taken to create GCPComputeInstanceManager: {end - start} seconds")
        return manager
