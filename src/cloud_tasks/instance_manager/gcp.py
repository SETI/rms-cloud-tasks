"""
Google Cloud Compute Engine implementation of the InstanceManager interface.
"""

import asyncio
import copy
import logging
import random
import re
import shortuuid
import threading
from typing import Any, Dict, List, Optional, Tuple, cast

from google.api_core.exceptions import NotFound  # type: ignore
from google.auth import default as get_default_credentials
from google.cloud import billing, compute_v1  # type: ignore
from google.oauth2 import service_account

from ..common.config import GCPConfig

from .instance_manager import InstanceManager

shortuuid.set_alphabet("abcdefghijklmnopqrstuvwxyz0123456789")

# Notes:
# - If "credentials_file" is not provided, the default application credentials will be
#   used.
# - If "project_id" is not provided, and "credentials_file" is provided, the project ID
#   will be extracted from the credentials file.
# - "project_id" is required if application default credentials are not used.
# - If "region" is not provided, it will be extracted from the zone. If zone is also not
#   provided, it is an error.
# - If "zone" is not provided and is not otherwise specified, a random zone will be chosen.
# - Compute Engine instances are tagged with "rmscr-<job_id>".
# - There are no instance types that have non-SSD storage so the values returned from
#   get_available_instance_types() will always have "boot_disk_gb" set to 0.
# - Compute Engine instance types are per-zone, and if a zone is not specified the default
#   zone for the region will be used. This is the first zone returned by GCP for the region.
# - "service_account" is optional, but if not provided the instance will not have any
#   credentials.
# - Zone names can end with a wildcard "-*" to indicate that the instance can be started
#   in any zone in the region.
# - GCP pricing is per-region for both on-demand and spot pricing; wildcards are returned
#   for the zone.


class GCPComputeInstanceManager(InstanceManager):
    """Google Cloud Compute Engine implementation of the InstanceManager interface."""

    _HOURS_PER_MONTH = 730.5

    _JOB_ID_TAG_PREFIX = "rmscr-"

    _DEFAULT_OPERATION_TIMEOUT = 240  # seconds

    # Map of instance statuses to standardized statuses
    _STATUS_MAP = {
        "PROVISIONING": "starting",
        "STAGING": "starting",
        "RUNNING": "running",
        "STOPPING": "stopping",
        "SUSPENDING": "stopping",
        "SUSPENDED": "stopped",
        "TERMINATED": "terminated",
    }

    _MACHINE_TYPE_FAMILY_TO_PROCESSOR_TYPE = {
        # General purpose
        "c4": "Intel Emerald Rapids",
        "c4a": "Google Axion",
        "c4d": "AMD Turin",
        "n4": "Intel Emerald Rapids",
        "c3": "Intel Sapphire Rapids",
        "c3d": "AMD Genoa",
        "e2": "Intel Broadwell",
        "n2": "Intel Cascade Lake",
        "n2d": "AMD Milan",
        "t2a": "Ampere Altra",
        "t2d": "AMD Milan",
        "n1": "Intel Haswell",
        "f1": "Intel Haswell",
        "g1": "Intel Haswell",
        # Compute-optimized
        "h3": "Intel Sapphire Rapids",
        "c2": "Intel Cascade Lake",
        "c2d": "AMD Milan",
        # Memory-optimized
        "m4": "Intel Emerald Rapids",
        "x4": "Intel Sapphire Rapids",
        "m3": "Intel Ice Lake",
        "m2": "Intel Cascade Lake",
        "m1": "Intel Skylake",
        # Storage-optimized
        "z3": "Intel Sapphire Rapids",
        # Accelerator-optimized
        "a4": "Intel Emerald Rapids",
        "a3": "Intel Sapphire Rapids",
        "a2": "Intel Cascade Lake",
        "g2": "Intel Cascade Lake",
        "ct6e": "Unknown",  # TPU v6e
    }

    # hd- is HyperDisk; requires provisioned IOPS and provisioned throughput
    #   hd-balanced
    #       Size is 10 to 524288 GB
    #       Default IOPS = 3120
    #       Default throughput = 170 MB/s
    # pd- is Persistent Disk
    #   pd-standard
    #       Size is 10 to 65536 GB
    #   pd-balanced
    #       Size is 10 to 65536 GB
    #   pd-ssd
    #       Size is 10 to 65536 GB
    #   pd-extreme requires provisioned IOPS
    #       Size is 500 to 65536 GB
    #       Default IOPS = 2,500
    _DISK_TYPES = ["pd-standard", "pd-balanced", "pd-ssd", "pd-extreme", "hd-balanced"]
    _DEFAULT_BOOT_DISK_IOPS = 3120
    _DEFAULT_BOOT_DISK_THROUGHPUT = 170
    _MACHINE_TYPE_FAMILY_TO_DISK_TYPES = {
        # General purpose
        "c3": ["hd-balanced", "pd-balanced", "pd-ssd"],
        "c3d": ["hd-balanced", "pd-balanced", "pd-ssd"],
        "c4": ["hd-balanced"],
        "c4a": ["hd-balanced"],
        "c4d": [],
        "e2": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd"],
        "f1": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd"],  #
        "g1": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd"],  #
        "n1": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd"],
        "n2": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd"],
        "n2d": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd", "hd-balanced"],
        "n4": ["hd-balanced"],
        "t2a": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd"],
        "t2d": ["pd-standard", "pd-balanced", "pd-ssd"],
        # Compute-optimized
        "c2": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd"],
        "c2d": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd"],
        "h3": ["pd-balanced", "hd-balanced"],
        # Memory-optimized
        "m1": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd", "hd-balanced"],
        "m2": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd", "hd-balanced"],
        "m3": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd", "hd-balanced"],
        "m4": ["hd-balanced"],
        "x4": ["hd-balanced"],
        # Storage-optimized
        "z3": ["pd-balanced", "pd-ssd", "hd-balanced"],
        # Accelerator-optimized
        "a2": ["pd-standard", "pd-balanced", "pd-extreme", "pd-ssd"],
        "a3": ["pd-balanced", "pd-ssd", "hd-balanced"],
        "a4": ["hd-balanced"],
        "ct6e": ["hd-balanced"],  # This is a guess
        "g2": ["pd-standard", "pd-balanced", "pd-ssd"],
    }

    # This is derived by looking at the pricing tables; beware!
    _ONE_LOCAL_SSD_SIZE = 375  # Size of one local SSD in GB

    def __init__(self, gcp_config: GCPConfig):
        """Initialize the GCP Compute Engine instance manager.

        Args:
            gcp_config: Dictionary with GCP configuration. If credentials_file is not
                provided, the default application credentials from the environment will
                be used.

        Raises:
            RuntimeError: If required configuration is missing
        """
        super().__init__(gcp_config)

        self._logger = logging.getLogger(__name__)
        self._logger.debug("Initializing GCP Compute Engine instance manager")

        self._project_id = gcp_config.project_id

        # Add thread-local storage for compute client
        self._thread_local = threading.local()

        # Handle credentials - use provided service account file or default application credentials
        if gcp_config.credentials_file:
            try:
                self._credentials = service_account.Credentials.from_service_account_file(
                    gcp_config.credentials_file
                )
                self._logger.debug(f"Using credentials from file: {gcp_config.credentials_file}")
            except Exception as e:
                raise RuntimeError(
                    f"Error loading credentials file: {gcp_config.credentials_file}: {e}"
                )
        else:
            # Use default credentials from environment
            try:
                self._credentials, project_id = get_default_credentials()
                self._logger.debug("Using default application credentials")
                if not self._project_id and project_id:
                    self._project_id = project_id
                    self._logger.info(
                        f"Using project ID from default credentials: {self._project_id}"
                    )
            except Exception as e:
                raise RuntimeError(
                    f"Error getting default credentials: {e}. "
                    "Please ensure you're authenticated with 'gcloud auth application-default "
                    "login' or provide a credentials_file entry in the GCP configuration."
                )

        if self._project_id is None:
            raise RuntimeError("Missing required GCP configuration 'project_id'")

        # Initialize region/zone from config if provided
        self._region = gcp_config.region
        self._zone = gcp_config.zone

        # If zone is provided but not region, extract region from zone
        region_from_zone = None
        if self._zone:
            # Extract region from zone (e.g., us-central1-a -> us-central1)
            region_from_zone = self._zone.rsplit("-", 1)[0]
            self._logger.debug(f"Extracted region {self._region} from zone {self._zone}")
            if self._region is not None and self._region != region_from_zone:
                raise ValueError(
                    f"Region {self._region} does not match region {region_from_zone} extracted "
                    f"from zone {self._zone}"
                )
        if self._region is None and region_from_zone is not None:
            self._region = region_from_zone

        # if not self._region:
        #     raise RuntimeError("Missing required GCP configuration: region")
        # It's OK for there to be no specific zone

        # Service account for authorization on worker instances
        self._service_account = gcp_config.service_account

        # Initialize clients
        self._zones_client = compute_v1.ZonesClient(credentials=self._credentials)
        self._regions_client = compute_v1.RegionsClient(credentials=self._credentials)
        self._machine_type_client = compute_v1.MachineTypesClient(credentials=self._credentials)
        self._disks_client = compute_v1.DisksClient(credentials=self._credentials)
        self._images_client = compute_v1.ImagesClient(credentials=self._credentials)
        self._billing_client = billing.CloudCatalogClient(credentials=self._credentials)
        self._billing_compute_skus: List[billing.Sku] | None = None
        self._instance_pricing_cache: Dict[
            Tuple[str, bool], Dict[str, Dict[str, float | str | None]] | None
        ] = {}

        self._logger.debug(
            f"Initialized GCP Compute Engine: project '{self._project_id}', "
            f"region '{self._region}', zone '{self._zone}'"
        )

    def _get_compute_client(self):
        """Get or create a thread-local compute client."""
        if not hasattr(self._thread_local, "compute_client"):
            self._thread_local.compute_client = compute_v1.InstancesClient(
                credentials=self._credentials
            )
        return self._thread_local.compute_client

    def _job_id_to_tag(self, job_id: str) -> str:
        return f"{self._JOB_ID_TAG_PREFIX}{job_id}"

    async def get_available_instance_types(
        self, constraints: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Get available instance types with their specifications.

        Args:
            constraints: Dictionary of constraints to filter instance types by. Constraints
                include::
                    "instance_types": List of regex patterns to filter instance types by name
                    "architecture": Architecture (X86_64 or ARM64)
                    "min_cpu_rank": Minimum acceptable CPU performance rank
                    "max_cpu_rank": Maximum acceptable CPU performance rank
                    "cpus_per_task": Number of vCPUs per task
                    "min_tasks_per_instance": Minimum number of tasks per instance
                    "max_tasks_per_instance": Maximum number of tasks per instance
                    "min_cpu": Minimum number of vCPUs
                    "max_cpu": Maximum number of vCPUs
                    "min_total_memory": Minimum total memory in GB
                    "max_total_memory": Maximum total memory in GB
                    "min_memory_per_cpu": Minimum memory per vCPU in GB
                    "max_memory_per_cpu": Maximum memory per vCPU in GB
                    "min_memory_per_task": Minimum memory per task in GB
                    "max_memory_per_task": Maximum memory per task in GB
                    "min_local_ssd": Minimum amount of local SSD storage in GB
                    "max_local_ssd": Maximum amount of local SSD storage in GB
                    "local_ssd_base_size": Base amount of local SSD storage in GB
                    "min_local_ssd_per_cpu": Minimum amount of local SSD storage per vCPU
                    "max_local_ssd_per_cpu": Maximum amount of local SSD storage per vCPU
                    "min_local_ssd_per_task": Minimum amount of local SSD storage per task
                    "max_local_ssd_per_task": Maximum amount of local SSD storage per task
                    "boot_disk_types": List of boot disk types to use
                    "total_boot_disk_size": Total amount of boot disk storage in GB
                    "boot_disk_base_size": Base amount of boot disk storage in GB
                    "boot_disk_per_cpu": Amount of boot disk storage per vCPU
                    "boot_disk_per_task": Amount of boot disk storage per task
                    "use_spot": Whether to filter for spot-capable instance types

        Returns:
            Dictionary mapping instance type to a dictionary of instance type specifications::
                "name": instance type name
                "cpu_family": processor type
                "cpu_rank": performance rank of the CPU model (higher is better)
                "vcpu": number of vCPUs
                "mem_gb": amount of RAM in GB
                "local_ssd_gb": amount of local SSD storage in GB
                "supported_boot_disk_types": list of boot disk types supported by the instance
                "available_boot_disk_types": list of available boot disk types after applying
                    constraints
                "boot_disk_iops": amount of boot disk IOPS (only applicable for some boot disk
                    types)
                "boot_disk_throughput": amount of boot disk throughput in MB/s (only applicable
                    for some boot disk types)
                "boot_disk_gb": amount of boot disk storage in GB
                "architecture": architecture of the instance type
                "supports_spot": whether the instance type supports spot pricing
                "description": description of the instance type
                "url": URL to the instance type details
        """
        if constraints is None:
            constraints = {}

        # GCP instance types are per-zone
        zone = await self._get_default_zone()

        self._logger.debug(f"Listing available Compute Engine instance types in zone {zone}")
        self._logger.debug(f"Constraints: {constraints}")

        # List instance types in the zone
        request = compute_v1.ListMachineTypesRequest(project=self._project_id, zone=zone)
        machine_types = self._machine_type_client.list(request=request)

        instance_types = {}
        for machine_type in machine_types:
            if constraints.get("instance_types") is not None:
                match_ok = False
                for type_filter in constraints["instance_types"]:
                    if re.match(type_filter, machine_type.name):
                        match_ok = True
                        break
                if not match_ok:
                    continue

            local_ssd_size = 0
            if "local ssd" in machine_type.description.lower():
                # Write a regex to extract the size from the description:
                # "32 vCPUs, 256 GB RAM, 6 local SSD"
                match = re.search(r", (\d+) local ssd", machine_type.description.lower())
                if match:
                    local_ssd_size = int(match.group(1)) * self._ONE_LOCAL_SSD_SIZE

            processor_family = None
            performance_rank = 0
            machine_type_family = machine_type.name.split("-")[0]
            if machine_type_family in self._MACHINE_TYPE_FAMILY_TO_PROCESSOR_TYPE:
                processor_family = self._MACHINE_TYPE_FAMILY_TO_PROCESSOR_TYPE[machine_type_family]
                if processor_family in self._PROCESSOR_FAMILY_TO_PERFORMANCE_RANKING:
                    performance_rank = self._PROCESSOR_FAMILY_TO_PERFORMANCE_RANKING[
                        processor_family
                    ]
                else:
                    self._logger.warning(
                        f"Processor family '{processor_family}' is not in the processor type "
                        "ranking; ranking will be 0"
                    )
            else:
                self._logger.warning(
                    f"Instance type {machine_type.name} with family "
                    f"'{machine_type_family}' is not in the processor family mapping; "
                    "skipping this type"
                )
                continue

            boot_disk_iops = constraints.get("boot_disk_iops")
            if boot_disk_iops is None:
                boot_disk_iops = self._DEFAULT_BOOT_DISK_IOPS
            boot_disk_throughput = constraints.get("boot_disk_throughput")
            if boot_disk_throughput is None:
                boot_disk_throughput = self._DEFAULT_BOOT_DISK_THROUGHPUT
            if machine_type_family in self._MACHINE_TYPE_FAMILY_TO_DISK_TYPES:
                supported_boot_disk_types = self._MACHINE_TYPE_FAMILY_TO_DISK_TYPES[
                    machine_type_family
                ]
            else:
                self._logger.warning(
                    f"Instance type {machine_type.name} with family "
                    f"'{machine_type_family}' is not in the machine type family mapping; "
                    "skipping this instance type"
                )
                continue
            boot_disk_types = constraints.get("boot_disk_types")
            if boot_disk_types is None:
                available_boot_disk_types = supported_boot_disk_types
            else:
                available_boot_disk_types = []
                for boot_disk_type in boot_disk_types:
                    if boot_disk_type not in self._DISK_TYPES:
                        raise ValueError(f"Invalid boot disk type: {boot_disk_type}")
                    if boot_disk_type in supported_boot_disk_types:
                        available_boot_disk_types.append(boot_disk_type)

            instance_info = {
                "name": machine_type.name,
                "cpu_family": processor_family,
                "cpu_rank": performance_rank,
                "vcpu": machine_type.guest_cpus,
                "mem_gb": machine_type.memory_mb / 1024.0,
                "architecture": (
                    machine_type.architecture.upper() if machine_type.architecture else "X86_64"
                ),
                "local_ssd_gb": local_ssd_size,  # Except for dedicated SSDs
                "boot_disk_gb": 0,  # Will fill in later
                "supported_boot_disk_types": supported_boot_disk_types,
                "available_boot_disk_types": available_boot_disk_types,
                "boot_disk_iops": boot_disk_iops,
                "boot_disk_throughput": boot_disk_throughput,
                "supports_spot": True,  # There is no other informationa available
                "description": machine_type.description,
                # https://www.googleapis.com/compute/v1/projects/[project]/zones/
                # [zone]/machineTypes/[name]
                "url": machine_type.self_link,
            }

            boot_disk_gb = self._get_boot_disk_size(instance_info, constraints)
            instance_info["boot_disk_gb"] = boot_disk_gb

            if self._instance_matches_constraints(instance_info, constraints):
                instance_types[machine_type.name] = instance_info

                instance_types[machine_type.name] = instance_info

        return instance_types

    async def _get_billing_compute_skus(self) -> List[billing.Sku]:
        """Get and cache the billing compute SKUs."""
        if self._billing_compute_skus is not None:
            return self._billing_compute_skus

        # Get pricing from Cloud Billing Catalog API
        service_name = "Compute Engine"

        # Make pricing request
        request = billing.ListServicesRequest()
        services = self._billing_client.list_services(request=request)

        # Find compute service
        compute_service = None
        for service in services:
            if service.display_name == service_name:
                compute_service = service
                break

        if not compute_service:
            raise RuntimeError(
                f"Could not find compute service '{service_name}' in billing catalog"
            )

        # Get SKUs for the service
        self._logger.debug("Retrieving all SKUs for compute service")
        sku_request = billing.ListSkusRequest(parent=compute_service.name)
        self._billing_compute_skus = list(self._billing_client.list_skus(request=sku_request))

        return self._billing_compute_skus

    def _extract_pricing_info(
        self,
        machine_family: str,
        sku: Any,
        expected_units: str | Tuple[str, ...],
        component_name: str,
    ) -> Optional[Any]:
        """
        Extract pricing information from a SKU.

        Args:
            sku: The SKU object containing pricing information
            expected_units: Expected usage unit (e.g., 'h', 'GiBy.h', 'GiBy.mo')
            component_name: Name of the component (e.g., 'core', 'ram', 'local SSD')

        Returns:
            Pricing tier information if successful, None if any validation fails
        """
        if isinstance(expected_units, str):
            expected_units = (expected_units,)

        pricing_info = list(sku.pricing_info)
        if len(pricing_info) == 0:
            self._logger.warning(
                f"No pricing info found for {machine_family} {component_name} SKU "
                f'"{sku.description}"; these instance types will be ignored'
            )
            return None
        if len(pricing_info) > 1:
            self._logger.warning(
                f"Multiple pricing info found for {machine_family} {component_name} SKU "
                f'"{sku.description}"; these instance types will be ignored'
            )
            return None

        usage_unit = pricing_info[0].pricing_expression.usage_unit
        if usage_unit not in expected_units:
            self._logger.warning(
                f'{machine_family} {component_name} SKU "{sku.description}" '
                f"has unknown pricing unit: {usage_unit} (expected {expected_units}); "
                "these instance types will be ignored"
            )
            return None

        pricing_tiers = list(pricing_info[0].pricing_expression.tiered_rates)
        if len(pricing_tiers) == 0:
            self._logger.warning(
                f"No tiered rates found for {machine_family} {component_name} SKU "
                f'"{sku.description}"; these instance types will be ignored'
            )
            return None
        good_pricing_tier = None
        first_pricing_tier = None
        for pricing_tier in pricing_tiers:
            if first_pricing_tier is None:
                first_pricing_tier = pricing_tier
            if pricing_tier.unit_price.nanos > 0:  # We really need to handle tiered pricing
                if good_pricing_tier is not None:
                    self._logger.warning(
                        f"Multiple pricing tiers found for {machine_family} {component_name} SKU "
                        f'"{sku.description}"; using first one'
                    )
                else:
                    good_pricing_tier = pricing_tier
        if good_pricing_tier is None:
            good_pricing_tier = first_pricing_tier  # Take the first even if it's zero
        if good_pricing_tier is None:
            self._logger.warning(
                f"No pricing tier found for {machine_family} {component_name} SKU "
                f'"{sku.description}"; these instance types will be ignored'
            )
            return None

        return good_pricing_tier

    def _match_sku(self, match_string, error_string, set_sku, sku, sku_description, machine_family):
        if match_string in sku_description:
            if set_sku is not None:
                self._logger.warning(
                    f"Multiple {error_string} SKUs found for {machine_family} in region "
                    f"{self._region} (choosing first one):"
                )
                self._logger.warning(f"  {set_sku.description}")
                self._logger.warning(f"  {sku.description}")
                return set_sku, True
            else:
                return sku, True
        return set_sku, False

    async def get_instance_pricing(
        self,
        instance_types: Dict[str, Dict[str, Any]],
        *,
        use_spot: bool = False,
        boot_disk_constraints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Dict[str, float | str | None]]]:
        """
        Get the hourly price for one or more specific instance types.

        Note that GCP pricing is per-region (not per-zone) for both on-demand and spot instances
        so we return the pricing for the region followed by a wildcard for the zone like
        us-central1-*.

        Args:
            instance_types: A dictionary mapping instance type to a dictionary of instance type
                specifications as returned by get_available_instance_types().
            use_spot: Whether to use spot pricing
            boot_disk_constraints: Dictionary of constraints used to determine the boot disk type and
                size. These are from the same config as the instance type constraints but are not
                used to filter instances.

        Returns:
            A dictionary mapping instance type to a dictionary of zones, which contains a dictionary
            of boot disk types, which contains a dictionary of hourly price in USD::
                "cpu_price": Total price of CPU in USD/hour
                "per_cpu_price": Price of CPU in USD/vCPU/hour
                "mem_price": Total price of RAM in USD/hour
                "mem_per_gb_price": Price of RAM in USD/GB/hour
                "boot_disk_type": Type of boot disk being used for pricing
                "boot_disk_price": Total price of boot disk in USD/hour
                "boot_disk_per_gb_price": Price of boot disk in USD/GB/hour
                "boot_disk_per_iops_price": Price of one boot disk IOPS in USD/hour
                "boot_disk_iops_price": Price of provisioned boot disk IOPS in USD/hour
                "boot_disk_per_throughput_price": Price of boot disk throughput in USD/GiBi/hour
                "boot_disk_throughput_price": Price of provisioned boot disk throughput in USD/hour
                "local_ssd_price": Total price of local SSD in USD/hour
                "local_ssd_per_gb_price": Price of local SSD in USD/GB/hour
                "total_price": Total price of instance in USD/hour
                "total_price_per_cpu": Total price of instance in USD/vCPU/hour
                "zone": availability zone
            Plus the original instance type info keyed by availability zone. If any price is not
            available, it is set to None.
        """
        self._logger.debug(
            f"Getting pricing for {len(instance_types)} instance types (spot: {use_spot})"
        )
        self._logger.debug(f"Boot disk constraints: {boot_disk_constraints}")

        ret: Dict[str, Dict[str, Dict[str, float | str | None]]] = {}

        if len(instance_types) == 0:
            self._logger.debug("No instance types provided")
            return ret

        # Lookup pricing for each instance type
        for machine_type, machine_info in instance_types.items():
            self._logger.debug(
                f"Getting pricing for instance type: {machine_type} (spot: {use_spot})"
            )

            # Extract the machine family from the machine type name
            # This could look like "n1-standard-1" or "c4a-highmem-72-lssd"
            machine_name_parts = machine_type.split("-")
            machine_family = machine_name_parts[0]
            # Normally the size of the machine type (e.g. -64) is at the end of the name, but
            # machine types ending in -lssd are a special case. Here the size is at the second to
            # last position. Also we have to infer the size of the local SSD.
            is_lssd = False
            if machine_name_parts[-1] == "lssd":
                is_lssd = True
                machine_family_for_cache = machine_name_parts[0] + "-" + machine_name_parts[-1]
            else:
                machine_family_for_cache = machine_name_parts[0]
            cache_key = (machine_family_for_cache, use_spot)

            # GCP pricing is specific to a machine family, which is then scaled by the number of
            # vCPUs and memory, so we just cache the pricing for the machine family and use that for
            # all instance types in the family.
            if cache_key in self._instance_pricing_cache:
                # Cache hit!
                ret_val = self._instance_pricing_cache[cache_key]
                if ret_val is None:  # No pricing info available
                    ret[machine_type] = {}
                    continue
                ret_val = copy.deepcopy(ret_val)  # Since we're going to mutate it
                zone_val = ret_val.get(f"{self._region}-*")
                if zone_val is None:  # pragma: no cover
                    raise RuntimeError("Internal error while finding pricing: region has changed")
                for boot_disk_type, zone_boot_val in zone_val.items():
                    # Add the instance type info to the return value
                    zone_boot_val.update(machine_info)
                    # Update the pricing info with the new vCPU and memory info
                    per_cpu_price = cast(float, zone_boot_val["per_cpu_price"])
                    per_gb_ram_price = cast(float, zone_boot_val["mem_per_gb_price"])
                    per_gb_local_ssd_price = cast(float, zone_boot_val["local_ssd_per_gb_price"])
                    per_gb_boot_disk_price = cast(float, zone_boot_val["boot_disk_per_gb_price"])
                    cpu_price = per_cpu_price * machine_info["vcpu"]
                    ram_price = per_gb_ram_price * machine_info["mem_gb"]
                    local_ssd_price = per_gb_local_ssd_price * machine_info["local_ssd_gb"]
                    boot_disk_per_iops_price = 0
                    boot_disk_iops_price = 0
                    boot_disk_per_throughput_price = 0
                    boot_disk_throughput_price = 0
                    match boot_disk_type:
                        case "pd-extreme":
                            boot_disk_per_iops_price = zone_boot_val["boot_disk_per_iops_price"]
                            boot_disk_iops_price = (
                                boot_disk_per_iops_price * machine_info["boot_disk_iops"]
                            )
                        case "hd-balanced":
                            boot_disk_per_iops_price = zone_boot_val["boot_disk_per_iops_price"]
                            boot_disk_per_throughput_price = zone_boot_val[
                                "boot_disk_per_throughput_price"
                            ]
                            boot_disk_iops_price = (
                                boot_disk_per_iops_price * machine_info["boot_disk_iops"]
                            )
                            boot_disk_throughput_price = (
                                boot_disk_per_throughput_price
                                * machine_info["boot_disk_throughput"]
                            )
                    boot_disk_price = (
                        per_gb_boot_disk_price * machine_info["boot_disk_gb"]
                        + boot_disk_iops_price
                        + boot_disk_throughput_price
                    )
                    total_price = cpu_price + ram_price + local_ssd_price + boot_disk_price
                    zone_boot_val["cpu_price"] = round(cpu_price, 6)
                    zone_boot_val["mem_price"] = round(ram_price, 6)
                    zone_boot_val["local_ssd_price"] = round(local_ssd_price, 6)
                    zone_boot_val["boot_disk_per_iops_price"] = boot_disk_per_iops_price
                    zone_boot_val["boot_disk_iops_price"] = round(boot_disk_iops_price, 6)
                    zone_boot_val["boot_disk_per_throughput_price"] = boot_disk_per_throughput_price
                    zone_boot_val["boot_disk_throughput_price"] = round(
                        boot_disk_throughput_price, 6
                    )
                    zone_boot_val["boot_disk_price"] = round(boot_disk_price, 6)
                    zone_boot_val["total_price"] = round(total_price, 6)
                    zone_boot_val["total_price_per_cpu"] = round(
                        total_price / machine_info["vcpu"], 6
                    )
                ret[machine_type] = ret_val
                continue

            # Cache miss! Now we search through all the SKUs to find the ones we need.

            core_sku = None
            ram_sku = None
            boot_disk_skus = {
                "pd-standard": None,
                "pd-balanced": None,
                "pd-ssd": None,
                "pd-extreme": None,
                "hd-balanced": None,
            }
            pd_extreme_iops_sku = None
            hd_balanced_iops_sku = None
            hd_balanced_throughput_sku = None
            local_ssd_sku = None

            # Save None to mark we tried, even if we don't eventually find a match
            self._instance_pricing_cache[cache_key] = None

            # Find matching SKU for the instance type in this region.
            # This is kind of a hack because we are figuring out which SKU to use based on
            # its description, but there's nothing else we can do. However, there may be errors
            # here if there are descriptions that we don't parse correctly.
            for sku in await self._get_billing_compute_skus():
                sku_description = sku.description.lower()

                # We don't like SKUs that are for various types of reserved instances
                if (
                    "sole tenancy" in sku_description
                    or "custom" in sku_description
                    or "commitment" in sku_description
                ):
                    continue

                # Skip if this SKU doesn't match our region
                if self._region not in sku.service_regions:
                    continue
                # print(sku)

                # Check if this is the SKU for the boot disk type we want
                if (
                    "regional" in sku_description
                    or "asynchronous" in sku_description
                    or "confidential" in sku_description
                ):
                    continue

                boot_disk_skus["pd-standard"], found = self._match_sku(
                    "storage pd capacity",
                    "PD Standard",
                    boot_disk_skus["pd-standard"],
                    sku,
                    sku_description,
                    machine_family,
                )
                if found:
                    continue

                boot_disk_skus["pd-balanced"], found = self._match_sku(
                    "balanced pd capacity",
                    "PD Balanced",
                    boot_disk_skus["pd-balanced"],
                    sku,
                    sku_description,
                    machine_family,
                )
                if found:
                    continue

                boot_disk_skus["pd-ssd"], found = self._match_sku(
                    "ssd backed pd capacity",
                    "PD SSD",
                    boot_disk_skus["pd-ssd"],
                    sku,
                    sku_description,
                    machine_family,
                )
                if found:
                    continue

                boot_disk_skus["pd-extreme"], found = self._match_sku(
                    "extreme pd capacity",
                    "PD Extreme",
                    boot_disk_skus["pd-extreme"],
                    sku,
                    sku_description,
                    machine_family,
                )
                if found:
                    continue

                boot_disk_skus["hd-balanced"], found = self._match_sku(
                    "hyperdisk balanced capacity",
                    "HD Balanced",
                    boot_disk_skus["hd-balanced"],
                    sku,
                    sku_description,
                    machine_family,
                )
                if found:
                    continue

                pd_extreme_iops_sku, found = self._match_sku(
                    "extreme pd iops",
                    "PD Extreme IOPS",
                    pd_extreme_iops_sku,
                    sku,
                    sku_description,
                    machine_family,
                )
                if found:
                    continue

                hd_balanced_iops_sku, found = self._match_sku(
                    "hyperdisk balanced iops",
                    "HD Balanced IOPS",
                    hd_balanced_iops_sku,
                    sku,
                    sku_description,
                    machine_family,
                )
                if found:
                    continue

                hd_balanced_throughput_sku, found = self._match_sku(
                    "hyperdisk balanced throughput",
                    "HD Balanced Throughput",
                    hd_balanced_throughput_sku,
                    sku,
                    sku_description,
                    machine_family,
                )

                # Skip if this SKU doesn't match our instance type
                if f"{machine_family} " not in sku_description:
                    continue

                # Skip if this SKU is for local SSDs and our instance type doesn't have them
                # This is necessary because there are instance types like "n1-standard-2"
                # and "n1-standard-2-lssd" and we only want to include the LSSD pricing for the
                # -lssd instance type.
                if "local ssd" in sku_description and not is_lssd:
                    continue

                # Skip if this SKU is for preemptible instances and we don't want preemptible
                # instances, or if it's for non-preemptible instances and we do want preemptible
                # instances
                if ("preemptible" in sku_description and not use_spot) or (
                    "preemptible" not in sku_description and use_spot
                ):
                    continue

                # Save the SKU for core, ram, and local SSD
                core_sku, found = self._match_sku(
                    "instance core", "core", core_sku, sku, sku_description, machine_family
                )
                if found:
                    continue

                ram_sku, found = self._match_sku(
                    "instance ram", "ram", ram_sku, sku, sku_description, machine_family
                )
                if found:
                    continue

                local_ssd_sku, found = self._match_sku(
                    "local ssd", "local SSD", local_ssd_sku, sku, sku_description, machine_family
                )
                if found:
                    continue

            # From this point on we at least want the machine type to be in the return value
            ret[machine_type] = {}

            if core_sku is None:
                self._logger.warning(
                    f"No core SKU found for instance family {machine_family} in region "
                    f"{self._region}; ignoring these instance types"
                )
                continue
            if ram_sku is None:
                self._logger.warning(
                    f"No ram SKU found for instance family {machine_family} in region "
                    f"{self._region}; ignoring these instance types"
                )
                continue

            if boot_disk_skus["pd-standard"] is None:
                self._logger.warning(
                    f"No PD Standard boot disk SKU found for instance family {machine_family} "
                    f"in region {self._region}; ignoring these instance types"
                )
                continue
            if boot_disk_skus["pd-balanced"] is None:
                self._logger.warning(
                    f"No PD Balanced boot disk SKU found for instance family {machine_family} "
                    f"in region {self._region}; ignoring these instance types"
                )
                continue
            if boot_disk_skus["pd-ssd"] is None:
                self._logger.warning(
                    f"No PD SSD boot disk SKU found for instance family {machine_family} "
                    f"in region {self._region}; ignoring these instance types"
                )
                continue
            if boot_disk_skus["pd-extreme"] is None:
                self._logger.warning(
                    f"No PD Extreme boot disk SKU found for instance family {machine_family} "
                    f"in region {self._region}; ignoring these instance types"
                )
                continue
            if boot_disk_skus["hd-balanced"] is None:
                self._logger.warning(
                    f"No HD Balanced boot disk SKU found for instance family {machine_family} "
                    f"in region {self._region}; ignoring these instance types"
                )
                continue

            if pd_extreme_iops_sku is None:
                self._logger.warning(
                    f"No PD Extreme IOPS SKU found for instance family {machine_family} "
                    f"in region {self._region}; ignoring these instance types"
                )
                continue
            if hd_balanced_iops_sku is None:
                self._logger.warning(
                    f"No HD Balanced IOPS SKU found for instance family {machine_family} "
                    f"in region {self._region}; ignoring these instance types"
                )
                continue
            if hd_balanced_throughput_sku is None:
                self._logger.warning(
                    f"No HD Balanced Throughput SKU found for instance family {machine_family} "
                    f"in region {self._region}; ignoring these instance types"
                )
                continue

            if is_lssd and local_ssd_sku is None:
                self._logger.warning(
                    f"No local SSD SKU found for instance family {machine_family} "
                    f"in region {self._region}; ignoring these instance types"
                )
                continue

            self._logger.debug(f'Matching        core SKU found: "{core_sku.description}"')
            self._logger.debug(f'Matching         ram SKU found: "{ram_sku.description}"')
            self._logger.debug(
                f'Matching PD Standard SKU found: "{boot_disk_skus["pd-standard"].description}"'
            )
            self._logger.debug(
                f'Matching PD Balanced SKU found: "{boot_disk_skus["pd-balanced"].description}"'
            )
            self._logger.debug(
                f'Matching PD SSD boot SKU found: "{boot_disk_skus["pd-ssd"].description}"'
            )
            self._logger.debug(
                f'Matching PD Extreme  SKU found: "{boot_disk_skus["pd-extreme"].description}"'
            )
            self._logger.debug(
                f'Matching HD Balanced SKU found: "{boot_disk_skus["hd-balanced"].description}"'
            )
            self._logger.debug(
                f'Matching PD Extreme IOPS SKU found: "{pd_extreme_iops_sku.description}"'
            )
            self._logger.debug(
                f'Matching HD Balanced IOPS SKU found: "{hd_balanced_iops_sku.description}"'
            )
            self._logger.debug(
                f'Matching HD Balanced Throughput SKU found: "{hd_balanced_throughput_sku.description}"'
            )

            if local_ssd_sku is not None:
                self._logger.debug(f'Matching LSSD SKU found: "{local_ssd_sku.description}"')

            # Extract price info for CPU
            cpu_pricing_info = self._extract_pricing_info(machine_family, core_sku, "h", "core")
            if cpu_pricing_info is None:
                continue

            # Extract price info for RAM
            # TODO Supporting both GiBy.h and GBy.h is probably wrong because we're dividing
            # by the wrong number, but hopefully it won't make a big difference
            ram_pricing_info = self._extract_pricing_info(
                machine_family, ram_sku, ("GiBy.h", "GBy.h"), "ram"
            )
            if ram_pricing_info is None:
                continue

            # Extract price info for boot disk
            boot_disk_pricing_info = {}
            for boot_disk_type in boot_disk_skus:
                boot_disk_pricing_info[boot_disk_type] = self._extract_pricing_info(
                    machine_family, boot_disk_skus[boot_disk_type], "GiBy.mo", "boot disk"
                )
                if boot_disk_pricing_info[boot_disk_type] is None:
                    continue

            pd_extreme_iops_pricing_info = self._extract_pricing_info(
                machine_family, pd_extreme_iops_sku, "mo", "PD Extreme IOPS"
            )
            if pd_extreme_iops_pricing_info is None:
                continue
            hd_balanced_iops_pricing_info = self._extract_pricing_info(
                machine_family, hd_balanced_iops_sku, "mo", "HD Balanced IOPS"
            )
            if hd_balanced_iops_pricing_info is None:
                continue
            hd_balanced_throughput_pricing_info = self._extract_pricing_info(
                machine_family, hd_balanced_throughput_sku, "mo", "HD Balanced Throughput"
            )
            if hd_balanced_throughput_pricing_info is None:
                continue

            # Extract price info for local SSD if present
            local_ssd_pricing_info = None
            if local_ssd_sku is not None:
                local_ssd_pricing_info = self._extract_pricing_info(
                    machine_family, local_ssd_sku, "GiBy.mo", "local SSD"
                )
                if local_ssd_pricing_info is None:
                    continue

            per_cpu_price = cast(float, cpu_pricing_info.unit_price.nanos / 1e9)
            per_gb_ram_price = cast(float, ram_pricing_info.unit_price.nanos / 1e9)

            cpu_price = per_cpu_price * machine_info["vcpu"]
            ram_price = per_gb_ram_price * machine_info["mem_gb"]
            no_boot_disk_total_price = cpu_price + ram_price

            self._logger.debug(f"Core price: ${per_cpu_price:.6f}/vCPU/hour ({cpu_price:.6f}/hour)")
            self._logger.debug(
                f"Ram price:  ${per_gb_ram_price:.6f}/GB/hour ({ram_price:.6f}/hour)"
            )

            local_ssd_price = 0
            per_gb_local_ssd_price = 0
            if local_ssd_pricing_info is not None:
                if not is_lssd:  # pragma: no cover
                    raise RuntimeError(
                        f"Local SSD SKU found for non-LSSD instance type: {machine_type}"
                    )
                per_gb_local_ssd_price = (
                    local_ssd_pricing_info.unit_price.nanos / 1e9 / self._HOURS_PER_MONTH
                )  # GiBy.mo -> GiBy/hour
                local_ssd_price = per_gb_local_ssd_price * machine_info["local_ssd_gb"]
                no_boot_disk_total_price += local_ssd_price
                self._logger.debug(
                    f"Local SSD price: ${per_gb_local_ssd_price:.6f}/GB/hour "
                    f"({local_ssd_price:.6f}/hour)"
                )

            # Now go through each boot disk type and create a separate pricing entry for
            # it for this instance type

            pricing_by_boot_disk = {}
            ret_val = {f"{self._region}-*": pricing_by_boot_disk}

            # We return pricing for all supported boot disk types, even if they have not been
            # selected by the constraints; the user of the pricing info has to enforce those
            # constraints
            for boot_disk_type in machine_info["supported_boot_disk_types"]:
                self._logger.debug(f"For boot disk type: {boot_disk_type}")
                if (
                    boot_disk_type not in boot_disk_pricing_info
                    or boot_disk_pricing_info[boot_disk_type] is None
                ):
                    self._logger.warning(
                        f"  No pricing info was found for boot disk type {boot_disk_type}; skipping"
                    )
                    continue
                per_gb_boot_disk_price = (
                    boot_disk_pricing_info[boot_disk_type].unit_price.nanos
                    / 1e9
                    / self._HOURS_PER_MONTH
                )  # GiBy.mo -> GiBy/hour
                boot_disk_price = per_gb_boot_disk_price * machine_info["boot_disk_gb"]
                self._logger.debug(
                    f"  Boot disk storage price: ${per_gb_boot_disk_price:.6f}/GB/hour "
                    f"({boot_disk_price:.6f}/hour)"
                )

                per_iops_price = (
                    pd_extreme_iops_pricing_info.unit_price.nanos / 1e9 / self._HOURS_PER_MONTH
                )
                per_throughput_price = (
                    hd_balanced_throughput_pricing_info.unit_price.nanos
                    / 1e9
                    / self._HOURS_PER_MONTH
                )
                boot_disk_per_iops_price = 0
                boot_disk_per_throughput_price = 0
                iops_price = 0
                throughput_price = 0

                match boot_disk_type:
                    case "pd-extreme":
                        boot_disk_per_iops_price = per_iops_price
                        iops_price = per_iops_price * machine_info["boot_disk_iops"]
                        self._logger.debug(
                            f"  Boot disk {machine_info['boot_disk_iops']} IOPS price: "
                            f"${iops_price:.6f}/hour"
                        )
                    case "hd-balanced":
                        boot_disk_per_iops_price = per_iops_price
                        boot_disk_per_throughput_price = per_throughput_price
                        iops_price = per_iops_price * machine_info["boot_disk_iops"]
                        throughput_price = (
                            per_throughput_price * machine_info["boot_disk_throughput"]
                        )
                        self._logger.debug(
                            f"  Boot disk {machine_info['boot_disk_iops']} IOPS price: "
                            f"${iops_price:.6f}/hour"
                        )
                        self._logger.debug(
                            f"  Boot disk {machine_info['boot_disk_throughput']} "
                            f"throughput price: ${throughput_price:.6f}/hour"
                        )

                total_price = (
                    no_boot_disk_total_price + boot_disk_price + iops_price + throughput_price
                )

                # Round off the total price to 6 decimal places to avoid floating point
                # precision issues
                total_price = round(total_price, 6)
                self._logger.debug(f"  Total price: ${total_price:.6f}/hour")

                pricing_by_boot_disk[boot_disk_type] = {
                    "cpu_price": round(cpu_price, 6),  # CPU price
                    "per_cpu_price": round(per_cpu_price, 6),  # Per-CPU price
                    "mem_price": round(ram_price, 6),  # Memory price
                    "mem_per_gb_price": round(per_gb_ram_price, 6),  # Per-GB price
                    "local_ssd_price": round(local_ssd_price, 6),  # Local SSD price
                    "local_ssd_per_gb_price": round(per_gb_local_ssd_price, 6),  # Per-GB price
                    "boot_disk_type": boot_disk_type,
                    "boot_disk_price": round(boot_disk_price, 6),  # Boot disk price
                    "boot_disk_per_gb_price": round(per_gb_boot_disk_price, 6),  # Per-GB price
                    "boot_disk_per_iops_price": boot_disk_per_iops_price,  # Per-IOPS price
                    "boot_disk_iops_price": round(iops_price, 6),  # Boot disk IOPS price
                    "boot_disk_per_throughput_price": boot_disk_per_throughput_price,  # Per-throughput price
                    "boot_disk_throughput_price": round(
                        throughput_price, 6
                    ),  # Boot disk throughput price
                    "total_price": round(total_price, 6),  # Total price
                    "total_price_per_cpu": round(
                        total_price / machine_info["vcpu"], 6
                    ),  # Total price per CPU
                    "zone": f"{self._region}-*",
                    **machine_info,
                }

            # We only cache the pricing info for the machine family
            self._instance_pricing_cache[cache_key] = ret_val

            if pricing_by_boot_disk == {}:
                self._logger.warning(
                    "No pricing info found for any boot disk type for instance type "
                    f"{machine_type}; skipping"
                )
                continue

            # Add the instance type info to the return value
            ret[machine_type] = ret_val

        return ret

    async def get_optimal_instance_type(
        self, constraints: Optional[Dict[str, Any]] = None
    ) -> Dict[str, float | str | None]:
        """
        Get the most cost-effective GCP instance type that meets the constraints.

        Args:
            constraints: Dictionary of constraints to filter instance types by. Constraints
                include::
                    "instance_types": List of regex patterns to filter instance types by name
                    "architecture": Architecture (X86_64 or ARM64)
                    "min_cpu_rank": Minimum acceptable CPU performance rank
                    "max_cpu_rank": Maximum acceptable CPU performance rank
                    "cpus_per_task": Number of vCPUs per task
                    "min_tasks_per_instance": Minimum number of tasks per instance
                    "max_tasks_per_instance": Maximum number of tasks per instance
                    "min_cpu": Minimum number of vCPUs
                    "max_cpu": Maximum number of vCPUs
                    "min_total_memory": Minimum total memory in GB
                    "max_total_memory": Maximum total memory in GB
                    "min_memory_per_cpu": Minimum memory per vCPU in GB
                    "max_memory_per_cpu": Maximum memory per vCPU in GB
                    "min_memory_per_task": Minimum memory per task in GB
                    "max_memory_per_task": Maximum memory per task in GB
                    "min_local_ssd": Minimum amount of local SSD storage in GB
                    "max_local_ssd": Maximum amount of local SSD storage in GB
                    "local_ssd_base_size": Base amount of local SSD storage in GB
                    "min_local_ssd_per_cpu": Minimum amount of local SSD storage per vCPU
                    "max_local_ssd_per_cpu": Maximum amount of local SSD storage per vCPU
                    "min_local_ssd_per_task": Minimum amount of local SSD storage per task
                    "max_local_ssd_per_task": Maximum amount of local SSD storage per task
                    "boot_disk_types": List of boot disk types to use
                    "total_boot_disk_size": Total amount of boot disk storage in GB
                    "boot_disk_base_size": Base amount of boot disk storage in GB
                    "boot_disk_per_cpu": Amount of boot disk storage per vCPU
                    "boot_disk_per_task": Amount of boot disk storage per task
                    "use_spot": Whether to filter for spot-capable instance types

        Returns:
            Dictionary of instance type pricing info as would be returned by get_instance_pricing
        """
        if constraints is None:
            constraints = {}

        self._logger.debug(
            f"Getting optimal instance type in region {self._region} and zone " f"{self._zone}"
        )
        self._logger.debug(f"Constraints: {constraints}")

        if constraints.get("boot_disk_types") is None or constraints.get("boot_disk_types") == []:
            self._logger.warning(
                "No boot disk types specified; this will make all relevant types available and "
                "likely result in the selection of the slowest boot disk available"
            )

        avail_instance_types = await self.get_available_instance_types(constraints)
        self._logger.debug(
            f"Found {len(avail_instance_types)} available instance types in region "
            f"{self._region} and zone {self._zone}"
        )

        if not avail_instance_types:
            raise ValueError("No instance type meets requirements")

        use_spot = constraints.get("use_spot")
        if use_spot is None:
            use_spot = False
        pricing_data = await self.get_instance_pricing(
            avail_instance_types,
            use_spot=use_spot,
            boot_disk_constraints=constraints,
        )

        # Rearrange the pricing data into a dictionary of (machine_type, zone) -> price
        zone_pricing_data = {}
        for machine_type, price in pricing_data.items():
            if price is None:
                self._logger.debug(f"No pricing data found for {machine_type}; ignoring")
                continue
            for zone, price_in_zone in price.items():
                if price_in_zone is None:
                    self._logger.debug(
                        f"No pricing data found for {machine_type} in zone {zone}; ignoring"
                    )
                    continue
                for boot_disk_type, price_info in price_in_zone.items():
                    # Filter out the boot disk types that were not selected by the constraints
                    if boot_disk_type not in price_info["available_boot_disk_types"]:
                        continue
                    zone_pricing_data[(machine_type, zone, boot_disk_type)] = price_info

        if len(zone_pricing_data) == 0:
            raise ValueError("No pricing data found for any instance types")

        # Select instance with the lowest price
        priced_instances = [
            (machine_type, zone, boot_disk_type, price_info)
            for (machine_type, zone, boot_disk_type), price_info in zone_pricing_data.items()
        ]
        # Sort by price per vCPU, then by decreasing vCPU (this gives us the cheapest
        # instance type with the most vCPUs). Instead of using the price_per_vcpu field,
        # we use the total_price field and divide by the number of vCPUs. This gives us a
        # more accurate comparison of the cost of the instance including memory and disk.
        # We round the price to 4 decimal places so that small differences in price don't
        # make us choose an instance with fewer vCPUs that would otherwise cost the same.
        priced_instances.sort(
            key=lambda x: (
                round(cast(float, x[3]["total_price"]) / x[3]["vcpu"], 4),
                -cast(int, x[3]["vcpu"]),
            )
        )

        self._logger.debug("Instance types sorted by price (cheapest and most vCPUs first):")
        for i, (machine_type, zone, boot_disk_type, price_info) in enumerate(priced_instances):
            self._logger.debug(
                f"  [{i+1:3d}] {machine_type:20s} ({boot_disk_type:12s}) "
                f"in {zone:15s}: "
                f"${price_info['total_price']:10.6f}/hour = "
                f"${price_info['total_price'] / price_info['vcpu']:10.6f}/vCPU/hour"
            )

        selected_type, selected_zone, selected_boot_disk_type, selected_price_info = (
            priced_instances[0]
        )
        total_price = selected_price_info["total_price"]
        self._logger.debug(
            f"Selected {selected_type} ({selected_boot_disk_type:12s}) in "
            f"{selected_zone} at ${total_price:.6f} per hour "
            f"{' (spot)' if use_spot else '(on demand)'}"
        )

        return selected_price_info

    async def start_instance(
        self,
        *,
        instance_type: str,
        startup_script: str,
        job_id: str,
        use_spot: bool,
        image_uri: str,
        boot_disk_type: str,
        boot_disk_size: int,  # GB
        boot_disk_iops: Optional[int] = None,
        boot_disk_throughput: Optional[int] = None,  # MB/s
        zone: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        Start a new GCP Compute Engine instance.

        Args:
            instance_type: GCP instance type (e.g., 'n1-standard-1')
            startup_script: The startup script
            job_id: Job ID to use for the instance
            use_spot: Whether to use a spot instance
            image_uri: Image URI to use
            boot_disk_type: Boot disk type to use
            boot_disk_size: Boot disk size in GB
            provisioned_iops: Number of provisioned IOPS; must be specified for `pd-extreme` and
                `hd-balanced`
            provisioned_throughput: Amount of provisioned throughput (MB/s); must be specified for
                `hd-balanced`
            zone: Zone to use for the instance; if not specified use the default zone,
                or if none choose a random zone

        Returns:
            A tuple containing the ID of the started instance and the zone it was started
            in
        """
        self._logger.debug(f"Starting new instance with type: {instance_type}, spot: {use_spot}")

        # Get thread-local compute client
        compute_client = self._get_compute_client()

        # Generate a unique name for the instance
        instance_id = f"{self._JOB_ID_TAG_PREFIX}{job_id}-{str(shortuuid.uuid())}"

        if zone is not None and self._zone is not None and zone != self._zone:
            self._logger.debug(f"Overriding default zone {self._zone} with {zone}")
        if zone is None or (zone is not None and zone[-2:] == "-*"):
            random_zone = await self._get_random_zone()
            self._logger.debug(
                f"Zone {zone} for optimal instance type is a wildcard zone, using "
                f"random zone {random_zone}"
            )
            zone = random_zone

        # Encode the startup script as metadata
        # https://cloud.google.com/python/docs/reference/compute/latest/google.cloud.compute_v1.types.Metadata
        metadata = {"items": [{"key": "startup-script", "value": startup_script}]}

        # Prepare the disk configuration
        # https://cloud.google.com/python/docs/reference/compute/latest/google.cloud.compute_v1.types.AttachedDisk

        disk_type_map = {
            "pd-standard": "pd-standard",
            "pd-balanced": "pd-balanced",
            "pd-ssd": "pd-ssd",
            "pd-extreme": "pd-extreme",
            "hd-balanced": "hyperdisk-balanced",
        }

        if boot_disk_type not in disk_type_map:
            raise ValueError(f"Invalid boot disk type: {boot_disk_type}")

        gcp_disk_type = disk_type_map[boot_disk_type]

        disk_config = {
            "boot": True,
            "auto_delete": True,
            "initialize_params": {
                "source_image": image_uri,
                "disk_size_gb": boot_disk_size,
                "disk_type": f"zones/{zone}/diskTypes/{gcp_disk_type}",
            },
        }

        # Add IOPS for pd-extreme and hd-balanced
        if boot_disk_type in ["pd-extreme", "hd-balanced"]:
            if boot_disk_iops is None:
                raise ValueError("boot_disk_iops must be specified for pd-extreme and hd-balanced")
            disk_config["initialize_params"]["provisioned_iops"] = boot_disk_iops

        # Add throughput for hd-balanced
        if boot_disk_type == "hd-balanced":
            if boot_disk_throughput is None:
                raise ValueError("boot_disk_throughput must be specified for hd-balanced")
            disk_config["initialize_params"]["provisioned_throughput"] = boot_disk_throughput

        # Prepare the network interface configuration
        # https://cloud.google.com/python/docs/reference/compute/latest/google.cloud.compute_v1.types.NetworkInterface
        network_interface = {
            "network": "global/networks/default",
            "access_configs": [{"type": "ONE_TO_ONE_NAT", "name": "External NAT"}],
        }

        # Prepare scheduling configuration for preemptible instances
        # https://cloud.google.com/python/docs/reference/compute/latest/google.cloud.compute_v1.types.Scheduling
        scheduling = {}
        if use_spot:
            scheduling = {
                "preemptible": True,
                "automatic_restart": False,
                "on_host_maintenance": "TERMINATE",
            }

        service_accounts = []
        if self._service_account:
            service_accounts.append(
                {
                    "email": self._service_account,
                    # Using the cloud-platform scope allows the instance to access
                    # all GCP services that are available to the service account
                    "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
                }
            )
        else:
            self._logger.warning(
                "While starting instance: No service account specified; "
                "the instance will not have access to any GCP services"
            )

        # Add network tags so we can find these instances later
        tags = {
            "items": [self._job_id_to_tag(job_id)],
        }

        # Prepare the instance configuration
        inst_config = compute_v1.Instance(
            name=instance_id,
            machine_type=f"zones/{zone}/machineTypes/{instance_type}",
            disks=[disk_config],
            network_interfaces=[network_interface],
            metadata=metadata,
            scheduling=scheduling,
            service_accounts=service_accounts,
            tags=tags,
        )

        self._logger.debug(
            f"Creating {'spot' if use_spot else 'on-demand'} instance "
            f"{instance_id} ({instance_type})"
        )

        # Create the instance
        try:
            operation = await asyncio.to_thread(
                compute_client.insert,
                project=self._project_id,
                zone=zone,
                instance_resource=inst_config,
            )

            # Wait for the create operation to complete
            await self._wait_for_operation(operation, f"Creation of instance {instance_id}")
            self._logger.debug(
                f"Instance {instance_id} created successfully ({instance_type} in zone {zone})"
            )
            return instance_id, zone
        except Exception as e:
            self._logger.error(
                f"Failed to create {'spot' if use_spot else 'on-demand'} instance: {e}",
                exc_info=True,
            )
            raise

    async def terminate_instance(self, instance_id: str, zone: Optional[str] = None) -> None:
        """
        Terminate a Compute Engine instance by ID.

        Args:
            instance_id: Instance name
            zone: The zone the instance is in; if not specified use the default zone
        """
        if zone is None:
            zone = self._zone

        if zone is None:
            raise ValueError("Zone is required")

        self._logger.debug(f"Terminating instance {instance_id} in zone {zone}")

        # Get thread-local compute client
        compute_client = self._get_compute_client()

        try:
            operation = await asyncio.to_thread(
                compute_client.delete, project=self._project_id, zone=zone, instance=instance_id
            )

            # Wait for the operation to complete asynchronously
            await self._wait_for_operation(operation, f"Termination of instance {instance_id}")
            self._logger.debug(f"Instance {instance_id} terminated successfully")
        except NotFound:
            self._logger.warning(
                f"Instance {instance_id} not found in project {self._project_id}, zone {zone}"
            )
            raise
        except Exception as e:
            self._logger.error(
                f"Error terminating instance {instance_id} in project {self._project_id}: {e}"
            )
            raise

    async def list_running_instances(
        self, job_id: Optional[str] = None, include_non_job: bool = False
    ) -> List[Dict[str, Any]]:
        """
        List currently running Compute Engine instances, optionally filtered by job_id.

        Args:
            job_id: Job ID to filter instances
            include_non_job: Include instances that do not have a job_id tag

        Returns:
            List of instance dictionaries with id, type, state, creation_time, and zone.

        Raises:
            ValueError: If no zone or region is specified and listing all zones fails
        """
        if job_id:
            self._logger.debug(f"Listing running instances with job_id filter '{job_id}'")
        else:
            self._logger.debug("Listing running instances")

        instances = []

        # If zone is specified, only query that zone
        if self._zone:
            # List instances in the specified zone
            self._logger.debug(f"Listing instances in zone {self._zone}")
            request = compute_v1.ListInstancesRequest(project=self._project_id, zone=self._zone)

            try:
                instances_list = self._get_compute_client().list(request=request)
                instances.extend(
                    self._standardize_instance_data(instances_list, job_id, include_non_job)
                )
            except Exception as e:
                self._logger.error(
                    f"Error listing instances in zone {self._zone}: {e}", exc_info=True
                )
                raise
        else:
            self._logger.debug(f"Listing instances in all zones of region {self._region}")

            # List all zones in the project
            try:
                zones_request = compute_v1.ListZonesRequest(project=self._project_id)
                zones = self._zones_client.list(request=zones_request)

                # Filter zones by region if specified
                filtered_zones = []
                for zone in zones:
                    # Extract region from zone name (e.g., us-central1-a -> us-central1)
                    zone_region = "-".join(zone.name.split("-")[:-1])

                    # Add to filtered list if in target region or no region filter
                    if zone_region == self._region:
                        filtered_zones.append(zone)

                if not filtered_zones:
                    self._logger.warning(f"No zones found in region {self._region}")
                    return []  # Return empty list if no zones match the region filter

                # For each zone, list instances
                for zone in filtered_zones:
                    self._logger.debug(f"Listing instances in zone {zone.name}")
                    request = compute_v1.ListInstancesRequest(
                        project=self._project_id, zone=zone.name
                    )

                    try:
                        instances_list = self._get_compute_client().list(request=request)
                        instances.extend(
                            self._standardize_instance_data(instances_list, job_id, include_non_job)
                        )
                    except Exception as e:
                        self._logger.warning(
                            f"Error listing instances in zone {zone.name}: {e}", exc_info=True
                        )
                        # Continue to next zone instead of failing
                        continue
            except Exception as e:
                error_msg = f"Error listing zones in project {self._project_id}: {e}"
                if "permission" in str(e).lower():
                    error_msg += "\nYou may not have sufficient permissions to list zones."

                self._logger.error(error_msg)
                raise ValueError(error_msg)

        return instances

    def _get_boot_disk_info(self, instance) -> Tuple[str, int, int, int]:
        """
        Retrieves full information about a running instance's boot disk,
        including its type and provisioned IOPS/throughput if applicable.

        Args:
            instance_name: The name of the running instance.

        Returns:
            Tuple of (boot_disk_type, boot_disk_size, boot_disk_iops, boot_disk_throughput)
        """

        boot_disk = None
        for attached_disk in instance.disks:
            if attached_disk.boot:
                boot_disk = attached_disk
                break

        if not boot_disk:
            self._logger.warning(f"No boot disk found for instance '{instance.name}'.")
            return None, None, None, None

        # The 'source' field of the attached_disk is a URL to the actual disk resource.
        # We need to parse it to get the disk name and zone.
        # Example: https://www.googleapis.com/compute/v1/projects/your-project/zones/your-zone/disks/your-disk-name
        disk_url_parts = boot_disk.source.split("/")
        disk_name = disk_url_parts[-1]
        # Zone is two parts before the disk name in the URL
        # The URL structure implies: projects/PROJECT_ID/zones/ZONE/disks/DISK_NAME
        disk_zone = disk_url_parts[-3]

        # Get full disk details
        disk_request = compute_v1.GetDiskRequest(
            project=self._project_id,
            zone=disk_zone,
            disk=disk_name,
        )
        full_disk_details = self._disks_client.get(request=disk_request)

        # Extract human-readable disk type from the URL
        disk_type = full_disk_details.type.split("/")[-1]
        disk_size = full_disk_details.size_gb
        provisioned_iops = full_disk_details.provisioned_iops
        provisioned_throughput = full_disk_details.provisioned_throughput

        return disk_type, disk_size, provisioned_iops, provisioned_throughput

    def _standardize_instance_data(
        self, instances_list, job_id: Optional[str], include_non_job: bool
    ) -> List[Dict[str, Any]]:
        """
        Process the list of GCP instances into a standardized format.

        Args:
            instances_list: List of GCP instance objects

        Returns:
            List of instance dictionaries with standardized fields:
                "id": Instance ID
                "type": Instance type
                "state": Instance state
                "creation_time": Instance creation time
                "zone": Instance zone
                "boot_disk_type": Boot disk type
                "boot_disk_iops": Boot disk IOPS
                "boot_disk_throughput": Boot disk throughput
                "private_ip": Private IP address
                "public_ip": Public IP address
                "job_id": Job ID
        """
        instances = []
        for instance in instances_list:
            # Extract instance type from URL (e.g., ".../machineTypes/n1-standard-1")
            instance_type = instance.machine_type.split("/")[-1]

            instance_info = {
                "id": instance.name,
                "type": instance_type,
                "state": self._STATUS_MAP.get(instance.status, "unknown"),
                "creation_time": instance.creation_timestamp,
                "zone": instance.zone.split("/")[-1],  # Extract zone name from URL
                "boot_disk_type": None,
                "boot_disk_iops": None,
                "boot_disk_throughput": None,
            }

            boot_disk_type, boot_disk_size, boot_disk_iops, boot_disk_throughput = (
                self._get_boot_disk_info(instance)
            )
            disk_type_map = {
                "pd-standard": "pd-standard",
                "pd-balanced": "pd-balanced",
                "pd-ssd": "pd-ssd",
                "pd-extreme": "pd-extreme",
                "hyperdisk-balanced": "hd-balanced",
            }
            instance_info["boot_disk_type"] = disk_type_map.get(boot_disk_type)
            if instance_info["boot_disk_type"] is None:
                self._logger.warning(f"Unknown boot disk type: {boot_disk_type}")
            instance_info["boot_disk_iops"] = boot_disk_iops
            instance_info["boot_disk_throughput"] = boot_disk_throughput

            if instance_info["state"] == "unknown":
                self._logger.error(
                    f"Unknown instance state for instance {instance.name}: {instance.status}"
                )
            if instance.tags and instance.tags.items:
                for tag in instance.tags.items:
                    if tag.startswith(self._JOB_ID_TAG_PREFIX):
                        # black keeps reformatting this line in a way flake8 doesn't like
                        inst_job_id = tag[len(self._JOB_ID_TAG_PREFIX) :]  # noqa
                        if job_id and inst_job_id != job_id:
                            self._logger.debug(
                                f"Skipping instance {instance.name} because it has job_id "
                                f"{inst_job_id}"
                            )
                            break
                        instance_info["job_id"] = inst_job_id
                        break
            if "job_id" not in instance_info and not include_non_job:
                self._logger.debug(
                    f"Skipping instance {instance.name} because it has no job_id tag"
                )
                continue  # Skip if no job_id tag found

            # Add IP addresses if available
            if instance.network_interfaces and len(instance.network_interfaces) > 0:
                # Private IP
                if instance.network_interfaces[0].network_i_p:
                    instance_info["private_ip"] = instance.network_interfaces[0].network_i_p

                # Public IP
                if (
                    instance.network_interfaces[0].access_configs
                    and len(instance.network_interfaces[0].access_configs) > 0
                    and instance.network_interfaces[0].access_configs[0].nat_i_p
                ):
                    instance_info["public_ip"] = (
                        instance.network_interfaces[0].access_configs[0].nat_i_p
                    )

            instances.append(instance_info)

        return instances

    async def list_available_images(self) -> List[Dict[str, Any]]:
        """
        List available VM images in GCP.
        Returns common public OS images and the user's own custom images.

        Returns:
            List of dictionaries with image information. The dictionaries have the following keys::
                "id": Image ID
                "name": Image name
                "description": Image description
                "family": Image family
                "creation_date": Image creation date
                "source": Source of the image (GCP or User)
                "project": Project the image belongs to
                "self_link": Image self link
                "status": Image status
        """
        self._logger.info("Listing available Compute Engine images")

        # List of common public OS image projects
        public_projects = [
            "cos-cloud",
            "debian-cloud",
            "rocky-linux-cloud",
            "ubuntu-os-cloud",
            "centos-cloud",
            "fedora-coreos-cloud",
            "opensuse-cloud",
            "oracle-linux-cloud",
            "rhel-cloud",
            "rhel-sap-cloud",
            "suse-cloud",
            "suse-sap-cloud",
            "ubuntu-os-pro-cloud",
            # "windows-cloud",
            # "windows-sql-cloud",
        ]

        # Dictionary to store all images
        all_images = []

        # Get public images from standard projects
        for project in public_projects:
            try:
                # Don't use a filter in the API request, as it's causing issues
                # We'll filter the results programmatically instead
                request = compute_v1.ListImagesRequest(project=project)

                self._logger.debug(f"Fetching images from {project}")
                images = list(self._images_client.list(request=request))

                # Filter out deprecated images in the code instead of in the API query
                filtered_images = []
                for image in images:
                    # Include image if it's not deprecated or obsolete
                    if (
                        not hasattr(image, "deprecated")
                        or not image.deprecated
                        or (
                            image.deprecated.state != "DEPRECATED"
                            and image.deprecated.state != "OBSOLETE"
                        )
                    ):
                        filtered_images.append(image)

                # Group filtered images by family
                family_images: Dict[str, List[compute_v1.Image]] = {}
                for image in filtered_images:
                    if image.family:
                        if image.family not in family_images:
                            family_images[image.family] = []
                        family_images[image.family].append(image)

                # Get the newest image from each family
                for family, family_imgs in family_images.items():
                    # Sort by creation timestamp (newest first)
                    family_imgs.sort(key=lambda x: x.creation_timestamp, reverse=True)
                    newest_image = family_imgs[0]

                    all_images.append(
                        {
                            "id": newest_image.id,
                            "name": newest_image.name,
                            "description": newest_image.description,
                            "family": newest_image.family,
                            "creation_date": newest_image.creation_timestamp,
                            "source": "GCP",
                            "project": project,
                            "self_link": newest_image.self_link,
                            "status": newest_image.status,
                        }
                    )

            except Exception as e:
                self._logger.warning(f"Error fetching images from {project}: {e}")
                continue

        # Get user's own custom images
        try:
            request = compute_v1.ListImagesRequest(
                project=self._project_id,
            )

            self._logger.debug(f"Fetching custom images from project {self._project_id}")
            image_list = list(self._images_client.list(request=request))

            # Process each image
            for image in image_list:
                all_images.append(
                    {
                        "id": image.id,
                        "name": image.name,
                        "description": image.description,
                        "family": image.family,
                        "creation_date": image.creation_timestamp,
                        "source": "User",
                        "project": self._project_id,
                        "self_link": image.self_link,
                        "status": image.status,
                    }
                )

        except Exception as e:
            self._logger.warning(
                f"Error fetching custom images from project {self._project_id}: {e}"
            )

        # Sort by creation date
        all_images.sort(key=lambda x: x.get("creation_date", ""), reverse=True)  # type: ignore

        self._logger.debug(f"Found {len(all_images)} available images")

        return all_images

    async def get_image_from_family(self, family_name: str) -> str | None:
        """
        Get the latest image from a specific family.

        Args:
            family_name: Image family name

        Returns:
            Image URI
        """
        self._logger.debug(f"Retrieving latest image from family {family_name}")

        images = await self.list_available_images()
        ret_image = None
        for image in images:
            if image["family"] == family_name:
                if ret_image is not None:
                    raise ValueError(
                        f"Multiple images found for family {family_name}: "
                        f"{ret_image['name']} and {image['name']}"
                    )
                ret_image = image["self_link"]
        return ret_image

    async def get_default_image(self) -> str | None:
        """
        Get the latest Ubuntu 24.04 LTS image for Compute Engine.

        Returns:
            Image URI
        """
        self._logger.debug("Retrieving latest Ubuntu 24.04 LTS image")

        image = self._images_client.get_from_family(
            project="ubuntu-os-cloud", family="ubuntu-2404-lts-amd64"
        )

        if image is None:
            return None

        self._logger.debug(f"Found image: {image.name}, created on {image.creation_timestamp}")
        return image.self_link

    async def _wait_for_operation(self, operation, verbose_name: str) -> Any:
        """
        Wait for a Compute Engine operation to complete.

        Args:
            operation_name: Name of the operation
        """
        self._logger.debug(f"Waiting for operation {operation.name} to complete")

        result = operation.result(timeout=self._DEFAULT_OPERATION_TIMEOUT)

        if operation.error_code:
            self._logger.error(
                f"Error during {verbose_name}: [Code: {operation.error_code}]: {operation.error_message}"
            )
            self._logger.error(f"Operation ID: {operation.name}")
            raise operation.exception() or RuntimeError(operation.error_message)

        if operation.warnings:
            self._logger.warning(f"Warnings during {verbose_name}:")
            for warning in operation.warnings:
                self._logger.warning(f" - {warning.code}: {warning.message}")

        self._logger.debug(f"Operation {operation.name} completed with result: {result}")
        return result

    async def get_available_regions(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        """
        Return all available GCP regions and their attributes.

        Args:
            prefix: Optional prefix to filter regions by name

        Returns:
            Dictionary of region names mapped to their information:
            - name: Region name (e.g., 'us-central1')
            - description: Region description
            - endpoint: Region endpoint URL
            - status: Region status
            - zones: List of availability zones in the region
        """
        self._logger.debug("Finding available GCP regions")

        # Create the request to list regions
        request = compute_v1.ListRegionsRequest(project=self._project_id)
        regions = self._regions_client.list(request=request)

        region_dict = {}
        for region in regions:
            if prefix and not region.name.startswith(prefix):
                continue

            # Extract zone names from the zone URLs
            zone_names = [zone.split("/")[-1] for zone in region.zones]

            # Construct the endpoint URL for the region
            endpoint = f"https://{region.name}-compute.googleapis.com"

            region_info = {
                "name": region.name,
                "description": region.description,
                "endpoint": endpoint,
                "status": region.status,
                "zones": zone_names,
            }
            region_dict[region.name] = region_info

        self._logger.debug(
            f"Found {len(region_dict)} available regions: {', '.join(sorted(region_dict.keys()))}"
        )
        return region_dict

    async def _get_default_zone(self, region: Optional[str] = None) -> str:
        """
        Get the default zone for the region.

        Args:
            region: Region to get the default zone for; if not specified use the default region

        Returns:
            The default zone for the region
        """
        if self._zone:
            self._logger.debug(f"Using specified zone {self._zone}")
            return self._zone

        if region is None:
            region = self._region

        if region is None:
            raise RuntimeError("Region or zone must be specified")

        self._logger.debug(f"Getting default zone for region {region}")

        # Create the request to list zones
        request = compute_v1.ListZonesRequest(
            project=self._project_id, filter=f"name eq {region}-.*"
        )
        zones = list(self._zones_client.list(request=request))

        if len(zones) == 0:
            raise ValueError(f"No zones found for region {region}")

        # Get the first zone in the list
        self._logger.debug(f"Default zone is {zones[0].name}")
        return zones[0].name

    async def _get_random_zone(self, region: Optional[str] = None) -> str:
        """
        Get a random zone for the region.

        Args:
            region: Region to get a random zone for; if not specified use the default region

        Returns:
            A random zone for the region
        """
        if self._zone:
            self._logger.debug(f"Using specified zone {self._zone}")
            return self._zone

        if region is None:
            region = self._region

        self._logger.debug(f"Getting default zone for region {region}")

        # Create the request to list zones
        request = compute_v1.ListZonesRequest(
            project=self._project_id, filter=f"name eq {region}-.*"
        )
        zones = list(self._zones_client.list(request=request))

        if len(zones) == 0:
            raise ValueError(f"No zones found for region {region}")

        # Get the first zone in the list
        self._logger.debug(f"Default zone is {zones[0].name}")
        return zones[random.randint(0, len(zones) - 1)].name
