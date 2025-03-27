"""
Google Cloud Compute Engine implementation of the InstanceManager interface.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
import uuid

from google.api_core.exceptions import NotFound  # type: ignore
from google.auth import default as get_default_credentials
from google.cloud import billing, compute_v1  # type: ignore
from google.oauth2 import service_account

from cloud_tasks.common.config import GCPConfig

from .instance_manager import InstanceManager


class GCPComputeInstanceManager(InstanceManager):
    """Google Cloud Compute Engine implementation of the InstanceManager interface."""

    _DEFAULT_REGION = "us-central1"
    _DEFAULT_ZONE = "us-central1-a"

    # Map of instance statuses to standardized statuses
    STATUS_MAP = {
        "PROVISIONING": "starting",
        "STAGING": "starting",
        "RUNNING": "running",
        "STOPPING": "stopping",
        "TERMINATED": "terminated",
        "SUSPENDED": "stopped",
    }

    def __init__(self, gcp_config: GCPConfig):
        """Initialize the GCP Compute Engine instance manager.

        Args:
            config: Dictionary with GCP configuration. If credentials_file is not provided,
                   the default application credentials from the environment will be used.

        Raises:
            ValueError: If required configuration is missing
        """
        super().__init__(gcp_config)
        self._compute_client = None
        self._billing_client = None
        self._billing_compute_skus = None
        self._instance_pricing_cache = {}
        self._region = None
        self._zone = None
        self._credentials = None
        self._logger = logging.getLogger(__name__)

        self._logger.info(f"Initializing GCP Compute Engine instance manager")

        self._project_id = gcp_config.project_id

        # Store instance_types configuration if present
        self._instance_types = gcp_config.instance_types
        if self._instance_types:
            if isinstance(self._instance_types, str):
                # If a single string was provided, convert to a list
                self._instance_types = [self._instance_types]
            self._logger.info(f"Instance types restricted to patterns: {self._instance_types}")

        # Handle credentials - use provided service account file or default application credentials
        if gcp_config.credentials_file is not None:
            try:
                self._credentials = service_account.Credentials.from_service_account_file(
                    gcp_config.credentials_file
                )
                self._logger.info(f"Using credentials from file: {gcp_config.credentials_file}")
            except Exception as e:
                self._logger.error(
                    f"Error loading credentials file: {gcp_config.credentials_file}: {e}"
                )
                raise ValueError(
                    f"Error loading credentials file: {gcp_config.credentials_file}: {e}"
                )
        else:
            # Use default credentials from environment
            try:
                self._credentials, project_id = get_default_credentials()
                self._logger.info("Using default application credentials from environment")
                if not self._project_id and project_id:
                    self._project_id = project_id
                    self._logger.info(
                        f"Using project ID from default credentials: {self._project_id}"
                    )
            except Exception as e:
                raise ValueError(
                    f"Error getting default credentials from environment: {e}. "
                    "Please ensure you're authenticated with 'gcloud auth application-default login' "
                    "or provide a credentials_file."
                )

        if self._project_id is None:
            raise ValueError("Missing required GCP configuration: project_id")

        # Initialize region/zone from config if provided
        self._region = gcp_config.region
        self._zone = gcp_config.zone

        # If zone is provided but not region, extract region from zone
        if self._zone and not self._region:
            # Extract region from zone (e.g., us-central1-a -> us-central1)
            self._region = "-".join(self._zone.split("-")[:-1])
            self._logger.info(f"Extracted region {self._region} from zone {self._zone}")
        elif self._region and not self._zone:
            # Extract zone from region (e.g., us-central1 -> us-central1-a)
            self._zone = "-".join(self._region.split("-") + ["a"])
            self._logger.info(f"Extracted zone {self._zone} from region {self._region}")

        # Initialize clients
        self._compute_client = compute_v1.InstancesClient(credentials=self._credentials)
        self._zones_client = compute_v1.ZonesClient(credentials=self._credentials)
        self._regions_client = compute_v1.RegionsClient(credentials=self._credentials)
        self._machine_type_client = compute_v1.MachineTypesClient(credentials=self._credentials)
        self._images_client = compute_v1.ImagesClient(credentials=self._credentials)
        self._billing_client = billing.CloudCatalogClient(credentials=self._credentials)

        # Log project and region info
        if self._region:
            self._logger.info(
                f"Initialized GCP Compute Engine: project '{self._project_id}', "
                f"region '{self._region}', zone '{self._zone}'"
            )
        else:
            self._logger.info(f"Initialized GCP Compute Engine: project '{self._project_id}'")
            self._logger.warning(
                "No region specified, will determine cheapest region during instance selection"
            )

    async def find_cheapest_region(self, instance_type: str = "n1-standard-1") -> Dict[str, str]:
        """
        Find the cheapest GCP region and zone for the given instance type.

        Args:
            instance_type: instance type to check prices for (default: n1-standard-1)

        Returns:
            Dictionary with 'region' and 'zone' keys
        """
        try:
            # Get all available regions
            request = compute_v1.ListRegionsRequest(project=self._project_id)
            regions = self._regions_client.list(request=request)
            region_names = [region.name for region in regions]

            self._logger.info(
                f"Checking prices across {len(region_names)} regions for {instance_type}"
            )

            region_prices = {}
            for region_name in region_names:
                try:
                    # Get zones in this region
                    zones_request = compute_v1.ListZonesRequest(
                        project=self._project_id, filter=f"name eq {region_name}-.*"
                    )
                    zones = self._zones_client.list(request=zones_request)
                    zone_names = [zone.name for zone in zones]

                    if not zone_names:
                        continue

                    # Choose the first zone in the region for pricing check
                    zone = zone_names[0]

                    # Get pricing from Cloud Billing Catalog API
                    service_name = "compute.googleapis.com"

                    # Make pricing request
                    request = billing.ListServicesRequest()
                    services = self._billing_client.list_services(request=request)

                    # Find compute service
                    compute_service = None
                    for service in services:
                        if service.name.endswith(service_name):
                            compute_service = service
                            break

                    if compute_service:
                        # Get SKUs for the service
                        sku_request = billing.ListSkusRequest(parent=compute_service.name)
                        skus = self._billing_client.list_skus(request=sku_request)

                        # Find matching SKU for the instance type in this region
                        price = None
                        for sku in skus:
                            if (
                                instance_type in sku.description
                                and "Core" in sku.description
                                and region_name in sku.service_regions
                            ):
                                # Get the price in USD
                                for pricing_info in sku.pricing_info:
                                    for (
                                        price_by_currency
                                    ) in pricing_info.pricing_expression.tiered_rates[
                                        0
                                    ].unit_price.currency_code:
                                        if price_by_currency == "USD":
                                            price = (
                                                pricing_info.pricing_expression.tiered_rates[
                                                    0
                                                ].unit_price.nanos
                                                / 1e9
                                            )
                                            region_prices[region_name] = {
                                                "price": price,
                                                "zone": zone,
                                            }
                                            self._logger.info(
                                                f"  {region_name} ({zone}): ${price:.6f}/hour"
                                            )
                                            break
                                    if price:
                                        break
                            if price:
                                break

                except Exception as e:
                    self._logger.warning(f"  Error getting price for {region_name}: {e}")
                    continue

            if not region_prices:
                self._logger.warning(
                    "Could not retrieve prices for any region, using us-central1 as default"
                )
                return {"region": self._DEFAULT_REGION, "zone": self._DEFAULT_ZONE}

            # Find the cheapest region
            cheapest_region = min(region_prices.items(), key=lambda x: x[1]["price"])[0]
            cheapest_zone = region_prices[cheapest_region]["zone"]
            cheapest_price = region_prices[cheapest_region]["price"]

            self._logger.info(
                f"Cheapest region is {cheapest_region} ({cheapest_zone}) at ${cheapest_price:.6f}/hour"
            )
            return {"region": cheapest_region, "zone": cheapest_zone}

        except Exception as e:
            self._logger.error(f"Error finding cheapest region: {e}")
            self._logger.info("Using us-central1 as default region")
            return {"region": "us-central1", "zone": "us-central1-a"}

    async def get_optimal_instance_type(
        self,
        cpu_required: int,
        memory_required_gb: int,
        disk_required_gb: int,
        use_spot: bool = False,
    ) -> str:
        """
        Get the most cost-effective GCP instance type that meets requirements.
        If no region was specified during initialization, this method will also
        find and use the cheapest region.

        Args:
            cpu_required: Minimum number of vCPUs
            memory_required_gb: Minimum memory in GB
            disk_required_gb: Minimum disk space in GB
            use_spot: Whether to use preemptible instances (GCP equivalent of spot)

        Returns:
            GCP instance type name (e.g., 'n1-standard-2')
        """
        self._logger.info(
            f"Finding optimal instance type with: CPU={cpu_required}, Memory={memory_required_gb}GB, "
            f"Disk={disk_required_gb}GB, Spot/Preemptible={use_spot}"
        )

        # If no region/zone was specified, find the cheapest one
        if not self._region or not self._zone:
            self._logger.info("No region specified, searching for cheapest region...")
            region_info = await self.find_cheapest_region()
            self._region = region_info["region"]
            self._zone = region_info["zone"]
            self._logger.info(f"Selected region {self._region}, zone {self._zone} for lowest cost")
        else:
            self._logger.info(f"Using specified region {self._region}, zone {self._zone}")

        # Get available instance types
        instance_types = await self.list_available_instance_types()
        self._logger.debug(
            f"Found {len(instance_types)} available instance types in zone {self._zone}"
        )

        # Filter to instance types that meet requirements
        eligible_types = []
        for machine in instance_types:
            if machine["vcpu"] >= cpu_required and machine["memory_gb"] >= memory_required_gb:
                # We don't filter on disk since persistent disks can be attached
                eligible_types.append(machine)

        self._logger.debug(f"Found {len(eligible_types)} instance types that meet requirements:")
        for idx, machine in enumerate(eligible_types):
            self._logger.debug(
                f"  [{idx+1}] {machine['name']}: {machine['vcpu']} vCPU, {machine['memory_gb']:.2f} GB memory"
            )

        # Filter by instance_types if specified in configuration
        if self._instance_types:
            filtered_types = []
            for machine in eligible_types:
                machine_name = machine["name"]
                # Check if machine matches any prefix or exact name
                for pattern in self._instance_types:
                    if machine_name.startswith(pattern) or machine_name == pattern:
                        filtered_types.append(machine)
                        break

            # Update eligible types with filtered list
            if filtered_types:
                eligible_types = filtered_types
                self._logger.debug(
                    f"Filtered to {len(eligible_types)} instance types based on instance_types configuration:"
                )
                for idx, machine in enumerate(eligible_types):
                    self._logger.debug(
                        f"  [{idx+1}] {machine['name']}: {machine['vcpu']} vCPU, {machine['memory_gb']:.2f} GB memory"
                    )
            else:
                error_msg = f"No machines match the instance_types patterns: {self._instance_types}. Available instance types meeting requirements: {[m['name'] for m in eligible_types]}"
                self._logger.error(error_msg)
                raise ValueError(error_msg)

        if not eligible_types:
            raise ValueError(
                f"No instance type meets requirements: {cpu_required} vCPU, "
                f"{memory_required_gb} GB memory"
            )

        # Use GCP Cloud Catalog API to get current prices
        pricing_data = {}
        self._logger.debug(
            f"Retrieving pricing data for {len(eligible_types)} eligible instance types..."
        )

        for machine in eligible_types:
            instance_type = machine["name"]
            self._logger.debug(f"Getting pricing for instance type: {instance_type}")

            try:
                # Get pricing from Cloud Billing Catalog API
                service_name = "compute.googleapis.com"

                # Make pricing request
                request = billing.ListServicesRequest()
                services = self._billing_client.list_services(request=request)

                # Find compute service
                compute_service = None
                for service in services:
                    if service.name.endswith(service_name):
                        compute_service = service
                        break

                if compute_service:
                    self._logger.debug(f"Found compute service: {compute_service.display_name}")
                    # Get SKUs for the service
                    sku_request = billing.ListSkusRequest(parent=compute_service.name)
                    skus = self._billing_client.list_skus(request=sku_request)

                    # Find matching SKU for the instance type in this region
                    for sku in skus:
                        price_found = False
                        sku_description = sku.description.lower()

                        machine_name_parts = instance_type.split("-")
                        machine_family = machine_name_parts[0]

                        # Debug log for SKU matching
                        sku_match = (
                            machine_family in sku_description
                            and (
                                ("core" in sku_description and not use_spot)
                                or ("preemptible" in sku_description and use_spot)
                            )
                            and self._region in sku.service_regions
                        )

                        if sku_match:
                            self._logger.debug(
                                f"Matching SKU found: {sku.description} in region {self._region}"
                            )

                        # Check if this SKU matches our instance type and region
                        if sku_match:
                            # Extract price info
                            for pricing_info in sku.pricing_info:
                                for tier in pricing_info.pricing_expression.tiered_rates:
                                    unit_price = tier.unit_price
                                    # Convert to USD dollars
                                    price = unit_price.units + (unit_price.nanos / 1e9)

                                    self._logger.debug(
                                        f"  Price: ${price:.6f} per {pricing_info.pricing_expression.usage_unit}"
                                    )

                                    # Store price for this instance type
                                    if (
                                        instance_type not in pricing_data
                                        or price < pricing_data[instance_type]
                                    ):
                                        pricing_data[instance_type] = price
                                        price_found = True
                                        break

                                if price_found:
                                    break

            except Exception as e:
                self._logger.warning(f"Error getting pricing for {instance_type}: {e}")
                continue

        # Log the complete pricing data found
        if pricing_data:
            self._logger.debug("Retrieved pricing data for the following instance types:")
            for instance_type, price in pricing_data.items():
                self._logger.debug(f"  {instance_type}: ${price:.6f} per hour")
        else:
            self._logger.warning("Could not retrieve any pricing data from GCP API")

        # If we couldn't get pricing from API, fall back to our heuristic
        if not pricing_data:
            self._logger.warning(
                "Could not get pricing data from GCP API, falling back to heuristic"
            )
            # Sort by vCPU + memory as a simple cost heuristic
            eligible_types.sort(key=lambda x: x["vcpu"] + x["memory_gb"])
            selected_type = eligible_types[0]["name"]
            self._logger.info(f"Selected {selected_type} based on heuristic (lowest vCPU + memory)")
            return selected_type

        # Select instance with the lowest price
        priced_instances = [(instance_type, price) for instance_type, price in pricing_data.items()]
        if not priced_instances:
            self._logger.warning(
                "No pricing data found for eligible instance types, falling back to heuristic"
            )
            eligible_types.sort(key=lambda x: x["vcpu"] + x["memory_gb"])
            selected_type = eligible_types[0]["name"]
            self._logger.info(f"Selected {selected_type} based on heuristic (lowest vCPU + memory)")
            return selected_type

        priced_instances.sort(key=lambda x: x[1])  # Sort by price

        # Debug log for all priced instances in order
        self._logger.debug("instance types sorted by price (cheapest first):")
        for i, (instance_type, price) in enumerate(priced_instances):
            self._logger.debug(f"  {i+1}. {instance_type}: ${price:.6f}/hour")

        selected_type = priced_instances[0][0]
        price = priced_instances[0][1]
        self._logger.info(
            f"Selected {selected_type} at ${price:.6f} per hour in {self._region} ({self._zone}){' (spot/preemptible)' if use_spot else ''}"
        )
        return selected_type

    async def list_available_instance_types(self) -> List[Dict[str, Any]]:
        """
        List available Compute Engine instance types with their specifications.

        Returns:
            List of dictionaries with instance types and their specifications:
                "name": instance type name
                "vcpu": number of vCPUs
                "ram_gb": amount of RAM in GB
                "architecture": architecture of the instance type
                "storage_gb": amount of storage in GB
                "supports_spot": whether the instance type supports spot pricing
                "description": description of the instance type
                "url": URL to the instance type details
        """
        self._logger.debug("Listing available Compute Engine instance types")

        # Ensure we have a zone
        zone = await self._get_default_zone()

        # List instance types in the zone
        request = compute_v1.ListMachineTypesRequest(project=self._project_id, zone=zone)

        machine_types = self._machine_type_client.list(request=request)

        instance_types = []
        for machine_type in machine_types:
            instance_info = {
                "name": machine_type.name,
                "vcpu": machine_type.guest_cpus,
                "ram_gb": machine_type.memory_mb / 1024.0,
                "architecture": machine_type.architecture,
                "storage_gb": 0,  # GCP separates storage from instance type
                "supports_spot": True,  # TODO: Check if this is correct
                "description": machine_type.description,
                # https://www.googleapis.com/compute/v1/projects/[project]/zones/
                # [zone]/machineTypes/[name]
                "url": machine_type.self_link,
            }

            instance_types.append(instance_info)

        return instance_types

    async def _get_billing_compute_skus(self) -> List[billing.Sku]:
        """Get the billing compute SKUs."""
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
            self._logger.warning("Could not find compute service in billing catalog")
            return None

        self._logger.debug(f"Found compute service: {compute_service.display_name}")

        # Get SKUs for the service
        sku_request = billing.ListSkusRequest(parent=compute_service.name)
        self._billing_compute_skus = list(self._billing_client.list_skus(request=sku_request))

        return self._billing_compute_skus

    async def get_instance_pricing(
        self, instance_type: List[str] | str, use_spot: bool = False
    ) -> Dict[str, Dict[str, float | None]]:
        """
        Get the hourly price for one or more specific instance types.

        Note that GCP pricing is per-region for both on-demand and spot instances.

        Args:
            instance_type: The instance type name (e.g., 'n1-standard-1') or a list of instance
                type names.
            use_spot: Whether to use spot/preemptible pricing

        Returns:
            A dictionary mapping instance type to a dictionary of hourly price in USD as a tuple of
            (cpu_price, per_cpu_price, ram_price, ram_per_gb_price, total_price) keyed by
            availability zone. If any price is not available, it is set to None.
        """
        if isinstance(instance_type, str):
            instance_type = [instance_type]

        self._logger.debug(
            f"Getting pricing for {len(instance_type)} instance types (spot: {use_spot})"
        )

        ret = {}

        if len(instance_type) == 0:
            self._logger.warning("No instance types provided")
            return ret

        for machine_type in instance_type:
            self._logger.debug(
                f"Getting pricing for instance type: {machine_type} (spot: {use_spot})"
            )

            # Extract the machine family from the machine type name
            machine_name_parts = machine_type.split("-")
            machine_family = machine_name_parts[0]

            if (machine_family, use_spot) in self._instance_pricing_cache:
                ret[machine_type] = self._instance_pricing_cache[(machine_family, use_spot)]
                continue

            core_sku = None
            ram_sku = None
            # Save None to mark we tried, even if we don't eventually find a match
            self._instance_pricing_cache[(machine_family, use_spot)] = None

            # Find matching SKU for the instance type in this region
            for sku in await self._get_billing_compute_skus():
                sku_description = sku.description.lower()
                # Check if this SKU matches our instance type and region
                if (
                    f"{machine_family} " in sku_description
                    and (
                        ("preemptible" not in sku_description and not use_spot)
                        or ("preemptible" in sku_description and use_spot)
                    )
                    and "custom" not in sku_description
                    and self._region in sku.service_regions
                ):
                    if "instance core" in sku_description:
                        core_sku = sku
                    elif "instance ram" in sku_description:
                        ram_sku = sku

            if core_sku is None:
                self._logger.warning(
                    f"No core SKU found for {machine_family} in region {self._region}"
                )
                ret[machine_type] = None
                continue
            if ram_sku is None:
                self._logger.warning(
                    f"No ram SKU found for {machine_family} in region {self._region}"
                )
                ret[machine_type] = None
                continue

            self._logger.debug(
                f'Matching core SKU found: "{core_sku.description}" in region {self._region}'
            )
            self._logger.debug(
                f'Matching  ram SKU found: "{ram_sku.description}" in region {self._region}'
            )

            # Extract price info - we do it this weird way because the pricing info isn't subscriptable
            core_pricing_info = list(core_sku.pricing_info)
            if len(core_pricing_info) == 0:
                self._logger.warning(f'No pricing info found for core SKU "{core_sku.description}"')
                ret[machine_type] = None
                continue
            core_pricing_tier = list(core_pricing_info[0].pricing_expression.tiered_rates)
            if len(core_pricing_tier) == 0:
                self._logger.warning(f'No tiered rates found for core SKU "{core_sku.description}"')
                ret[machine_type] = None
                continue
            if len(core_pricing_tier) > 1:
                self._logger.warning(
                    f'Multiple tiered rates found for core SKU "{core_sku.description}"'
                )
                ret[machine_type] = None
                continue
            ram_pricing_info = list(ram_sku.pricing_info)
            if len(ram_pricing_info) == 0:
                self._logger.warning(f'No pricing info found for ram SKU "{ram_sku.description}"')
                ret[machine_type] = None
                continue
            ram_pricing_tier = list(ram_pricing_info[0].pricing_expression.tiered_rates)
            if len(ram_pricing_tier) == 0:
                self._logger.warning(f'No tiered rates found for ram SKU "{ram_sku.description}"')
                ret[machine_type] = None
                continue
            if len(ram_pricing_tier) > 1:
                self._logger.warning(
                    f'Multiple tiered rates found for ram SKU "{ram_sku.description}"'
                )
                ret[machine_type] = None
                continue

            core_pricing_info = core_pricing_tier[0]
            ram_pricing_info = ram_pricing_tier[0]

            core_price = core_pricing_info.unit_price.nanos / 1e9
            ram_price = ram_pricing_info.unit_price.nanos / 1e9

            self._logger.debug(f"Core price: ${core_price:.6f}/vCPU/hour")
            self._logger.debug(f"Ram price:  ${ram_price:.6f}/GB/hour")

            ret_val = {
                f"{self._region}-*": {
                    "cpu_price": None,  # CPU price (we don't have this)
                    "per_cpu_price": core_price,  # Per-CPU price
                    "ram_price": None,  # Memory price (we don't have this)
                    "ram_per_gb_price": ram_price,  # Per-GB price
                    "total_price": None,  # Total price
                }
            }
            self._instance_pricing_cache[(machine_family, use_spot)] = ret_val
            ret[machine_type] = ret_val

        return ret

    async def start_instance(
        self,
        instance_type: str,
        startup_script: str,
        labels: Dict[str, str],
        use_spot: bool = False,
        custom_image: Optional[str] = None,
    ) -> str:
        """
        Start a new GCP Compute Engine instance.

        Args:
            instance_type: GCP instance type (e.g., 'n1-standard-1')
            startup_script: Base64-encoded startup script
            labels: Dictionary of labels to apply to the instance
            use_spot: Whether to use a preemptible VM (cheaper but can be terminated)
            custom_image: Custom image to use (if provided)

        Returns:
            ID of the started instance
        """
        if not self._compute_client:
            raise RuntimeError("GCP Compute Engine client not initialized")

        self._logger.info(
            f"Starting new instance with type: {instance_type}, spot/preemptible: {use_spot}"
        )

        # Generate a unique name for the instance
        instance_id = f"{labels.get('job_id', 'job')}-{str(uuid.uuid4())[:8]}"

        # Encode the startup script as metadata
        metadata = {"items": [{"key": "startup-script", "value": startup_script}]}

        # Get image - either custom or default
        if custom_image:
            self._logger.info(f"Using custom image: {custom_image}")
            # If it's a full URI, use it directly
            if custom_image.startswith("https://") or "/" in custom_image:
                source_image = custom_image
            else:
                # Assuming it's a family name in ubuntu-os-cloud
                source_image = await self._get_image_from_family(custom_image)
        else:
            # Get default Ubuntu 24.04 LTS image
            source_image = await self._get_default_image()

        # Prepare the disk configuration
        disk_config = {
            "boot": True,
            "auto_delete": True,
            "initialize_params": {
                "source_image": source_image,
                "disk_size_gb": 10,  # Default size, can be increased as needed
            },
        }

        # Prepare the network interface configuration
        network_interface = {
            "network": "global/networks/default",
            "access_configs": [{"type": "ONE_TO_ONE_NAT", "name": "External NAT"}],
        }

        # Prepare scheduling configuration for preemptible instances
        scheduling = {}
        if use_spot:
            scheduling = {
                "preemptible": True,
                "automatic_restart": False,
                "on_host_maintenance": "TERMINATE",
            }

        # Prepare the instance configuration
        config = {
            "name": instance_id,
            "instance_type": f"zones/{self._zone}/machineTypes/{instance_type}",
            "disks": [disk_config],
            "network_interfaces": [network_interface],
            "metadata": metadata,
            "labels": labels,
            "scheduling": scheduling,
        }

        # Create the instance
        try:
            operation = await asyncio.to_thread(
                self._compute_client.insert,
                project=self._project_id,
                zone=self._zone,
                instance_resource=config,
            )

            # Wait for the create operation to complete
            self._logger.info(
                f"Creating {'spot/preemptible' if use_spot else 'standard'} instance {config['name']} ({instance_type})"
            )
            await self._wait_for_operation(operation["name"])
            self._logger.info(f"Instance {config['name']} created successfully")
            return config["name"]
        except Exception as e:
            self._logger.error(
                f"Failed to create {'spot/preemptible' if use_spot else 'standard'} instance: {e}"
            )
            raise

    async def terminate_instance(self, instance_id: str) -> None:
        """
        Terminate a Compute Engine instance by ID.

        Args:
            instance_id: Instance name
        """
        self._logger.info(
            f"Terminating instance {instance_id} in project {self._project_id}, zone {self._zone}"
        )

        try:
            operation = self._compute_client.delete(
                project=self._project_id, zone=self._zone, instance=instance_id
            )

            # Wait for the operation to complete asynchronously
            await self._wait_for_operation(operation.name)
            self._logger.info(f"Instance {instance_id} terminated successfully")
        except NotFound:
            self._logger.warning(
                f"Instance {instance_id} not found in project {self._project_id}, zone {self._zone}"
            )
        except Exception as e:
            self._logger.error(
                f"Error terminating instance {instance_id} in project {self._project_id}: {e}"
            )
            raise

    async def list_running_instances(
        self, tag_filter: Optional[Dict[str, str]] = None, region: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List currently running Compute Engine instances, optionally filtered by labels.

        Args:
            tag_filter: Dictionary of labels to filter instances
            region: Optional region to filter instances by

        Returns:
            List of instance dictionaries with id, type, state, and creation_time

        Raises:
            ValueError: If no zone or region is specified and listing all zones fails
        """
        # Build filter string if tags are provided
        filter_str = ""
        if tag_filter:
            filters = []
            for key, value in tag_filter.items():
                filters.append(f"labels.{key}={value}")
            filter_str = " AND ".join(filters)

        instances = []

        # If zone is specified, only query that zone
        if self._zone:
            # List instances in the specified zone
            self._logger.info(f"Listing instances in zone {self._zone}")
            request = compute_v1.ListInstancesRequest(
                project=self._project_id, zone=self._zone, filter=filter_str
            )

            try:
                instances_list = self._compute_client.list(request=request)
                instances.extend(self._process_instances(instances_list))
            except Exception as e:
                self._logger.error(f"Error listing instances in zone {self._zone}: {e}")
                raise
        else:
            # Use region parameter if provided
            target_region = region or self._region

            if target_region:
                self._logger.info(f"Listing instances in all zones of region {target_region}")
            else:
                self._logger.warning(
                    "No zone or region specified. Attempting to list instances across all regions."
                )
                self._logger.warning(
                    "This may fail if you don't have permissions or if the project has many zones."
                )
                self._logger.warning(
                    "Consider specifying --region or adding a zone in your config file."
                )

            # List all zones in the project
            try:
                zones_request = compute_v1.ListZonesRequest(project=self._project_id)
                zones_client = compute_v1.ZonesClient(credentials=self._credentials)
                zones = zones_client.list(request=zones_request)

                # Filter zones by region if specified
                filtered_zones = []
                for zone in zones:
                    # Extract region from zone name (e.g., us-central1-a -> us-central1)
                    zone_region = "-".join(zone.name.split("-")[:-1])

                    # Add to filtered list if in target region or no region filter
                    if not target_region or zone_region == target_region:
                        filtered_zones.append(zone)

                if target_region and not filtered_zones:
                    self._logger.warning(f"No zones found in region {target_region}")
                    return []  # Return empty list if no zones match the region filter

                if not filtered_zones:
                    self._logger.error("No zones found in the project")
                    raise ValueError(
                        "No zones found in the project. Please check your permissions or specify a region/zone."
                    )

                # For each zone, list instances
                for zone in filtered_zones:
                    self._logger.debug(f"Listing instances in zone {zone.name}")
                    request = compute_v1.ListInstancesRequest(
                        project=self._project_id, zone=zone.name, filter=filter_str
                    )

                    try:
                        instances_list = self._compute_client.list(request=request)
                        instances.extend(self._process_instances(instances_list))
                    except Exception as e:
                        self._logger.warning(f"Error listing instances in zone {zone.name}: {e}")
                        # Continue to next zone instead of failing
                        continue
            except Exception as e:
                error_msg = f"Error listing zones in project {self._project_id}: {e}"
                if "permission" in str(e).lower():
                    error_msg += "\nYou may not have sufficient permissions to list zones."

                if not target_region:
                    error_msg += "\nPlease specify a region using the --region parameter or add a zone in your config file."

                self._logger.error(error_msg)
                raise ValueError(error_msg)

        return instances

    def _process_instances(self, instances_list) -> List[Dict[str, Any]]:
        """
        Process the list of GCP instances into a standardized format.

        Args:
            instances_list: List of GCP instance objects

        Returns:
            List of instance dictionaries with standardized fields
        """
        instances = []
        for instance in instances_list:
            # Extract instance type from URL (e.g., ".../machineTypes/n1-standard-1")
            instance_type = instance.instance_type.split("/")[-1]

            instance_info = {
                "id": instance.name,
                "type": instance_type,
                "state": self.STATUS_MAP.get(instance.status, "unknown"),
                "creation_time": instance.creation_timestamp,
                "zone": instance.zone.split("/")[-1],  # Extract zone name from URL
            }

            # Add IP addresses if available
            if instance.network_interfaces and len(instance.network_interfaces) > 0:
                # Private IP
                if instance.network_interfaces[0].network_ip:
                    instance_info["private_ip"] = instance.network_interfaces[0].network_ip

                # Public IP
                if (
                    instance.network_interfaces[0].access_configs
                    and len(instance.network_interfaces[0].access_configs) > 0
                    and instance.network_interfaces[0].access_configs[0].nat_ip
                ):
                    instance_info["public_ip"] = (
                        instance.network_interfaces[0].access_configs[0].nat_ip
                    )

            # Add labels
            if instance.labels:
                instance_info["tags"] = instance.labels

            instances.append(instance_info)

        return instances

    async def get_instance_status(self, instance_id: str) -> str:
        """
        Get the current status of a Compute Engine instance.

        Args:
            instance_id: Instance name

        Returns:
            Standardized status string
        """
        try:
            instance = self._compute_client.get(
                project=self._project_id, zone=self._zone, instance=instance_id
            )

            return self.STATUS_MAP.get(instance.status, "unknown")

        except NotFound:
            return "not_found"

    async def _get_image_from_family(
        self, family_name: str, project: str = "ubuntu-os-cloud"
    ) -> str:
        """
        Get the latest image from a specific family.

        Args:
            family_name: Image family name
            project: Project that contains the image family (default: ubuntu-os-cloud)

        Returns:
            Image URI
        """
        self._logger.debug(
            f"Retrieving latest image from family {family_name} in project {project}"
        )

        request = compute_v1.GetFromFamilyImageRequest(project=project, family=family_name)

        try:
            image = self._images_client.get_from_family(request=request)
            self._logger.debug(f"Found image: {image.name}, created on {image.creation_timestamp}")
            return image.self_link
        except Exception as e:
            self._logger.error(f"Error getting image from family {family_name}: {e}")
            raise ValueError(
                f"Could not find image in family {family_name} in project {project}: {e}"
            )

    async def _get_default_image(self) -> str:
        """
        Get the latest Ubuntu 24.04 LTS image for Compute Engine.

        Returns:
            Image URI
        """
        # Note: For public images, we use the 'ubuntu-os-cloud' project, not our project ID
        # This is an intentional exception to our rule of always using self._project_id
        image_project = "ubuntu-os-cloud"

        self._logger.debug(f"Retrieving latest Ubuntu 24.04 LTS image from {image_project} project")

        # Get the latest Ubuntu 24.04 LTS image
        request = compute_v1.ListImagesRequest(
            project=image_project,  # This is intentionally using the public image project
            filter="family = 'ubuntu-2404-lts'",
        )

        images = self._images_client.list(request=request)
        newest_image = None

        for image in images:
            if newest_image is None or image.creation_timestamp > newest_image.creation_timestamp:
                newest_image = image

        if newest_image is None:
            raise ValueError(f"No Ubuntu 24.04 LTS image found in {image_project} project")

        self._logger.debug(
            f"Found image: {newest_image.name}, created on {newest_image.creation_timestamp}"
        )
        return newest_image.self_link

    async def list_available_images(self) -> List[Dict[str, Any]]:
        """
        List available VM images in GCP.
        Returns common public OS images and the user's own custom images.

        Returns:
            List of dictionaries with image information
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
                family_images = {}
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
        all_images.sort(key=lambda x: x.get("creation_date", ""), reverse=True)

        self._logger.info(f"Found {len(all_images)} available images")
        return all_images

    async def _wait_for_operation(self, operation_name: str) -> None:
        """
        Wait for a Compute Engine operation to complete.

        Args:
            operation_name: Name of the operation
        """
        self._logger.debug(
            f"Waiting for operation {operation_name} to complete in project {self._project_id}"
        )

        operation = self._compute_client.get(
            project=self._project_id, zone=self._zone, operation=operation_name
        )

        while operation.status != "DONE":
            time.sleep(1)  # Wait between checks
            operation = self._compute_client.get(
                project=self._project_id, zone=self._zone, operation=operation_name
            )

        if operation.error:
            error_msg = f"Operation {operation_name} failed in project {self._project_id}: {operation.error}"
            self._logger.error(error_msg)
            raise Exception(error_msg)

        self._logger.debug(
            f"Operation {operation_name} completed successfully in project {self._project_id}"
        )

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

    async def _get_default_zone(self) -> str:
        """
        Get the default zone for the region.
        """
        if self._zone:
            self._logger.debug(f"Using specified zone {self._zone}")
            return self._zone

        self._logger.debug(f"Getting default zone for region {self._region}")

        # Create the request to list zones
        request = compute_v1.ListZonesRequest(project=self._project_id)
        zones = self._zones_client.list(request=request)

        # Get the first zone in the list
        self._logger.debug(f"Default zone is {zones[0].name}")
        return zones[0].name
