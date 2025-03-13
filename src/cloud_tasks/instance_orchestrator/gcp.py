"""
Google Cloud Compute Engine implementation of the InstanceManager interface.
"""
import base64
import time
import asyncio
import uuid
import logging
import traceback
from typing import Any, Dict, List, Optional

from google.cloud import compute_v1  # type: ignore
from google.api_core.exceptions import NotFound  # type: ignore
from google.cloud import billing_v1  # type: ignore
from google.oauth2 import service_account
from google.cloud import billing

from cloud_tasks.common.base import InstanceManager

# Configure logging with periods for fractions of a second
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S.%f'  # Explicitly use period for fractions
)
logger = logging.getLogger(__name__)

class GCPComputeInstanceManager(InstanceManager):
    """Google Cloud Compute Engine implementation of the InstanceManager interface."""

    # Map of instance statuses to standardized statuses
    STATUS_MAP = {
        'PROVISIONING': 'starting',
        'STAGING': 'starting',
        'RUNNING': 'running',
        'STOPPING': 'stopping',
        'TERMINATED': 'terminated',
        'SUSPENDED': 'stopped'
    }

    def __init__(self):
        """Initialize the GCP Compute Engine instance manager."""
        self.compute_client = None
        self.billing_client = None
        self.billing_compute_skus = None
        self.project_id = None
        self.region = None
        self.zone = None
        self.credentials = None
        self.instance_types = None
        super().__init__()

    async def initialize(self, config: Dict[str, Any]) -> None:
        """
        Initialize GCP clients with the provided configuration.

        Args:
            config: Dictionary with GCP configuration. If credentials_file is not provided,
                   the default application credentials from the environment will be used.

        Raises:
            ValueError: If required configuration is missing
        """
        # Only project_id is required now, credentials_file is optional
        if 'project_id' not in config:
            raise ValueError("Missing required GCP configuration: project_id")

        self.project_id = config['project_id']

        # Store instance_types configuration if present
        self.instance_types = config.get('instance_types')
        if self.instance_types:
            if isinstance(self.instance_types, str):
                # If a single string was provided, convert to a list
                self.instance_types = [self.instance_types]
            logger.info(f"Instance types restricted to patterns: {self.instance_types}")

        # Handle credentials - use provided service account file or default application credentials
        if 'credentials_file' in config:
            try:
                self.credentials = service_account.Credentials.from_service_account_file(
                    config['credentials_file']
                )
                logger.info(f"Using credentials from file: {config['credentials_file']}")
            except Exception as e:
                raise ValueError(f"Error loading credentials file {config['credentials_file']}: {e}")
        else:
            # Use default credentials from environment
            try:
                # Import needed here since we're using it conditionally
                from google.auth import default as get_default_credentials
                self.credentials, project_id = get_default_credentials()
                if not self.project_id and project_id:
                    self.project_id = project_id
                    logger.info("Using project ID from default credentials")
                logger.info("Using default application credentials from environment")
            except Exception as e:
                raise ValueError(f"Error getting default credentials from environment: {e}. "
                               "Please ensure you're authenticated with 'gcloud auth application-default login' "
                               "or provide a credentials_file.")

        # Initialize region/zone from config if provided
        self.region = config.get('region')
        self.zone = config.get('zone')

        # If zone is provided but not region, extract region from zone
        if self.zone and not self.region:
            # Extract region from zone (e.g., us-central1-a -> us-central1)
            self.region = '-'.join(self.zone.split('-')[:-1])
            logger.info(f"Extracted region {self.region} from zone {self.zone}")

        # Initialize compute client
        self.compute_client = compute_v1.InstancesClient(credentials=self.credentials)
        self.zones_client = compute_v1.ZonesClient(credentials=self.credentials)
        self.regions_client = compute_v1.RegionsClient(credentials=self.credentials)
        self.machine_types_client = compute_v1.MachineTypesClient(credentials=self.credentials)
        self.images_client = compute_v1.ImagesClient(credentials=self.credentials)

        # Initialize billing client for pricing information
        self.billing_client = billing.CloudCatalogClient(credentials=self.credentials)

        # Log project and region info
        logger.info(f"Initialized GCP Compute Engine with project ID: {self.project_id}")
        if self.region:
            logger.info(f"Using region {self.region}, zone {self.zone}")
        else:
            logger.warning("No region specified, will determine cheapest region during instance selection")

    async def find_cheapest_region(self, machine_type: str = 'n1-standard-1') -> Dict[str, str]:
        """
        Find the cheapest GCP region and zone for the given machine type.

        Args:
            machine_type: Machine type to check prices for (default: n1-standard-1)

        Returns:
            Dictionary with 'region' and 'zone' keys
        """
        try:
            # Get all available regions
            request = compute_v1.ListRegionsRequest(project=self.project_id)
            regions = self.regions_client.list(request=request)
            region_names = [region.name for region in regions]

            logger.info(f"Checking prices across {len(region_names)} regions for {machine_type}")

            region_prices = {}
            for region_name in region_names:
                try:
                    # Get zones in this region
                    zones_request = compute_v1.ListZonesRequest(
                        project=self.project_id,
                        filter=f"name eq {region_name}-.*"
                    )
                    zones = self.zones_client.list(request=zones_request)
                    zone_names = [zone.name for zone in zones]

                    if not zone_names:
                        continue

                    # Choose the first zone in the region for pricing check
                    zone = zone_names[0]

                    # Get pricing from Cloud Billing Catalog API
                    service_name = "compute.googleapis.com"

                    # Make pricing request
                    request = billing.ListServicesRequest()
                    services = self.billing_client.list_services(request=request)

                    # Find compute service
                    compute_service = None
                    for service in services:
                        if service.name.endswith(service_name):
                            compute_service = service
                            break

                    if compute_service:
                        # Get SKUs for the service
                        sku_request = billing.ListSkusRequest(
                            parent=compute_service.name
                        )
                        skus = self.billing_client.list_skus(request=sku_request)

                        # Find matching SKU for the machine type in this region
                        price = None
                        for sku in skus:
                            if (
                                machine_type in sku.description
                                and "Core" in sku.description
                                and region_name in sku.service_regions
                            ):
                                # Get the price in USD
                                for pricing_info in sku.pricing_info:
                                    for price_by_currency in pricing_info.pricing_expression.tiered_rates[0].unit_price.currency_code:
                                        if price_by_currency == "USD":
                                            price = pricing_info.pricing_expression.tiered_rates[0].unit_price.nanos / 1e9
                                            region_prices[region_name] = {
                                                'price': price,
                                                'zone': zone
                                            }
                                            logger.info(f"  {region_name} ({zone}): ${price:.6f}/hour")
                                            break
                                    if price:
                                        break
                            if price:
                                break

                except Exception as e:
                    logger.warning(f"  Error getting price for {region_name}: {e}")
                    continue

            if not region_prices:
                logger.warning("Could not retrieve prices for any region, using us-central1 as default")
                return {'region': 'us-central1', 'zone': 'us-central1-a'}

            # Find the cheapest region
            cheapest_region = min(region_prices.items(), key=lambda x: x[1]['price'])[0]
            cheapest_zone = region_prices[cheapest_region]['zone']
            cheapest_price = region_prices[cheapest_region]['price']

            logger.info(f"Cheapest region is {cheapest_region} ({cheapest_zone}) at ${cheapest_price:.6f}/hour")
            return {'region': cheapest_region, 'zone': cheapest_zone}

        except Exception as e:
            logger.error(f"Error finding cheapest region: {e}")
            logger.info("Using us-central1 as default region")
            return {'region': 'us-central1', 'zone': 'us-central1-a'}

    async def get_optimal_instance_type(
        self, cpu_required: int, memory_required_gb: int, disk_required_gb: int, use_spot: bool = False
    ) -> str:
        """
        Get the most cost-effective GCP machine type that meets requirements.
        If no region was specified during initialization, this method will also
        find and use the cheapest region.

        Args:
            cpu_required: Minimum number of vCPUs
            memory_required_gb: Minimum memory in GB
            disk_required_gb: Minimum disk space in GB
            use_spot: Whether to use preemptible instances (GCP equivalent of spot)

        Returns:
            GCP machine type name (e.g., 'n1-standard-2')
        """
        logger.info(f"Finding optimal instance type with: CPU={cpu_required}, Memory={memory_required_gb}GB, "
                   f"Disk={disk_required_gb}GB, Spot/Preemptible={use_spot}")

        # If no region/zone was specified, find the cheapest one
        if not self.region or not self.zone:
            logger.info("No region specified, searching for cheapest region...")
            region_info = await self.find_cheapest_region()
            self.region = region_info['region']
            self.zone = region_info['zone']
            logger.info(f"Selected region {self.region}, zone {self.zone} for lowest cost")

        # Get available machine types
        machine_types = await self.list_available_instance_types()
        logger.debug(f"Found {len(machine_types)} available machine types in zone {self.zone}")

        # Filter to machine types that meet requirements
        eligible_types = []
        for machine in machine_types:
            if (machine['vcpu'] >= cpu_required and
                machine['memory_gb'] >= memory_required_gb):
                # We don't filter on disk since persistent disks can be attached
                eligible_types.append(machine)

        logger.debug(f"Found {len(eligible_types)} machine types that meet requirements:")
        for idx, machine in enumerate(eligible_types):
            logger.debug(f"  [{idx+1}] {machine['name']}: {machine['vcpu']} vCPU, {machine['memory_gb']:.2f} GB memory")

        # Filter by instance_types if specified in configuration
        if self.instance_types:
            filtered_types = []
            for machine in eligible_types:
                machine_name = machine['name']
                # Check if machine matches any prefix or exact name
                for pattern in self.instance_types:
                    if machine_name.startswith(pattern) or machine_name == pattern:
                        filtered_types.append(machine)
                        break

            # Update eligible types with filtered list
            if filtered_types:
                eligible_types = filtered_types
                logger.debug(f"Filtered to {len(eligible_types)} machine types based on instance_types configuration:")
                for idx, machine in enumerate(eligible_types):
                    logger.debug(f"  [{idx+1}] {machine['name']}: {machine['vcpu']} vCPU, {machine['memory_gb']:.2f} GB memory")
            else:
                error_msg = f"No machines match the instance_types patterns: {self.instance_types}. Available machine types meeting requirements: {[m['name'] for m in eligible_types]}"
                logger.error(error_msg)
                raise ValueError(error_msg)

        if not eligible_types:
            raise ValueError(
                f"No machine type meets requirements: {cpu_required} vCPU, "
                f"{memory_required_gb} GB memory"
            )

        # Use GCP Cloud Catalog API to get current prices
        pricing_data = {}
        logger.debug(f"Retrieving pricing data for {len(eligible_types)} eligible machine types...")

        for machine in eligible_types:
            machine_type = machine['name']
            logger.debug(f"Getting pricing for machine type: {machine_type}")

            try:
                # Get pricing from Cloud Billing Catalog API
                service_name = "compute.googleapis.com"

                # Make pricing request
                request = billing.ListServicesRequest()
                services = self.billing_client.list_services(request=request)

                # Find compute service
                compute_service = None
                for service in services:
                    if service.name.endswith(service_name):
                        compute_service = service
                        break

                if compute_service:
                    logger.debug(f"Found compute service: {compute_service.display_name}")
                    # Get SKUs for the service
                    sku_request = billing.ListSkusRequest(
                        parent=compute_service.name
                    )
                    skus = self.billing_client.list_skus(request=sku_request)

                    # Find matching SKU for the machine type in this region
                    for sku in skus:
                        price_found = False
                        sku_description = sku.description.lower()

                        machine_name_parts = machine_type.split('-')
                        machine_family = machine_name_parts[0]

                        # Debug log for SKU matching
                        sku_match = (
                            machine_family in sku_description
                            and (
                                ("core" in sku_description and not use_spot) or
                                ("preemptible" in sku_description and use_spot)
                            )
                            and self.region in sku.service_regions
                        )

                        if sku_match:
                            logger.debug(f"Matching SKU found: {sku.description} in region {self.region}")

                        # Check if this SKU matches our machine type and region
                        if sku_match:
                            # Extract price info
                            for pricing_info in sku.pricing_info:
                                for tier in pricing_info.pricing_expression.tiered_rates:
                                    unit_price = tier.unit_price
                                    # Convert to USD dollars
                                    price = unit_price.units + (unit_price.nanos / 1e9)

                                    logger.debug(f"  Price: ${price:.6f} per {pricing_info.pricing_expression.usage_unit}")

                                    # Store price for this machine type
                                    if machine_type not in pricing_data or price < pricing_data[machine_type]:
                                        pricing_data[machine_type] = price
                                        price_found = True
                                        break

                                if price_found:
                                    break

            except Exception as e:
                logger.warning(f"Error getting pricing for {machine_type}: {e}")
                continue

        # Log the complete pricing data found
        if pricing_data:
            logger.debug("Retrieved pricing data for the following machine types:")
            for machine_type, price in pricing_data.items():
                logger.debug(f"  {machine_type}: ${price:.6f} per hour")
        else:
            logger.warning("Could not retrieve any pricing data from GCP API")

        # If we couldn't get pricing from API, fall back to our heuristic
        if not pricing_data:
            logger.warning("Could not get pricing data from GCP API, falling back to heuristic")
            # Sort by vCPU + memory as a simple cost heuristic
            eligible_types.sort(key=lambda x: x['vcpu'] + x['memory_gb'])
            selected_type = eligible_types[0]['name']
            logger.info(f"Selected {selected_type} based on heuristic (lowest vCPU + memory)")
            return selected_type

        # Select instance with the lowest price
        priced_instances = [(machine_type, price) for machine_type, price in pricing_data.items()]
        if not priced_instances:
            logger.warning("No pricing data found for eligible machine types, falling back to heuristic")
            eligible_types.sort(key=lambda x: x['vcpu'] + x['memory_gb'])
            selected_type = eligible_types[0]['name']
            logger.info(f"Selected {selected_type} based on heuristic (lowest vCPU + memory)")
            return selected_type

        priced_instances.sort(key=lambda x: x[1])  # Sort by price

        # Debug log for all priced instances in order
        logger.debug("Machine types sorted by price (cheapest first):")
        for i, (machine_type, price) in enumerate(priced_instances):
            logger.debug(f"  {i+1}. {machine_type}: ${price:.6f}/hour")

        selected_type = priced_instances[0][0]
        price = priced_instances[0][1]
        logger.info(f"Selected {selected_type} at ${price:.6f} per hour in {self.region} ({self.zone}){' (spot/preemptible)' if use_spot else ''}")
        return selected_type

    async def list_available_instance_types(self) -> List[Dict[str, Any]]:
        """
        List available Compute Engine machine types with their specifications.

        Returns:
            List of dictionaries with machine types and their specifications
        """
        # Ensure we have a zone
        if not hasattr(self, 'zone') or not self.zone:
            logger.warning("No zone specified for listing instance types, using us-central1-a as default")
            self.zone = 'us-central1-a'
            if not self.region:
                self.region = 'us-central1'

        # List machine types in the zone
        request = compute_v1.ListMachineTypesRequest(
            project=self.project_id,
            zone=self.zone
        )

        machine_types = self.machine_types_client.list(request=request)

        instance_types = []
        for machine_type in machine_types:
            instance_info = {
                'name': machine_type.name,
                'vcpu': machine_type.guest_cpus,
                'memory_gb': machine_type.memory_mb / 1024.0,
                'architecture': 'x86_64',  # Assuming x86_64 for simplicity
                'storage_gb': 0  # GCP separates storage from instance type
            }

            instance_types.append(instance_info)

        return instance_types

    async def get_instance_pricing(self, machine_type: str, use_spot: bool = False) -> float:
        """
        Get the hourly price for a specific machine type.

        Args:
            machine_type: The machine type name (e.g., 'n1-standard-1')
            use_spot: Whether to use preemptible pricing (cheaper but can be terminated)

        Returns:
            Hourly price in USD
        """
        logger.debug(f"Getting pricing for machine type: {machine_type} (spot/preemptible: {use_spot})")

        try:
            if self.billing_compute_skus is None:
                # Get pricing from Cloud Billing Catalog API
                service_name = "Compute Engine"

                # Make pricing request
                request = billing.ListServicesRequest()
                services = self.billing_client.list_services(request=request)

                # Find compute service
                compute_service = None
                for service in services:
                    if service.display_name == service_name:
                        compute_service = service
                        break

                if not compute_service:
                    logger.warning("Could not find compute service in billing catalog")
                    return 0.0

                logger.debug(f"Found compute service: {compute_service.display_name}")

                # Get SKUs for the service
                sku_request = billing.ListSkusRequest(
                    parent=compute_service.name
                )
                self.billing_compute_skus = list(self.billing_client.list_skus(request=sku_request))

            # Extract the machine family from the machine type name
            machine_name_parts = machine_type.split('-')
            machine_family = machine_name_parts[0]

            # Find matching SKU for the machine type in this region
            for sku in self.billing_compute_skus:
                sku_description = sku.description.lower()

                # Check if this SKU matches our machine type and region
                sku_match = (
                    machine_family in sku_description
                    and (
                        ("core" in sku_description and not use_spot) or
                        ("preemptible" in sku_description and use_spot)
                    )
                    and self.region in sku.service_regions
                )

                if sku_match:
                    logger.debug(f"Matching SKU found: {sku.description} in region {self.region}")

                    # Extract price info
                    for pricing_info in sku.pricing_info:
                        for tier in pricing_info.pricing_expression.tiered_rates:
                            unit_price = tier.unit_price
                            # Convert to USD dollars
                            price = unit_price.units + (unit_price.nanos / 1e9)
                            logger.debug(f"  Price: ${price:.6f} per {pricing_info.pricing_expression.usage_unit}")
                            return price

            logger.warning(f"No pricing information found for {machine_type} in region {self.region}")
            return 0.0

        except Exception as e:
            logger.warning(f"Error getting pricing for {machine_type}: {e}")
            return 0.0

    async def start_instance(
        self, instance_type: str, startup_script: str, labels: Dict[str, str],
        use_spot: bool = False, custom_image: Optional[str] = None
    ) -> str:
        """
        Start a new GCP Compute Engine instance.

        Args:
            instance_type: GCP machine type (e.g., 'n1-standard-1')
            startup_script: Base64-encoded startup script
            labels: Dictionary of labels to apply to the instance
            use_spot: Whether to use a preemptible VM (cheaper but can be terminated)
            custom_image: Custom image to use (if provided)

        Returns:
            ID of the started instance
        """
        if not self.compute_client:
            raise RuntimeError("GCP Compute Engine client not initialized")

        logger.info(f"Starting new instance with type: {instance_type}, spot/preemptible: {use_spot}")

        # Generate a unique name for the instance
        instance_id = f"{labels.get('job_id', 'job')}-{str(uuid.uuid4())[:8]}"

        # Encode the startup script as metadata
        metadata = {
            'items': [
                {
                    'key': 'startup-script',
                    'value': startup_script
                }
            ]
        }

        # Get image - either custom or default
        if custom_image:
            logger.info(f"Using custom image: {custom_image}")
            # If it's a full URI, use it directly
            if custom_image.startswith('https://') or '/' in custom_image:
                source_image = custom_image
            else:
                # Assuming it's a family name in ubuntu-os-cloud
                source_image = await self._get_image_from_family(custom_image)
        else:
            # Get default Ubuntu 24.04 LTS image
            source_image = await self._get_default_image()

        # Prepare the disk configuration
        disk_config = {
            'boot': True,
            'auto_delete': True,
            'initialize_params': {
                'source_image': source_image,
                'disk_size_gb': 10  # Default size, can be increased as needed
            }
        }

        # Prepare the network interface configuration
        network_interface = {
            'network': 'global/networks/default',
            'access_configs': [
                {
                    'type': 'ONE_TO_ONE_NAT',
                    'name': 'External NAT'
                }
            ]
        }

        # Prepare scheduling configuration for preemptible instances
        scheduling = {}
        if use_spot:
            scheduling = {
                'preemptible': True,
                'automatic_restart': False,
                'on_host_maintenance': 'TERMINATE'
            }

        # Prepare the instance configuration
        config = {
            'name': instance_id,
            'machine_type': f"zones/{self.zone}/machineTypes/{instance_type}",
            'disks': [disk_config],
            'network_interfaces': [network_interface],
            'metadata': metadata,
            'labels': labels,
            'scheduling': scheduling
        }

        # Create the instance
        try:
            operation = await asyncio.to_thread(
                self.compute_client.insert,
                project=self.project_id,
                zone=self.zone,
                instance_resource=config
            )

            # Wait for the create operation to complete
            logger.info(f"Creating {'spot/preemptible' if use_spot else 'standard'} instance {config['name']} ({instance_type})")
            await self._wait_for_operation(operation['name'])
            logger.info(f"Instance {config['name']} created successfully")
            return config['name']
        except Exception as e:
            logger.error(f"Failed to create {'spot/preemptible' if use_spot else 'standard'} instance: {e}")
            raise

    async def terminate_instance(self, instance_id: str) -> None:
        """
        Terminate a Compute Engine instance by ID.

        Args:
            instance_id: Instance name
        """
        logger.info(f"Terminating instance {instance_id} in project {self.project_id}, zone {self.zone}")

        try:
            operation = self.compute_client.delete(
                project=self.project_id,
                zone=self.zone,
                instance=instance_id
            )

            # Wait for the operation to complete asynchronously
            await self._wait_for_operation(operation.name)
            logger.info(f"Instance {instance_id} terminated successfully")
        except NotFound:
            logger.warning(f"Instance {instance_id} not found in project {self.project_id}, zone {self.zone}")
        except Exception as e:
            logger.error(f"Error terminating instance {instance_id} in project {self.project_id}: {e}")
            raise

    async def list_running_instances(self, tag_filter: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """
        List currently running Compute Engine instances, optionally filtered by labels.

        Args:
            tag_filter: Dictionary of labels to filter instances

        Returns:
            List of instance dictionaries with id, type, state, and creation_time
        """
        # Build filter string if tags are provided
        filter_str = ''
        if tag_filter:
            filters = []
            for key, value in tag_filter.items():
                filters.append(f"labels.{key}={value}")
            filter_str = ' AND '.join(filters)

        # List instances
        request = compute_v1.ListInstancesRequest(
            project=self.project_id,
            zone=self.zone,
            filter=filter_str
        )

        instances_list = self.compute_client.list(request=request)

        instances = []
        for instance in instances_list:
            # Extract machine type from URL (e.g., ".../machineTypes/n1-standard-1")
            machine_type = instance.machine_type.split('/')[-1]

            instance_info = {
                'id': instance.name,
                'type': machine_type,
                'state': self.STATUS_MAP.get(instance.status, 'unknown'),
                'creation_time': instance.creation_timestamp,
            }

            # Add IP addresses if available
            if instance.network_interfaces and len(instance.network_interfaces) > 0:
                # Private IP
                if instance.network_interfaces[0].network_ip:
                    instance_info['private_ip'] = instance.network_interfaces[0].network_ip

                # Public IP
                if (instance.network_interfaces[0].access_configs and
                    len(instance.network_interfaces[0].access_configs) > 0 and
                    instance.network_interfaces[0].access_configs[0].nat_ip):
                    instance_info['public_ip'] = instance.network_interfaces[0].access_configs[0].nat_ip

            # Add labels
            if instance.labels:
                instance_info['tags'] = instance.labels

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
            instance = self.compute_client.get(
                project=self.project_id,
                zone=self.zone,
                instance=instance_id
            )

            return self.STATUS_MAP.get(instance.status, 'unknown')

        except NotFound:
            return 'not_found'

    async def _get_image_from_family(self, family_name: str, project: str = 'ubuntu-os-cloud') -> str:
        """
        Get the latest image from a specific family.

        Args:
            family_name: Image family name
            project: Project that contains the image family (default: ubuntu-os-cloud)

        Returns:
            Image URI
        """
        logger.debug(f"Retrieving latest image from family {family_name} in project {project}")

        request = compute_v1.GetFromFamilyImageRequest(
            project=project,
            family=family_name
        )

        try:
            image = self.images_client.get_from_family(request=request)
            logger.debug(f"Found image: {image.name}, created on {image.creation_timestamp}")
            return image.self_link
        except Exception as e:
            logger.error(f"Error getting image from family {family_name}: {e}")
            raise ValueError(f"Could not find image in family {family_name} in project {project}: {e}")

    async def _get_default_image(self) -> str:
        """
        Get the latest Ubuntu 24.04 LTS image for Compute Engine.

        Returns:
            Image URI
        """
        # Note: For public images, we use the 'ubuntu-os-cloud' project, not our project ID
        # This is an intentional exception to our rule of always using self.project_id
        image_project = 'ubuntu-os-cloud'

        logger.debug(f"Retrieving latest Ubuntu 24.04 LTS image from {image_project} project")

        # Get the latest Ubuntu 24.04 LTS image
        request = compute_v1.ListImagesRequest(
            project=image_project,  # This is intentionally using the public image project
            filter="family = 'ubuntu-2404-lts'"
        )

        images = self.images_client.list(request=request)
        newest_image = None

        for image in images:
            if newest_image is None or image.creation_timestamp > newest_image.creation_timestamp:
                newest_image = image

        if newest_image is None:
            raise ValueError(f"No Ubuntu 24.04 LTS image found in {image_project} project")

        logger.debug(f"Found image: {newest_image.name}, created on {newest_image.creation_timestamp}")
        return newest_image.self_link

    async def list_available_images(self) -> List[Dict[str, Any]]:
        """
        List available VM images in GCP.
        Returns common public OS images and the user's own custom images.

        Returns:
            List of dictionaries with image information
        """
        logger.info("Listing available Compute Engine images")

        # List of common public OS image projects
        public_projects = [
            'ubuntu-os-cloud',       # Ubuntu images
            'debian-cloud',          # Debian images
            'centos-cloud',          # CentOS images
            'rhel-cloud',            # Red Hat Enterprise Linux images
            'fedora-cloud',          # Fedora images
            'suse-cloud',            # SUSE Linux images
            'rocky-linux-cloud',     # Rocky Linux images
            'cos-cloud',             # Container-Optimized OS images
            'windows-cloud',         # Windows Server images
        ]

        # Dictionary to store all images
        all_images = []

        # Get public images from standard projects
        for project in public_projects:
            try:
                # Don't use a filter in the API request, as it's causing issues
                # We'll filter the results programmatically instead
                request = compute_v1.ListImagesRequest(
                    project=project
                )

                logger.debug(f"Fetching images from {project}")
                images = list(self.images_client.list(request=request))

                # Filter out deprecated images in the code instead of in the API query
                filtered_images = []
                for image in images:
                    # Include image if it's not deprecated or obsolete
                    if not hasattr(image, 'deprecated') or not image.deprecated or \
                       (image.deprecated.state != "DEPRECATED" and image.deprecated.state != "OBSOLETE"):
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

                    all_images.append({
                        'id': newest_image.id,
                        'name': newest_image.name,
                        'description': newest_image.description,
                        'family': newest_image.family,
                        'creation_date': newest_image.creation_timestamp,
                        'source': 'GCP',
                        'project': project,
                        'self_link': newest_image.self_link,
                        'status': newest_image.status,
                    })

            except Exception as e:
                logger.warning(f"Error fetching images from {project}: {e}")
                continue

        # Get user's own custom images
        try:
            request = compute_v1.ListImagesRequest(
                project=self.project_id,
            )

            logger.debug(f"Fetching custom images from project {self.project_id}")
            image_list = self.images_client.list(request=request)

            # Process each image
            for image in image_list:
                all_images.append({
                    'id': image.id,
                    'name': image.name,
                    'description': image.description,
                    'family': image.family,
                    'creation_date': image.creation_timestamp,
                    'source': 'User',
                    'project': self.project_id,
                    'self_link': image.self_link,
                    'status': image.status,
                })

        except Exception as e:
            logger.warning(f"Error fetching custom images from project {self.project_id}: {e}")

        # Sort by creation date
        all_images.sort(key=lambda x: x.get('creation_date', ''), reverse=True)

        logger.info(f"Found {len(all_images)} available images")
        return all_images

    async def _wait_for_operation(self, operation_name: str) -> None:
        """
        Wait for a Compute Engine operation to complete.

        Args:
            operation_name: Name of the operation
        """
        logger.debug(f"Waiting for operation {operation_name} to complete in project {self.project_id}")

        operation = self.compute_client.get(
            project=self.project_id,
            zone=self.zone,
            operation=operation_name
        )

        while operation.status != 'DONE':
            time.sleep(1)  # Wait between checks
            operation = self.compute_client.get(
                project=self.project_id,
                zone=self.zone,
                operation=operation_name
            )

        if operation.error:
            error_msg = f"Operation {operation_name} failed in project {self.project_id}: {operation.error}"
            logger.error(error_msg)
            raise Exception(error_msg)

        logger.debug(f"Operation {operation_name} completed successfully in project {self.project_id}")