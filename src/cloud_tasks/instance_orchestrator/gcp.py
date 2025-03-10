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
        self.project_id = None
        self.region = None
        self.zone = None
        self.credentials = None
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

        # Initialize with specified region or default
        self.region = config.get('region')

        # If zone is specified but not region, extract region from zone
        if not self.region and 'zone' in config:
            # Zone format is typically 'region-zone', e.g., 'us-central1-a'
            self.zone = config['zone']
            zone_parts = self.zone.split('-')
            if len(zone_parts) >= 2:
                # For 'us-central1-a', this should get 'us-central1'
                self.region = '-'.join(zone_parts[:-1])
        elif self.region and 'zone' in config:
            self.zone = config['zone']
        elif self.region:
            # If region is specified but zone is not, use the first zone in the region
            self.zone = f"{self.region}-a"

        # Initialize clients
        self.compute_client = compute_v1.InstancesClient(credentials=self.credentials)
        self.machine_types_client = compute_v1.MachineTypesClient(credentials=self.credentials)
        self.zones_client = compute_v1.ZonesClient(credentials=self.credentials)
        self.regions_client = compute_v1.RegionsClient(credentials=self.credentials)
        self.images_client = compute_v1.ImagesClient(credentials=self.credentials)
        self.operations_client = compute_v1.ZoneOperationsClient(credentials=self.credentials)
        self.billing_client = billing.CloudCatalogClient(credentials=self.credentials)

        if self.region:
            logger.info(f"Initialized GCP Compute Engine in region {self.region}, zone {self.zone}")
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
        self, cpu_required: int, memory_required_gb: int, disk_required_gb: int, use_preemptible: bool = False
    ) -> str:
        """
        Get the most cost-effective GCP machine type that meets requirements.
        If no region was specified during initialization, this method will also
        find and use the cheapest region.

        Args:
            cpu_required: Minimum number of vCPUs
            memory_required_gb: Minimum amount of memory in GB
            disk_required_gb: Minimum amount of disk space in GB
            use_preemptible: Whether to use preemptible instance pricing

        Returns:
            GCP machine type (e.g., 'n1-standard-1')
        """
        # If no region/zone was specified, find the cheapest one
        if not self.region or not self.zone:
            logger.info("No region specified, searching for cheapest region...")
            region_info = await self.find_cheapest_region()
            self.region = region_info['region']
            self.zone = region_info['zone']
            logger.info(f"Selected region {self.region}, zone {self.zone} for lowest cost")

        # Get available machine types
        machine_types = await self.list_available_machine_types()

        # Filter to machine types that meet requirements
        eligible_types = []
        for machine in machine_types:
            if (machine['vcpu'] >= cpu_required and
                machine['memory_gb'] >= memory_required_gb):
                # We don't filter on disk since persistent disks can be attached
                eligible_types.append(machine)

        if not eligible_types:
            raise ValueError(
                f"No machine type meets requirements: {cpu_required} vCPU, "
                f"{memory_required_gb} GB memory"
            )

        # Use GCP Cloud Catalog API to get current prices
        pricing_data = {}
        for machine in eligible_types:
            machine_type = machine['name']

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

                        # Check if this SKU matches our machine type and region
                        if (
                            machine_family in sku_description
                            and (
                                ("core" in sku_description and not use_preemptible) or
                                ("preemptible" in sku_description and use_preemptible)
                            )
                            and self.region in sku.service_regions
                        ):
                            # Extract price info
                            for pricing_info in sku.pricing_info:
                                for tier in pricing_info.pricing_expression.tiered_rates:
                                    unit_price = tier.unit_price
                                    # Convert to USD dollars
                                    price = unit_price.units + (unit_price.nanos / 1e9)

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

        # If we couldn't get pricing from API, fall back to our heuristic
        if not pricing_data:
            logger.warning("Could not get pricing data from GCP API, falling back to heuristic")
            # Sort by vCPU + memory as a simple cost heuristic
            eligible_types.sort(key=lambda x: x['vcpu'] + x['memory_gb'])
            return eligible_types[0]['name']

        # Select instance with the lowest price
        priced_instances = [(machine_type, price) for machine_type, price in pricing_data.items()]
        if not priced_instances:
            logger.warning("No pricing data found for eligible machine types, falling back to heuristic")
            eligible_types.sort(key=lambda x: x['vcpu'] + x['memory_gb'])
            return eligible_types[0]['name']

        priced_instances.sort(key=lambda x: x[1])  # Sort by price

        selected_type = priced_instances[0][0]
        price = priced_instances[0][1]
        logger.info(f"Selected {selected_type} at ${price:.6f} per hour in {self.region} ({self.zone}){' (preemptible)' if use_preemptible else ''}")
        return selected_type

    async def list_available_instance_types(self) -> List[Dict[str, Any]]:
        """
        List available Compute Engine machine types with their specifications.

        Returns:
            List of dictionaries with machine types and their specifications
        """
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

    async def start_instance(
        self, instance_type: str, startup_script: str, labels: Dict[str, str], use_preemptible: bool = False
    ) -> str:
        """
        Start a new GCP Compute Engine instance.

        Args:
            instance_type: Machine type for the instance
            startup_script: Startup script to run on instance boot
            labels: Key-value pairs for instance labels
            use_preemptible: Whether to use preemptible instance

        Returns:
            Instance name
        """
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

        # Get default image
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
        if use_preemptible:
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
            logger.info(f"Creating {'preemptible' if use_preemptible else 'standard'} instance {config['name']} ({instance_type})")
            await self._wait_for_operation(operation['name'])
            logger.info(f"Instance {config['name']} created successfully")
            return config['name']
        except Exception as e:
            logger.error(f"Failed to create {'preemptible' if use_preemptible else 'standard'} instance: {e}")
            raise

    async def terminate_instance(self, instance_id: str) -> None:
        """
        Terminate a Compute Engine instance by ID.

        Args:
            instance_id: Instance name
        """
        operation = self.compute_client.delete(
            project=self.project_id,
            zone=self.zone,
            instance=instance_id
        )

        # Wait for the operation to complete
        # In a real implementation, you might want to make this asynchronous
        operation.result()

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

    async def _get_default_image(self) -> str:
        """
        Get the latest Debian image for Compute Engine.

        Returns:
            Image URI
        """
        # Get the latest Debian 11 image
        request = compute_v1.ListImagesRequest(
            project='debian-cloud',
            filter="family = 'debian-11'"
        )

        images = self.images_client.list(request=request)
        newest_image = None

        for image in images:
            if newest_image is None or image.creation_timestamp > newest_image.creation_timestamp:
                newest_image = image

        if newest_image is None:
            raise ValueError("No Debian 11 image found")

        return newest_image.self_link

    async def _wait_for_operation(self, operation_name: str) -> None:
        """
        Wait for a Compute Engine operation to complete.

        Args:
            operation_name: Name of the operation
        """
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
            raise Exception(f"Operation failed: {operation.error}")