"""
Azure Virtual Machines implementation of the InstanceManager interface.
"""
import time
import base64
import asyncio
import aiohttp
import logging
import traceback
from typing import Any, Dict, List, Optional, Tuple

from azure.identity import ClientSecretCredential  # type: ignore
from azure.mgmt.compute import ComputeManagementClient  # type: ignore
from azure.mgmt.network import NetworkManagementClient  # type: ignore
from azure.core.exceptions import ResourceNotFoundError  # type: ignore
from azure.mgmt.resource import ResourceManagementClient  # type: ignore

from cloud_tasks.common.base import InstanceManager

# Configure logging with periods for fractions of a second
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S.%f'  # Explicitly use period for fractions
)
logger = logging.getLogger(__name__)

class AzureVMInstanceManager(InstanceManager):
    """Azure Virtual Machines implementation of the InstanceManager interface."""

    # Map of Azure VM statuses to standardized statuses
    STATUS_MAP = {
        'VM starting': 'starting',
        'VM running': 'running',
        'VM deallocating': 'stopping',
        'VM stopped': 'stopped',
        'VM stopping': 'stopping',
        'VM deallocated': 'terminated'
    }

    def __init__(self):
        """Initialize without connecting to Azure yet."""
        self.compute_client = None
        self.network_client = None
        self.resource_client = None
        self.subscription_id = None
        self.resource_group = None
        self.location = None
        self.credentials = None
        super().__init__()

    async def initialize(self, config: Dict[str, Any]) -> None:
        """
        Initialize Azure clients with the provided configuration.

        Args:
            config: Dictionary with Azure configuration

        Raises:
            ValueError: If required configuration is missing
        """
        required_keys = ['subscription_id', 'tenant_id', 'client_id', 'client_secret', 'resource_group']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required Azure configuration: {key}")

        # Create credential
        self.credentials = ClientSecretCredential(
            tenant_id=config['tenant_id'],
            client_id=config['client_id'],
            client_secret=config['client_secret']
        )

        # Initialize clients
        self.subscription_id = config['subscription_id']
        self.resource_group = config['resource_group']

        # Initialize with specified region (location) or determine later
        self.location = config.get('location')

        # Create clients
        self.compute_client = ComputeManagementClient(
            credential=self.credentials,
            subscription_id=self.subscription_id
        )

        self.network_client = NetworkManagementClient(
            credential=self.credentials,
            subscription_id=self.subscription_id
        )

        self.resource_client = ResourceManagementClient(
            credential=self.credentials,
            subscription_id=self.subscription_id
        )

        # Create resource group if it doesn't exist
        if not self.resource_group_exists():
            # Use location parameter if provided
            if self.location:
                print(f"Creating resource group {self.resource_group} in location {self.location}")
                self.create_resource_group()
            else:
                print("Cannot create resource group: no location specified")

        if self.location:
            print(f"Initialized Azure VM in location {self.location}")
        else:
            print("No location specified, will determine cheapest location during instance selection")

    def resource_group_exists(self) -> bool:
        """Check if the resource group exists."""
        return self.resource_client.resource_groups.check_existence(self.resource_group)

    def create_resource_group(self) -> None:
        """Create the resource group."""
        if not self.location:
            raise ValueError("Cannot create resource group without a location")

        # Create the resource group
        self.resource_client.resource_groups.create_or_update(
            self.resource_group,
            {"location": self.location}
        )

    async def find_cheapest_location(self, vm_size: str = 'Standard_B1s') -> str:
        """
        Find the cheapest Azure location for the given VM size.

        Args:
            vm_size: VM size to check prices for (default: Standard_B1s)

        Returns:
            The location with the lowest price
        """
        try:
            # Get all available locations
            locations = self.compute_client.resource_skus.list()
            available_locations = set()

            for sku in locations:
                if sku.resource_type == 'virtualMachines':
                    for location in sku.locations:
                        available_locations.add(location.lower().replace(' ', ''))

            location_list = list(available_locations)
            print(f"Checking prices across {len(location_list)} locations for {vm_size}")

            location_prices = {}
            for location in location_list:
                try:
                    # Get pricing for this VM size and location using Azure Retail Pricing API
                    # We'll use HTTP requests since there's no official SDK for the pricing API
                    url = "https://prices.azure.com/api/retail/prices"
                    params = {
                        'api-version': '2021-10-01-preview',
                        '$filter': f"serviceName eq 'Virtual Machines' and armRegionName eq '{location}' and armSkuName eq '{vm_size}' and priceType eq 'Consumption'"
                    }

                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, params=params) as response:
                            if response.status == 200:
                                data = await response.json()
                                if 'Items' in data and data['Items']:
                                    # Find the lowest price for this VM in this location
                                    prices = [item['unitPrice'] for item in data['Items'] if item['unitPrice'] > 0]
                                    if prices:
                                        price = min(prices)
                                        location_prices[location] = price
                                        print(f"  {location}: ${price:.6f}/hour")
                except Exception as e:
                    print(f"  Error getting price for {location}: {e}")
                    continue

            if not location_prices:
                print("Could not retrieve prices for any location, using eastus as default")
                return 'eastus'

            # Find the cheapest location
            cheapest_location = min(location_prices.items(), key=lambda x: x[1])[0]
            cheapest_price = location_prices[cheapest_location]

            print(f"Cheapest location is {cheapest_location} at ${cheapest_price:.6f}/hour")
            return cheapest_location

        except Exception as e:
            print(f"Error finding cheapest location: {e}")
            print("Using eastus as default location")
            return 'eastus'

    async def get_optimal_instance_type(
        self, cpu_required: int, memory_required_gb: int, disk_required_gb: int, use_spot: bool = False
    ) -> str:
        """
        Get the most cost-effective Azure VM size that meets requirements.
        If no location was specified during initialization, this method will also
        find and use the cheapest location.

        Args:
            cpu_required: Minimum number of vCPUs
            memory_required_gb: Minimum amount of memory in GB
            disk_required_gb: Minimum amount of disk space in GB
            use_spot: Whether to use spot instance pricing

        Returns:
            Azure VM size (e.g., 'Standard_B1s')
        """
        # If no location was specified, find the cheapest one
        if not self.location:
            print("No location specified, searching for cheapest location...")
            self.location = await self.find_cheapest_location()
            print(f"Selected location {self.location} for lowest cost")

            # Create resource group if it doesn't exist
            if not self.resource_group_exists():
                print(f"Creating resource group {self.resource_group} in location {self.location}")
                self.create_resource_group()

        # Get available VM sizes
        vm_sizes = await self.list_available_vm_sizes()

        # Filter to VM sizes that meet requirements
        eligible_vms = []
        for vm in vm_sizes:
            if (vm['vcpu'] >= cpu_required and
                vm['memory_gb'] >= memory_required_gb and
                vm.get('storage_gb', 10) >= disk_required_gb):
                eligible_vms.append(vm)

        if not eligible_vms:
            raise ValueError(
                f"No VM size meets requirements: {cpu_required} vCPU, "
                f"{memory_required_gb} GB memory, {disk_required_gb} GB disk"
            )

        # Use Azure Retail Prices API to get current prices
        pricing_data = {}
        for vm in eligible_vms:
            vm_size = vm['name']

            try:
                # Get pricing using Azure Retail Pricing API
                url = "https://prices.azure.com/api/retail/prices"
                filters = [
                    f"serviceName eq 'Virtual Machines'",
                    f"armRegionName eq '{self.location}'",
                    f"armSkuName eq '{vm_size}'",
                ]

                if use_spot:
                    filters.append("priceType eq 'Spot'")
                else:
                    filters.append("priceType eq 'Consumption'")

                params = {
                    'api-version': '2021-10-01-preview',
                    '$filter': " and ".join(filters)
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'Items' in data and data['Items']:
                                # Find the lowest price for this VM size
                                prices = [item['unitPrice'] for item in data['Items'] if item['unitPrice'] > 0]
                                if prices:
                                    pricing_data[vm_size] = min(prices)
            except Exception as e:
                print(f"Error getting pricing for {vm_size}: {e}")
                continue

        # If we couldn't get pricing from API, fall back to our heuristic
        if not pricing_data:
            print("Could not get pricing data from Azure API, falling back to heuristic")
            # Sort by vCPU + memory as a simple cost heuristic
            eligible_vms.sort(key=lambda x: x['vcpu'] + x['memory_gb'])
            return eligible_vms[0]['name']

        # Select VM size with the lowest price
        priced_vms = [(vm_size, price) for vm_size, price in pricing_data.items()]
        if not priced_vms:
            print("No pricing data found for eligible VM sizes, falling back to heuristic")
            eligible_vms.sort(key=lambda x: x['vcpu'] + x['memory_gb'])
            return eligible_vms[0]['name']

        priced_vms.sort(key=lambda x: x[1])  # Sort by price

        selected_type = priced_vms[0][0]
        price = priced_vms[0][1]
        print(f"Selected {selected_type} at ${price:.6f} per hour in {self.location}{' (spot)' if use_spot else ''}")
        return selected_type

    async def list_available_instance_types(self) -> List[Dict[str, Any]]:
        """
        List available VM sizes with their specifications.

        Returns:
            List of dictionaries with VM sizes and their specifications
        """
        # Get VM sizes for the location
        vm_sizes = self.compute_client.virtual_machine_sizes.list(location=self.location)

        instance_types = []
        for size in vm_sizes:
            instance_info = {
                'name': size.name,
                'vcpu': size.number_of_cores,
                'memory_gb': size.memory_in_mb / 1024.0,
                'storage_gb': size.resource_disk_size_in_mb / 1024.0,
                'architecture': 'x86_64',  # Assuming x86_64 for simplicity
            }

            instance_types.append(instance_info)

        return instance_types

    async def start_instance(
        self, vm_size: str, startup_script: str, tags: Dict[str, str], use_spot: bool = False
    ) -> str:
        """
        Start a new Azure VM instance.

        Args:
            vm_size: Azure VM size (e.g., 'Standard_B1s')
            startup_script: Startup script for the instance
            tags: Dictionary of tags to apply to the instance
            use_spot: Whether to use spot instances (cheaper but can be terminated)

        Returns:
            VM instance ID
        """
        # Generate unique names
        instance_name = f"worker-{int(time.time())}"
        nic_name = f"{instance_name}-nic"

        # Create a NIC for the VM
        nic_params = {
            'location': self.location,
            'ip_configurations': [{
                'name': 'ipconfig1',
                'subnet': {
                    'id': self.subnet_id
                }
            }]
        }

        nic = await asyncio.to_thread(
            self.network_client.network_interfaces.begin_create_or_update,
            self.resource_group,
            nic_name,
            nic_params
        )
        nic_result = nic.result()

        # Define the VM parameters
        vm_parameters = {
            'location': self.location,
            'os_profile': {
                'computer_name': instance_name,
                'admin_username': 'azureuser',
                'custom_data': base64.b64encode(startup_script.encode()).decode()
            },
            'hardware_profile': {
                'vm_size': vm_size
            },
            'storage_profile': {
                'image_reference': {
                    'publisher': 'Canonical',
                    'offer': 'UbuntuServer',
                    'sku': '18.04-LTS',
                    'version': 'latest'
                }
            },
            'network_profile': {
                'network_interfaces': [{
                    'id': nic_result.id
                }]
            },
            'tags': tags
        }

        # If spot instance is requested
        if use_spot:
            vm_parameters['priority'] = 'Spot'
            vm_parameters['eviction_policy'] = 'Deallocate'
            # Set max price to standard on-demand price (this means we only pay spot price)
            vm_parameters['billing_profile'] = {
                'max_price': -1
            }

        try:
            # Create the VM
            logger.info(f"Creating {'spot' if use_spot else 'on-demand'} VM: {instance_name} ({vm_size})")
            poller = await asyncio.to_thread(
                self.compute_client.virtual_machines.begin_create_or_update,
                self.resource_group,
                instance_name,
                vm_parameters
            )
            vm_result = poller.result()
            logger.info(f"VM {instance_name} created successfully")
            return vm_result.id
        except Exception as e:
            logger.error(f"Failed to create {'spot' if use_spot else 'on-demand'} VM: {e}")
            # Try to clean up the network interface if VM creation fails
            try:
                await asyncio.to_thread(
                    self.network_client.network_interfaces.begin_delete,
                    self.resource_group,
                    nic_name
                )
            except Exception as cleanup_err:
                logger.warning(f"Failed to clean up network interface: {cleanup_err}")
            raise

    async def terminate_instance(self, instance_id: str) -> None:
        """
        Terminate an Azure VM by ID.

        Args:
            instance_id: VM name
        """
        # Delete the VM
        self.compute_client.virtual_machines.begin_delete(
            self.resource_group,
            instance_id
        ).wait()

        # Clean up associated resources in a real implementation

    async def list_running_instances(self, tag_filter: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """
        List currently running Azure VMs, optionally filtered by tags.

        Args:
            tag_filter: Dictionary of tags to filter VMs

        Returns:
            List of VM dictionaries with id, type, state, and creation_time
        """
        instances = []

        # Get all VMs in the resource group
        vms = self.compute_client.virtual_machines.list(self.resource_group)

        for vm in vms:
            # Skip if tag filter provided and VM doesn't match
            if tag_filter and not self._match_tags(vm.tags, tag_filter):
                continue

            # Get instance view to determine VM status
            instance_view = self.compute_client.virtual_machines.instance_view(
                self.resource_group, vm.name
            )

            # Determine VM status from statuses
            status = 'unknown'
            for stat in instance_view.statuses:
                if stat.code.startswith('PowerState/'):
                    azure_status = stat.code.replace('PowerState/', 'VM ')
                    status = self.STATUS_MAP.get(azure_status, 'unknown')
                    break

            # Get network interfaces to extract IP addresses
            public_ip = ''
            private_ip = ''

            if vm.network_profile and vm.network_profile.network_interfaces:
                primary_nic_id = vm.network_profile.network_interfaces[0].id
                nic_name = primary_nic_id.split('/')[-1]

                try:
                    nic = self.network_client.network_interfaces.get(
                        self.resource_group, nic_name
                    )

                    if nic.ip_configurations and len(nic.ip_configurations) > 0:
                        # Get private IP
                        private_ip = nic.ip_configurations[0].private_ip_address

                        # Get public IP if available
                        if nic.ip_configurations[0].public_ip_address:
                            public_ip_id = nic.ip_configurations[0].public_ip_address.id
                            public_ip_name = public_ip_id.split('/')[-1]

                            ip_address = self.network_client.public_ip_addresses.get(
                                self.resource_group, public_ip_name
                            )

                            public_ip = ip_address.ip_address
                except Exception:
                    # Handle case where NIC might be deleted or inaccessible
                    pass

            instance_info = {
                'id': vm.name,
                'type': vm.hardware_profile.vm_size,
                'state': status,
                'location': vm.location,
                'public_ip': public_ip,
                'private_ip': private_ip
            }

            # Add tags
            if vm.tags:
                instance_info['tags'] = vm.tags

            instances.append(instance_info)

        return instances

    async def get_instance_status(self, instance_id: str) -> str:
        """
        Get the current status of an Azure VM.

        Args:
            instance_id: VM name

        Returns:
            Standardized status string
        """
        try:
            # Get instance view to determine VM status
            instance_view = self.compute_client.virtual_machines.instance_view(
                self.resource_group, instance_id
            )

            # Determine VM status from statuses
            for stat in instance_view.statuses:
                if stat.code.startswith('PowerState/'):
                    azure_status = stat.code.replace('PowerState/', 'VM ')
                    return self.STATUS_MAP.get(azure_status, 'unknown')

            return 'unknown'

        except ResourceNotFoundError:
            return 'not_found'

    def _match_tags(self, vm_tags: Dict[str, str], filter_tags: Dict[str, str]) -> bool:
        """
        Check if VM tags match the filter tags.

        Args:
            vm_tags: Tags on the VM
            filter_tags: Tags to filter by

        Returns:
            True if all filter tags are present with matching values in VM tags
        """
        if not vm_tags:
            return False

        for key, value in filter_tags.items():
            if key not in vm_tags or vm_tags[key] != value:
                return False

        return True