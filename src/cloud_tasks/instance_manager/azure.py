"""
Azure Virtual Machines implementation of the InstanceManager interface.
"""

# TODO Fix type errors
# mypy: ignore-errors

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
from azure.mgmt.commerce import UsageManagementClient  # type: ignore

from ..common.base import InstanceManager

# Configure logging with periods for fractions of a second
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S.%f",  # Explicitly use period for fractions
)
logger = logging.getLogger(__name__)


class AzureVMInstanceManager(InstanceManager):
    """Azure Virtual Machines implementation of the InstanceManager interface."""

    # Map of Azure VM statuses to standardized statuses
    STATUS_MAP = {
        "VM starting": "starting",
        "VM running": "running",
        "VM deallocating": "stopping",
        "VM stopped": "stopped",
        "VM stopping": "stopping",
        "VM deallocated": "terminated",
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
        self.instance_types = None
        self.retail_client = None
        super().__init__()

    async def initialize(self, config: Dict[str, Any]) -> None:
        """
        Initialize Azure clients with the provided configuration.

        Args:
            config: Dictionary with Azure configuration

        Raises:
            ValueError: If required configuration is missing
        """
        required_keys = [
            "subscription_id",
            "tenant_id",
            "client_id",
            "client_secret",
            "resource_group",
        ]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required Azure configuration: {key}")

        # Store instance_types configuration if present
        self.instance_types = config.get("instance_types")
        if self.instance_types:
            if isinstance(self.instance_types, str):
                # If a single string was provided, convert to a list
                self.instance_types = [self.instance_types]
            logger.info(f"Instance types restricted to patterns: {self.instance_types}")

        # Create credential
        self.credentials = ClientSecretCredential(
            tenant_id=config["tenant_id"],
            client_id=config["client_id"],
            client_secret=config["client_secret"],
        )

        # Initialize clients
        self.subscription_id = config["subscription_id"]
        self.resource_group = config["resource_group"]

        # Initialize with specified region (location) or determine later
        self.location = config.get("location")

        # Create clients
        self.compute_client = ComputeManagementClient(
            credential=self.credentials, subscription_id=self.subscription_id
        )
        self.network_client = NetworkManagementClient(
            credential=self.credentials, subscription_id=self.subscription_id
        )
        self.resource_client = ResourceManagementClient(
            credential=self.credentials, subscription_id=self.subscription_id
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
            print(
                "No location specified, will determine cheapest location during instance selection"
            )

    def resource_group_exists(self) -> bool:
        """Check if the resource group exists."""
        return self.resource_client.resource_groups.check_existence(self.resource_group)

    def create_resource_group(self) -> None:
        """Create the resource group."""
        if not self.location:
            raise ValueError("Cannot create resource group without a location")

        # Create the resource group
        self.resource_client.resource_groups.create_or_update(
            self.resource_group, {"location": self.location}
        )

    async def find_cheapest_location(self, vm_size: str = "Standard_B1s") -> str:
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
                if sku.resource_type == "virtualMachines":
                    for location in sku.locations:
                        available_locations.add(location.lower().replace(" ", ""))

            location_list = list(available_locations)
            print(f"Checking prices across {len(location_list)} locations for {vm_size}")

            location_prices = {}
            for location in location_list:
                try:
                    # Get pricing for this VM size and location using Azure Retail Pricing API
                    # We'll use HTTP requests since there's no official SDK for the pricing API
                    url = "https://prices.azure.com/api/retail/prices"
                    params = {
                        "api-version": "2021-10-01-preview",
                        "$filter": f"serviceName eq 'Virtual Machines' and armRegionName eq '{location}' and armSkuName eq '{vm_size}' and priceType eq 'Consumption'",
                    }

                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, params=params) as response:
                            if response.status == 200:
                                data = await response.json()
                                if "Items" in data and data["Items"]:
                                    # Find the lowest price for this VM in this location
                                    prices = [
                                        item["unitPrice"]
                                        for item in data["Items"]
                                        if item["unitPrice"] > 0
                                    ]
                                    if prices:
                                        price = min(prices)
                                        location_prices[location] = price
                                        print(f"  {location}: ${price:.6f}/hour")
                except Exception as e:
                    print(f"  Error getting price for {location}: {e}")
                    continue

            if not location_prices:
                print("Could not retrieve prices for any location, using eastus as default")
                return "eastus"

            # Find the cheapest location
            cheapest_location = min(location_prices.items(), key=lambda x: x[1])[0]
            cheapest_price = location_prices[cheapest_location]

            print(f"Cheapest location is {cheapest_location} at ${cheapest_price:.6f}/hour")
            return cheapest_location

        except Exception as e:
            print(f"Error finding cheapest location: {e}")
            print("Using eastus as default location")
            return "eastus"

    async def get_optimal_instance_type(
        self,
        cpu_required: int,
        memory_required_gb: int,
        disk_required_gb: int,
        use_spot: bool = False,
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
        logger.info(
            f"Finding optimal instance type with: CPU={cpu_required}, Memory={memory_required_gb}GB, "
            f"Disk={disk_required_gb}GB, Spot={use_spot}"
        )

        # If no location was specified, find the cheapest one
        if not self.location:
            logger.info("No location specified, searching for cheapest location...")
            self.location = await self.find_cheapest_location()
            logger.info(f"Selected location {self.location} for lowest cost")

            # Create resource group if it doesn't exist
            if not self.resource_group_exists():
                logger.info(
                    f"Creating resource group {self.resource_group} in location {self.location}"
                )
                self.create_resource_group()
        else:
            logger.info(f"Using specified location {self.location}")

        # Get available VM sizes
        vm_sizes = await self.list_available_vm_sizes()
        logger.debug(f"Found {len(vm_sizes)} available VM sizes in location {self.location}")

        # Filter to VM sizes that meet requirements
        eligible_vms = []
        for vm in vm_sizes:
            if (
                vm["vcpu"] >= cpu_required
                and vm["memory_gb"] >= memory_required_gb
                and vm.get("storage_gb", 10) >= disk_required_gb
            ):
                eligible_vms.append(vm)

        logger.debug(f"Found {len(eligible_vms)} VM sizes that meet requirements:")
        for idx, vm in enumerate(eligible_vms):
            logger.debug(
                f"  [{idx+1}] {vm['name']}: {vm['vcpu']} vCPU, {vm['memory_gb']:.2f} GB memory, {vm.get('storage_gb', 0):.2f} GB storage"
            )

        # Filter by instance_types if specified in configuration
        if self.instance_types:
            filtered_vms = []
            for vm in eligible_vms:
                vm_name = vm["name"]
                # Check if VM size matches any prefix or exact name
                for pattern in self.instance_types:
                    if vm_name.startswith(pattern) or vm_name == pattern:
                        filtered_vms.append(vm)
                        break

            # Update eligible VMs with filtered list
            if filtered_vms:
                eligible_vms = filtered_vms
                logger.debug(
                    f"Filtered to {len(eligible_vms)} VM sizes based on instance_types configuration:"
                )
                for idx, vm in enumerate(eligible_vms):
                    logger.debug(
                        f"  [{idx+1}] {vm['name']}: {vm['vcpu']} vCPU, {vm['memory_gb']:.2f} GB memory, {vm.get('storage_gb', 0):.2f} GB storage"
                    )
            else:
                error_msg = f"No VM sizes match the instance_types patterns: {self.instance_types}. Available VM sizes meeting requirements: {[v['name'] for v in eligible_vms]}"
                logger.error(error_msg)
                raise ValueError(error_msg)

        if not eligible_vms:
            msg = f"No VM size meets requirements: {cpu_required} vCPU, {memory_required_gb} GB memory, {disk_required_gb} GB disk"
            logger.error(msg)
            raise ValueError(msg)

        # Use Azure Retail Prices API to get current prices
        pricing_data = {}
        logger.debug(f"Retrieving pricing data for {len(eligible_vms)} eligible VM sizes...")

        for vm in eligible_vms:
            vm_size = vm["name"]
            logger.debug(f"Getting pricing for VM size: {vm_size}")

            try:
                # Get pricing using Azure Retail Pricing API
                url = "https://prices.azure.com/api/retail/prices"

                # Build filter for the API
                filters = [
                    f"serviceName eq 'Virtual Machines'",
                    f"armRegionName eq '{self.location}'",
                    f"armSkuName eq '{vm_size}'",
                ]

                if use_spot:
                    filters.append("priceType eq 'Spot'")
                else:
                    filters.append("priceType eq 'Consumption'")

                filter_string = " and ".join(filters)
                logger.debug(f"Azure Retail Pricing API filter: {filter_string}")

                params = {"api-version": "2021-10-01-preview", "$filter": filter_string}

                logger.debug(f"Calling Azure Retail Pricing API for {vm_size}...")
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params) as response:
                        status = response.status
                        logger.debug(f"API response status: {status}")

                        if status == 200:
                            data = await response.json()
                            item_count = len(data.get("Items", []))
                            logger.debug(f"API returned {item_count} price items for {vm_size}")

                            if "Items" in data and data["Items"]:
                                # Log all returned price items
                                for idx, item in enumerate(data["Items"]):
                                    logger.debug(
                                        f"  Price item {idx+1}: ${item.get('unitPrice', 0):.6f} per {item.get('unitOfMeasure', 'hour')}"
                                    )
                                    logger.debug(
                                        f"    - Product name: {item.get('productName', 'Unknown')}"
                                    )
                                    logger.debug(
                                        f"    - Sku name: {item.get('skuName', 'Unknown')}"
                                    )
                                    logger.debug(
                                        f"    - Meter name: {item.get('meterName', 'Unknown')}"
                                    )
                                    logger.debug(
                                        f"    - Price type: {item.get('priceType', 'Unknown')}"
                                    )

                                # Find the lowest price for this VM size
                                prices = [
                                    item["unitPrice"]
                                    for item in data["Items"]
                                    if item["unitPrice"] > 0
                                ]
                                if prices:
                                    min_price = min(prices)
                                    pricing_data[vm_size] = min_price
                                    logger.debug(
                                        f"Selected lowest price for {vm_size}: ${min_price:.6f} per hour"
                                    )
                                else:
                                    logger.debug(f"No valid prices found for {vm_size}")
                            else:
                                logger.debug(f"No price items found for {vm_size}")
                        else:
                            logger.debug(f"API call failed with status {status} for {vm_size}")
            except Exception as e:
                logger.warning(f"Error getting pricing for {vm_size}: {e}")
                continue

        # Log the complete pricing data found
        if pricing_data:
            logger.debug("Retrieved pricing data for the following VM sizes:")
            for vm_size, price in pricing_data.items():
                logger.debug(f"  {vm_size}: ${price:.6f} per hour")
        else:
            logger.warning("Could not retrieve any pricing data from Azure API")

        # If we couldn't get pricing from API, fall back to our heuristic
        if not pricing_data:
            logger.warning("Could not get pricing data from Azure API, falling back to heuristic")
            # Sort by vCPU + memory as a simple cost heuristic
            eligible_vms.sort(key=lambda x: x["vcpu"] + x["memory_gb"])
            selected_type = eligible_vms[0]["name"]
            logger.info(f"Selected {selected_type} based on heuristic (lowest vCPU + memory)")
            return selected_type

        # Select VM size with the lowest price
        priced_vms = [(vm_size, price) for vm_size, price in pricing_data.items()]
        if not priced_vms:
            logger.warning("No pricing data found for eligible VM sizes, falling back to heuristic")
            eligible_vms.sort(key=lambda x: x["vcpu"] + x["memory_gb"])
            selected_type = eligible_vms[0]["name"]
            logger.info(f"Selected {selected_type} based on heuristic (lowest vCPU + memory)")
            return selected_type

        priced_vms.sort(key=lambda x: x[1])  # Sort by price

        # Debug log for all priced VMs in order
        logger.debug("VM sizes sorted by price (cheapest first):")
        for i, (vm_size, price) in enumerate(priced_vms):
            logger.debug(f"  {i+1}. {vm_size}: ${price:.6f}/hour")

        selected_type = priced_vms[0][0]
        price = priced_vms[0][1]
        logger.info(
            f"Selected {selected_type} at ${price:.6f} per hour in {self.location}{' (spot)' if use_spot else ''}"
        )
        return selected_type

    async def list_available_instance_types(self) -> List[Dict[str, Any]]:
        """
        List available Azure VM sizes with their specifications.

        Returns:
            List of dictionaries with VM sizes and their specifications
        """
        # Ensure we have a location
        if not hasattr(self, "location") or not self.location:
            logger.warning(
                "No location specified for listing instance types, using eastus as default"
            )
            self.location = "eastus"

        # List available VM sizes
        vm_sizes = self.compute_client.virtual_machine_sizes.list(location=self.location)

        instance_types = []
        for vm_size in vm_sizes:
            instance_info = {
                "name": vm_size.name,
                "vcpu": vm_size.number_of_cores,
                "memory_gb": vm_size.memory_in_mb / 1024.0,
                "storage_gb": vm_size.max_data_disk_count
                * 1024,  # Rough estimate, 1TB per data disk
            }

            instance_types.append(instance_info)

        return instance_types

    async def get_instance_pricing(self, vm_size: str, use_spot: bool = False) -> float:
        """
        Get the hourly price for a specific VM size.

        Args:
            vm_size: The VM size name (e.g., 'Standard_B1s')
            use_spot: Whether to use spot pricing (cheaper but can be terminated)

        Returns:
            Hourly price in USD
        """
        logger.debug(f"Getting pricing for VM size: {vm_size} (spot: {use_spot})")

        try:
            # Create retail prices client
            if not hasattr(self, "retail_client"):
                self.retail_client = UsageManagementClient(
                    credentials=self.credentials, subscription_id=self.subscription_id
                )

            # Use the retail API to get pricing
            # Filter for the specific VM size in the current location
            rate_filter = f"OfferDurableId eq 'MS-AZR-0003p' and Currency eq 'USD' and Locale eq 'en-US' and RegionInfo eq '{self.location}' and ServiceInfo eq 'Virtual Machines' and skuName eq '{vm_size}'"
            if use_spot:
                rate_filter += " and meterName eq 'Spot'"

            # Get rate card info
            rate_info = self.retail_client.rate_card.get(filter=rate_filter)

            # Find the VM price in the response
            for meter in rate_info.meters.values():
                if vm_size in meter.meter_name and "Windows" not in meter.meter_name:
                    # Found our VM size, get the price
                    rates = meter.meter_rates
                    # Generally, the first rate is the primary one
                    if rates and 0 in rates:
                        price = float(rates[0])
                        logger.debug(
                            f"Found {'spot' if use_spot else 'on-demand'} price for {vm_size}: ${price:.4f}/hour"
                        )
                        return price

            # If retail API fails, fall back to a direct REST API call
            logger.debug("Retail API failed, trying direct REST API call")
            import requests

            # Use the subscriptions/resources REST API
            url = f"https://prices.azure.com/api/retail/prices?$filter=serviceName eq 'Virtual Machines' and armRegionName eq '{self.location}' and priceType eq 'Consumption' and skuName eq '{vm_size}'"

            if use_spot:
                url += " and productName eq 'Virtual Machines Spot'"

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                items = data.get("Items", [])
                for item in items:
                    if (
                        "Windows" not in item.get("productName", "")
                        and item.get("unitOfMeasure") == "1 Hour"
                    ):
                        price = float(item.get("retailPrice", 0))
                        logger.debug(
                            f"Found {'spot' if use_spot else 'on-demand'} price for {vm_size}: ${price:.4f}/hour"
                        )
                        return price

            logger.warning(f"No pricing found for {vm_size} in location {self.location}")
            return 0.0

        except Exception as e:
            logger.warning(f"Error getting pricing for {vm_size}: {e}")
            return 0.0

    async def start_instance(
        self,
        vm_size: str,
        startup_script: str,
        tags: Dict[str, str],
        use_spot: bool = False,
        custom_image: Optional[str] = None,
    ) -> str:
        """
        Start a new Azure VM instance.

        Args:
            vm_size: Azure VM size (e.g., 'Standard_B1s')
            startup_script: Startup script for the instance
            tags: Dictionary of tags to apply to the instance
            use_spot: Whether to use spot instances (cheaper but can be terminated)
            custom_image: Custom image to use instead of default Ubuntu 24.04 LTS

        Returns:
            VM instance ID
        """
        # Generate unique names
        instance_name = f"worker-{int(time.time())}"
        nic_name = f"{instance_name}-nic"
        ip_name = f"{instance_name}-ip"
        disk_name = f"{instance_name}-disk"

        # Create network interface
        async_nic_creation = self.network_client.network_interfaces.begin_create_or_update(
            self.resource_group,
            nic_name,
            {
                "location": self.location,
                "ip_configurations": [
                    {
                        "name": ip_name,
                        "public_ip_address": {
                            "location": self.location,
                            "name": ip_name,
                            "public_ip_allocation_method": "Dynamic",
                        },
                        "subnet": {"id": self.subnet_id},
                    }
                ],
            },
        )
        nic = await asyncio.to_thread(lambda: async_nic_creation.result())

        # Prepare VM parameters
        vm_parameters = {
            "location": self.location,
            "os_profile": {
                "computer_name": instance_name,
                "admin_username": "azureuser",
                "custom_data": base64.b64encode(startup_script.encode()).decode(),
                "linux_configuration": {
                    "disable_password_authentication": True,
                    "ssh": {
                        "public_keys": [
                            {
                                "path": "/home/azureuser/.ssh/authorized_keys",
                                "key_data": self.ssh_key,
                            }
                        ]
                    },
                },
            },
            "network_profile": {"network_interfaces": [{"id": nic.id}]},
            "hardware_profile": {"vm_size": vm_size},
            "tags": tags,
        }

        # Configure image
        if custom_image:
            # Check if it's a full resource ID
            if custom_image.startswith("/subscriptions/"):
                vm_parameters["storage_profile"] = {
                    "image_reference": {"id": custom_image},
                    "os_disk": {
                        "name": disk_name,
                        "caching": "ReadWrite",
                        "create_option": "FromImage",
                    },
                }
                logger.info(f"Using custom image by ID: {custom_image}")
            else:
                # Check if it's in URN format: publisher:offer:sku:version
                parts = custom_image.split(":")
                if len(parts) >= 3:
                    publisher, offer, sku = parts[0:3]
                    version = parts[3] if len(parts) > 3 else "latest"

                    vm_parameters["storage_profile"] = {
                        "image_reference": {
                            "publisher": publisher,
                            "offer": offer,
                            "sku": sku,
                            "version": version,
                        },
                        "os_disk": {
                            "name": disk_name,
                            "caching": "ReadWrite",
                            "create_option": "FromImage",
                        },
                    }
                    logger.info(f"Using custom image: {publisher}:{offer}:{sku}:{version}")
                else:
                    # Use default Ubuntu 24.04 with warning
                    logger.warning(
                        f"Invalid custom image format: {custom_image}, using default Ubuntu 24.04 LTS"
                    )
                    vm_parameters["storage_profile"] = {
                        "image_reference": {
                            "publisher": "Canonical",
                            "offer": "UbuntuServer",
                            "sku": "24_04-lts",
                            "version": "latest",
                        },
                        "os_disk": {
                            "name": disk_name,
                            "caching": "ReadWrite",
                            "create_option": "FromImage",
                        },
                    }
        else:
            # Use default Ubuntu 24.04 LTS
            vm_parameters["storage_profile"] = {
                "image_reference": {
                    "publisher": "Canonical",
                    "offer": "UbuntuServer",
                    "sku": "24_04-lts",
                    "version": "latest",
                },
                "os_disk": {
                    "name": disk_name,
                    "caching": "ReadWrite",
                    "create_option": "FromImage",
                },
            }
            logger.info("Using default Ubuntu 24.04 LTS image")

        # Configure spot instance if requested
        if use_spot:
            vm_parameters["priority"] = "Spot"
            vm_parameters["eviction_policy"] = "Deallocate"
            vm_parameters["billing_profile"] = {
                "max_price": -1
            }  # -1 means pay the current spot price
            logger.info("Using spot instance (pay-as-you-go pricing)")

        # Create the VM
        try:
            logger.info(f"Creating VM {instance_name} with size {vm_size}")
            async_vm_creation = self.compute_client.virtual_machines.begin_create_or_update(
                self.resource_group, instance_name, vm_parameters
            )
            vm = await asyncio.to_thread(lambda: async_vm_creation.result())
            logger.info(f"Created VM: {instance_name}, ID: {vm.id}")
            return vm.id
        except Exception as e:
            logger.error(f"Error creating VM: {e}")
            raise

    async def terminate_instance(self, instance_id: str) -> None:
        """
        Terminate an Azure VM by ID.

        Args:
            instance_id: VM name
        """
        # Delete the VM
        self.compute_client.virtual_machines.begin_delete(self.resource_group, instance_id).wait()

        # Clean up associated resources in a real implementation

    async def list_running_instances(
        self, tag_filter: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
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
            status = "unknown"
            for stat in instance_view.statuses:
                if stat.code.startswith("PowerState/"):
                    azure_status = stat.code.replace("PowerState/", "VM ")
                    status = self.STATUS_MAP.get(azure_status, "unknown")
                    break

            # Get network interfaces to extract IP addresses
            public_ip = ""
            private_ip = ""

            if vm.network_profile and vm.network_profile.network_interfaces:
                primary_nic_id = vm.network_profile.network_interfaces[0].id
                nic_name = primary_nic_id.split("/")[-1]

                try:
                    nic = self.network_client.network_interfaces.get(self.resource_group, nic_name)

                    if nic.ip_configurations and len(nic.ip_configurations) > 0:
                        # Get private IP
                        private_ip = nic.ip_configurations[0].private_ip_address

                        # Get public IP if available
                        if nic.ip_configurations[0].public_ip_address:
                            public_ip_id = nic.ip_configurations[0].public_ip_address.id
                            public_ip_name = public_ip_id.split("/")[-1]

                            ip_address = self.network_client.public_ip_addresses.get(
                                self.resource_group, public_ip_name
                            )

                            public_ip = ip_address.ip_address
                except Exception:
                    # Handle case where NIC might be deleted or inaccessible
                    pass

            instance_info = {
                "id": vm.name,
                "type": vm.hardware_profile.vm_size,
                "state": status,
                "location": vm.location,
                "public_ip": public_ip,
                "private_ip": private_ip,
            }

            # Add tags
            if vm.tags:
                instance_info["tags"] = vm.tags

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
                if stat.code.startswith("PowerState/"):
                    azure_status = stat.code.replace("PowerState/", "VM ")
                    return self.STATUS_MAP.get(azure_status, "unknown")

            return "unknown"

        except ResourceNotFoundError:
            return "not_found"

    def _match_tags(self, vm_tags: Dict[str, str], filter_tags: Dict[str, str]) -> bool:
        """
        Check if VM tags match the filter tags.

        Args:
            vm_tags: Tags of the VM
            filter_tags: Tags to filter by

        Returns:
            True if all filter tags are in VM tags with matching values
        """
        if not filter_tags:
            return True

        if not vm_tags:
            return False

        for key, value in filter_tags.items():
            if key not in vm_tags or vm_tags[key] != value:
                return False

        return True

    async def list_available_images(self) -> List[Dict[str, Any]]:
        """
        List available VM images in Azure.
        Returns standard marketplace images from common publishers and user's own images.

        Returns:
            List of dictionaries with image information
        """
        logger.info("Listing available VM images in Azure")
        all_images = []

        # List of common OS publishers
        publishers = [
            "Canonical",  # Ubuntu
            "MicrosoftWindowsServer",  # Windows Server
            "RedHat",  # RHEL
            "OpenLogic",  # CentOS
            "SUSE",  # SUSE Linux
            "Debian",  # Debian
            "MicrosoftCBLMariner",  # Microsoft CBL-Mariner Linux
            "microsoftwindowsdesktop",  # Windows desktop
        ]

        # Get marketplace images for common publishers
        logger.info(f"Fetching marketplace images from {len(publishers)} publishers")
        for publisher in publishers:
            try:
                # Get offers for this publisher
                offers = self.compute_client.virtual_machine_images.list_offers(
                    location=self.location, publisher_name=publisher
                )

                for offer in offers:
                    try:
                        # Get SKUs for this offer
                        skus = self.compute_client.virtual_machine_images.list_skus(
                            location=self.location, publisher_name=publisher, offer=offer.name
                        )

                        for sku in skus:
                            try:
                                # Get latest version for this SKU
                                versions = list(
                                    self.compute_client.virtual_machine_images.list(
                                        location=self.location,
                                        publisher_name=publisher,
                                        offer=offer.name,
                                        skus=sku.name,
                                    )
                                )

                                if versions:
                                    # Sort by version (descending) and get the latest
                                    versions.sort(key=lambda x: x.name, reverse=True)
                                    latest_version = versions[0]

                                    # Use get_latest to get detailed image info for newest version
                                    image_info = {
                                        "id": f"{publisher}:{offer.name}:{sku.name}:{latest_version.name}",
                                        "name": f"{publisher} {offer.name} {sku.name}",
                                        "publisher": publisher,
                                        "offer": offer.name,
                                        "sku": sku.name,
                                        "version": latest_version.name,
                                        "location": self.location,
                                        "source": "Azure",
                                    }
                                    all_images.append(image_info)
                            except Exception as e:
                                logger.debug(
                                    f"Error getting versions for {publisher}:{offer.name}:{sku.name}: {e}"
                                )
                                continue
                    except Exception as e:
                        logger.debug(f"Error getting SKUs for {publisher}:{offer.name}: {e}")
                        continue
            except Exception as e:
                logger.warning(f"Error getting offers for publisher {publisher}: {e}")
                continue

        # Get user's custom images
        try:
            logger.info("Fetching custom images")
            custom_images = self.compute_client.images.list()

            for image in custom_images:
                image_info = {
                    "id": image.id,
                    "name": image.name,
                    "location": image.location,
                    "source": "User",
                    "resource_group": image.id.split("/")[4] if image.id else "Unknown",
                    "hyper_v_generation": (
                        image.hyper_vgeneration.value if image.hyper_vgeneration else "Unknown"
                    ),
                    "os_type": (
                        image.storage_profile.os_disk.os_type.value
                        if image.storage_profile and image.storage_profile.os_disk
                        else "Unknown"
                    ),
                }
                all_images.append(image_info)
        except Exception as e:
            logger.warning(f"Error fetching custom images: {e}")

        logger.info(f"Found {len(all_images)} available images")
        return all_images

    async def get_available_regions(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        """
        Return all available Azure regions and their attributes.

        Args:
            prefix: Optional prefix to filter regions by name

        Returns:
            Dictionary of region names mapped to their information:
            - name: Region name (e.g., 'eastus')
            - description: Region description
            - endpoint: Region endpoint
            - zones: List of availability zones in the region
        """
        self._logger.debug("Listing available Azure regions")

        # Get all regions/locations from the subscription
        subscription_client = ResourceManagementClient(
            credential=self.credentials, subscription_id=self.subscription_id
        )
        locations = subscription_client.subscriptions.list_locations(self.subscription_id)

        region_dict = {}
        for location in locations:
            # Apply prefix filtering if specified
            if prefix and not location.name.startswith(prefix):
                continue

            try:
                # Get availability zones for this location
                zones_response = self.compute_client.resource_skus.list(
                    filter=f"location eq '{location.name}'"
                )

                # Find zones from any VM SKU that supports zones
                zones = set()
                for sku in zones_response:
                    if sku.resource_type == "virtualMachines":
                        for zone_capability in sku.location_info:
                            if zone_capability.zones:
                                zones.update(zone_capability.zones)

                region_info = {
                    "name": location.name,
                    "description": location.display_name,
                    "endpoint": f"{location.name}.management.azure.com",  # Standard Azure endpoint format
                    "zones": sorted(list(zones)),  # Convert set to sorted list
                    "metadata": (
                        {
                            "geography": location.metadata.geography,
                            "geography_group": location.metadata.geography_group,
                            "regional_display_name": location.regional_display_name,
                            "physical_location": location.metadata.physical_location,
                        }
                        if location.metadata
                        else {}
                    ),
                }
                region_dict[location.name] = region_info

            except Exception as e:
                logger.warning(f"Error getting availability zones for region {location.name}: {e}")
                # Still include the region, just without availability zones
                region_info = {
                    "name": location.name,
                    "description": location.display_name,
                    "endpoint": f"{location.name}.management.azure.com",
                    "zones": [],
                    "metadata": (
                        {
                            "geography": location.metadata.geography,
                            "geography_group": location.metadata.geography_group,
                            "regional_display_name": location.regional_display_name,
                            "physical_location": location.metadata.physical_location,
                        }
                        if location.metadata
                        else {}
                    ),
                }
                region_dict[location.name] = region_info

        self._logger.debug(
            f"Found {len(region_dict)} available regions: "
            f"{', '.join(sorted(region_dict.keys()))}"
        )
        return region_dict
