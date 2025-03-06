"""
Azure Virtual Machines implementation of the InstanceManager interface.
"""
import time
import base64
from typing import Any, Dict, List, Optional

from azure.identity import ClientSecretCredential  # type: ignore
from azure.mgmt.compute import ComputeManagementClient  # type: ignore
from azure.mgmt.network import NetworkManagementClient  # type: ignore
from azure.core.exceptions import ResourceNotFoundError  # type: ignore

from cloud_tasks.common.base import InstanceManager


class AzureVMInstanceManager(InstanceManager):
    """Azure Virtual Machines implementation of the InstanceManager interface."""

    # Map of instance statuses to standardized statuses
    STATUS_MAP = {
        'VM starting': 'starting',
        'VM running': 'running',
        'VM stopping': 'stopping',
        'VM stopped': 'stopped',
        'VM deallocating': 'stopping',
        'VM deallocated': 'stopped',
    }

    def __init__(self):
        self.compute_client = None
        self.network_client = None
        self.subscription_id = None
        self.location = None
        self.resource_group = None

    async def initialize(self, config: Dict[str, Any]) -> None:
        """
        Initialize the Azure VM instance manager with configuration.

        Args:
            config: Azure configuration with subscription_id, tenant_id, client_id, and client_secret
        """
        self.subscription_id = config['subscription_id']
        tenant_id = config['tenant_id']
        client_id = config['client_id']
        client_secret = config['client_secret']

        # Location and resource group settings
        self.location = config.get('location', 'eastus')
        self.resource_group = config.get('resource_group', 'cloud-tasks-rg')

        # Create credential object
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        # Create clients
        self.compute_client = ComputeManagementClient(
            credential=credential,
            subscription_id=self.subscription_id
        )

        self.network_client = NetworkManagementClient(
            credential=credential,
            subscription_id=self.subscription_id
        )

        # Ensure resource group exists
        # In a real implementation, this would check if the group exists
        # and create it if it doesn't

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
        self, instance_type: str, user_data: str, tags: Dict[str, str]
    ) -> str:
        """
        Start a new Azure VM and return its ID.

        Args:
            instance_type: Azure VM size (e.g., 'Standard_B1s')
            user_data: Custom data script (will be base64 encoded)
            tags: Dictionary of tags to apply to the VM

        Returns:
            VM name
        """
        # Create a unique VM name based on timestamp
        vm_name = f"worker-{int(time.time())}"

        # Base64 encode the user data script
        custom_data = base64.b64encode(user_data.encode('utf-8')).decode('utf-8')

        # Create public IP address
        poller = self.network_client.public_ip_addresses.begin_create_or_update(
            self.resource_group,
            f"{vm_name}-ip",
            {
                'location': self.location,
                'public_ip_allocation_method': 'Dynamic',
                'tags': tags
            }
        )
        ip_address_result = poller.result()

        # Create virtual network
        poller = self.network_client.virtual_networks.begin_create_or_update(
            self.resource_group,
            f"{vm_name}-vnet",
            {
                'location': self.location,
                'address_space': {
                    'address_prefixes': ['10.0.0.0/16']
                },
                'tags': tags
            }
        )
        network = poller.result()

        # Create subnet
        poller = self.network_client.subnets.begin_create_or_update(
            self.resource_group,
            f"{vm_name}-vnet",
            f"{vm_name}-subnet",
            {'address_prefix': '10.0.0.0/24'}
        )
        subnet = poller.result()

        # Create network interface
        poller = self.network_client.network_interfaces.begin_create_or_update(
            self.resource_group,
            f"{vm_name}-nic",
            {
                'location': self.location,
                'ip_configurations': [{
                    'name': f"{vm_name}-ipconfig",
                    'subnet': {
                        'id': subnet.id
                    },
                    'public_ip_address': {
                        'id': ip_address_result.id
                    }
                }],
                'tags': tags
            }
        )
        nic_result = poller.result()

        # Create VM
        poller = self.compute_client.virtual_machines.begin_create_or_update(
            self.resource_group,
            vm_name,
            {
                'location': self.location,
                'hardware_profile': {
                    'vm_size': instance_type
                },
                'storage_profile': {
                    'image_reference': {
                        'publisher': 'Canonical',
                        'offer': 'UbuntuServer',
                        'sku': '18.04-LTS',
                        'version': 'latest'
                    },
                    'os_disk': {
                        'name': f"{vm_name}-disk",
                        'caching': 'ReadWrite',
                        'create_option': 'FromImage',
                        'managed_disk': {
                            'storage_account_type': 'Standard_LRS'
                        }
                    }
                },
                'os_profile': {
                    'computer_name': vm_name,
                    'admin_username': 'azureuser',
                    'custom_data': custom_data,
                    'linux_configuration': {
                        'disable_password_authentication': True,
                        'ssh': {
                            'public_keys': [
                                {
                                    'path': '/home/azureuser/.ssh/authorized_keys',
                                    'key_data': 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC+wWK73dCr+jgQOAxNsHAnNNNMEMWOHYEccp6wJm2gotpr9katuF/ZAdou5AaW1C61slRkHRkpRRX9FA9CYBiitZgvCCz+3nWNN7l/Up54Zps/pHWGZLHNJZRYyAB6j5yVLMVHIHriY49d/GZTZVNB8GoJv9Gakwc/fuEZYYl4YDFiGMBP///TzlI4jhiJzjKnEvqPFki5p2ZRJqcbCiF4pJrxUQR/RXqVFQdbRLZgYfJ8xGB878RENq3yQ39d8dVOkq4edbkzwcUmwwwkYVPIoDGsYLaRHnG+To7FvMeyO7xDVQkMKzopTQV8AuKpyvpqu0a9pWOMaiCyDytO7GGN'
                                }
                            ]
                        }
                    }
                },
                'network_profile': {
                    'network_interfaces': [{
                        'id': nic_result.id
                    }]
                },
                'tags': tags
            }
        )
        vm_result = poller.result()

        return vm_name

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

    async def get_optimal_instance_type(
        self, cpu_required: int, memory_required_gb: int, disk_required_gb: int
    ) -> str:
        """
        Get the most cost-effective Azure VM size that meets requirements.

        Args:
            cpu_required: Minimum number of vCPUs
            memory_required_gb: Minimum amount of memory in GB
            disk_required_gb: Minimum amount of disk space in GB

        Returns:
            VM size name (e.g., 'Standard_B1s')
        """
        # Get VM sizes
        instance_types = await self.list_available_instance_types()

        # Filter to instances that meet requirements
        eligible_instances = []
        for instance in instance_types:
            if (instance['vcpu'] >= cpu_required and
                instance['memory_gb'] >= memory_required_gb and
                instance['storage_gb'] >= disk_required_gb):
                eligible_instances.append(instance)

        if not eligible_instances:
            raise ValueError(
                f"No instance type meets requirements: {cpu_required} vCPU, "
                f"{memory_required_gb} GB memory, {disk_required_gb} GB disk"
            )

        # Sort by vCPUs first, then memory as a cost proxy
        eligible_instances.sort(key=lambda x: (x['vcpu'], x['memory_gb']))

        # Return the name of the most cost-effective instance
        return eligible_instances[0]['name']

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