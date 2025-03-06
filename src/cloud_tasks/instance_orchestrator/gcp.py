"""
Google Cloud Compute Engine implementation of the InstanceManager interface.
"""
import base64
import time
from typing import Any, Dict, List, Optional

from google.cloud import compute_v1  # type: ignore
from google.api_core.exceptions import NotFound  # type: ignore

from cloud_tasks.common.base import InstanceManager


class GCPComputeInstanceManager(InstanceManager):
    """Google Cloud Compute Engine implementation of the InstanceManager interface."""

    # Map of instance statuses to standardized statuses
    STATUS_MAP = {
        'PROVISIONING': 'starting',
        'STAGING': 'starting',
        'RUNNING': 'running',
        'STOPPING': 'stopping',
        'TERMINATED': 'stopped',
        'SUSPENDED': 'stopped'
    }

    def __init__(self):
        self.instance_client = None
        self.machine_type_client = None
        self.images_client = None
        self.project_id = None
        self.zone = None

    async def initialize(self, config: Dict[str, Any]) -> None:
        """
        Initialize the Compute Engine instance manager with configuration.

        Args:
            config: GCP configuration with project_id and optionally credentials_file
        """
        self.project_id = config['project_id']
        self.zone = config.get('zone', 'us-central1-a')  # Default to us-central1-a
        credentials_file = config.get('credentials_file')

        # Create clients
        if credentials_file:
            # Use specified credentials file
            self.instance_client = compute_v1.InstancesClient.from_service_account_file(credentials_file)
            self.machine_type_client = compute_v1.MachineTypesClient.from_service_account_file(credentials_file)
            self.images_client = compute_v1.ImagesClient.from_service_account_file(credentials_file)
        else:
            # Use default credentials
            self.instance_client = compute_v1.InstancesClient()
            self.machine_type_client = compute_v1.MachineTypesClient()
            self.images_client = compute_v1.ImagesClient()

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

        machine_types = self.machine_type_client.list(request=request)

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
        self, instance_type: str, user_data: str, tags: Dict[str, str]
    ) -> str:
        """
        Start a new Compute Engine instance and return its ID.

        Args:
            instance_type: Compute Engine machine type (e.g., 'n1-standard-1')
            user_data: Startup script (will be base64 encoded)
            tags: Dictionary of labels to apply to the instance

        Returns:
            Instance name (ID in GCP)
        """
        # Create a unique instance name based on timestamp
        instance_name = f"worker-{int(time.time())}"

        # Get the latest Debian image
        image_uri = await self._get_default_image()

        # Create the instance specification
        instance = {
            'name': instance_name,
            'machine_type': f"zones/{self.zone}/machineTypes/{instance_type}",
            'labels': tags,
            'disks': [
                {
                    'boot': True,
                    'auto_delete': True,
                    'initialize_params': {
                        'source_image': image_uri,
                        'disk_size_gb': 10,  # Default disk size
                    }
                }
            ],
            'network_interfaces': [
                {
                    'network': 'global/networks/default',
                    'access_configs': [
                        {
                            'name': 'External NAT',
                            'type': 'ONE_TO_ONE_NAT'
                        }
                    ]
                }
            ],
            'metadata': {
                'items': [
                    {
                        'key': 'startup-script',
                        'value': user_data
                    }
                ]
            },
            # Service account, scopes, etc. would go here in a real implementation
        }

        # Create the instance
        operation = self.instance_client.insert(
            project=self.project_id,
            zone=self.zone,
            instance_resource=instance
        )

        # Wait for the operation to complete
        # In a real implementation, you might want to make this asynchronous
        operation.result()

        return instance_name

    async def terminate_instance(self, instance_id: str) -> None:
        """
        Terminate a Compute Engine instance by ID.

        Args:
            instance_id: Instance name
        """
        operation = self.instance_client.delete(
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

        instances_list = self.instance_client.list(request=request)

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
            instance = self.instance_client.get(
                project=self.project_id,
                zone=self.zone,
                instance=instance_id
            )

            return self.STATUS_MAP.get(instance.status, 'unknown')

        except NotFound:
            return 'not_found'

    async def get_optimal_instance_type(
        self, cpu_required: int, memory_required_gb: int, disk_required_gb: int
    ) -> str:
        """
        Get the most cost-effective Compute Engine machine type that meets requirements.

        Args:
            cpu_required: Minimum number of vCPUs
            memory_required_gb: Minimum amount of memory in GB
            disk_required_gb: Minimum amount of disk space in GB (not used in GCP)

        Returns:
            Machine type name (e.g., 'n1-standard-1')
        """
        # Get machine types
        instance_types = await self.list_available_instance_types()

        # Filter to instances that meet CPU and memory requirements
        # (disk is separate in GCP)
        eligible_instances = []
        for instance in instance_types:
            if (instance['vcpu'] >= cpu_required and
                instance['memory_gb'] >= memory_required_gb):
                eligible_instances.append(instance)

        if not eligible_instances:
            raise ValueError(
                f"No instance type meets requirements: {cpu_required} vCPU, "
                f"{memory_required_gb} GB memory"
            )

        # Sort by vCPU and then memory as a cost proxy
        eligible_instances.sort(key=lambda x: (x['vcpu'], x['memory_gb']))

        # Return the name of the most cost-effective instance
        return eligible_instances[0]['name']

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