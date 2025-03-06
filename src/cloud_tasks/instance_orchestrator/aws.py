"""
AWS EC2 implementation of the InstanceManager interface.
"""
import time
from typing import Any, Dict, List, Optional

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from cloud_tasks.common.base import InstanceManager


class AWSEC2InstanceManager(InstanceManager):
    """AWS EC2 implementation of the InstanceManager interface."""

    # Map of instance statuses to standardized statuses
    STATUS_MAP = {
        'pending': 'starting',
        'running': 'running',
        'shutting-down': 'stopping',
        'terminated': 'terminated',
        'stopping': 'stopping',
        'stopped': 'stopped'
    }

    def __init__(self):
        self.ec2_client = None
        self.ec2_resource = None
        self.region = None

    async def initialize(self, config: Dict[str, Any]) -> None:
        """
        Initialize the EC2 instance manager with configuration.

        Args:
            config: AWS configuration with access_key, secret_key, and region
        """
        self.region = config['region']

        # Create EC2 client and resource
        self.ec2_client = boto3.client(
            'ec2',
            aws_access_key_id=config['access_key'],
            aws_secret_access_key=config['secret_key'],
            region_name=config['region']
        )

        self.ec2_resource = boto3.resource(
            'ec2',
            aws_access_key_id=config['access_key'],
            aws_secret_access_key=config['secret_key'],
            region_name=config['region']
        )

    async def list_available_instance_types(self) -> List[Dict[str, Any]]:
        """
        List available EC2 instance types with their specifications.

        Returns:
            List of dictionaries with instance types and their specifications
        """
        # Get all instance types that are offered in the region
        response = self.ec2_client.describe_instance_types()

        instance_types = []
        for instance_type in response['InstanceTypes']:
            # Extract relevant information
            instance_info = {
                'name': instance_type['InstanceType'],
                'vcpu': instance_type['VCpuInfo']['DefaultVCpus'],
                'memory_gb': instance_type['MemoryInfo']['SizeInMiB'] / 1024,
                'architecture': instance_type.get('ProcessorInfo', {}).get('SupportedArchitectures', ['x86_64'])[0],
            }

            # Add storage info if available
            if 'InstanceStorageInfo' in instance_type:
                instance_info['storage_gb'] = instance_type['InstanceStorageInfo'].get('TotalSizeInGB', 0)
            else:
                instance_info['storage_gb'] = 0

            instance_types.append(instance_info)

        return instance_types

    async def start_instance(
        self, instance_type: str, user_data: str, tags: Dict[str, str]
    ) -> str:
        """
        Start a new EC2 instance and return its ID.

        Args:
            instance_type: EC2 instance type (e.g., 't2.micro')
            user_data: Base64-encoded user data script
            tags: Dictionary of tags to apply to the instance

        Returns:
            EC2 instance ID
        """
        # Convert tags to AWS format
        aws_tags = [{'Key': key, 'Value': value} for key, value in tags.items()]

        # Create instance run request
        run_args = {
            'ImageId': await self._get_default_ami(),
            'InstanceType': instance_type,
            'MinCount': 1,
            'MaxCount': 1,
            'UserData': user_data,
            'TagSpecifications': [{
                'ResourceType': 'instance',
                'Tags': aws_tags
            }],
            # Use default security group
            # Add instance profile if needed for permissions
        }

        # Launch instance
        response = self.ec2_client.run_instances(**run_args)

        # Return the instance ID
        return response['Instances'][0]['InstanceId']

    async def terminate_instance(self, instance_id: str) -> None:
        """
        Terminate an EC2 instance by ID.

        Args:
            instance_id: EC2 instance ID
        """
        self.ec2_client.terminate_instances(InstanceIds=[instance_id])

    async def list_running_instances(self, tag_filter: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """
        List currently running EC2 instances, optionally filtered by tags.

        Args:
            tag_filter: Dictionary of tags to filter instances

        Returns:
            List of instance dictionaries with id, type, state, and launch_time
        """
        filters = []

        # Add tag filters if provided
        if tag_filter:
            for key, value in tag_filter.items():
                filters.append({
                    'Name': f'tag:{key}',
                    'Values': [value]
                })

        # Get instances
        response = self.ec2_client.describe_instances(Filters=filters)

        instances = []
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                # Skip terminated instances
                if instance['State']['Name'] == 'terminated':
                    continue

                # Extract relevant information
                instance_info = {
                    'id': instance['InstanceId'],
                    'type': instance['InstanceType'],
                    'state': self.STATUS_MAP[instance['State']['Name']],
                    'launch_time': instance['LaunchTime'].isoformat(),
                    'public_ip': instance.get('PublicIpAddress', ''),
                    'private_ip': instance.get('PrivateIpAddress', '')
                }

                # Extract tags
                if 'Tags' in instance:
                    instance_info['tags'] = {tag['Key']: tag['Value'] for tag in instance['Tags']}

                instances.append(instance_info)

        return instances

    async def get_instance_status(self, instance_id: str) -> str:
        """
        Get the current status of an EC2 instance.

        Args:
            instance_id: EC2 instance ID

        Returns:
            Standardized status string
        """
        try:
            response = self.ec2_client.describe_instances(InstanceIds=[instance_id])

            # Check if instance exists
            if not response['Reservations'] or not response['Reservations'][0]['Instances']:
                return 'not_found'

            # Get AWS state and map to standardized state
            aws_state = response['Reservations'][0]['Instances'][0]['State']['Name']
            return self.STATUS_MAP.get(aws_state, 'unknown')

        except ClientError as e:
            # Handle case where instance doesn't exist
            if e.response['Error']['Code'] == 'InvalidInstanceID.NotFound':
                return 'not_found'
            raise

    async def get_optimal_instance_type(
        self, cpu_required: int, memory_required_gb: int, disk_required_gb: int
    ) -> str:
        """
        Get the most cost-effective EC2 instance type that meets requirements.

        Args:
            cpu_required: Minimum number of vCPUs
            memory_required_gb: Minimum amount of memory in GB
            disk_required_gb: Minimum amount of disk space in GB

        Returns:
            Instance type name (e.g., 't2.micro')
        """
        # Get instance types with pricing
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

        # Sort by CPU + memory as a simple cost proxy (better would be to use actual pricing API)
        # This is a simplification - real implementation would use pricing data
        eligible_instances.sort(key=lambda x: x['vcpu'] + x['memory_gb'])

        # Return the name of the most cost-effective instance
        return eligible_instances[0]['name']

    async def _get_default_ami(self) -> str:
        """
        Get the latest Amazon Linux 2 AMI ID for the current region.

        Returns:
            AMI ID
        """
        # Get the latest Amazon Linux 2 AMI
        response = self.ec2_client.describe_images(
            Owners=['amazon'],
            Filters=[
                {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
                {'Name': 'state', 'Values': ['available']}
            ]
        )

        # Sort by creation date and get the latest
        amis = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)

        if not amis:
            raise ValueError(f"No Amazon Linux 2 AMI found in region {self.region}")

        return amis[0]['ImageId']