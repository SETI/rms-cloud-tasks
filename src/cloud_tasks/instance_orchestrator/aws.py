"""
AWS EC2 implementation of the InstanceManager interface.
"""
import time
import json
import logging
import traceback
from typing import Any, Dict, List, Optional
import datetime
import base64

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from cloud_tasks.common.base import InstanceManager

# Configure logging with periods for fractions of a second
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S.%f'  # Explicitly use period for fractions
)
logger = logging.getLogger(__name__)

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
        """Initialize without connecting to AWS yet."""
        self.ec2 = None
        self.ec2_client = None
        self.pricing_client = None
        self.region = None
        self.credentials = {}
        super().__init__()

    async def initialize(self, config: Dict[str, Any]) -> None:
        """
        Initialize AWS clients with the provided configuration.

        Args:
            config: Dictionary with AWS configuration

        Raises:
            ValueError: If required configuration is missing
        """
        required_keys = ['access_key', 'secret_key']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required AWS configuration: {key}")

        self.credentials = {
            'aws_access_key_id': config['access_key'],
            'aws_secret_access_key': config['secret_key'],
        }

        # Initialize with specified region or default
        self.region = config.get('region')

        # Store instance_types configuration if present
        self.instance_types = config.get('instance_types')
        if self.instance_types:
            if isinstance(self.instance_types, str):
                # If a single string was provided, convert to a list
                self.instance_types = [self.instance_types]
            logger.info(f"Instance types restricted to patterns: {self.instance_types}")

        # If no region specified, we'll find the cheapest one
        if self.region:
            self.ec2 = boto3.resource('ec2', region_name=self.region, **self.credentials)
            self.ec2_client = boto3.client('ec2', region_name=self.region, **self.credentials)
            self.pricing_client = boto3.client('pricing', region_name='us-east-1', **self.credentials)
            print(f"Initialized AWS EC2 in region {self.region}")
        else:
            # Just create clients with default region for now, will update later when finding cheapest region
            self.ec2 = boto3.resource('ec2', region_name='us-east-1', **self.credentials)
            self.ec2_client = boto3.client('ec2', region_name='us-east-1', **self.credentials)
            self.pricing_client = boto3.client('pricing', region_name='us-east-1', **self.credentials)
            print("No region specified, will determine cheapest region during instance selection")

    async def find_cheapest_region(self, instance_type: str = 't3.micro') -> str:
        """
        Find the cheapest AWS region for the given instance type.

        Args:
            instance_type: Instance type to check prices for (default: t3.micro)

        Returns:
            The region code with the lowest price
        """
        try:
            # Create pricing client in us-east-1 (only region that supports the pricing API)
            pricing_client = boto3.client('pricing', region_name='us-east-1', **self.credentials)

            # Get available regions
            ec2_client = boto3.client('ec2', region_name='us-east-1', **self.credentials)
            regions_response = ec2_client.describe_regions()
            regions = [region['RegionName'] for region in regions_response['Regions']]

            print(f"Checking prices across {len(regions)} regions for {instance_type}")

            region_prices = {}
            for region in regions:
                try:
                    # Get current price for the instance type in this region
                    response = pricing_client.get_products(
                        ServiceCode='AmazonEC2',
                        Filters=[
                            {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                            {'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': region},
                            {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
                            {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
                            {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'},
                        ],
                        MaxResults=10
                    )

                    if response['PriceList']:
                        price_data = json.loads(response['PriceList'][0])
                        on_demand = price_data['terms']['OnDemand']
                        price_dimensions = list(on_demand.values())[0]['priceDimensions']
                        price = float(list(price_dimensions.values())[0]['pricePerUnit']['USD'])
                        region_prices[region] = price
                        print(f"  {region}: ${price:.4f}/hour")
                except Exception as e:
                    print(f"  Error getting price for {region}: {e}")
                    continue

            if not region_prices:
                print("Could not retrieve prices for any region, using us-east-1 as default")
                return 'us-east-1'

            # Find the cheapest region
            cheapest_region = min(region_prices.items(), key=lambda x: x[1])[0]
            print(f"Cheapest region is {cheapest_region} at ${region_prices[cheapest_region]:.4f}/hour")
            return cheapest_region

        except Exception as e:
            print(f"Error finding cheapest region: {e}")
            print("Using us-east-1 as default region")
            return 'us-east-1'

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
        self, instance_type: str, user_data: str, tags: Dict[str, str],
        use_spot: bool = False, custom_image: Optional[str] = None
    ) -> str:
        """
        Start a new EC2 instance.

        Args:
            instance_type: EC2 instance type (e.g., 't3.micro')
            user_data: User data script to run at instance startup
            tags: Dictionary of tags to apply to the instance
            use_spot: Whether to use spot instances (cheaper but can be terminated)
            custom_image: Custom AMI ID or name to use

        Returns:
            EC2 instance ID
        """
        logger.info(f"Creating {'spot' if use_spot else 'on-demand'} instance of type {instance_type}")

        # Get a default AMI or use custom image
        if custom_image:
            # If it looks like an AMI ID, use it directly
            if custom_image.startswith('ami-'):
                ami_id = custom_image
                logger.info(f"Using custom AMI: {ami_id}")
            else:
                # Otherwise, search for an AMI by name
                try:
                    response = self.ec2_client.describe_images(
                        Filters=[
                            {'Name': 'name', 'Values': [custom_image]},
                            {'Name': 'state', 'Values': ['available']}
                        ]
                    )
                    if response['Images']:
                        # Sort by creation date to get the newest
                        images = sorted(response['Images'],
                                      key=lambda x: x.get('CreationDate', ''),
                                      reverse=True)
                        ami_id = images[0]['ImageId']
                        logger.info(f"Found AMI {ami_id} for name: {custom_image}")
                    else:
                        logger.warning(f"No AMI found for name: {custom_image}, using default")
                        ami_id = await self._get_default_ami()
                except Exception as e:
                    logger.error(f"Error finding AMI by name: {e}")
                    ami_id = await self._get_default_ami()
        else:
            ami_id = await self._get_default_ami()

        # Convert tags dictionary to AWS format
        aws_tags = [
            {'Key': key, 'Value': value}
            for key, value in tags.items()
        ]

        # Prepare instance run parameters
        run_params = {
            'ImageId': ami_id,
            'InstanceType': instance_type,
            'MinCount': 1,
            'MaxCount': 1,
            'UserData': user_data,
            'TagSpecifications': [
                {
                    'ResourceType': 'instance',
                    'Tags': aws_tags
                }
            ],
            'NetworkInterfaces': [
                {
                    'DeviceIndex': 0,
                    'AssociatePublicIpAddress': True,
                    'DeleteOnTermination': True
                }
            ]
        }

        # Use spot instances if requested
        if use_spot:
            # Create spot instance request
            spot_params = {
                'InstanceCount': 1,
                'Type': 'one-time',
                'LaunchSpecification': {
                    'ImageId': ami_id,
                    'InstanceType': instance_type,
                    'UserData': base64.b64encode(user_data.encode()).decode('utf-8'),
                    'NetworkInterfaces': [
                        {
                            'DeviceIndex': 0,
                            'AssociatePublicIpAddress': True,
                            'DeleteOnTermination': True
                        }
                    ]
                }
            }

            try:
                response = self.ec2_client.request_spot_instances(**spot_params)
                request_id = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']

                logger.info(f"Waiting for spot instance request {request_id} to be fulfilled")

                # Wait for the spot request to be fulfilled
                waiter = self.ec2_client.get_waiter('spot_instance_request_fulfilled')
                waiter.wait(SpotInstanceRequestIds=[request_id])

                # Get the instance ID from the spot request
                response = self.ec2_client.describe_spot_instance_requests(
                    SpotInstanceRequestIds=[request_id]
                )
                instance_id = response['SpotInstanceRequests'][0]['InstanceId']

                # Apply tags to the instance
                self.ec2_client.create_tags(
                    Resources=[instance_id],
                    Tags=aws_tags
                )

                logger.info(f"Created spot instance: {instance_id}")
                return instance_id

            except Exception as e:
                logger.error(f"Failed to create spot instance: {e}")
                logger.info("Falling back to on-demand instance")
                # Fall back to on-demand if spot request fails

        # Create on-demand instance
        try:
            response = self.ec2_client.run_instances(**run_params)
            instance_id = response['Instances'][0]['InstanceId']
            logger.info(f"Created on-demand instance: {instance_id}")
            return instance_id
        except Exception as e:
            logger.error(f"Failed to create instance: {e}")
            raise

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
        self, cpu_required: int, memory_required_gb: int, disk_required_gb: int, use_spot: bool = False
    ) -> str:
        """
        Get the most cost-effective EC2 instance type that meets requirements.
        If no region was specified during initialization, this method will also
        find and use the cheapest region.

        Args:
            cpu_required: Minimum number of vCPUs
            memory_required_gb: Minimum amount of memory in GB
            disk_required_gb: Minimum amount of disk space in GB
            use_spot: Whether to use spot instance pricing

        Returns:
            EC2 instance type (e.g., 't3.micro')
        """
        logger.info(f"Finding optimal instance type with: CPU={cpu_required}, Memory={memory_required_gb}GB, "
                   f"Disk={disk_required_gb}GB, Spot={use_spot}")

        # If no region was specified, find the cheapest one
        if not self.region:
            logger.info("No region specified, searching for cheapest region...")
            self.region = await self.find_cheapest_region()

            # Reinitialize clients with the new region
            self.ec2 = boto3.resource('ec2', region_name=self.region, **self.credentials)
            self.ec2_client = boto3.client('ec2', region_name=self.region, **self.credentials)
            logger.info(f"Selected region {self.region} for lowest cost")

        # Get available instance types
        instance_types = await self.list_available_instance_types()
        logger.debug(f"Found {len(instance_types)} available instance types in region {self.region}")

        # Filter to instance types that meet requirements
        eligible_instances = []
        for instance in instance_types:
            if (instance['vcpu'] >= cpu_required and
                instance['memory_gb'] >= memory_required_gb):
                # We don't filter on disk since EBS volumes can be attached
                eligible_instances.append(instance)

        logger.debug(f"Found {len(eligible_instances)} instance types that meet requirements:")
        for idx, instance in enumerate(eligible_instances):
            logger.debug(f"  [{idx+1}] {instance['name']}: {instance['vcpu']} vCPU, {instance['memory_gb']:.2f} GB memory")

        # Filter by instance_types if specified in configuration
        if self.instance_types:
            filtered_instances = []
            for instance in eligible_instances:
                instance_name = instance['name']
                # Check if instance matches any prefix or exact name
                for instance_type_pattern in self.instance_types:
                    if instance_name.startswith(instance_type_pattern) or instance_name == instance_type_pattern:
                        filtered_instances.append(instance)
                        break

            # Update eligible instances with filtered list
            if filtered_instances:
                eligible_instances = filtered_instances
                logger.debug(f"Filtered to {len(eligible_instances)} instance types based on instance_types configuration:")
                for idx, instance in enumerate(eligible_instances):
                    logger.debug(f"  [{idx+1}] {instance['name']}: {instance['vcpu']} vCPU, {instance['memory_gb']:.2f} GB memory")
            else:
                error_msg = f"No instances match the instance_types patterns: {self.instance_types}. Available instances meeting requirements: {[i['name'] for i in eligible_instances]}"
                logger.error(error_msg)
                raise ValueError(error_msg)

        if not eligible_instances:
            msg = f"No instance type meets requirements: {cpu_required} vCPU, {memory_required_gb} GB memory"
            logger.error(msg)
            raise ValueError(msg)

        # Use AWS Pricing API to get current prices
        pricing_data = {}
        logger.debug(f"Retrieving pricing data for {len(eligible_instances)} eligible instance types...")

        for instance in eligible_instances:
            instance_type = instance['name']
            logger.debug(f"Getting pricing for instance type: {instance_type}")

            try:
                if use_spot:
                    # Get spot price history
                    current_time = datetime.datetime.now()
                    start_time = current_time - datetime.timedelta(hours=1)

                    logger.debug(f"Retrieving spot price history for {instance_type} from {start_time} to {current_time}")

                    spot_response = self.ec2_client.describe_spot_price_history(
                        InstanceTypes=[instance_type],
                        ProductDescriptions=['Linux/UNIX'],
                        StartTime=start_time,
                        EndTime=current_time,
                        MaxResults=10
                    )

                    if spot_response['SpotPriceHistory']:
                        logger.debug(f"Found {len(spot_response['SpotPriceHistory'])} spot price records for {instance_type}")

                        # Log all spot prices found
                        for spot_price in spot_response['SpotPriceHistory']:
                            logger.debug(f"  Spot price: ${float(spot_price['SpotPrice']):.6f} in {spot_price['AvailabilityZone']} at {spot_price['Timestamp']}")

                        # Use the most recent spot price
                        price = float(spot_response['SpotPriceHistory'][0]['SpotPrice'])
                        pricing_data[instance_type] = price
                        logger.debug(f"  Selected spot price for {instance_type}: ${price:.6f}")
                    else:
                        logger.debug(f"No spot price history found for {instance_type}")
                else:
                    # Get on-demand price
                    logger.debug(f"Retrieving on-demand price for {instance_type} in region {self.region}")

                    response = self.pricing_client.get_products(
                        ServiceCode='AmazonEC2',
                        Filters=[
                            {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                            {'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': self.region},
                            {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
                            {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
                            {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'},
                        ],
                        MaxResults=10
                    )

                    if response['PriceList']:
                        logger.debug(f"Found pricing data for {instance_type}")
                        price_data = json.loads(response['PriceList'][0])

                        # Log the product details
                        product = price_data.get('product', {})
                        attributes = product.get('attributes', {})
                        logger.debug(f"  Product: {attributes.get('instanceType')} - {attributes.get('instanceFamily')}")

                        # Extract actual price
                        on_demand = price_data['terms']['OnDemand']
                        price_dimensions = list(on_demand.values())[0]['priceDimensions']
                        price = float(list(price_dimensions.values())[0]['pricePerUnit']['USD'])
                        pricing_data[instance_type] = price
                        logger.debug(f"  On-demand price for {instance_type}: ${price:.6f}")
                    else:
                        logger.debug(f"No pricing data found for {instance_type}")
            except Exception as e:
                logger.warning(f"Error getting pricing for {instance_type}: {e}")
                continue

        # Log the complete pricing data found
        if pricing_data:
            logger.debug("Retrieved pricing data for the following instance types:")
            for instance_type, price in pricing_data.items():
                logger.debug(f"  {instance_type}: ${price:.6f} per hour")
        else:
            logger.warning("Could not retrieve any pricing data from AWS API")

        # If we couldn't get pricing from API, fall back to our heuristic
        if not pricing_data:
            logger.warning("Could not get pricing data from AWS API, falling back to heuristic")
            # Sort by vCPU + memory as a simple cost heuristic
            eligible_instances.sort(key=lambda x: x['vcpu'] + x['memory_gb'])
            selected_type = eligible_instances[0]['name']
            logger.info(f"Selected {selected_type} based on heuristic (lowest vCPU + memory)")
            return selected_type

        # Select instance with the lowest price
        priced_instances = [(instance_type, price) for instance_type, price in pricing_data.items()]
        if not priced_instances:
            logger.warning("No pricing data found for eligible instance types, falling back to heuristic")
            eligible_instances.sort(key=lambda x: x['vcpu'] + x['memory_gb'])
            selected_type = eligible_instances[0]['name']
            logger.info(f"Selected {selected_type} based on heuristic (lowest vCPU + memory)")
            return selected_type

        priced_instances.sort(key=lambda x: x[1])  # Sort by price

        # Debug log for all priced instances in order
        logger.debug("Instance types sorted by price (cheapest first):")
        for i, (instance_type, price) in enumerate(priced_instances):
            logger.debug(f"  {i+1}. {instance_type}: ${price:.6f}/hour")

        selected_type = priced_instances[0][0]
        price = priced_instances[0][1]
        logger.info(f"Selected {selected_type} at ${price:.4f} per hour in {self.region}{' (spot)' if use_spot else ''}")
        return selected_type

    async def _get_default_ami(self) -> str:
        """
        Get the latest Ubuntu 24.04 LTS AMI ID for the current region.

        Returns:
            AMI ID
        """
        # Get the latest Ubuntu 24.04 LTS AMI (Canonical's AMIs)
        response = self.ec2_client.describe_images(
            Owners=['099720109477'],  # Canonical's AWS account ID
            Filters=[
                {'Name': 'name', 'Values': ['ubuntu/images/hvm-ssd/ubuntu-noble-24.04-amd64-server-*']},
                {'Name': 'state', 'Values': ['available']}
            ]
        )

        # Sort by creation date and get the latest
        amis = sorted(response['Images'], key=lambda x: x['CreationDate'], reverse=True)

        if not amis:
            raise ValueError(f"No Ubuntu 24.04 LTS AMI found in region {self.region}")

        return amis[0]['ImageId']