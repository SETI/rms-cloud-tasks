"""
AWS EC2 implementation of the InstanceManager interface.
"""

import base64
import datetime
import json
import logging
from typing import Any, Dict, List, Optional

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from cloud_tasks.common.config import AWSConfig

from .instance_manager import InstanceManager


class AWSEC2InstanceManager(InstanceManager):
    """AWS EC2 implementation of the InstanceManager interface."""

    _DEFAULT_REGION = "us-west-1"
    # The pricing API is only available in us-east-1, eu-central-1, and ap-south-1
    _PRICING_REGION = "us-east-1"

    # Map of instance statuses to standardized statuses
    STATUS_MAP = {
        "pending": "starting",
        "running": "running",
        "shutting-down": "stopping",
        "terminated": "terminated",
        "stopping": "stopping",
        "stopped": "stopped",
    }

    def __init__(self, aws_config: AWSConfig) -> None:
        """Initialize the AWS EC2 instance manager.

        Args:
            aws_config: Dictionary with AWS configuration

        Raises:
            ValueError: If required configuration is missing
        """
        super().__init__(aws_config)
        self._logger = logging.getLogger(__name__)

        self._logger.info(f"Initializing AWS EC2 instance manager")

        self._credentials = {
            "aws_access_key_id": aws_config.access_key,
            "aws_secret_access_key": aws_config.secret_key,
        }

        # Store instance_types configuration if present
        self._instance_type = aws_config.instance_types
        if self._instance_type:
            if isinstance(self._instance_type, str):
                # If a single string was provided, convert to a list
                self._instance_type = [self._instance_type]
            self._logger.info(f"Instance types restricted to patterns: {self._instance_type}")

        # Initialize with specified region
        self._region = aws_config.region

        # TODO How do we know what the default region is so we can log it?

        self._ec2 = boto3.resource("ec2", region_name=self._region, **self._credentials)
        self._ec2_client = boto3.client("ec2", region_name=self._region, **self._credentials)
        self._pricing_client = boto3.client(
            "pricing", region_name=self._PRICING_REGION, **self._credentials
        )

        if self._region:
            self._logger.info(f"Initialized AWS EC2: region '{self._region}'")
        else:
            self._logger.info(
                "No region specified, will determine cheapest region during instance selection"
            )

    async def find_cheapest_region(self, instance_type: str = "t3.micro") -> str:
        """
        Find the cheapest AWS region for the given instance type.

        Args:
            instance_type: Instance type to check prices for (default: t3.micro)

        Returns:
            The region code with the lowest price
        """
        try:
            # Create pricing client in us-east-1 (only region that supports the pricing API)
            pricing_client = boto3.client(
                "pricing", region_name=self._PRICING_REGION, **self._credentials
            )

            # Get available regions
            ec2_client = boto3.client("ec2", region_name=self._DEFAULT_REGION, **self._credentials)
            regions_response = ec2_client.describe_regions()
            regions = [region["RegionName"] for region in regions_response["Regions"]]

            self._logger.info(f"Checking prices across {len(regions)} regions for {instance_type}")

            region_prices = {}
            for region in regions:
                try:
                    # Get current price for the instance type in this region
                    response = pricing_client.get_products(
                        ServiceCode="AmazonEC2",
                        Filters=[
                            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                            {"Type": "TERM_MATCH", "Field": "regionCode", "Value": region},
                            {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                            {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                            {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
                        ],
                        MaxResults=10,
                    )

                    if response["PriceList"]:
                        price_data = json.loads(response["PriceList"][0])
                        on_demand = price_data["terms"]["OnDemand"]
                        price_dimensions = list(on_demand.values())[0]["priceDimensions"]
                        price = float(list(price_dimensions.values())[0]["pricePerUnit"]["USD"])
                        region_prices[region] = price
                        print(f"  {region}: ${price:.4f}/hour")
                except Exception as e:
                    print(f"  Error getting price for {region}: {e}")
                    continue

            if not region_prices:
                print("Could not retrieve prices for any region, using us-east-1 as default")
                return self._DEFAULT_REGION

            # Find the cheapest region
            cheapest_region = min(region_prices.items(), key=lambda x: x[1])[0]
            self._logger.info(
                f"Cheapest region is {cheapest_region} at ${region_prices[cheapest_region]:.4f}/hour"
            )
            return cheapest_region

        except Exception as e:
            print(f"Error finding cheapest region: {e}")
            print("Using us-east-1 as default region")
            return self._DEFAULT_REGION

    async def list_available_instance_types(self) -> List[Dict[str, Any]]:
        """
        List available EC2 instance types with their specifications.

        This skips instance types that are bare metal or that don't support on-demand
        pricing (there really shouldn't be any instance types that support spot but not
        on-demand pricing).

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
        self._logger.debug("Listing available EC2 instance types")

        # List instance types
        paginator = self._ec2_client.get_paginator("describe_instance_types")
        instance_types = []

        # Paginate through all instance types
        for page in paginator.paginate():
            for instance_type in page["InstanceTypes"]:
                if (
                    instance_type["BareMetal"]
                    or "on-demand" not in instance_type["SupportedUsageClasses"]
                ):
                    continue
                instance_info = {
                    "name": instance_type["InstanceType"],
                    "vcpu": instance_type["VCpuInfo"]["DefaultVCpus"],
                    "ram_gb": instance_type["MemoryInfo"]["SizeInMiB"] / 1024.0,
                    "architecture": instance_type["ProcessorInfo"]["SupportedArchitectures"][0],
                    "storage_gb": 0,  # AWS separates storage from instance type
                    "supports_spot": "spot" in instance_type["SupportedUsageClasses"],
                    "description": instance_type["InstanceType"],
                    "url": None,
                }

                # Add storage info if available
                if "InstanceStorageInfo" in instance_type:
                    instance_info["storage_gb"] = instance_type["InstanceStorageInfo"].get(
                        "TotalSizeInGB", 0
                    )

                instance_types.append(instance_info)

        return instance_types

    async def get_instance_pricing(
        self, instance_type: List[str] | str, use_spot: bool = False
    ) -> Dict[str, Dict[str, Dict[str, float | None]]]:
        """
        Get the hourly price for one or more specific instance types.

        Note that AWS pricing is per-region for on-demand pricing and per-zone
        for spot instances.

        Args:
            instance_type: The instance type name (e.g., 't3.micro') or a list of instance type
                names
            use_spot: Whether to use spot pricing

        Returns:
            A dictionary mapping instance type to a dictionary of hourly price in USD:
                "cpu_price": CPU price in USD/hour
                "per_cpu_price": Per-CPU price in USD/hour
                "ram_price": RAM price in USD/hour
                "ram_per_gb_price": Per-GB RAM price in USD/hour
                "total_price": Total price in USD/hour
            If any price is not available, it is set to None.
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

        if use_spot:
            # Spot pricing

            # We get the most recent spot price for each instance type and availability
            # zone in this region
            now = datetime.datetime.now()
            spot_prices = self._ec2_client.describe_spot_price_history(
                InstanceTypes=instance_type,
                ProductDescriptions=["Linux/UNIX"],
                StartTime=now,
                EndTime=now,
                MaxResults=len(instance_type) * 10,  # For different availability zones
            )
            for price in spot_prices["SpotPriceHistory"]:
                if price["InstanceType"] not in ret:
                    ret[price["InstanceType"]] = {}
                ret[price["InstanceType"]][price["AvailabilityZone"]] = {
                    "cpu_price": float(price["SpotPrice"]),  # CPU price (combined CPU and memory)
                    "per_cpu_price": None,  # Per-CPU price (we don't have this)
                    "ram_price": 0.0,  # Memory price
                    "ram_per_gb_price": 0.0,  # Per-GB price (we don't have this)
                    "total_price": float(price["SpotPrice"]),  # Total price
                }
                self._logger.debug(
                    f"Price for spot instance type: \"{price['InstanceType']}\" in "
                    f"zone \"{price['AvailabilityZone']}\" is ${float(price['SpotPrice']):.4f}/hour"
                )

        else:
            # Non-spot pricing
            pricing_dict = {}  # inst_name -> pricing_data
            filter_list = [
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "regionCode", "Value": self._region},
                {"Type": "TERM_MATCH", "Field": "marketoption", "Value": "OnDemand"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            ]
            if len(instance_type) <= 25:
                # If there are 25 or fewer instance types, use the instance type filter.
                # We choose this because there are 26 pages of responses in the pricing
                # API as of 2025-03-25 so this balances the number of API calls.
                for inst_name in instance_type:
                    new_filter_list = filter_list + [
                        {"Type": "TERM_MATCH", "Field": "instanceType", "Value": inst_name}
                    ]
                    self._logger.debug(f"Getting on-demand price for instance type: {inst_name}")
                    response = self._pricing_client.get_products(
                        ServiceCode="AmazonEC2",
                        Filters=new_filter_list,
                        MaxResults=10,
                    )
                    if not response["PriceList"]:
                        pricing_dict[inst_name] = None
                    else:
                        pricing_dict[inst_name] = json.loads(response["PriceList"][0])
            else:
                # For lots of instance types, get on-demand price for all instance types and filter
                # later
                self._logger.debug(f"Getting on-demand price for all instance types")
                next_token = None
                page_no = 1
                while True:
                    self._logger.debug(f"Retrieving pricing data page {page_no}")
                    if next_token is None:
                        response = self._pricing_client.get_products(
                            ServiceCode="AmazonEC2",
                            Filters=filter_list,
                            MaxResults=100,  # AWS limit
                        )
                    else:
                        response = self._pricing_client.get_products(
                            ServiceCode="AmazonEC2",
                            Filters=filter_list,
                            MaxResults=100,
                            NextToken=next_token,
                        )
                    if not response["PriceList"]:
                        # We're missing pricing data for a huge chunk, so just give up
                        self._logger.error("No pricing data found - aborting")
                        for inst_name in instance_type:
                            ret[inst_name] = None
                        return ret
                    for price_item in response["PriceList"]:
                        price_data = json.loads(price_item)
                        attributes = price_data.get("product", {}).get("attributes", {})
                        if attributes is None:
                            continue
                        pricing_dict[attributes["instanceType"]] = price_data
                    page_no += 1
                    next_token = response.get("NextToken")
                    if next_token is None:
                        break

            # Now go through the instance types and match against the pricing data
            for inst_name in instance_type:
                price_data = pricing_dict.get(inst_name)
                if price_data is None:
                    self._logger.warning(f"Could not find pricing data for {inst_name}")
                    ret[inst_name] = None
                    continue
                attributes = price_data.get("product", {}).get("attributes", {})
                if attributes is None:
                    continue
                terms = price_data.get("terms", {}).get("OnDemand", {})
                for term_id, term in terms.items():
                    price_dimensions = term.get("priceDimensions", {})
                    for dim_id, dimension in price_dimensions.items():
                        description = dimension.get("description")
                        if description is None:
                            continue
                        desc_lower = description.lower()
                        if "reserved" in desc_lower or "reservation" in desc_lower:
                            continue
                        price_per_unit = dimension.get("pricePerUnit", {}).get("USD")
                        if price_per_unit:
                            price = float(price_per_unit)
                            self._logger.debug(
                                f"Found on-demand price for {inst_name}: ${price:.4f}/hour"
                            )
                            ret[inst_name] = {
                                f"{self._region}-*": {
                                    "cpu_price": price,  # CPU price (combined CPU and memory)
                                    "per_cpu_price": price
                                    / float(attributes["vcpu"]),  # Per-CPU price
                                    "ram_price": 0.0,  # Memory price
                                    "ram_per_gb_price": 0.0,  # Per-GB price (we don't have this)
                                    "total_price": price,  # Total price
                                }
                            }
                            break
                    if inst_name in ret:
                        break
                if inst_name not in ret:
                    self._logger.warning(f"Could not find pricing data for {inst_name}")
                    ret[inst_name] = None

        return ret

    async def start_instance(
        self,
        instance_type: str,
        user_data: str,
        tags: Dict[str, str],
        use_spot: bool = False,
        custom_image: Optional[str] = None,
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
        self._logger.info(
            f"Creating {'spot' if use_spot else 'on-demand'} instance of type {instance_type}"
        )

        # Get a default AMI or use custom image
        if custom_image:
            # If it looks like an AMI ID, use it directly
            if custom_image.startswith("ami-"):
                ami_id = custom_image
                self._logger.info(f"Using custom AMI: {ami_id}")
            else:
                # Otherwise, search for an AMI by name
                try:
                    response = self._ec2_client.describe_images(
                        Filters=[
                            {"Name": "name", "Values": [custom_image]},
                            {"Name": "state", "Values": ["available"]},
                        ]
                    )
                    if response["Images"]:
                        # Sort by creation date to get the newest
                        images = sorted(
                            response["Images"],
                            key=lambda x: x.get("CreationDate", ""),
                            reverse=True,
                        )
                        ami_id = images[0]["ImageId"]
                        self._logger.info(f"Found AMI {ami_id} for name: {custom_image}")
                    else:
                        self._logger.warning(
                            f"No AMI found for name: {custom_image}, using default"
                        )
                        ami_id = await self._get_default_ami()
                except Exception as e:
                    self._logger.error(f"Error finding AMI by name: {e}")
                    ami_id = await self._get_default_ami()
        else:
            ami_id = await self._get_default_ami()

        # Convert tags dictionary to AWS format
        aws_tags = [{"Key": key, "Value": value} for key, value in tags.items()]

        # Prepare instance run parameters
        run_params = {
            "ImageId": ami_id,
            "InstanceType": instance_type,
            "MinCount": 1,
            "MaxCount": 1,
            "UserData": user_data,
            "TagSpecifications": [{"ResourceType": "instance", "Tags": aws_tags}],
            "NetworkInterfaces": [
                {"DeviceIndex": 0, "AssociatePublicIpAddress": True, "DeleteOnTermination": True}
            ],
        }

        # Use spot instances if requested
        if use_spot:
            # Create spot instance request
            spot_params = {
                "InstanceCount": 1,
                "Type": "one-time",
                "LaunchSpecification": {
                    "ImageId": ami_id,
                    "InstanceType": instance_type,
                    "UserData": base64.b64encode(user_data.encode()).decode("utf-8"),
                    "NetworkInterfaces": [
                        {
                            "DeviceIndex": 0,
                            "AssociatePublicIpAddress": True,
                            "DeleteOnTermination": True,
                        }
                    ],
                },
            }

            try:
                response = self._ec2_client.request_spot_instances(**spot_params)
                request_id = response["SpotInstanceRequests"][0]["SpotInstanceRequestId"]

                self._logger.info(f"Waiting for spot instance request {request_id} to be fulfilled")

                # Wait for the spot request to be fulfilled
                waiter = self._ec2_client.get_waiter("spot_instance_request_fulfilled")
                waiter.wait(SpotInstanceRequestIds=[request_id])

                # Get the instance ID from the spot request
                response = self._ec2_client.describe_spot_instance_requests(
                    SpotInstanceRequestIds=[request_id]
                )
                instance_id = response["SpotInstanceRequests"][0]["InstanceId"]

                # Apply tags to the instance
                self._ec2_client.create_tags(Resources=[instance_id], Tags=aws_tags)

                self._logger.info(f"Created spot instance: {instance_id}")
                return instance_id

            except Exception as e:
                self._logger.error(f"Failed to create spot instance: {e}")
                self._logger.info("Falling back to on-demand instance")
                # Fall back to on-demand if spot request fails

        # Create on-demand instance
        try:
            response = self._ec2_client.run_instances(**run_params)
            instance_id = response["Instances"][0]["InstanceId"]
            self._logger.info(f"Created on-demand instance: {instance_id}")
            return instance_id
        except Exception as e:
            self._logger.error(f"Failed to create instance: {e}")
            raise

    async def terminate_instance(self, instance_id: str) -> None:
        """
        Terminate an EC2 instance by ID.

        Args:
            instance_id: EC2 instance ID
        """
        self._ec2_client.terminate_instances(InstanceIds=[instance_id])

    async def list_running_instances(
        self, job_id: Optional[str] = None, include_non_job: bool = False
    ) -> List[Dict[str, Any]]:
        """
        List currently running Compute Engine instances, optionally filtered by job_id.

        Args:
            job_id: Job ID to filter instances
            include_non_job: Include instances that do not have a job_id tag

        Returns:
            List of instance dictionaries with id, type, state, and creation_time
        """
        filters = []

        if job_id:
            self._logger.debug(f"Listing running instances with job_id filter '{job_id}'")
            filters.append({"Name": "tag:rms_cloud_run_job_id", "Values": [job_id]})
        else:
            self._logger.debug("Listing running instances")

        # Get instances
        response = self._ec2_client.describe_instances(Filters=filters)

        instances = []
        for reservation in response["Reservations"]:
            for instance in reservation["Instances"]:
                # Extract relevant information
                instance_info = {
                    "id": instance["InstanceId"],
                    "type": instance["InstanceType"],
                    "state": self.STATUS_MAP[instance["State"]["Name"]],
                    "creation_time": instance["LaunchTime"].isoformat(),
                    "zone": instance["Placement"]["AvailabilityZone"],
                }

                if "Tags" in instance:
                    for tag in instance["Tags"]:
                        if tag["Key"] == "rms_cloud_run_job_id":
                            inst_job_id = tag["Value"]
                            if job_id and inst_job_id != job_id:
                                self._logger.debug(
                                    f"Skipping instance {instance['InstanceId']} because it has "
                                    f"job_id {inst_job_id}"
                                )
                                break
                            instance_info["job_id"] = inst_job_id
                            break
                if "job_id" not in instance_info and not include_non_job:
                    self._logger.debug(
                        f"Skipping instance {instance['InstanceId']} because it has no job_id tag"
                    )
                    continue  # Skip if no job_id tag found

                if "PrivateIpAddress" in instance:
                    instance_info["private_ip"] = instance["PrivateIpAddress"]
                if "PublicIpAddress" in instance:
                    instance_info["public_ip"] = instance["PublicIpAddress"]

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
            response = self._ec2_client.describe_instances(InstanceIds=[instance_id])

            # Check if instance exists
            if not response["Reservations"] or not response["Reservations"][0]["Instances"]:
                return "not_found"

            # Get AWS state and map to standardized state
            aws_state = response["Reservations"][0]["Instances"][0]["State"]["Name"]
            return self.STATUS_MAP.get(aws_state, "unknown")

        except ClientError as e:
            # Handle case where instance doesn't exist
            if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
                return "not_found"
            raise

    async def get_optimal_instance_type(
        self,
        cpu_required: int,
        memory_required_gb: int,
        disk_required_gb: int,
        use_spot: bool = False,
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
        self._logger.info(
            f"Finding optimal instance type with: CPU={cpu_required}, Memory={memory_required_gb}GB, "
            f"Disk={disk_required_gb}GB, Spot={use_spot}"
        )

        # If no region was specified, find the cheapest one
        if not self._region:
            self._logger.info("No region specified, searching for cheapest region...")
            self._region = await self.find_cheapest_region()

            # Reinitialize clients with the new region
            self._ec2 = boto3.resource("ec2", region_name=self._region, **self._credentials)
            self._ec2_client = boto3.client("ec2", region_name=self._region, **self._credentials)
            self._logger.info(f"Selected region {self._region} for lowest cost")
        else:
            self._logger.info(f"Using specified region {self._region}")

        # Get available instance types
        instance_types = await self.list_available_instance_types()
        self._logger.debug(
            f"Found {len(instance_types)} available instance types in region {self._region}"
        )

        # Filter to instance types that meet requirements
        eligible_instances = []
        for instance in instance_types:
            if instance["vcpu"] >= cpu_required and instance["memory_gb"] >= memory_required_gb:
                # We don't filter on disk since EBS volumes can be attached
                eligible_instances.append(instance)

        self._logger.debug(
            f"Found {len(eligible_instances)} instance types that meet requirements:"
        )
        for idx, instance in enumerate(eligible_instances):
            self._logger.debug(
                f"  [{idx+1}] {instance['name']}: {instance['vcpu']} vCPU, {instance['memory_gb']:.2f} GB memory"
            )

        # Filter by instance_types if specified in configuration
        if self._instance_type:
            filtered_instances = []
            for instance in eligible_instances:
                instance_name = instance["name"]
                # Check if instance matches any prefix or exact name
                for instance_type_pattern in self._instance_type:
                    if (
                        instance_name.startswith(instance_type_pattern)
                        or instance_name == instance_type_pattern
                    ):
                        filtered_instances.append(instance)
                        break

            # Update eligible instances with filtered list
            if filtered_instances:
                eligible_instances = filtered_instances
                self._logger.debug(
                    f"Filtered to {len(eligible_instances)} instance types based on instance_types configuration:"
                )
                for idx, instance in enumerate(eligible_instances):
                    self._logger.debug(
                        f"  [{idx+1}] {instance['name']}: {instance['vcpu']} vCPU, {instance['memory_gb']:.2f} GB memory"
                    )
            else:
                error_msg = f"No instances match the instance_types patterns: {self._instance_type}. Available instances meeting requirements: {[i['name'] for i in eligible_instances]}"
                self._logger.error(error_msg)
                raise ValueError(error_msg)

        if not eligible_instances:
            msg = f"No instance type meets requirements: {cpu_required} vCPU, {memory_required_gb} GB memory"
            self._logger.error(msg)
            raise ValueError(msg)

        # Use AWS Pricing API to get current prices
        pricing_data = {}
        self._logger.debug(
            f"Retrieving pricing data for {len(eligible_instances)} eligible instance types..."
        )

        for instance in eligible_instances:
            instance_type = instance["name"]
            self._logger.debug(f"Getting pricing for instance type: {instance_type}")

            try:
                if use_spot:
                    # Get spot price history
                    current_time = datetime.datetime.now()
                    start_time = current_time - datetime.timedelta(hours=1)

                    self._logger.debug(
                        f"Retrieving spot price history for {instance_type} from {start_time} to {current_time}"
                    )

                    spot_response = self._ec2_client.describe_spot_price_history(
                        InstanceTypes=[instance_type],
                        ProductDescriptions=["Linux/UNIX"],
                        StartTime=start_time,
                        EndTime=current_time,
                        MaxResults=10,
                    )

                    if spot_response["SpotPriceHistory"]:
                        self._logger.debug(
                            f"Found {len(spot_response['SpotPriceHistory'])} spot price records for {instance_type}"
                        )

                        # Log all spot prices found
                        for spot_price in spot_response["SpotPriceHistory"]:
                            self._logger.debug(
                                f"  Spot price: ${float(spot_price['SpotPrice']):.6f} in {spot_price['AvailabilityZone']} at {spot_price['Timestamp']}"
                            )

                        # Use the most recent spot price
                        price = float(spot_response["SpotPriceHistory"][0]["SpotPrice"])
                        pricing_data[instance_type] = price
                        self._logger.debug(
                            f"  Selected spot price for {instance_type}: ${price:.6f}"
                        )
                    else:
                        self._logger.debug(f"No spot price history found for {instance_type}")
                else:
                    # Get on-demand price
                    self._logger.debug(
                        f"Retrieving on-demand price for {instance_type} in region {self._region}"
                    )

                    response = self._pricing_client.get_products(
                        ServiceCode="AmazonEC2",
                        Filters=[
                            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
                            {"Type": "TERM_MATCH", "Field": "regionCode", "Value": self._region},
                            {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                            {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                            {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
                        ],
                        MaxResults=10,
                    )

                    if response["PriceList"]:
                        self._logger.debug(f"Found pricing data for {instance_type}")
                        price_data = json.loads(response["PriceList"][0])

                        # Log the product details
                        product = price_data.get("product", {})
                        attributes = product.get("attributes", {})
                        self._logger.debug(
                            f"  Product: {attributes.get('instanceType')} - {attributes.get('instanceFamily')}"
                        )

                        # Extract actual price
                        on_demand = price_data["terms"]["OnDemand"]
                        price_dimensions = list(on_demand.values())[0]["priceDimensions"]
                        price = float(list(price_dimensions.values())[0]["pricePerUnit"]["USD"])
                        pricing_data[instance_type] = price
                        self._logger.debug(f"  On-demand price for {instance_type}: ${price:.6f}")
                    else:
                        self._logger.debug(f"No pricing data found for {instance_type}")
            except Exception as e:
                self._logger.warning(f"Error getting pricing for {instance_type}: {e}")
                continue

        # Log the complete pricing data found
        if pricing_data:
            self._logger.debug("Retrieved pricing data for the following instance types:")
            for instance_type, price in pricing_data.items():
                self._logger.debug(f"  {instance_type}: ${price:.6f} per hour")
        else:
            self._logger.warning("Could not retrieve any pricing data from AWS API")

        # If we couldn't get pricing from API, fall back to our heuristic
        if not pricing_data:
            self._logger.warning(
                "Could not get pricing data from AWS API, falling back to heuristic"
            )
            # Sort by vCPU + memory as a simple cost heuristic
            eligible_instances.sort(key=lambda x: x["vcpu"] + x["memory_gb"])
            selected_type = eligible_instances[0]["name"]
            self._logger.info(f"Selected {selected_type} based on heuristic (lowest vCPU + memory)")
            return selected_type

        # Select instance with the lowest price
        priced_instances = [(instance_type, price) for instance_type, price in pricing_data.items()]
        if not priced_instances:
            self._logger.warning(
                "No pricing data found for eligible instance types, falling back to heuristic"
            )
            eligible_instances.sort(key=lambda x: x["vcpu"] + x["memory_gb"])
            selected_type = eligible_instances[0]["name"]
            self._logger.info(f"Selected {selected_type} based on heuristic (lowest vCPU + memory)")
            return selected_type

        priced_instances.sort(key=lambda x: x[1])  # Sort by price

        # Debug log for all priced instances in order
        self._logger.debug("Instance types sorted by price (cheapest first):")
        for i, (instance_type, price) in enumerate(priced_instances):
            self._logger.debug(f"  {i+1}. {instance_type}: ${price:.6f}/hour")

        selected_type = priced_instances[0][0]
        price = priced_instances[0][1]
        self._logger.info(
            f"Selected {selected_type} at ${price:.4f} per hour in {self._region}{' (spot)' if use_spot else ''}"
        )
        return selected_type

    async def _get_default_ami(self) -> str:
        """
        Get the latest Ubuntu 24.04 LTS AMI ID for the current region.

        Returns:
            AMI ID
        """
        # Get the latest Ubuntu 24.04 LTS AMI (Canonical's AMIs)
        response = self._ec2_client.describe_images(
            Owners=["099720109477"],  # Canonical's AWS account ID
            Filters=[
                {
                    "Name": "name",
                    "Values": ["ubuntu/images/hvm-ssd/ubuntu-noble-24.04-amd64-server-*"],
                },
                {"Name": "state", "Values": ["available"]},
            ],
        )

        # Sort by creation date and get the latest
        amis = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)

        if not amis:
            raise ValueError(f"No Ubuntu 24.04 LTS AMI found in region {self._region}")

        return amis[0]["ImageId"]

    async def list_available_images(self) -> List[Dict[str, Any]]:
        """
        List available AMIs in the current region.
        Returns only standard AWS images and user's own images, excludes third-party Marketplace images.

        Returns:
            List of dictionaries with AMI information including id, name, description, and platform
        """
        self._logger.info(f"Listing available AMIs in region {self._region}")

        # List standard AWS images
        aws_images_response = self._ec2_client.describe_images(
            Owners=["amazon"],  # Standard AWS-owned images
            Filters=[
                {"Name": "state", "Values": ["available"]},
                # Limit to common operating systems to avoid an excessive number of results
                {
                    "Name": "name",
                    "Values": [
                        "amzn2-ami-hvm-*",  # Amazon Linux 2
                        "al2023-ami-*",  # Amazon Linux 2023
                        "ubuntu/images/hvm-ssd/ubuntu-*",  # Ubuntu
                        "RHEL-*",  # Red Hat Enterprise Linux
                        "debian-*",  # Debian
                        "fedora-*",  # Fedora
                        "suse-*",  # SUSE Linux
                    ],
                },
            ],
        )

        # List user's own images
        user_images_response = self._ec2_client.describe_images(
            Owners=["self"],  # Images owned by the user
        )

        # Combine results
        all_images = aws_images_response["Images"] + user_images_response["Images"]

        # Sort by creation date
        all_images = sorted(all_images, key=lambda x: x.get("CreationDate", ""), reverse=True)

        # Format for return
        formatted_images = []
        for image in all_images:
            # {'PlatformDetails': 'Linux/UNIX', 'UsageOperation': 'RunInstances',
            # 'BlockDeviceMappings': [{'Ebs': {'DeleteOnTermination': True, 'Iops': 3000,
            # 'SnapshotId': 'snap-01e17fe7a2a2b97c4', 'VolumeSize': 2, 'VolumeType':
            # 'gp3', 'Throughput': 125, 'Encrypted': False}, 'DeviceName': '/dev/xvda'}],
            # 'Description': 'Amazon Linux 2023 AMI 2023.6.20250317.2 x86_64 Minimal HVM
            # kernel-6.1', 'EnaSupport': True, 'Hypervisor': 'xen', 'ImageOwnerAlias':
            # 'amazon', 'Name': 'al2023-ami-minimal-2023.6.20250317.2-kernel-6.1-x86_64',
            # 'RootDeviceName': '/dev/xvda', 'RootDeviceType': 'ebs', 'SriovNetSupport':
            # 'simple', 'VirtualizationType': 'hvm', 'BootMode': 'uefi-preferred',
            # 'DeprecationTime': '2025-06-22T21:09:00.000Z', 'ImdsSupport': 'v2.0',
            # 'ImageId': 'ami-06e58da439b5eef26', 'ImageLocation':
            # 'amazon/al2023-ami-minimal-2023.6.20250317.2-kernel-6.1-x86_64', 'State':
            # 'available', 'OwnerId': '137112412989', 'CreationDate':
            # '2025-03-24T21:09:23.000Z', 'Public': True, 'Architecture': 'x86_64',
            # 'ImageType': 'machine'}
            if image.get("State") != "available":
                continue
            image_info = {
                "id": image["ImageId"],
                "name": image.get("Name", "No Name"),
                "description": image.get("Description", "No Description"),
                "family": image.get("PlatformDetails", "No Family"),
                "creation_date": image.get("CreationDate", "Unknown"),
                "source": "AWS" if image.get("ImageOwnerAlias") == "amazon" else "User",
                "project": "N/A",
                "status": image.get(
                    "State", "unknown"
                ),  # status for consistency with other providers
            }
            formatted_images.append(image_info)

        self._logger.info(f"Found {len(formatted_images)} available AMIs")
        return formatted_images

    async def get_available_regions(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        """
        Return all available AWS regions and their attributes.

        Args:
            prefix: Optional prefix to filter regions by name

        Returns:
            Dictionary of region names mapped to their information:
            - name: Region name (e.g., 'us-west-1')
            - description: Region description
            - endpoint: Region endpoint
            - zones: List of availability zones in the region
        """
        self._logger.debug("Listing available AWS regions")

        # Get all regions in a single API call
        regions_response = self._ec2_client.describe_regions(AllRegions=True)

        # Build the region dictionary
        region_dict = {}
        for region in regions_response["Regions"]:
            region_name = region["RegionName"]

            # Apply prefix filtering if specified
            if prefix and not region_name.startswith(prefix):
                continue

            # Create a client for this specific region
            regional_ec2_client = boto3.client("ec2", region_name=region_name, **self._credentials)

            # Get zones for this specific region
            zone_names = []
            try:
                zones_response = regional_ec2_client.describe_availability_zones(
                    AllAvailabilityZones=True
                )
                zone_names = [
                    zone["ZoneName"]
                    for zone in zones_response["AvailabilityZones"]
                    if zone["ZoneType"] == "availability-zone"
                ]
            except Exception as e:
                self._logger.warning(
                    f"Error getting availability zones for region {region_name}: {e}"
                )

            region_info = {
                "name": region_name,
                "description": f"AWS Region {region_name}",
                "endpoint": region["Endpoint"],
                "zones": zone_names,
                "opt_in_status": region.get("OptInStatus", "unknown"),
            }
            region_dict[region_name] = region_info

        self._logger.debug(
            f"Found {len(region_dict)} available regions: "
            f"{', '.join(sorted(region_dict.keys()))}"
        )
        return region_dict
