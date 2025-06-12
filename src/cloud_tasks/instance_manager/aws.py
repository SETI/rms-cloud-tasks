"""
AWS EC2 implementation of the InstanceManager interface.
"""

import base64
import datetime
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple, cast

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from ..common.config import AWSConfig

from .instance_manager import InstanceManager


# Notes:
# - AWS EC2 instances are per-region, not per-zone
# - AWS pricing is per-region for on-demand pricing and per-zone for spot instances
# - If a zone is not specified, the spot pricing for each zone in the region will be
#   returned; otherwise, the pricing for the specified zone will be returned.
# - For on-demand pricing, the pricing is returned for the region as a whole using
#   wildcards.
# - This means that when choosing an optimal instance type with spot pricing, we will
#   be locked to a single zone.


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

    _INSTANCE_TYPE_FAMILY_TO_PROCESSOR_FAMILY = {
        # General Purpose
        "m8g": "AWS Graviton4",
        "m8gd": "AWS Graviton4",
        "m7g": "AWS Graviton3",
        "m7gd": "AWS Graviton3",
        "m7i": "Intel Sapphire Rapids",
        "m7i-flex": "Intel Sapphire Rapids",
        "m7a": "AMD Genoa",
        "mac1": "Intel Core i7",
        "mac2": "Apple M1",
        "mac2-m2pro": "Apple M2 Pro",
        "m6g": "AWS Graviton2",
        "m6gd": "AWS Graviton2",
        "m6i": "Intel Ice Lake",
        "m6id": "Intel Ice Lake",
        "m6idn": "Intel Ice Lake",
        "m6in": "Intel Ice Lake",
        "m6a": "AMD Milan",
        "m5": "Intel Cascade Lake",
        "m5d": "Intel Cascade Lake",
        "m5n": "Intel Cascade Lake",
        "m5dn": "Intel Cascade Lake",
        "m5zn": "Intel Cascade Lake",
        "m5a": "AMD Naples",
        "m5ad": "AMD Naples",
        "m4": "Intel Haswell",
        "m1": "Intel Nehalem",
        "t4g": "AWS Graviton2",
        "t3": "Intel Skylake",
        "t3a": "AMD Naples",
        "t2": "Intel",  # Various generations
        # Compute Optimized
        "c8g": "AWS Graviton4",
        "c8gd": "AWS Graviton4",
        "c7g": "AWS Graviton3",
        "c7gd": "AWS Graviton3",
        "c7i": "Intel Sapphire Rapids",
        "c7i-flex": "Intel Sapphire Rapids",
        "c7id": "Intel Sapphire Rapids",
        "c7gn": "AWS Graviton3E",
        "c7a": "AMD Genoa",
        "c6g": "AWS Graviton2",
        "c6gd": "AWS Graviton2",
        "c6gn": "AWS Graviton2",
        "c6i": "Intel Ice Lake",
        "c6id": "Intel Ice Lake",
        "c6idn": "Intel Ice Lake",
        "c6in": "Intel Ice Lake",
        "c6a": "AMD Milan",
        "c6dn": "Intel Ice Lake",
        "c5": "Intel Cascade Lake",
        "c5d": "Intel Cascade Lake",
        "c5n": "Intel Cascade Lake",
        "c5a": "AMD Naples",
        "c5ad": "AMD Naples",
        "c4": "Intel Haswell",
        # Memory Optimized
        "r8g": "AWS Graviton4",
        "r8gd": "AWS Graviton4",
        "r7g": "AWS Graviton3",
        "r7gd": "AWS Graviton3",
        "r7i": "Intel Sapphire Rapids",
        "r7iz": "Intel Sapphire Rapids",
        "r7a": "AMD Genoa",
        "r6g": "AWS Graviton2",
        "r6gd": "AWS Graviton2",
        "r6i": "Intel Ice Lake",
        "r6id": "Intel Ice Lake",
        "r6idn": "Intel Ice Lake",
        "r6in": "Intel Ice Lake",
        "r6a": "AMD Milan",
        "r5": "Intel Cascade Lake",
        "r5d": "Intel Cascade Lake",
        "r5n": "Intel Cascade Lake",
        "r5dn": "Intel Cascade Lake",
        "r5b": "Intel Cascade Lake",
        "r5ad": "AMD Naples",
        "r5a": "AMD Naples",
        "r4": "Intel Broadwell",
        "x2gd": "AWS Graviton2",
        "x2idn": "Intel Ice Lake",
        "x2iedn": "Intel Ice Lake",
        "x2iezn": "Intel Cascade Lake",
        "x1": "Intel Haswell",
        "x1e": "Intel Haswell",
        "z1d": "Intel Skylake",
        "x8g": "AWS Graviton4",
        "u-6tb1": "Intel Cascade Lake",
        "u-9tb1": "Intel Cascade Lake",
        "u-12tb1": "Intel Cascade Lake",
        "u-18tb1": "Intel Cascade Lake",
        "u-24tb1": "Intel Cascade Lake",
        "u-3tb1": "Intel Cascade Lake",
        "u7i-6tb": "Intel Sapphire Rapids",
        "u7in-24tb": "Intel Sapphire Rapids",
        "u7i-12tb": "Intel Sapphire Rapids",
        # Storage Optimized
        "i8g": "AWS Graviton3E",
        "i7g": "AWS Graviton2",
        "i7gd": "AWS Graviton2",
        "i7i": "Intel Sapphire Rapids",
        "i7id": "Intel Sapphire Rapids",
        "i7ie": "Intel Sapphire Rapids",
        "i7iz": "Intel Sapphire Rapids",
        "i4g": "AWS Graviton2",
        "i4i": "Intel Ice Lake",
        "i3": "Intel Broadwell",
        "i3en": "Intel Broadwell",
        "i2": "Intel Ivy Bridge",
        "d3": "Intel Cascade Lake",
        "d3en": "Intel Cascade Lake",
        "d2": "Intel Haswell",
        "f2": "Intel Broadwell",
        "im4gn": "AWS Graviton2",
        "is4gen": "AWS Graviton2",
        "h1": "Intel Broadwell",
        # Accelerated Computing
        "p5": "Intel Sapphire Rapids",  # NVIDIA H100
        "p5e": "Intel Sapphire Rapids",  # NVIDIA H100
        "p5en": "Intel Sapphire Rapids",  # NVIDIA H100
        "p4d": "Intel Cascade Lake",  # NVIDIA A100
        "p4de": "Intel Cascade Lake",  # NVIDIA A100
        "p3": "Intel Broadwell",  # NVIDIA V100
        "p3dn": "Intel Broadwell",  # NVIDIA V100
        "p2": "Intel Broadwell",  # NVIDIA K80
        "inf2": "AWS Inferentia2",
        "dl2q": "AWS Inferentia2",
        "inf1": "Intel Cascade Lake",  # AWS Inferentia
        "trn1": "AWS Graviton",  # AWS Trainium
        "trn1n": "AWS Graviton",  # AWS Trainium
        "dl1": "Intel Cascade Lake",  # Habana Gaudi
        "f1": "Intel Broadwell",  # Xilinx UltraScale+ VU9P FPGA
        "g6": "Intel Sapphire Rapids",  # NVIDIA L4
        "g6e": "Intel Sapphire Rapids",  # NVIDIA L4
        "g6gd": "Intel Sapphire Rapids",  # NVIDIA L4
        "g5": "AMD Rome",  # NVIDIA A10G
        "g5g": "AWS Graviton2",  # NVIDIA T4G
        "g5gd": "AWS Graviton2",  # NVIDIA T4G, local NVMe
        "g4dn": "Intel Cascade Lake",  # NVIDIA T4
        "g4ad": "AMD Rome",  # NVIDIA Radeon Pro V520
        "g3": "Intel Broadwell",  # NVIDIA M60
        "vt1": "Intel Cascade Lake",  # Xilinx Alveo U30
        "gr6": "AWS Graviton4",
        # HPC Optimized
        "hpc6id": "Intel Ice Lake",
        "hpc6a": "AMD Milan",
        "hpc7g": "AWS Graviton3E",
        # Burstable Performance
        "t4g": "AWS Graviton2",
        "t3": "Intel Skylake",
        "t3a": "AMD Naples",
        "t2": "Intel",  # Various generations
        # Dense Storage
        "d3": "Intel Cascade Lake",
        "d3en": "Intel Cascade Lake",
        "h1": "Intel Broadwell",
        # Other/Legacy
        "a1": "AWS Graviton",
        "m3": "Intel Ivy Bridge",
        "c3": "Intel Ivy Bridge",
        "c1": "Intel Sandy Bridge",
        "r3": "Intel Ivy Bridge",
        "t1": "Unknown",
        "m2": "Intel Westmere",
        "cr1": "Intel Sandy Bridge",
        "hs1": "Intel Sandy Bridge",
        "u7in-16tb": "Intel Sapphire Rapids",
        "u7in-32tb": "Intel Sapphire Rapids",
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

        self._logger.info("Initializing AWS EC2 instance manager")

        self._credentials = {
            "aws_access_key_id": aws_config.access_key,
            "aws_secret_access_key": aws_config.secret_key,
        }

        # Initialize with specified region
        self._region = aws_config.region
        self._zone = aws_config.zone

        # If zone is provided but not region, extract region from zone
        region_from_zone = None
        if self._zone:
            # Extract region from zone (e.g., us-central1-a -> us-central1)
            region_from_zone = self._zone[:-1]
            self._logger.debug(f"Extracted region {self._region} from zone {self._zone}")
            if self._region is not None and self._region != region_from_zone:
                raise ValueError(
                    f"Region {self._region} does not match region {region_from_zone} extracted "
                    f"from zone {self._zone}"
                )
        if self._region is None and region_from_zone is not None:
            self._region = region_from_zone

        # TODO How do we know what the default region is so we can log it?

        self._ec2_client = boto3.client("ec2", region_name=self._region, **self._credentials)
        self._pricing_client = boto3.client(
            "pricing", region_name=self._PRICING_REGION, **self._credentials
        )

        self._logger.debug(f"Initialized AWS EC2: region '{self._region}', zone '{self._zone}'")

    async def get_available_instance_types(
        self, constraints: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Get available EC2 instance types with their specifications.

        This skips instance types that are bare metal or that don't support on-demand
        pricing (there really shouldn't be any instance types that support spot but not
        on-demand pricing).

        Args:
            constraints: Dictionary of constraints to filter instance types by. Constraints
                include::
                    "instance_types": List of regex patterns to filter instance types by name
                    "architecture": Architecture (X86_64 or ARM64)
                    "min_cpu_performance": Minimum acceptable CPU performance
                    "max_cpu_performance": Maximum acceptable CPU performance
                    "cpus_per_task": Number of vCPUs per task
                    "min_tasks_per_instance": Minimum number of tasks per instance
                    "max_tasks_per_instance": Maximum number of tasks per instance
                    "min_cpu": Minimum number of vCPUs
                    "max_cpu": Maximum number of vCPUs
                    "min_total_memory": Minimum total memory in GB
                    "max_total_memory": Maximum total memory in GB
                    "min_memory_per_cpu": Minimum memory per vCPU in GB
                    "max_memory_per_cpu": Maximum memory per vCPU in GB
                    "min_memory_per_task": Minimum memory per task in GB
                    "max_memory_per_task": Maximum memory per task in GB
                    "min_local_ssd": Minimum amount of local SSD storage in GB
                    "max_local_ssd": Maximum amount of local SSD storage in GB
                    "local_ssd_base_size": Base amount of local SSD storage in GB
                    "min_local_ssd_per_cpu": Minimum amount of local SSD storage per vCPU
                    "max_local_ssd_per_cpu": Maximum amount of local SSD storage per vCPU
                    "min_local_ssd_per_task": Minimum amount of local SSD storage per task
                    "max_local_ssd_per_task": Maximum amount of local SSD storage per task
                    "total_boot_disk_size": Total amount of boot disk storage in GB
                    "boot_disk_base_size": Base amount of boot disk storage in GB
                    "boot_disk_per_cpu": Amount of boot disk storage per vCPU
                    "boot_disk_per_task": Amount of boot disk storage per task
                    "use_spot": Whether to filter for spot-capable instance types

        Returns:
            Dictionary mapping instance type to a dictionary of instance type specifications::
                "name": instance type name
                "processor_type": processor type
                "performance_rank": performance rank of the instance type
                "vcpu": number of vCPUs
                "mem_gb": amount of RAM in GB
                "local_ssd_gb": amount of local SSD storage in GB
                "boot_disk_gb": amount of boot disk storage in GB
                "architecture": architecture of the instance type
                "supports_spot": whether the instance type supports spot pricing
                "description": description of the instance type
                "url": URL to the instance type details
        """
        if constraints is None:
            constraints = {}

        self._logger.debug("Listing available EC2 instance types")
        self._logger.debug(f"Constraints: {constraints}")

        # List instance types
        paginator = self._ec2_client.get_paginator("describe_instance_types")
        instance_types = {}

        # Paginate through all instance types
        for page in paginator.paginate():
            for instance_type in page["InstanceTypes"]:
                if constraints["instance_types"]:
                    match_ok = False
                    for type_filter in constraints["instance_types"]:
                        if re.match(type_filter, instance_type["InstanceType"]):
                            match_ok = True
                            break
                    if not match_ok:
                        continue

                if (
                    instance_type["BareMetal"]
                    or "on-demand" not in instance_type["SupportedUsageClasses"]
                ):
                    continue

                instance_type_family = instance_type["InstanceType"].split(".")[0]
                processor_family = None
                performance_rank = 0
                if instance_type_family in self._INSTANCE_TYPE_FAMILY_TO_PROCESSOR_FAMILY:
                    processor_family = self._INSTANCE_TYPE_FAMILY_TO_PROCESSOR_FAMILY[
                        instance_type_family
                    ]
                    if processor_family in self._PROCESSOR_FAMILY_TO_PERFORMANCE_RANKING:
                        performance_rank = self._PROCESSOR_FAMILY_TO_PERFORMANCE_RANKING[
                            processor_family
                        ]
                    else:
                        self._logger.warning(
                            f'Processor family "{processor_family}" is not in the processor type '
                            "ranking; ranking will be 0"
                        )
                else:
                    self._logger.warning(
                        f'Instance type "{instance_type["InstanceType"]}" with family '
                        f'"{instance_type_family}" is not in the processor family mapping; '
                        "performance ranking will be 0"
                    )

                instance_info = {
                    "name": instance_type["InstanceType"],
                    "vcpu": instance_type["VCpuInfo"]["DefaultVCpus"],
                    "processor_type": processor_family,
                    "performance_rank": performance_rank,
                    "architecture": instance_type["ProcessorInfo"]["SupportedArchitectures"][0],
                    "mem_gb": instance_type["MemoryInfo"]["SizeInMiB"] / 1024.0,
                    "boot_disk_gb": 0,  # Will fill in later
                    "local_ssd_gb": 0,  # Will fill in later
                    "supports_spot": "spot" in instance_type["SupportedUsageClasses"],
                    "description": instance_type["InstanceType"],
                    "url": None,
                }

                boot_disk_gb = self._get_boot_disk_size(instance_info, constraints)
                instance_info["boot_disk_gb"] = boot_disk_gb

                # Add storage info if available
                if "InstanceStorageInfo" in instance_type:
                    instance_info["local_ssd_gb"] = instance_type["InstanceStorageInfo"].get(
                        "TotalSizeInGB", 0
                    )

                if self._instance_matches_constraints(instance_info, constraints):
                    instance_types[instance_info["name"]] = instance_info

        return instance_types

    async def get_instance_pricing(
        self,
        instance_types: Dict[str, Dict[str, Any]],
        *,
        use_spot: bool = False,
        boot_disk_constraints: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Dict[str, float | str | None]]]:
        """
        Get the hourly price for one or more specific instance types.

        Note that AWS pricing is per-region for on-demand pricing and per-zone
        for spot instances.

        Args:
            instance_types: A dictionary mapping instance type to a dictionary of instance type
                specifications as returned by get_available_instance_types().
            use_spot: Whether to use spot pricing
            boot_disk_constraints: Dictionary of constraints used to determine the boot disk type and
                size. These are from the same config as the instance type constraints but are not
                used to filter instances.

        Returns:
            A dictionary mapping instance type to a dictionary of hourly price in USD::
                "cpu_price": Total price of CPU in USD/hour
                "per_cpu_price": Price of CPU in USD/vCPU/hour
                "mem_price": Total price of RAM in USD/hour
                "mem_per_gb_price": Price of RAM in USD/GB/hour
                "boot_disk_price": Total price of boot disk in USD/hour
                "boot_disk_per_gb_price": Price of boot disk in USD/GB/hour
                "local_ssd_price": Total price of local SSD in USD/hour
                "local_ssd_per_gb_price": Price of local SSD in USD/GB/hour
                "total_price": Total price of instance in USD/hour
                "total_price_per_cpu": Total price of instance in USD/vCPU/hour
                "zone": availability zone
            Plus the original instance type info keyed by availability zone. If any price is not
            available, it is set to None.
        """
        self._logger.debug(
            f"Getting pricing for {len(instance_types)} instance types (spot: {use_spot})"
        )
        self._logger.debug(f"Boot disk constraints: {boot_disk_constraints}")

        ret: Dict[str, Dict[str, Dict[str, float | str | None]]] = {}

        if len(instance_types) == 0:
            self._logger.warning("No instance types provided")
            return ret

        if self._region is None:
            raise RuntimeError("Region must be specified")

        boot_disk_type = "gp3"
        if boot_disk_constraints is not None:
            boot_disk_type = boot_disk_constraints.get("boot_disk_type", "gp3")
            if boot_disk_type is not None:
                boot_disk_type = boot_disk_type.lower()
                if boot_disk_type not in ["gp2", "gp3", "io1", "io2", "st1", "sc1", "standard"]:
                    raise ValueError(f"Invalid boot disk type: {boot_disk_type}")

        if use_spot:
            # Spot pricing

            # We get the most recent spot price for each instance type and availability
            # zone in this region
            now = datetime.datetime.now()
            spot_prices = self._ec2_client.describe_spot_price_history(
                InstanceTypes=list(instance_types.keys()),
                ProductDescriptions=["Linux/UNIX"],
                StartTime=now,
                EndTime=now,
                MaxResults=len(instance_types) * 10,  # For different availability zones
            )
            for price in spot_prices["SpotPriceHistory"]:
                zone = price["AvailabilityZone"]
                if self._zone is not None and self._zone != zone:
                    continue
                inst_type = price["InstanceType"]
                if inst_type not in ret:
                    ret[inst_type] = {}
                cpu_price = float(price["SpotPrice"])
                vcpus = instance_types[inst_type]["vcpu"]
                ret[inst_type][zone] = {
                    "cpu_price": round(cpu_price, 6),  # CPU price (combined CPU and memory)
                    "per_cpu_price": round(cpu_price / vcpus, 6),  # Per-CPU price
                    "mem_price": 0.0,  # Memory price (we don't have this)
                    "mem_per_gb_price": 0.0,  # Per-GB price (we don't have this)
                    "local_ssd_price": 0.0,  # Local SSD price (we don't have this)
                    "local_ssd_per_gb_price": 0.0,  # Local SSD per-GB price (we don't have this)
                    "boot_disk_price": 0.0,  # TODO
                    "boot_disk_per_gb_price": 0.0,  # TODO
                    "total_price": round(float(price["SpotPrice"]), 6),  # Total price
                    "total_price_per_cpu": round(float(price["SpotPrice"]) / vcpus, 6),
                    "zone": price["AvailabilityZone"],
                    **instance_types[price["InstanceType"]],
                }
                self._logger.debug(
                    f"Price for spot instance type: \"{price['InstanceType']}\" in "
                    f"zone \"{price['AvailabilityZone']}\" is ${float(price['SpotPrice']):.4f}/hour"
                )

        else:
            # Non-spot pricing
            pricing_dict: Dict[str, Dict[str, Any] | None] = {}  # inst_name -> pricing_data
            filter_list = [
                {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
                {"Type": "TERM_MATCH", "Field": "regionCode", "Value": self._region},
                {"Type": "TERM_MATCH", "Field": "marketoption", "Value": "OnDemand"},
                {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
                {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            ]
            if len(instance_types) <= 25:
                # If there are 25 or fewer instance types, use the instance type filter.
                # We choose this because there are 26 pages of responses in the pricing
                # API as of 2025-03-25 so this balances the number of API calls.
                for inst_name in instance_types:
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
                self._logger.debug("Getting on-demand price for all instance types")
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
                        for inst_name in instance_types:
                            ret[inst_name] = {}
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
            for inst_name, inst_info in instance_types.items():
                price_data = pricing_dict.get(inst_name)
                if price_data is None:
                    self._logger.warning(f"Could not find pricing data for {inst_name}")
                    ret[inst_name] = {}
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
                                f"{self._region}*": {
                                    "cpu_price": round(
                                        price, 6
                                    ),  # CPU price (combined CPU and memory)
                                    "per_cpu_price": round(
                                        price / float(attributes["vcpu"]), 6
                                    ),  # Per-CPU price
                                    "mem_price": 0.0,  # Memory price
                                    "mem_per_gb_price": 0.0,  # Per-GB price (we don't have this)
                                    "local_ssd_price": 0.0,  # Local SSD price (we don't have this)
                                    "local_ssd_per_gb_price": 0.0,  # Local SSD per-GB price (we don't have this)
                                    "boot_disk_price": 0.0,  # TODO
                                    "boot_disk_per_gb_price": 0.0,  # TODO
                                    "total_price": round(price, 6),  # Total price
                                    "total_price_per_cpu": round(
                                        price / float(attributes["vcpu"]), 6
                                    ),
                                    "zone": f"{self._region}*",
                                    **inst_info,
                                }
                            }
                            break
                    if inst_name in ret:
                        break
                if inst_name not in ret:
                    self._logger.warning(f"Could not find pricing data for {inst_name}")
                    ret[inst_name] = {}

        return ret

    async def start_instance(
        self,
        *,
        instance_type: str,
        boot_disk_size: int,  # GB
        startup_script: str,
        job_id: str,
        use_spot: bool,
        image: str,
        zone: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        Start a new EC2 instance.

        Args:
            instance_type: EC2 instance type (e.g., 't3.micro')
            user_data: User data script to run at instance startup
            tags: Dictionary of tags to apply to the instance
            use_spot: Whether to use spot instances (cheaper but can be terminated)
            custom_image: Custom AMI ID or name to use

        Returns:
            A tuple containing the ID of the started instance and the zone it was started
            in
        """
        self._logger.info(
            f"Creating {'spot' if use_spot else 'on-demand'} instance of type {instance_type}"
        )

        # Get a default AMI or use custom image
        if image:
            # If it looks like an AMI ID, use it directly
            if image.startswith("ami-"):
                ami_id = image
                self._logger.info(f"Using custom AMI: {ami_id}")
            else:
                # Otherwise, search for an AMI by name
                try:
                    response = self._ec2_client.describe_images(
                        Filters=[
                            {"Name": "name", "Values": [image]},
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
                        self._logger.info(f"Found AMI {ami_id} for name: {image}")
                    else:
                        self._logger.warning(f"No AMI found for name: {image}, using default")
                        ami_id = await self._get_default_ami()
                except Exception as e:
                    self._logger.error(f"Error finding AMI by name: {e}")
                    ami_id = await self._get_default_ami()
        else:
            ami_id = await self._get_default_ami()

        # Convert tags dictionary to AWS format
        aws_tags = [{"Key": "rms-cloud-tasks-job-id", "Value": job_id}]

        # Prepare instance run parameters
        run_params = {
            "ImageId": ami_id,
            "InstanceType": instance_type,
            "MinCount": 1,
            "MaxCount": 1,
            "UserData": startup_script,
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
                    "UserData": base64.b64encode(startup_script.encode()).decode("utf-8"),
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
            return instance_id, response["Instances"][0]["Placement"]["AvailabilityZone"]
        except Exception as e:
            self._logger.error(f"Failed to create instance: {e}")
            raise

    async def terminate_instance(self, instance_id: str, zone: Optional[str] = None) -> None:
        """
        Terminate an EC2 instance by ID.

        Args:
            instance_id: EC2 instance ID
            zone: Availability zone to terminate the instance in; not used for AWS
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
            filters.append({"Name": "tag:rms_cloud_tasks_job_id", "Values": [job_id]})
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
                        if tag["Key"] == "rms_cloud_tasks_job_id":
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
        self, constraints: Optional[Dict[str, Any]] = None
    ) -> Dict[str, float | str | None]:
        """
        Get the most cost-effective EC2 instance type that meets the constraints.

        Args:
            constraints: Dictionary of constraints to filter instance types by. Constraints
                include::
                    "instance_types": List of regex patterns to filter instance types by name
                    "architecture": Architecture (X86_64 or ARM64)
                    "min_cpu_performance": Minimum acceptable CPU performance
                    "max_cpu_performance": Maximum acceptable CPU performance
                    "cpus_per_task": Number of vCPUs per task
                    "min_tasks_per_instance": Minimum number of tasks per instance
                    "max_tasks_per_instance": Maximum number of tasks per instance
                    "min_cpu": Minimum number of vCPUs
                    "max_cpu": Maximum number of vCPUs
                    "min_total_memory": Minimum total memory in GB
                    "max_total_memory": Maximum total memory in GB
                    "min_memory_per_cpu": Minimum memory per vCPU in GB
                    "max_memory_per_cpu": Maximum memory per vCPU in GB
                    "min_memory_per_task": Minimum memory per task in GB
                    "max_memory_per_task": Maximum memory per task in GB
                    "min_local_ssd": Minimum amount of local SSD storage in GB
                    "max_local_ssd": Maximum amount of local SSD storage in GB
                    "local_ssd_base_size": Base amount of local SSD storage in GB
                    "min_local_ssd_per_cpu": Minimum amount of local SSD storage per vCPU
                    "max_local_ssd_per_cpu": Maximum amount of local SSD storage per vCPU
                    "min_local_ssd_per_task": Minimum amount of local SSD storage per task
                    "max_local_ssd_per_task": Maximum amount of local SSD storage per task
                    "total_boot_disk_size": Total amount of boot disk storage in GB
                    "boot_disk_base_size": Base amount of boot disk storage in GB
                    "boot_disk_per_cpu": Amount of boot disk storage per vCPU
                    "boot_disk_per_task": Amount of boot disk storage per task
                    "use_spot": Whether to filter for spot-capable instance types

        Returns:
            Dictionary of instance type pricing info as would be returned by get_instance_pricing
        """
        if constraints is None:
            constraints = {}

        self._logger.debug(
            f"Getting optimal instance type in region {self._region} and zone " f"{self._zone}"
        )
        self._logger.debug(f"Constraints: {constraints}")

        avail_instance_types = await self.get_available_instance_types(constraints)
        self._logger.debug(
            f"Found {len(avail_instance_types)} available instance types in region {self._region}"
        )

        if not avail_instance_types:
            raise ValueError("No instance type meets requirements")

        pricing_data = await self.get_instance_pricing(
            avail_instance_types,
            use_spot=constraints["use_spot"],
            boot_disk_constraints=constraints,
        )

        # Rearrange the pricing data into a dictionary of (machine_type, zone) -> price
        zone_pricing_data = {}
        for machine_type, price in pricing_data.items():
            if price is None:
                self._logger.debug(f"No pricing data found for {machine_type}; ignoring")
                continue
            for zone, price_in_zone in price.items():
                if price_in_zone is None:
                    self._logger.debug(
                        f"No pricing data found for {machine_type} in zone {zone}; ignoring"
                    )
                    continue
                zone_pricing_data[(machine_type, zone)] = price_in_zone

        if len(zone_pricing_data) == 0:
            raise ValueError("No pricing data found for any instance types")

        # Select instance with the lowest price
        priced_instances = [
            (machine_type, zone, price_info)
            for (machine_type, zone), price_info in zone_pricing_data.items()
        ]
        # Sort by price per vCPU, then by decreasing vCPU (this gives us the cheapest
        # instance type with the most vCPUs). We round the price to 2 decimal places so
        # that small differences in price don't make us choose an instance with fewer
        # vCPUs that would otherwise cost the same.
        priced_instances.sort(
            key=lambda x: (
                round(cast(float, x[2]["total_price_per_cpu"]), 2),
                -cast(int, x[2]["vcpu"]),
            )
        )

        self._logger.debug("Instance types sorted by price (cheapest and most vCPUs first):")
        for i, (machine_type, zone, price_info) in enumerate(priced_instances):
            self._logger.debug(
                f"  [{i+1:3d}] {machine_type:20s} in {zone:15s}: ${price_info['total_price']:10.6f}/hour"
            )

        selected_type, selected_zone, selected_price_info = priced_instances[0]
        total_price = selected_price_info["total_price"]
        self._logger.debug(
            f"Selected {selected_type} in {selected_zone} at ${total_price:.6f} per hour "
            f"{' (spot)' if constraints['use_spot'] else '(on demand)'}"
        )

        return selected_price_info

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
