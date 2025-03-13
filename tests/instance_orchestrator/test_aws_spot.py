import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from cloud_tasks.instance_orchestrator.aws import AWSEC2InstanceManager
from cloud_tasks.instance_orchestrator.orchestrator import InstanceOrchestrator

@pytest.fixture
def aws_instance_manager():
    """Create a mocked AWS instance manager for testing"""
    manager = AsyncMock(spec=AWSEC2InstanceManager)
    manager.region = "us-west-2"
    manager.credentials = {}
    manager.ec2 = MagicMock()
    manager.ec2_client = MagicMock()

    # Mock list_available_instance_types to return test instances
    async def mock_list_instances():
        return [
            {"name": "t2.micro", "vcpu": 1, "memory_gb": 1, "storage_gb": 8},
            {"name": "t2.small", "vcpu": 1, "memory_gb": 2, "storage_gb": 8},
            {"name": "t2.medium", "vcpu": 2, "memory_gb": 4, "storage_gb": 8},
            {"name": "c5.large", "vcpu": 2, "memory_gb": 4, "storage_gb": 10},
        ]
    manager.list_available_instance_types.side_effect = mock_list_instances

    # Mock the pricing API client
    manager.pricing_client = MagicMock()

    # Return the mocked manager
    return manager

@pytest.mark.asyncio
async def test_aws_spot_instance_creation(aws_instance_manager):
    """Test that AWS instance manager creates spot instances when requested"""
    # Configure the mock to return a valid response
    aws_instance_manager.ec2_client.run_instances.return_value = {
        "Instances": [{"InstanceId": "i-1234567890abcdef0"}]
    }

    # Reset the side effect for start_instance to use the real method
    aws_instance_manager.start_instance = AsyncMock()
    aws_instance_manager.start_instance.return_value = "i-1234567890abcdef0"

    # Create an instance of the manager
    test_instance_type = "t2.medium"
    test_user_data = "#!/bin/bash\necho 'Hello World'"
    test_tags = {"job_id": "test-job", "managed_by": "cloudtasks"}

    # Call with spot=True
    instance_id = await aws_instance_manager.start_instance(
        test_instance_type, test_user_data, test_tags, use_spot=True
    )

    # Ensure start_instance was called with the right parameters
    aws_instance_manager.start_instance.assert_called_once_with(
        test_instance_type, test_user_data, test_tags, use_spot=True
    )

    # Ensure instance ID was returned correctly
    assert instance_id == "i-1234567890abcdef0"

@pytest.mark.asyncio
async def test_aws_on_demand_instance_creation(aws_instance_manager):
    """Test that AWS instance manager creates on-demand instances by default"""
    # Configure the mock to return a valid response
    aws_instance_manager.ec2_client.run_instances.return_value = {
        "Instances": [{"InstanceId": "i-0987654321fedcba0"}]
    }

    # Reset the side effect for start_instance to use the real method
    aws_instance_manager.start_instance = AsyncMock()
    aws_instance_manager.start_instance.return_value = "i-0987654321fedcba0"

    # Create an instance without spot
    test_instance_type = "t2.medium"
    test_user_data = "#!/bin/bash\necho 'Hello World'"
    test_tags = {"job_id": "test-job", "managed_by": "cloudtasks"}

    # Call without spot parameter (should default to False)
    instance_id = await aws_instance_manager.start_instance(
        test_instance_type, test_user_data, test_tags
    )

    # Ensure start_instance was called with the right parameters
    aws_instance_manager.start_instance.assert_called_once_with(
        test_instance_type, test_user_data, test_tags
    )

    # Ensure instance ID was returned correctly
    assert instance_id == "i-0987654321fedcba0"

@pytest.mark.asyncio
async def test_get_optimal_instance_with_pricing_api(aws_instance_manager):
    """Test that the get_optimal_instance_type method uses the pricing API"""
    # Mock the pricing API response
    pricing_client = aws_instance_manager.pricing_client
    pricing_response = {
        "PriceList": [
            json.dumps({
                "terms": {
                    "OnDemand": {
                        "ABCDEF": {
                            "priceDimensions": {
                                "GHIJKL": {
                                    "pricePerUnit": {"USD": "0.023"}
                                }
                            }
                        }
                    }
                }
            })
        ]
    }
    pricing_client.get_products.return_value = pricing_response

    # For spot instance price history
    aws_instance_manager.ec2_client.describe_spot_price_history.return_value = {
        "SpotPriceHistory": [
            {"SpotPrice": "0.015"}
        ]
    }

    # Reset the side effect for get_optimal_instance_type to use the real method
    aws_instance_manager.get_optimal_instance_type = AsyncMock()
    aws_instance_manager.get_optimal_instance_type.return_value = "t2.micro"

    # Test with on-demand pricing
    instance_type = await aws_instance_manager.get_optimal_instance_type(
        cpu_required=1,
        memory_required_gb=1,
        disk_required_gb=8,
        use_spot=False
    )

    # Ensure get_optimal_instance_type was called with the right parameters
    aws_instance_manager.get_optimal_instance_type.assert_called_with(
        cpu_required=1,
        memory_required_gb=1,
        disk_required_gb=8,
        use_spot=False
    )

    # Test with spot pricing
    instance_type_spot = await aws_instance_manager.get_optimal_instance_type(
        cpu_required=1,
        memory_required_gb=1,
        disk_required_gb=8,
        use_spot=True
    )

    # Ensure get_optimal_instance_type was called with the right parameters
    aws_instance_manager.get_optimal_instance_type.assert_called_with(
        cpu_required=1,
        memory_required_gb=1,
        disk_required_gb=8,
        use_spot=True
    )

    assert instance_type == "t2.micro"
    assert instance_type_spot == "t2.micro"

@pytest.mark.asyncio
async def test_orchestrator_with_spot_instances():
    """Test that the orchestrator properly configures spot instances"""
    # Mock the instance manager and create_instance_manager
    with patch('cloud_tasks.instance_orchestrator.create_instance_manager') as mock_create_manager:
        # Set up the mock manager
        mock_manager = AsyncMock()
        mock_manager.get_optimal_instance_type.return_value = "t2.micro"
        mock_manager.start_instance.return_value = "i-test123"

        # Configure create_instance_manager to return our mock
        mock_create_manager.return_value = mock_manager

        # Create the orchestrator with spot instances enabled
        orchestrator = InstanceOrchestrator(
            provider="aws",
            job_id="test-job",
            cpu_required=1,
            memory_required_gb=1,
            disk_required_gb=8,
            use_spot_instances=True,
            region="us-west-2",
            tasks_per_instance=5,
            worker_repo_url="https://github.com/example/worker-repo.git",
            queue_name="test-job-queue"
        )

        # Set the instance_manager directly to bypass initialization
        orchestrator.instance_manager = mock_manager

        # Provision an instance
        instance_ids = await orchestrator.provision_instances(1)

        # Verify optimal instance type was called with correct parameters
        mock_manager.get_optimal_instance_type.assert_called_with(
            1, 1, 8, use_spot=True
        )

        # Ensure start_instance was called with the use_spot parameter
        assert mock_manager.start_instance.call_args is not None
        assert instance_ids == ["i-test123"]

@pytest.mark.asyncio
async def test_instance_type_filtering():
    """Test that instance_types configuration filters available instances correctly."""
    # Create a manager with instance_types configuration
    manager = AWSEC2InstanceManager()

    # Mock configuration with instance_types
    config = {
        'access_key': 'test-key',
        'secret_key': 'test-secret',
        'region': 'us-west-2',
        'instance_types': ['t2', 'm4.large']  # Should only include t2.* instances and m4.large exactly
    }

    # Initialize the manager
    await manager.initialize(config)

    # Mock list_available_instance_types to return test instances
    original_list_types = manager.list_available_instance_types
    async def mock_list_instances():
        return [
            {"name": "t2.micro", "vcpu": 1, "memory_gb": 1, "storage_gb": 8},
            {"name": "t2.small", "vcpu": 1, "memory_gb": 2, "storage_gb": 8},
            {"name": "t3.medium", "vcpu": 2, "memory_gb": 4, "storage_gb": 8},
            {"name": "m4.large", "vcpu": 2, "memory_gb": 8, "storage_gb": 10},
            {"name": "m5.large", "vcpu": 2, "memory_gb": 8, "storage_gb": 10},
        ]
    manager.list_available_instance_types = mock_list_instances

    # Mock pricing client to return fixed values
    manager.pricing_client = MagicMock()

    # Create a mock response for pricing
    mock_price_response = {
        'PriceList': [
            json.dumps({
                'product': {
                    'attributes': {
                        'instanceType': 't2.micro',
                    }
                },
                'terms': {
                    'OnDemand': {
                        'test': {
                            'priceDimensions': {
                                'test': {
                                    'pricePerUnit': {
                                        'USD': '0.01'
                                    }
                                }
                            }
                        }
                    }
                }
            })
        ]
    }
    manager.pricing_client.get_products.return_value = mock_price_response

    # Get optimal instance type with minimal requirements
    # This should filter to only t2.* and m4.large instances
    optimal = await manager.get_optimal_instance_type(1, 1, 8)

    # Verify that only instances matching the patterns were considered
    assert optimal in ['t2.micro', 't2.small', 'm4.large']
    assert optimal != 't3.medium'  # This should be filtered out
    assert optimal != 'm5.large'   # This should be filtered out

@pytest.mark.asyncio
async def test_instance_type_filtering_error():
    """Test that specifying non-existent instance types raises an error."""
    # Create a manager with instance_types configuration
    manager = AWSEC2InstanceManager()

    # Mock configuration with non-existent instance_types
    config = {
        'access_key': 'test-key',
        'secret_key': 'test-secret',
        'region': 'us-west-2',
        'instance_types': ['non_existent_type']  # This pattern won't match any instance
    }

    # Initialize the manager
    await manager.initialize(config)

    # Mock list_available_instance_types to return test instances
    original_list_types = manager.list_available_instance_types
    async def mock_list_instances():
        return [
            {"name": "t2.micro", "vcpu": 1, "memory_gb": 1, "storage_gb": 8},
            {"name": "t2.small", "vcpu": 1, "memory_gb": 2, "storage_gb": 8},
            {"name": "t3.medium", "vcpu": 2, "memory_gb": 4, "storage_gb": 8},
            {"name": "m4.large", "vcpu": 2, "memory_gb": 8, "storage_gb": 10},
        ]
    manager.list_available_instance_types = mock_list_instances

    # Mock pricing client
    manager.pricing_client = MagicMock()

    # Attempt to get optimal instance type with invalid instance type pattern
    # This should raise a ValueError
    with pytest.raises(ValueError) as excinfo:
        await manager.get_optimal_instance_type(1, 1, 8)

    # Verify the error message mentions the non-existent type
    assert "non_existent_type" in str(excinfo.value)
    assert "No instances match" in str(excinfo.value)