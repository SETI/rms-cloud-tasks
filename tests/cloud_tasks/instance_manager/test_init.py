"""
Tests for the instance manager factory function.
"""

import pytest
from unittest.mock import patch

from cloud_tasks.instance_manager import create_instance_manager
from cloud_tasks.common.config import Config, AWSConfig, GCPConfig


@pytest.fixture
def mock_aws_instance_manager():
    """Mock AWS EC2 instance manager implementation."""
    with patch("cloud_tasks.instance_manager.aws.AWSEC2InstanceManager") as mock:
        yield mock


@pytest.fixture
def mock_gcp_instance_manager():
    """Mock GCP Compute instance manager implementation."""
    with patch("cloud_tasks.instance_manager.gcp.GCPComputeInstanceManager") as mock:
        yield mock


@pytest.mark.asyncio
async def test_create_instance_manager_aws(mock_aws_instance_manager):
    """Test creating an AWS instance manager."""
    aws_config = AWSConfig(
        region="us-west-2", queue_name="test-queue", access_key="test-key", secret_key="test-secret"
    )
    config = Config(provider="AWS", aws=aws_config)

    instance_manager = await create_instance_manager(config)

    mock_aws_instance_manager.assert_called_once_with(aws_config)
    assert instance_manager == mock_aws_instance_manager.return_value


@pytest.mark.asyncio
async def test_create_instance_manager_gcp(mock_gcp_instance_manager):
    """Test creating a GCP instance manager."""
    gcp_config = GCPConfig(
        project_id="test-project", queue_name="test-queue", credentials_file=None
    )
    config = Config(provider="GCP", gcp=gcp_config)

    instance_manager = await create_instance_manager(config)

    mock_gcp_instance_manager.assert_called_once_with(gcp_config)
    assert instance_manager == mock_gcp_instance_manager.return_value


@pytest.mark.asyncio
async def test_create_instance_manager_unsupported_provider():
    """Test creating an instance manager with an unsupported provider."""
    # Create a Config object and bypass validation to set an unsupported provider
    config = Config()
    object.__setattr__(config, "provider", "UNSUPPORTED")

    with pytest.raises(ValueError) as exc_info:
        await create_instance_manager(config)

    assert "Unsupported provider: UNSUPPORTED" in str(exc_info.value)


@pytest.mark.asyncio
async def test_create_instance_manager_case_sensitive(mock_aws_instance_manager):
    """Test that provider string must be uppercase."""
    aws_config = AWSConfig(
        region="us-west-2", queue_name="test-queue", access_key="test-key", secret_key="test-secret"
    )
    config = Config(provider="AWS", aws=aws_config)  # Must be uppercase

    instance_manager = await create_instance_manager(config)

    mock_aws_instance_manager.assert_called_once()
    assert instance_manager == mock_aws_instance_manager.return_value


@pytest.mark.asyncio
async def test_create_instance_manager_missing_provider():
    """Test creating an instance manager without specifying a provider."""
    config = Config()

    with pytest.raises(ValueError) as exc_info:
        await create_instance_manager(config)

    assert "Provider name not provided or detected in config" in str(exc_info.value)
