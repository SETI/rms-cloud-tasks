# Manually verified 4/29/2025

import pytest
from unittest.mock import patch

from cloud_tasks.queue_manager import create_queue
from cloud_tasks.common.config import Config, AWSConfig, GCPConfig


@pytest.fixture
def mock_aws_queue():
    """Mock AWS queue implementation."""
    with patch("cloud_tasks.queue_manager.aws.AWSSQSQueue") as mock:
        # Create a mock instance that will be returned
        instance = mock.return_value
        # Add attributes that we'll verify
        instance._queue_name = None
        instance._sqs = None
        yield mock


@pytest.fixture
def mock_gcp_queue():
    """Mock GCP queue implementation."""
    with patch("cloud_tasks.queue_manager.gcp.GCPPubSubQueue") as mock:
        # Create a mock instance that will be returned
        instance = mock.return_value
        # Add attributes that we'll verify
        instance._queue_name = None
        instance._project_id = None
        yield mock


@pytest.mark.asyncio
async def test_create_queue_aws_with_config(mock_aws_queue):
    """Test creating an AWS queue with config."""
    aws_config = AWSConfig(
        region="us-west-2", queue_name="test-queue", access_key="test-key", secret_key="test-secret"
    )
    config = Config(provider="AWS", aws=aws_config)

    queue = await create_queue(config)

    # Verify the queue was created with the correct config
    mock_aws_queue.assert_called_once_with(aws_config, visibility_timeout=None, exactly_once=None)
    assert queue == mock_aws_queue.return_value

    # Verify config was passed correctly to the instance
    instance = mock_aws_queue.call_args[0][0]
    assert instance.region == "us-west-2"
    assert instance.queue_name == "test-queue"
    assert instance.access_key == "test-key"
    assert instance.secret_key == "test-secret"


@pytest.mark.asyncio
async def test_create_queue_gcp_with_config(mock_gcp_queue):
    """Test creating a GCP queue with config."""
    gcp_config = GCPConfig(
        project_id="test-project", queue_name="test-queue", credentials_file=None
    )
    config = Config(provider="GCP", gcp=gcp_config)

    queue = await create_queue(config)

    # Verify the queue was created with the correct config
    mock_gcp_queue.assert_called_once_with(gcp_config, visibility_timeout=None, exactly_once=None)
    assert queue == mock_gcp_queue.return_value

    # Verify config was passed correctly to the instance
    instance = mock_gcp_queue.call_args[0][0]
    assert instance.project_id == "test-project"
    assert instance.queue_name == "test-queue"
    assert instance.credentials_file is None


@pytest.mark.asyncio
async def test_create_queue_invalid_provider():
    """Test creating a queue with an invalid provider."""
    with pytest.raises(ValueError) as exc_info:
        await create_queue(provider="INVALID")

    assert "Unsupported queue provider: INVALID" in str(exc_info.value)


@pytest.mark.asyncio
async def test_create_queue_missing_provider():
    """Test creating a queue without specifying a provider."""
    with pytest.raises(ValueError) as exc_info:
        await create_queue()

    assert "provider argument is required when config is not given" in str(exc_info.value)


@pytest.mark.asyncio
async def test_create_queue_unsupported_provider_with_config():
    """Test creating a queue with an unsupported provider using Config object."""
    # Create a Config object and bypass validation to set an unsupported provider
    config = Config()
    object.__setattr__(config, "provider", "UNSUPPORTED")

    with pytest.raises(ValueError) as exc_info:
        await create_queue(config)

    assert "Unsupported provider: UNSUPPORTED" in str(exc_info.value)


@pytest.mark.asyncio
async def test_create_queue_unsupported_provider_no_config():
    """Test creating a queue with an unsupported provider without a Config object."""
    with pytest.raises(ValueError) as exc_info:
        await create_queue(provider="UNSUPPORTED")

    assert "Unsupported queue provider: UNSUPPORTED" in str(exc_info.value)
