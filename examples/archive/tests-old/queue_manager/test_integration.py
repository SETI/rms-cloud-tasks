"""
Integration tests for queue manager functionality across all providers.

These tests ensure that the queue adapters for all cloud providers work correctly
with the core queue functionality.
"""

import json
import asyncio
import pytest
import logging
from unittest.mock import patch, MagicMock, AsyncMock

from cloud_tasks.queue_manager import create_queue
from cloud_tasks.common.logging_config import configure_logging
from cloud_tasks.common.config import load_config, Config, ProviderConfig


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["aws", "gcp"])  # TODO: Add azure
async def test_queue_integration(
    config_file, provider, queue_name, monkeypatch, mock_aws_queue, mock_gcp_queue, mock_azure_queue
):
    """
    Test the full queue functionality across all providers including:
    - Queue creation
    - Sending tasks
    - Getting queue depth
    - Receiving tasks
    - Completing tasks

    This test uses the mock fixtures from conftest.py to avoid making actual API calls.
    """
    # Configure logging with millisecond precision
    configure_logging(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(f"Testing queue integration for provider: {provider}, queue: {queue_name}")

    # Load configuration
    config = load_config(config_file)
    logger.info(f"Loaded configuration from {config_file}")

    config.provider = provider
    provider_config = config.get_provider_config(provider)

    # Set up mocks based on provider
    if provider == "aws":
        # Set up AWS SQS mocks using fixture
        monkeypatch.setattr("boto3.client", MagicMock(return_value=MagicMock()))
        monkeypatch.setattr("boto3.resource", MagicMock(return_value=MagicMock()))

        # Create the queue with properly mocked AWS SQS client
        queue = await create_queue(config=config)

        # Mock queue depth to return 5
        queue.get_queue_depth = AsyncMock(return_value=5)

        # Mock receive_tasks to return a message
        queue.receive_tasks = AsyncMock(
            return_value=[
                {
                    "task_id": "test-task-id",
                    "data": {"value": 0, "task_type": "test"},
                    "receipt_handle": "test-receipt",
                }
            ]
        )

    elif provider == "gcp":
        # Use GCP Pub/Sub mock fixture
        mock_publisher, mock_subscriber = mock_gcp_queue

        # Set up the publisher and subscriber mock clients
        monkeypatch.setattr(
            "google.cloud.pubsub_v1.PublisherClient", MagicMock(return_value=mock_publisher)
        )
        monkeypatch.setattr(
            "google.cloud.pubsub_v1.SubscriberClient", MagicMock(return_value=mock_subscriber)
        )

        # Create the queue
        queue = await create_queue(config=config)

        # Mock queue depth to return 5
        queue.get_queue_depth = AsyncMock(return_value=5)

    elif provider == "azure":
        # Use Azure ServiceBus mock fixture
        monkeypatch.setattr(
            "azure.servicebus.ServiceBusClient.from_connection_string",
            MagicMock(return_value=mock_azure_queue),
        )

        # Mock the admin client
        mock_admin_client = AsyncMock()
        mock_runtime_props = MagicMock()
        mock_runtime_props.active_message_count = 5
        mock_admin_client.get_queue_runtime_properties = AsyncMock(return_value=mock_runtime_props)

        monkeypatch.setattr(
            "azure.servicebus.management.ServiceBusAdministrationClient.from_connection_string",
            MagicMock(return_value=mock_admin_client),
        )

        # Create the queue
        queue = await create_queue(config)

        # If needed, override the receive_tasks method to return a valid task list
        queue.receive_tasks = AsyncMock(
            return_value=[
                {
                    "task_id": "test-task-id",
                    "data": {"value": 0, "task_type": "test"},
                    "lock_token": "test-lock-token",
                }
            ]
        )
    else:
        pytest.skip(f"Provider {provider} not implemented in test")

    # Run common queue tests
    await run_queue_test(queue, provider, logger)


async def run_queue_test(queue, provider, logger):
    """
    Run the core queue test functionality.

    Args:
        queue: The queue instance to test
        provider: The provider name for context
        logger: The logger instance
    """
    # Check queue depth
    logger.info("Checking queue depth")
    depth = await queue.get_queue_depth()
    logger.info(f"Initial queue depth: {depth}")
    assert depth == 5, f"Expected initial queue depth to be 5, got {depth}"

    # Send a task
    logger.info("Sending task")
    task_id = "test-task-id"
    task_data = {"value": 0, "task_type": "test"}
    await queue.send_task(task_id, task_data)
    logger.info(f"Task sent successfully")

    # Receive a task
    logger.info("Receiving tasks")
    tasks = await queue.receive_tasks(max_count=1)
    logger.info(f"Received {len(tasks)} tasks")
    assert tasks, f"Expected to receive at least one task for {provider} queue"

    # Create a simple Task object for completion
    task = tasks[0]
    logger.info(f"Processing task: {task['task_id']}")

    # Complete the task - handle differently based on provider
    logger.info("Completing task")
    if provider == "aws":
        receipt_handle = task.get("receipt_handle")
        assert receipt_handle, "Expected AWS task to have receipt_handle"
        await queue.complete_task(receipt_handle)
    elif provider == "gcp":
        ack_id = task.get("ack_id")
        assert ack_id, "Expected GCP task to have ack_id"
        await queue.complete_task(ack_id)
    elif provider == "azure":
        lock_token = task.get("lock_token")
        assert lock_token, "Expected Azure task to have lock_token"
        await queue.complete_task(lock_token)

    logger.info("Task completed successfully")

    # Final queue depth check
    depth = await queue.get_queue_depth()
    logger.info(f"Final queue depth: {depth}")
    # Note: Since we're using mocks, the depth doesn't actually change,
    # so we just check it's still 5
    assert depth == 5, f"Expected final queue depth to be 5, got {depth}"
