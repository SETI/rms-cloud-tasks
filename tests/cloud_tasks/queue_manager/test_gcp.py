"""
Tests for the GCP Pub/Sub queue adapter.
"""

import pytest
import sys
import uuid
import warnings
from pathlib import Path
from unittest.mock import patch, MagicMock
from google.api_core import exceptions as gcp_exceptions

# Filter coroutine warnings for these tests
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

# Add the src directory to the path so we can import cloud_tasks modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from cloud_tasks.queue_manager.gcp import GCPPubSubQueue


@pytest.fixture
def mock_pubsub_client():
    """Create mocked Pub/Sub clients with all necessary method mocks."""
    with patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher_cls, patch(
        "google.cloud.pubsub_v1.SubscriberClient"
    ) as mock_subscriber_cls:
        # Create the mock instances
        publisher = MagicMock()
        subscriber = MagicMock()

        # Set up the class mocks to return our instances
        mock_publisher_cls.return_value = publisher
        mock_subscriber_cls.return_value = subscriber

        # Mock the from_service_account_file class methods
        mock_publisher_cls.from_service_account_file.return_value = publisher
        mock_subscriber_cls.from_service_account_file.return_value = subscriber

        # Setup mock topic and subscription paths
        publisher.topic_path.return_value = "projects/test-project/topics/test-queue-topic"
        subscriber.subscription_path.return_value = (
            "projects/test-project/subscriptions/test-queue-subscription"
        )

        # Mock the topic operations
        publisher.get_topic.side_effect = gcp_exceptions.NotFound("Topic not found")
        publisher.create_topic.return_value = MagicMock(name="test-topic")

        # Mock the subscription operations
        subscriber.get_subscription.side_effect = gcp_exceptions.NotFound("Subscription not found")
        subscriber.create_subscription.return_value = MagicMock(name="test-subscription")

        # Setup mock pull for queue depth
        mock_pull_response = MagicMock()
        mock_pull_response.received_messages = []
        subscriber.pull.return_value = mock_pull_response

        yield (publisher, subscriber)


@pytest.fixture
def gcp_config():
    """Create a mock GCP configuration."""
    return MagicMock(
        project_id="test-project",
        queue_name="test-queue",
        credentials_file=None,  # Test with default credentials
    )


@pytest.fixture
def gcp_queue(mock_pubsub_client, gcp_config):
    """Fixture to provide a GCPPubSubQueue instance."""
    return GCPPubSubQueue(gcp_config)


@pytest.mark.asyncio
async def test_initialize(gcp_queue, mock_pubsub_client):
    """Test initializing the GCP Pub/Sub queue."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Verify paths are set up correctly
    assert gcp_queue._topic_path == "projects/test-project/topics/test-queue-topic"
    assert (
        gcp_queue._subscription_path
        == "projects/test-project/subscriptions/test-queue-subscription"
    )

    # Topic and subscription should not be created yet (lazy initialization)
    assert not mock_publisher.create_topic.called
    assert not mock_subscriber.create_subscription.called

    # Trigger creation by sending a task
    task_id = f"test-task-{uuid.uuid4()}"
    task_data = {"value": 42}
    await gcp_queue.send_task(task_id, task_data)

    # Now verify that topic and subscription were created
    assert mock_publisher.create_topic.called
    assert mock_subscriber.create_subscription.called


@pytest.mark.asyncio
async def test_send_task(gcp_queue, mock_pubsub_client):
    """Test sending a task to the queue."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Setup mock for publisher.publish
    future = MagicMock()
    future.result.return_value = "message-id-1234"
    mock_publisher.publish.return_value = future

    # Send task
    task_id = f"test-task-{uuid.uuid4()}"
    task_data = {"value": 42}
    await gcp_queue.send_task(task_id, task_data)

    # Verify publish was called
    mock_publisher.publish.assert_called_once()
    args, kwargs = mock_publisher.publish.call_args
    assert args[0] == gcp_queue._topic_path
    assert "task_id" in kwargs
    assert kwargs["task_id"] == task_id


@pytest.mark.asyncio
async def test_receive_tasks(gcp_queue, mock_pubsub_client):
    """Test receiving tasks from the queue."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Create a mock message
    mock_message = MagicMock()
    mock_message.ack_id = "test-ack-id"
    mock_message.message = MagicMock()
    mock_message.message.data = ('{"task_id": "test-task-id", "data": {"key": "value"}}').encode(
        "utf-8"
    )

    # Create a mock response with the message
    mock_response = MagicMock()
    mock_response.received_messages = [mock_message]
    mock_subscriber.pull.return_value = mock_response

    # Receive tasks
    tasks = await gcp_queue.receive_tasks(max_count=2, visibility_timeout_seconds=60)

    # Verify pull was called
    mock_subscriber.pull.assert_called_with(
        request={
            "subscription": gcp_queue._subscription_path,
            "max_messages": 2,
        }
    )

    # Verify modify_ack_deadline was called
    mock_subscriber.modify_ack_deadline.assert_called_with(
        request={
            "subscription": gcp_queue._subscription_path,
            "ack_ids": ["test-ack-id"],
            "ack_deadline_seconds": 60,
        }
    )

    # Verify returned tasks
    assert len(tasks) == 1
    assert tasks[0]["task_id"] == "test-task-id"
    assert tasks[0]["data"] == {"key": "value"}
    assert tasks[0]["ack_id"] == "test-ack-id"


@pytest.mark.asyncio
async def test_complete_task(gcp_queue, mock_pubsub_client):
    """Test completing a task."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Complete task
    await gcp_queue.complete_task("test-ack-id")

    # Verify acknowledge was called
    mock_subscriber.acknowledge.assert_called_with(
        request={
            "subscription": gcp_queue._subscription_path,
            "ack_ids": ["test-ack-id"],
        }
    )


@pytest.mark.asyncio
async def test_fail_task(gcp_queue, mock_pubsub_client):
    """Test failing a task."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Fail task
    await gcp_queue.fail_task("test-ack-id")

    # Verify modify_ack_deadline was called with 0 seconds
    mock_subscriber.modify_ack_deadline.assert_called_with(
        request={
            "subscription": gcp_queue._subscription_path,
            "ack_ids": ["test-ack-id"],
            "ack_deadline_seconds": 0,
        }
    )


@pytest.mark.asyncio
async def test_get_queue_depth(gcp_queue, mock_pubsub_client):
    """Test getting the queue depth."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Setup mock for pull to return messages
    message1 = MagicMock()
    message1.ack_id = "ack-id-1"
    message2 = MagicMock()
    message2.ack_id = "ack-id-2"

    mock_pull_response = MagicMock()
    mock_pull_response.received_messages = [message1, message2]
    mock_subscriber.pull.return_value = mock_pull_response

    # Get queue depth
    depth = await gcp_queue.get_queue_depth()

    # Verify depth is correct
    assert depth == 2

    # Verify modify_ack_deadline was called to return messages to the queue
    mock_subscriber.modify_ack_deadline.assert_called_once()
    args, kwargs = mock_subscriber.modify_ack_deadline.call_args
    assert kwargs["request"]["ack_ids"] == ["ack-id-1", "ack-id-2"]
    assert kwargs["request"]["ack_deadline_seconds"] == 0


@pytest.mark.asyncio
async def test_purge_queue(gcp_queue, mock_pubsub_client):
    """Test purging the queue."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Purge queue
    await gcp_queue.purge_queue()

    # Verify delete_subscription was called
    mock_subscriber.delete_subscription.assert_called_with(
        request={"subscription": gcp_queue._subscription_path}
    )

    # Verify create_subscription was called to recreate the queue
    mock_subscriber.create_subscription.assert_called()


@pytest.mark.asyncio
async def test_initialization(mock_pubsub_client, gcp_config):
    """Test that queue initialization properly sets up topic and subscription paths."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Create the queue, which should set up paths but not create resources yet
    gcp_queue = GCPPubSubQueue(gcp_config)

    # Verify topic and subscription paths are set up correctly
    assert gcp_queue._topic_path == "projects/test-project/topics/test-queue-topic"
    assert (
        gcp_queue._subscription_path
        == "projects/test-project/subscriptions/test-queue-subscription"
    )

    # Topic and subscription should not be created yet
    assert not mock_publisher.create_topic.called
    assert not mock_subscriber.create_subscription.called

    # Trigger creation by sending a task
    task_id = f"test-task-{uuid.uuid4()}"
    task_data = {"value": 42}
    await gcp_queue.send_task(task_id, task_data)

    # Now verify topic creation was attempted with correct parameters
    mock_publisher.get_topic.assert_called_with(
        request={"topic": "projects/test-project/topics/test-queue-topic"}
    )
    mock_publisher.create_topic.assert_called_with(
        request={"name": "projects/test-project/topics/test-queue-topic"}
    )

    # Verify subscription creation was attempted with correct parameters
    mock_subscriber.get_subscription.assert_called_with(
        request={"subscription": "projects/test-project/subscriptions/test-queue-subscription"}
    )
    mock_subscriber.create_subscription.assert_called()

    # Verify subscription creation parameters
    subscription_request = mock_subscriber.create_subscription.call_args[1]["request"]
    assert (
        subscription_request["name"]
        == "projects/test-project/subscriptions/test-queue-subscription"
    )
    assert subscription_request["topic"] == "projects/test-project/topics/test-queue-topic"
    assert subscription_request["message_retention_duration"]["seconds"] == 7 * 24 * 60 * 60
    assert subscription_request["ack_deadline_seconds"] == 30
