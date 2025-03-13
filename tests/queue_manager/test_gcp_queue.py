"""
Unit tests for the GCP Pub/Sub queue implementation.
"""
import asyncio
import pytest
import sys
import uuid
from pathlib import Path
from unittest import mock

# Add the src directory to the path so we can import cloud_tasks modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.cloud_tasks.queue_manager.gcp import GCPPubSubQueue


@pytest.fixture
def mock_pubsub_client():
    """Create mocked Pub/Sub clients."""
    with mock.patch('google.cloud.pubsub_v1.PublisherClient') as mock_publisher, \
         mock.patch('google.cloud.pubsub_v1.SubscriberClient') as mock_subscriber:

        # Setup mock topic and subscription paths
        mock_publisher.return_value.topic_path.return_value = "projects/test-project/topics/test-topic"
        mock_subscriber.return_value.subscription_path.return_value = "projects/test-project/subscriptions/test-subscription"

        # Setup mock get_topic
        mock_publisher.return_value.get_topic.side_effect = Exception("Topic not found")

        # Setup mock get_subscription
        mock_subscriber.return_value.get_subscription.side_effect = Exception("Subscription not found")

        # Setup mock pull for queue depth
        mock_pull_response = mock.MagicMock()
        mock_pull_response.received_messages = []
        mock_subscriber.return_value.pull.return_value = mock_pull_response

        yield (mock_publisher.return_value, mock_subscriber.return_value)


@pytest.mark.asyncio
async def test_initialize(mock_pubsub_client):
    """Test initializing the GCP Pub/Sub queue."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Create queue config
    config = {
        'project_id': 'test-project'
    }

    # Initialize queue
    queue = GCPPubSubQueue()
    await queue.initialize('test-queue', config)

    # Verify that topic and subscription were created
    assert mock_publisher.create_topic.called
    assert mock_subscriber.create_subscription.called

    # Verify topic and subscription paths
    assert queue.topic_path == "projects/test-project/topics/test-topic"
    assert queue.subscription_path == "projects/test-project/subscriptions/test-subscription"


@pytest.mark.asyncio
async def test_send_task(mock_pubsub_client):
    """Test sending a task to the queue."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Setup mock for publisher.publish
    future = mock.MagicMock()
    future.result.return_value = "message-id-1234"
    mock_publisher.publish.return_value = future

    # Create and initialize queue
    queue = GCPPubSubQueue()
    queue.project_id = 'test-project'
    queue.topic_path = "projects/test-project/topics/test-topic"
    queue.subscription_path = "projects/test-project/subscriptions/test-subscription"
    queue.publisher = mock_publisher
    queue.subscriber = mock_subscriber

    # Send task
    task_id = f"test-task-{uuid.uuid4()}"
    task_data = {"value": 42}
    await queue.send_task(task_id, task_data)

    # Verify publish was called
    mock_publisher.publish.assert_called_once()
    args, kwargs = mock_publisher.publish.call_args
    assert args[0] == queue.topic_path
    assert 'task_id' in kwargs
    assert kwargs['task_id'] == task_id


@pytest.mark.asyncio
async def test_get_queue_depth(mock_pubsub_client):
    """Test getting the queue depth."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Setup mock for pull to return messages
    message1 = mock.MagicMock()
    message1.ack_id = "ack-id-1"
    message2 = mock.MagicMock()
    message2.ack_id = "ack-id-2"

    mock_pull_response = mock.MagicMock()
    mock_pull_response.received_messages = [message1, message2]
    mock_subscriber.pull.return_value = mock_pull_response

    # Create and initialize queue
    queue = GCPPubSubQueue()
    queue.project_id = 'test-project'
    queue.topic_path = "projects/test-project/topics/test-topic"
    queue.subscription_path = "projects/test-project/subscriptions/test-subscription"
    queue.publisher = mock_publisher
    queue.subscriber = mock_subscriber

    # Get queue depth
    depth = await queue.get_queue_depth()

    # Verify depth is correct
    assert depth == 2

    # Verify modify_ack_deadline was called to return messages to the queue
    mock_subscriber.modify_ack_deadline.assert_called_once()
    args, kwargs = mock_subscriber.modify_ack_deadline.call_args
    assert kwargs['request']['ack_ids'] == ["ack-id-1", "ack-id-2"]
    assert kwargs['request']['ack_deadline_seconds'] == 0