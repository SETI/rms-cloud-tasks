"""
Tests for the GCP Pub/Sub queue adapter.
"""
import asyncio
import pytest
import sys
import uuid
import warnings
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock

# Filter coroutine warnings for these tests
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

# Add the src directory to the path so we can import cloud_tasks modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.cloud_tasks.queue_manager.gcp import GCPPubSubQueue


@pytest.fixture
def mock_pubsub_client():
    """Create mocked Pub/Sub clients."""
    with patch('google.cloud.pubsub_v1.PublisherClient') as mock_publisher, \
         patch('google.cloud.pubsub_v1.SubscriberClient') as mock_subscriber:

        # Setup mock topic and subscription paths
        mock_publisher.return_value.topic_path.return_value = "projects/test-project/topics/test-topic"
        mock_subscriber.return_value.subscription_path.return_value = "projects/test-project/subscriptions/test-subscription"

        # Setup mock get_topic
        mock_publisher.return_value.get_topic.side_effect = Exception("Topic not found")

        # Setup mock get_subscription
        mock_subscriber.return_value.get_subscription.side_effect = Exception("Subscription not found")

        # Setup mock pull for queue depth
        mock_pull_response = MagicMock()
        mock_pull_response.received_messages = []
        mock_subscriber.return_value.pull.return_value = mock_pull_response

        yield (mock_publisher.return_value, mock_subscriber.return_value)


@pytest.fixture
def gcp_queue():
    """Fixture to provide a GCPPubSubQueue instance."""
    queue = GCPPubSubQueue()
    return queue


@pytest.mark.asyncio
async def test_initialize(gcp_queue, mock_pubsub_client):
    """Test initializing the GCP Pub/Sub queue."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Create queue config
    config = {
        'project_id': 'test-project'
    }

    # Initialize queue
    await gcp_queue.initialize('test-queue', config)

    # Verify that topic and subscription were created
    assert mock_publisher.create_topic.called
    assert mock_subscriber.create_subscription.called

    # Verify topic and subscription paths
    assert gcp_queue.topic_path == "projects/test-project/topics/test-topic"
    assert gcp_queue.subscription_path == "projects/test-project/subscriptions/test-subscription"


@pytest.mark.asyncio
async def test_send_task(gcp_queue, mock_pubsub_client):
    """Test sending a task to the queue."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Setup mock for publisher.publish
    future = MagicMock()
    future.result.return_value = "message-id-1234"
    mock_publisher.publish.return_value = future

    # Create and initialize queue
    gcp_queue.project_id = 'test-project'
    gcp_queue.topic_path = "projects/test-project/topics/test-topic"
    gcp_queue.subscription_path = "projects/test-project/subscriptions/test-subscription"
    gcp_queue.publisher = mock_publisher
    gcp_queue.subscriber = mock_subscriber

    # Send task
    task_id = f"test-task-{uuid.uuid4()}"
    task_data = {"value": 42}
    await gcp_queue.send_task(task_id, task_data)

    # Verify publish was called
    mock_publisher.publish.assert_called_once()
    args, kwargs = mock_publisher.publish.call_args
    assert args[0] == gcp_queue.topic_path
    assert 'task_id' in kwargs
    assert kwargs['task_id'] == task_id


@pytest.mark.asyncio
async def test_receive_tasks(gcp_queue, mock_pubsub_client):
    """Test receiving tasks from the queue."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Create a mock message
    mock_message = MagicMock()
    mock_message.ack_id = "test-ack-id"
    mock_message.message = MagicMock()
    mock_message.message.data = ('{"task_id": "test-task-id", "data": {"key": "value"}}').encode('utf-8')

    # Create a mock response with the message
    mock_response = MagicMock()
    mock_response.received_messages = [mock_message]
    mock_subscriber.pull.return_value = mock_response

    # Set up queue
    gcp_queue.project_id = 'test-project'
    gcp_queue.topic_path = "projects/test-project/topics/test-topic"
    gcp_queue.subscription_path = "projects/test-project/subscriptions/test-subscription"
    gcp_queue.publisher = mock_publisher
    gcp_queue.subscriber = mock_subscriber

    # Receive tasks
    tasks = await gcp_queue.receive_tasks(max_count=2, visibility_timeout_seconds=60)

    # Verify pull was called
    mock_subscriber.pull.assert_called_with(
        request={
            "subscription": gcp_queue.subscription_path,
            "max_messages": 2,
        }
    )

    # Verify modify_ack_deadline was called
    mock_subscriber.modify_ack_deadline.assert_called_with(
        request={
            "subscription": gcp_queue.subscription_path,
            "ack_ids": ["test-ack-id"],
            "ack_deadline_seconds": 60,
        }
    )

    # Verify returned tasks
    assert len(tasks) == 1
    assert tasks[0]['task_id'] == 'test-task-id'
    assert tasks[0]['data'] == {'key': 'value'}
    assert tasks[0]['ack_id'] == 'test-ack-id'


@pytest.mark.asyncio
async def test_complete_task(gcp_queue, mock_pubsub_client):
    """Test completing a task."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Set up queue
    gcp_queue.project_id = 'test-project'
    gcp_queue.topic_path = "projects/test-project/topics/test-topic"
    gcp_queue.subscription_path = "projects/test-project/subscriptions/test-subscription"
    gcp_queue.publisher = mock_publisher
    gcp_queue.subscriber = mock_subscriber

    # Complete task
    await gcp_queue.complete_task('test-ack-id')

    # Verify acknowledge was called
    mock_subscriber.acknowledge.assert_called_with(
        request={
            "subscription": gcp_queue.subscription_path,
            "ack_ids": ['test-ack-id'],
        }
    )


@pytest.mark.asyncio
async def test_fail_task(gcp_queue, mock_pubsub_client):
    """Test failing a task."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Set up queue
    gcp_queue.project_id = 'test-project'
    gcp_queue.topic_path = "projects/test-project/topics/test-topic"
    gcp_queue.subscription_path = "projects/test-project/subscriptions/test-subscription"
    gcp_queue.publisher = mock_publisher
    gcp_queue.subscriber = mock_subscriber

    # Fail task
    await gcp_queue.fail_task('test-ack-id')

    # Verify modify_ack_deadline was called with 0 seconds
    mock_subscriber.modify_ack_deadline.assert_called_with(
        request={
            "subscription": gcp_queue.subscription_path,
            "ack_ids": ['test-ack-id'],
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

    # Create and initialize queue
    gcp_queue.project_id = 'test-project'
    gcp_queue.topic_path = "projects/test-project/topics/test-topic"
    gcp_queue.subscription_path = "projects/test-project/subscriptions/test-subscription"
    gcp_queue.publisher = mock_publisher
    gcp_queue.subscriber = mock_subscriber

    # Get queue depth
    depth = await gcp_queue.get_queue_depth()

    # Verify depth is correct
    assert depth == 2

    # Verify modify_ack_deadline was called to return messages to the queue
    mock_subscriber.modify_ack_deadline.assert_called_once()
    args, kwargs = mock_subscriber.modify_ack_deadline.call_args
    assert kwargs['request']['ack_ids'] == ["ack-id-1", "ack-id-2"]
    assert kwargs['request']['ack_deadline_seconds'] == 0


@pytest.mark.asyncio
async def test_purge_queue(gcp_queue, mock_pubsub_client):
    """Test purging the queue."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Set up queue
    gcp_queue.project_id = 'test-project'
    gcp_queue.topic_path = "projects/test-project/topics/test-topic"
    gcp_queue.subscription_path = "projects/test-project/subscriptions/test-subscription"
    gcp_queue.publisher = mock_publisher
    gcp_queue.subscriber = mock_subscriber

    # Purge queue
    await gcp_queue.purge_queue()

    # Verify delete_subscription was called
    mock_subscriber.delete_subscription.assert_called_with(
        request={"subscription": gcp_queue.subscription_path}
    )

    # Verify create_subscription was called to recreate the queue
    mock_subscriber.create_subscription.assert_called()