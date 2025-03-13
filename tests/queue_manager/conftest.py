"""
Configure test fixtures for queue manager tests.
"""
import pytest
from unittest import mock
import asyncio

@pytest.fixture
def mock_aws_queue():
    """Create a mock AWS SQS queue."""
    # Create mock for boto3 client
    with mock.patch('boto3.resource') as mock_resource:
        # Mock queue
        mock_queue = mock.MagicMock()
        mock_queue.send_message.return_value = {'MessageId': 'test-message-id'}

        # Mock SQS
        mock_sqs = mock.MagicMock()
        mock_sqs.get_queue_by_name.return_value = mock_queue
        mock_sqs.create_queue.return_value = mock_queue

        # Mock resource
        mock_resource.return_value = mock_sqs

        yield mock_queue

@pytest.fixture
def mock_gcp_queue():
    """Create mock GCP PubSub clients."""
    with mock.patch('google.cloud.pubsub_v1.PublisherClient') as mock_publisher, \
         mock.patch('google.cloud.pubsub_v1.SubscriberClient') as mock_subscriber:

        # Publisher setup
        future = mock.MagicMock()
        future.result.return_value = "message-id-123"
        mock_publisher.return_value.publish.return_value = future
        mock_publisher.return_value.topic_path.return_value = "projects/test-project/topics/test-topic"

        # Subscriber setup
        mock_message = mock.MagicMock()
        mock_message.ack_id = "test-ack-id"
        mock_message.message.data = b'{"task_id": "test-task", "data": {"key": "value"}}'

        mock_response = mock.MagicMock()
        mock_response.received_messages = [mock_message]
        mock_subscriber.return_value.pull.return_value = mock_response
        mock_subscriber.return_value.subscription_path.return_value = "projects/test-project/subscriptions/test-subscription"

        yield (mock_publisher.return_value, mock_subscriber.return_value)

@pytest.fixture
def mock_azure_queue():
    """Create mock Azure ServiceBus client."""
    with mock.patch('azure.servicebus.aio.ServiceBusClient') as mock_sb_client:
        # Mock sender
        mock_sender = mock.AsyncMock()
        mock_sender.send_messages.return_value = None

        # Mock receiver
        mock_message = mock.MagicMock()
        mock_message.message_id = "test-message-id"
        mock_message.body = b'{"task_id": "test-task", "data": {"key": "value"}}'
        mock_message.lock_token = "test-lock-token"

        mock_receiver = mock.AsyncMock()
        mock_receiver.receive_messages.return_value = [mock_message]

        # Mock client
        mock_sb_client.return_value.get_queue_sender.return_value.__aenter__.return_value = mock_sender
        mock_sb_client.return_value.get_queue_receiver.return_value.__aenter__.return_value = mock_receiver

        yield mock_sb_client.return_value