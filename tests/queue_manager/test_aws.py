"""
Tests for the AWS SQS queue adapter.
"""

import json
import asyncio
import pytest
import warnings
from unittest.mock import patch, MagicMock

# Filter coroutine warnings for these tests
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

from cloud_tasks.queue_manager.aws import AWSSQSQueue


@pytest.fixture
def mock_sqs_client():
    """Fixture to provide a mock SQS client."""
    with patch("boto3.client") as mock_client:
        client = MagicMock()
        mock_client.return_value = client
        yield client


def make_sqs_config():
    """Create a mock GCP configuration."""
    return MagicMock(
        access_key="test-key", secret_key="test-secret", region="us-east-1", queue_name="test-queue"
    )


def make_sqs_queue():
    """Fixture to provide a GCPPubSubQueue instance."""
    return AWSSQSQueue(make_sqs_config())


@pytest.mark.asyncio
async def test_initialize(mock_sqs_client):
    """Test initializing the SQS queue."""
    # Mock responses
    mock_sqs_client.create_queue.return_value = {
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
    }

    sqs_queue = make_sqs_queue()

    # Verify boto3 client was created with correct parameters
    import boto3

    boto3.client.assert_called_with(
        "sqs",
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        region_name="us-east-1",
    )

    # Verify create_queue was called with correct parameters
    mock_sqs_client.create_queue.assert_called_with(
        QueueName="test-queue",
        Attributes={
            "VisibilityTimeout": "30",
            "MessageRetentionPeriod": "1209600",
        },
    )

    # Verify queue URL was set
    assert sqs_queue._queue_url == "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"


@pytest.mark.asyncio
async def test_initialize_existing_queue(mock_sqs_client):
    """Test initializing with an existing queue."""
    # Mock client behavior for existing queue
    from botocore.exceptions import ClientError

    error_response = {"Error": {"Code": "QueueAlreadyExists"}}
    mock_sqs_client.create_queue.side_effect = ClientError(error_response, "CreateQueue")
    mock_sqs_client.get_queue_url.return_value = {
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
    }

    sqs_queue = make_sqs_queue()

    # Verify get_queue_url was called
    mock_sqs_client.get_queue_url.assert_called_with(QueueName="test-queue")

    # Verify queue URL was set
    assert sqs_queue._queue_url == "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"


@pytest.mark.asyncio
async def test_send_task(mock_sqs_client):
    """Test sending a task to the queue."""
    sqs_queue = make_sqs_queue()

    # Set up queue
    sqs_queue._queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
    sqs_queue._sqs = mock_sqs_client

    # Send task
    task_id = "test-task"
    task_data = {"key": "value"}
    await sqs_queue.send_task(task_id, task_data)

    # Verify send_message was called with correct parameters
    mock_sqs_client.send_message.assert_called_with(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        MessageBody=json.dumps({"task_id": task_id, "data": task_data}),
        MessageAttributes={"TaskId": {"DataType": "String", "StringValue": task_id}},
    )


@pytest.mark.asyncio
async def test_receive_tasks(mock_sqs_client):
    """Test receiving tasks from the queue."""
    sqs_queue = make_sqs_queue()

    # Set up queue
    sqs_queue._queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
    sqs_queue._sqs = mock_sqs_client

    # Mock response for receive_message
    mock_sqs_client.receive_message.return_value = {
        "Messages": [
            {
                "MessageId": "msg1",
                "ReceiptHandle": "receipt1",
                "Body": json.dumps({"task_id": "task1", "data": {"key": "value1"}}),
                "Attributes": {},
                "MessageAttributes": {},
            },
            {
                "MessageId": "msg2",
                "ReceiptHandle": "receipt2",
                "Body": json.dumps({"task_id": "task2", "data": {"key": "value2"}}),
                "Attributes": {},
                "MessageAttributes": {},
            },
        ]
    }

    # Receive tasks
    tasks = await sqs_queue.receive_tasks(max_count=2, visibility_timeout_seconds=60)

    # Verify receive_message was called with correct parameters
    mock_sqs_client.receive_message.assert_called_with(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        MaxNumberOfMessages=2,
        VisibilityTimeout=60,
        MessageAttributeNames=["All"],
        WaitTimeSeconds=10,
    )

    # Verify returned tasks
    assert len(tasks) == 2
    assert tasks[0]["task_id"] == "task1"
    assert tasks[0]["data"] == {"key": "value1"}
    assert tasks[0]["receipt_handle"] == "receipt1"
    assert tasks[1]["task_id"] == "task2"
    assert tasks[1]["data"] == {"key": "value2"}
    assert tasks[1]["receipt_handle"] == "receipt2"


@pytest.mark.asyncio
async def test_complete_task(mock_sqs_client):
    """Test completing a task."""
    sqs_queue = make_sqs_queue()

    # Set up queue
    sqs_queue._queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
    sqs_queue._sqs = mock_sqs_client

    # Complete task
    await sqs_queue.complete_task("receipt-handle")

    # Verify delete_message was called with correct parameters
    mock_sqs_client.delete_message.assert_called_with(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        ReceiptHandle="receipt-handle",
    )


@pytest.mark.asyncio
async def test_fail_task(mock_sqs_client):
    """Test failing a task."""
    sqs_queue = make_sqs_queue()

    # Set up queue
    sqs_queue._queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
    sqs_queue._sqs = mock_sqs_client

    # Fail task
    await sqs_queue.fail_task("receipt-handle")

    # Verify change_message_visibility was called with correct parameters
    mock_sqs_client.change_message_visibility.assert_called_with(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        ReceiptHandle="receipt-handle",
        VisibilityTimeout=0,
    )


@pytest.mark.asyncio
async def test_get_queue_depth(mock_sqs_client):
    """Test getting queue depth."""
    sqs_queue = make_sqs_queue()

    # Set up queue
    sqs_queue._queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
    sqs_queue._sqs = mock_sqs_client

    # Mock response for get_queue_attributes
    mock_sqs_client.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": "42"}
    }

    # Get queue depth
    depth = await sqs_queue.get_queue_depth()

    # Verify get_queue_attributes was called with correct parameters
    mock_sqs_client.get_queue_attributes.assert_called_with(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
        AttributeNames=["ApproximateNumberOfMessages"],
    )

    # Verify returned depth
    assert depth == 42


@pytest.mark.asyncio
async def test_purge_queue(mock_sqs_client):
    """Test purging the queue."""
    sqs_queue = make_sqs_queue()

    # Set up queue
    sqs_queue._queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
    sqs_queue._sqs = mock_sqs_client

    # Purge queue
    await sqs_queue.purge_queue()

    # Verify purge_queue was called with correct parameters
    mock_sqs_client.purge_queue.assert_called_with(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
    )
