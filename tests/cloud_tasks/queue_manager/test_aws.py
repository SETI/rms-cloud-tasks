# Manually verified 4/29/2025

import pytest
import sys
import uuid
import warnings
from pathlib import Path
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

# Add the src directory to the path so we can import cloud_tasks modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from cloud_tasks.queue_manager.aws import AWSSQSQueue, AWSConfig  # noqa

# Filter coroutine warnings for these tests
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

pytest.skip("Skipping this test file", allow_module_level=True)


@pytest.fixture
def mock_sqs_client():
    """Create mocked SQS client with all necessary method mocks."""
    with patch("boto3.client") as mock_client_cls:
        # Create the mock instance
        sqs = MagicMock()
        mock_client_cls.return_value = sqs

        # Setup mock queue URL
        sqs.get_queue_url.return_value = {
            "QueueUrl": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
        }

        # Mock queue operations
        sqs.create_queue.return_value = {
            "QueueUrl": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
        }
        sqs.get_queue_attributes.return_value = {"Attributes": {"ApproximateNumberOfMessages": "0"}}
        sqs.receive_message.return_value = {"Messages": []}
        sqs.send_message.return_value = {"MessageId": "test-message-id"}

        yield sqs


@pytest.fixture
def aws_config():
    """Create a mock AWS configuration."""
    return MagicMock(
        region="us-west-2",
        queue_name="test-queue",
        access_key="test-access-key",
        secret_key="test-secret-key",
    )


@pytest.fixture
def aws_queue(mock_sqs_client, aws_config):
    """Fixture to provide an AWSSQSQueue instance."""
    queue = AWSSQSQueue(aws_config)
    queue._sqs = mock_sqs_client  # Ensure we use our mock
    return queue


@pytest.mark.asyncio
async def test_initialize(aws_queue, mock_sqs_client):
    """Test initializing the AWS SQS queue."""
    # Verify queue URL is set up correctly
    assert aws_queue._queue_url == "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"

    # Queue should be marked as existing since get_queue_url succeeded
    assert aws_queue._queue_exists

    # Verify get_queue_url was called
    mock_sqs_client.get_queue_url.assert_called_with(QueueName="test-queue")


@pytest.mark.asyncio
async def test_send_task(aws_queue, mock_sqs_client):
    """Test sending a task to the queue."""
    # Send task
    task_id = f"test-task-{uuid.uuid4()}"
    task_data = {"value": 42}
    await aws_queue.send_task(task_id, task_data)

    # Verify send_message was called
    mock_sqs_client.send_message.assert_called_once()
    args, kwargs = mock_sqs_client.send_message.call_args
    assert kwargs["QueueUrl"] == aws_queue._queue_url
    assert "MessageBody" in kwargs
    assert "MessageAttributes" in kwargs
    assert kwargs["MessageAttributes"]["TaskId"]["StringValue"] == task_id


@pytest.mark.asyncio
async def test_receive_tasks(aws_queue, mock_sqs_client):
    """Test receiving tasks from the queue."""
    # Create a mock message
    mock_sqs_client.receive_message.return_value = {
        "Messages": [
            {
                "MessageId": "test-message-id",
                "ReceiptHandle": "test-receipt-handle",
                "Body": '{"task_id": "test-task-id", "data": {"key": "value"}}',
                "Attributes": {},
                "MessageAttributes": {
                    "TaskId": {"StringValue": "test-task-id", "DataType": "String"}
                },
            }
        ]
    }

    # Receive tasks
    tasks = await aws_queue.receive_tasks(max_count=2, visibility_timeout=60)

    # Verify receive_message was called
    mock_sqs_client.receive_message.assert_called_with(
        QueueUrl=aws_queue._queue_url,
        MaxNumberOfMessages=2,
        VisibilityTimeout=60,
        MessageAttributeNames=["All"],
        WaitTimeSeconds=10,
    )

    # Verify returned tasks
    assert len(tasks) == 1
    assert tasks[0]["task_id"] == "test-task-id"
    assert tasks[0]["data"] == {"key": "value"}
    assert tasks[0]["receipt_handle"] == "test-receipt-handle"


@pytest.mark.asyncio
async def test_acknowledge_task(aws_queue, mock_sqs_client):
    """Test completing a task."""
    # Complete task
    await aws_queue.acknowledge_task("test-receipt-handle")

    # Verify delete_message was called
    mock_sqs_client.delete_message.assert_called_with(
        QueueUrl=aws_queue._queue_url,
        ReceiptHandle="test-receipt-handle",
    )


@pytest.mark.asyncio
async def test_retry_task(aws_queue, mock_sqs_client):
    """Test failing a task."""
    # Fail task
    await aws_queue.retry_task("test-receipt-handle")

    # Verify change_message_visibility was called with 0 seconds
    mock_sqs_client.change_message_visibility.assert_called_with(
        QueueUrl=aws_queue._queue_url,
        ReceiptHandle="test-receipt-handle",
        VisibilityTimeout=0,
    )


@pytest.mark.asyncio
async def test_get_queue_depth(aws_queue, mock_sqs_client):
    """Test getting the queue depth."""
    # Setup mock for get_queue_attributes to return messages
    mock_sqs_client.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": "42"}
    }

    # Get queue depth
    depth = await aws_queue.get_queue_depth()

    # Verify depth is correct
    assert depth == 42

    # Verify get_queue_attributes was called correctly
    mock_sqs_client.get_queue_attributes.assert_called_with(
        QueueUrl=aws_queue._queue_url,
        AttributeNames=["ApproximateNumberOfMessages"],
    )


@pytest.mark.asyncio
async def test_purge_queue(aws_queue, mock_sqs_client):
    """Test purging the queue."""
    # Purge queue
    await aws_queue.purge_queue()

    # Verify purge_queue was called
    mock_sqs_client.purge_queue.assert_called_with(QueueUrl=aws_queue._queue_url)


@pytest.mark.asyncio
async def test_initialization_queue_exists(mock_sqs_client, aws_config):
    """Test initialization when queue exists."""
    # Create the queue
    queue = AWSSQSQueue(aws_config)
    queue._sqs = mock_sqs_client  # Ensure we use our mock

    # Verify get_queue_url was called
    mock_sqs_client.get_queue_url.assert_called_with(QueueName="test-queue")

    # Verify queue exists flag is set
    assert queue._queue_exists
    assert queue._queue_url == "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"


@pytest.mark.asyncio
async def test_initialization_queue_not_exists(mock_sqs_client, aws_config):
    """Test initialization when queue doesn't exist."""
    # Setup get_queue_url to fail
    mock_sqs_client.get_queue_url.side_effect = ClientError(
        {
            "Error": {
                "Code": "AWS.SimpleQueueService.NonExistentQueue",
                "Message": "Queue does not exist",
            }
        },
        "GetQueueUrl",
    )

    # Create the queue
    queue = AWSSQSQueue(aws_config)
    queue._sqs = mock_sqs_client  # Ensure we use our mock

    # Verify get_queue_url was called
    mock_sqs_client.get_queue_url.assert_called_with(QueueName="test-queue")

    # Verify queue exists flag is not set
    assert not queue._queue_exists
    assert queue._queue_url is None

    # Send a task to trigger queue creation
    await queue.send_task("test-task-id", {"key": "value"})

    # Verify create_queue was called
    mock_sqs_client.create_queue.assert_called_with(
        QueueName="test-queue",
        Attributes={
            "VisibilityTimeout": "30",
            "MessageRetentionPeriod": "1209600",
        },
    )


@pytest.mark.asyncio
async def test_initialization_with_kwargs():
    """Test initialization with kwargs instead of config object."""
    with patch("boto3.client") as mock_client_cls:
        sqs = MagicMock()
        mock_client_cls.return_value = sqs
        sqs.get_queue_url.return_value = {
            "QueueUrl": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
        }

        # Create queue with kwargs
        queue = AWSSQSQueue(
            queue_name="test-queue",
            region="us-west-2",
            access_key="test-access-key",
            secret_key="test-secret-key",
        )
        queue._sqs = sqs  # Ensure we use our mock

        # Verify SQS client was created with correct credentials
        mock_client_cls.assert_called_with(
            "sqs",
            aws_access_key_id="test-access-key",
            aws_secret_access_key="test-secret-key",
            region_name="us-west-2",
        )

        # Verify queue was initialized correctly
        assert queue._queue_name == "test-queue"
        assert queue._queue_exists
        assert queue._queue_url == "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"


@pytest.mark.asyncio
async def test_receive_tasks_no_messages(aws_queue, mock_sqs_client):
    """Test receiving tasks when queue is empty."""
    # Setup receive_message to return no messages
    mock_sqs_client.receive_message.return_value = {}

    # Receive tasks
    tasks = await aws_queue.receive_tasks(max_count=10)

    # Verify empty list is returned
    assert len(tasks) == 0

    # Verify receive_message was called correctly
    mock_sqs_client.receive_message.assert_called_with(
        QueueUrl=aws_queue._queue_url,
        MaxNumberOfMessages=10,
        VisibilityTimeout=30,
        MessageAttributeNames=["All"],
        WaitTimeSeconds=10,
    )


@pytest.mark.asyncio
async def test_receive_tasks_max_messages(aws_queue, mock_sqs_client):
    """Test receiving tasks respects SQS maximum message limit."""
    # Try to receive more than SQS maximum
    await aws_queue.receive_tasks(max_count=20)

    # Verify receive_message was called with capped value
    args, kwargs = mock_sqs_client.receive_message.call_args
    assert kwargs["MaxNumberOfMessages"] == 10  # SQS maximum


@pytest.mark.asyncio
async def test_error_handling(aws_queue, mock_sqs_client):
    """Test error handling for various operations."""
    # Setup error response
    error_response = {"Error": {"Code": "InternalError", "Message": "Internal Error"}}
    client_error = ClientError(error_response, "Operation")

    # Test send_task error handling
    mock_sqs_client.send_message.side_effect = client_error
    with pytest.raises(ClientError):
        await aws_queue.send_task("test-task-id", {"key": "value"})

    # Test receive_tasks error handling
    mock_sqs_client.receive_message.side_effect = client_error
    with pytest.raises(ClientError):
        await aws_queue.receive_tasks()

    # Test acknowledge_task error handling
    mock_sqs_client.delete_message.side_effect = client_error
    with pytest.raises(ClientError):
        await aws_queue.acknowledge_task("test-receipt-handle")

    # Test retry_task error handling
    mock_sqs_client.change_message_visibility.side_effect = client_error
    with pytest.raises(ClientError):
        await aws_queue.retry_task("test-receipt-handle")

    # Test get_queue_depth error handling
    mock_sqs_client.get_queue_attributes.side_effect = client_error
    with pytest.raises(ClientError):
        await aws_queue.get_queue_depth()

    # Test purge_queue error handling
    mock_sqs_client.purge_queue.side_effect = client_error
    with pytest.raises(ClientError):
        await aws_queue.purge_queue()


@pytest.mark.asyncio
async def test_delete_queue_success(aws_queue, mock_sqs_client):
    """Test successful deletion of queue."""
    # Delete the queue
    await aws_queue.delete_queue()

    # Verify get_queue_url was called to get the current URL
    mock_sqs_client.get_queue_url.assert_called_with(QueueName=aws_queue._queue_name)

    # Verify delete_queue was called with the queue URL
    mock_sqs_client.delete_queue.assert_called_with(
        QueueUrl="https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
    )

    # Verify internal state was updated
    assert not aws_queue._queue_exists
    assert aws_queue._queue_url is None


@pytest.mark.asyncio
async def test_delete_queue_not_found(aws_queue, mock_sqs_client):
    """Test deletion when queue doesn't exist."""
    # Set up NotFound error for get_queue_url
    mock_sqs_client.get_queue_url.side_effect = ClientError(
        {
            "Error": {
                "Code": "AWS.SimpleQueueService.NonExistentQueue",
                "Message": "Queue does not exist",
            }
        },
        "GetQueueUrl",
    )

    # Delete should succeed even if queue doesn't exist
    await aws_queue.delete_queue()

    # Verify get_queue_url was called
    mock_sqs_client.get_queue_url.assert_called_with(QueueName=aws_queue._queue_name)

    # Verify delete_queue was not called since queue didn't exist
    mock_sqs_client.delete_queue.assert_not_called()

    # Verify internal state
    assert not aws_queue._queue_exists
    assert aws_queue._queue_url is None


@pytest.mark.asyncio
async def test_delete_queue_error_handling():
    """Test handling of errors during queue deletion."""
    with patch("boto3.client") as mock_client_cls:
        sqs = MagicMock()
        mock_client_cls.return_value = sqs

        # Setup queue URL
        sqs.get_queue_url.return_value = {
            "QueueUrl": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
        }

        # Create queue
        queue = AWSSQSQueue(queue_name="test-queue")
        queue._sqs = sqs

        # Test different error types
        error_types = [
            ("ServiceUnavailable", "Service unavailable"),
            ("InternalError", "Internal server error"),
            ("InvalidRequest", "Invalid request"),
            ("NetworkError", "Network error"),
        ]

        for error_code, error_message in error_types:
            # Reset mocks
            sqs.get_queue_url.reset_mock()
            sqs.delete_queue.reset_mock()

            # Setup delete_queue to raise an error
            sqs.delete_queue.side_effect = ClientError(
                {"Error": {"Code": error_code, "Message": error_message}},
                "DeleteQueue",
            )

            # Attempt to delete queue
            with pytest.raises(ClientError) as exc_info:
                await queue.delete_queue()

            # Verify error details
            assert exc_info.value.response["Error"]["Code"] == error_code
            assert exc_info.value.response["Error"]["Message"] == error_message

            # Verify get_queue_url and delete_queue were called
            sqs.get_queue_url.assert_called_once_with(QueueName="test-queue")
            sqs.delete_queue.assert_called_once_with(
                QueueUrl="https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
            )


@pytest.mark.asyncio
async def test_delete_queue_concurrent_deletion(aws_queue, mock_sqs_client):
    """Test handling of concurrent deletion scenarios."""
    # Setup delete_queue to fail with NotFound
    mock_sqs_client.delete_queue.side_effect = ClientError(
        {
            "Error": {
                "Code": "AWS.SimpleQueueService.NonExistentQueue",
                "Message": "Queue was deleted concurrently",
            }
        },
        "DeleteQueue",
    )

    # Delete should succeed even with concurrent deletion
    await aws_queue.delete_queue()

    # Verify get_queue_url was called
    mock_sqs_client.get_queue_url.assert_called_with(QueueName=aws_queue._queue_name)

    # Verify delete_queue was called
    mock_sqs_client.delete_queue.assert_called_with(
        QueueUrl="https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
    )

    # Verify internal state was updated
    assert not aws_queue._queue_exists
    assert aws_queue._queue_url is None


@pytest.mark.asyncio
async def test_create_queue_already_exists(aws_queue, mock_sqs_client):
    """Test _create_queue when queue already exists (race condition scenario)."""
    # Setup create_queue to fail with QueueAlreadyExists error
    mock_sqs_client.create_queue.side_effect = ClientError(
        {
            "Error": {
                "Code": "QueueAlreadyExists",
                "Message": "Queue already exists",
            }
        },
        "CreateQueue",
    )

    # Setup get_queue_url to succeed after create_queue fails
    mock_sqs_client.get_queue_url.return_value = {
        "QueueUrl": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
    }

    # Force queue to not exist initially
    aws_queue._queue_exists = False
    aws_queue._queue_url = None

    # Try to send a task which will trigger queue creation
    await aws_queue.send_task("test-task-id", {"key": "value"})

    # Verify create_queue was called with correct parameters
    mock_sqs_client.create_queue.assert_called_with(
        QueueName="test-queue",
        Attributes={
            "VisibilityTimeout": "30",
            "MessageRetentionPeriod": "1209600",
        },
    )

    # Verify get_queue_url was called after create_queue failed
    mock_sqs_client.get_queue_url.assert_called_with(QueueName="test-queue")

    # Verify queue state was updated correctly
    assert aws_queue._queue_exists
    assert aws_queue._queue_url == "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"

    # Verify send_message was called after queue was "found"
    mock_sqs_client.send_message.assert_called_once()
    args, kwargs = mock_sqs_client.send_message.call_args
    assert kwargs["QueueUrl"] == aws_queue._queue_url
    assert "MessageBody" in kwargs
    assert "MessageAttributes" in kwargs
    assert kwargs["MessageAttributes"]["TaskId"]["StringValue"] == "test-task-id"


@pytest.mark.asyncio
async def test_initialization_with_explicit_queue_name(mock_sqs_client):
    """Test initialization with explicitly provided queue name."""
    # Create queue with explicit queue name
    queue = AWSSQSQueue(
        aws_config=MagicMock(queue_name="default-queue"),  # This should be ignored
        queue_name="explicit-queue",  # This should be used
    )
    queue._sqs = mock_sqs_client  # Ensure we use our mock

    # Verify the explicit queue name was used
    assert queue._queue_name == "explicit-queue"

    # Verify get_queue_url was called with the explicit queue name
    mock_sqs_client.get_queue_url.assert_called_with(QueueName="explicit-queue")

    # Verify queue URL is set correctly
    assert queue._queue_url == "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"


@pytest.mark.asyncio
async def test_initialization_missing_queue_name():
    """Test initialization fails when no queue name is provided."""
    # Create config without queue name
    config = AWSConfig(
        region="us-west-2",
        access_key="test-access-key",
        secret_key="test-secret-key",
        queue_name=None,  # Explicitly set queue_name to None
    )

    # Attempt to create queue without queue name
    with pytest.raises(ValueError, match="Queue name is required"):
        AWSSQSQueue(aws_config=config)


@pytest.mark.asyncio
async def test_initialization_boto3_client_error():
    """Test initialization fails when boto3.client raises an exception."""
    with patch("boto3.client") as mock_client_cls:
        # Make boto3.client raise an exception
        mock_client_cls.side_effect = Exception("Failed to create SQS client")

        # Attempt to create queue
        with pytest.raises(Exception, match="Failed to create SQS client"):
            AWSSQSQueue(
                aws_config=AWSConfig(
                    region="us-west-2",
                    access_key="test-access-key",
                    secret_key="test-secret-key",
                    queue_name="test-queue",
                )
            )


@pytest.mark.asyncio
async def test_initialization_get_queue_url_error():
    """Test initialization fails when get_queue_url raises a ClientError."""
    with patch("boto3.client") as mock_client_cls:
        # Create mock SQS client
        sqs = MagicMock()
        mock_client_cls.return_value = sqs

        # Make get_queue_url raise a ClientError
        sqs.get_queue_url.side_effect = ClientError(
            {
                "Error": {
                    "Code": "InvalidClientTokenId",
                    "Message": "The security token included in the request is invalid",
                }
            },
            "GetQueueUrl",
        )

        # Attempt to create queue
        with pytest.raises(ClientError) as exc_info:
            AWSSQSQueue(
                aws_config=AWSConfig(
                    region="us-west-2",
                    access_key="test-access-key",
                    secret_key="test-secret-key",
                    queue_name="test-queue",
                )
            )

        # Verify the error details
        assert exc_info.value.response["Error"]["Code"] == "InvalidClientTokenId"
        assert "security token" in exc_info.value.response["Error"]["Message"]


@pytest.mark.asyncio
async def test_create_queue_error(aws_queue, mock_sqs_client):
    """Test _create_queue error handling when create_queue raises a ClientError."""
    # Setup create_queue to fail with a different error
    mock_sqs_client.create_queue.side_effect = ClientError(
        {
            "Error": {
                "Code": "InvalidParameterValue",
                "Message": "Invalid queue name",
            }
        },
        "CreateQueue",
    )

    # Force queue to not exist initially
    aws_queue._queue_exists = False
    aws_queue._queue_url = None

    # Try to send a task which will trigger queue creation
    with pytest.raises(ClientError) as exc_info:
        await aws_queue.send_task("test-task-id", {"key": "value"})

    # Verify create_queue was called with correct parameters
    mock_sqs_client.create_queue.assert_called_with(
        QueueName="test-queue",
        Attributes={
            "VisibilityTimeout": "30",
            "MessageRetentionPeriod": "1209600",
        },
    )

    # Verify the error details
    assert exc_info.value.response["Error"]["Code"] == "InvalidParameterValue"
    assert "Invalid queue name" in exc_info.value.response["Error"]["Message"]

    # Verify queue state was not updated
    assert not aws_queue._queue_exists
    assert aws_queue._queue_url is None


@pytest.mark.asyncio
async def test_delete_queue_get_url_error(aws_queue, mock_sqs_client):
    """Test delete_queue error handling when get_queue_url raises a ClientError."""
    # Make get_queue_url raise a ClientError that's not NonExistentQueue
    mock_sqs_client.get_queue_url.side_effect = ClientError(
        {
            "Error": {
                "Code": "InvalidClientTokenId",
                "Message": "The security token included in the request is invalid",
            }
        },
        "GetQueueUrl",
    )

    # Attempt to delete queue
    with pytest.raises(ClientError) as exc_info:
        await aws_queue.delete_queue()

    # Verify get_queue_url was called
    mock_sqs_client.get_queue_url.assert_called_with(QueueName="test-queue")

    # Verify delete_queue was not called
    mock_sqs_client.delete_queue.assert_not_called()

    # Verify the error details
    assert exc_info.value.response["Error"]["Code"] == "InvalidClientTokenId"
    assert "security token" in exc_info.value.response["Error"]["Message"]


@pytest.mark.asyncio
async def test_delete_queue_delete_error(aws_queue, mock_sqs_client):
    """Test delete_queue error handling when delete_queue raises a ClientError."""
    # Make delete_queue raise a ClientError
    mock_sqs_client.delete_queue.side_effect = RuntimeError(
        "You do not have permission to delete this queue",
    )

    # Attempt to delete queue
    with pytest.raises(RuntimeError) as exc_info:
        await aws_queue.delete_queue()

    # Verify get_queue_url was called
    mock_sqs_client.get_queue_url.assert_called_with(QueueName="test-queue")

    # Verify delete_queue was called
    mock_sqs_client.delete_queue.assert_called_with(
        QueueUrl="https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
    )

    # Verify the error details
    assert "permission" in str(exc_info.value)

    # Verify queue state was not updated
    assert aws_queue._queue_exists
    assert aws_queue._queue_url == "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
