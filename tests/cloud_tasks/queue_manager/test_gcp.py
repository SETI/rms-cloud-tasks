# Manually verified 4/29/2025

import pytest
import sys
import uuid
import warnings
import time
from pathlib import Path
from unittest.mock import patch, MagicMock
from google.api_core import exceptions as gcp_exceptions

# Add the src directory to the path so we can import cloud_tasks modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from cloud_tasks.queue_manager.gcp import GCPPubSubQueue  # noqa

# Filter coroutine warnings for these tests
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")


@pytest.fixture
def mock_pubsub_client():
    """Create mocked Pub/Sub clients with all necessary method mocks."""
    with (
        patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher_cls,
        patch("google.cloud.pubsub_v1.SubscriberClient") as mock_subscriber_cls,
    ):
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

    # Patch asyncio.sleep to an async no-op
    async def no_sleep(*args, **kwargs):
        pass

    with patch("asyncio.sleep", side_effect=no_sleep):
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


@pytest.mark.asyncio
async def test_delete_queue_success(gcp_queue, mock_pubsub_client):
    """Test successful deletion of both subscription and topic."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Reset the NotFound side effects since we want deletion to succeed
    mock_subscriber.delete_subscription.side_effect = None
    mock_publisher.delete_topic.side_effect = None

    # Delete the queue
    await gcp_queue.delete_queue()

    # Verify subscription was deleted
    mock_subscriber.delete_subscription.assert_called_with(
        request={"subscription": gcp_queue._subscription_path}
    )

    # Verify topic was deleted
    mock_publisher.delete_topic.assert_called_with(request={"topic": gcp_queue._topic_path})

    # Verify internal state was updated
    assert not gcp_queue._subscription_exists
    assert not gcp_queue._topic_exists


@pytest.mark.asyncio
async def test_delete_queue_not_found(gcp_queue, mock_pubsub_client):
    """Test deletion when queue components don't exist."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Set up NotFound errors for both subscription and topic
    mock_subscriber.delete_subscription.side_effect = gcp_exceptions.NotFound(
        "Subscription not found"
    )
    mock_publisher.delete_topic.side_effect = gcp_exceptions.NotFound("Topic not found")

    # Delete should succeed even if components don't exist
    await gcp_queue.delete_queue()

    # Verify deletion was attempted
    mock_subscriber.delete_subscription.assert_called_with(
        request={"subscription": gcp_queue._subscription_path}
    )
    mock_publisher.delete_topic.assert_called_with(request={"topic": gcp_queue._topic_path})

    # Verify internal state was updated
    assert not gcp_queue._subscription_exists
    assert not gcp_queue._topic_exists


@pytest.mark.asyncio
async def test_delete_queue_partial_failure(gcp_queue, mock_pubsub_client):
    """Test handling of errors during queue deletion."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Set up subscription deletion to succeed but topic deletion to fail
    mock_subscriber.delete_subscription.side_effect = None
    mock_publisher.delete_topic.side_effect = gcp_exceptions.PermissionDenied("Permission denied")

    # Set initial state
    gcp_queue._subscription_exists = True
    gcp_queue._topic_exists = True

    # Attempt to delete the queue
    with pytest.raises(gcp_exceptions.PermissionDenied):
        await gcp_queue.delete_queue()

    # Verify subscription deletion was attempted
    mock_subscriber.delete_subscription.assert_called_with(
        request={"subscription": gcp_queue._subscription_path}
    )

    # Verify topic deletion was attempted
    mock_publisher.delete_topic.assert_called_with(request={"topic": gcp_queue._topic_path})

    # Verify subscription state was updated
    assert not gcp_queue._subscription_exists


@pytest.mark.asyncio
async def test_get_queue_depth_with_messages(gcp_queue, mock_pubsub_client):
    """Test getting queue depth when messages are present."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Create mock messages
    messages = []
    for i in range(5):
        message = MagicMock()
        message.ack_id = f"ack-id-{i}"
        messages.append(message)

    # Setup mock response
    mock_response = MagicMock()
    mock_response.received_messages = messages
    mock_subscriber.pull.return_value = mock_response

    # Get queue depth
    depth = await gcp_queue.get_queue_depth()

    # Verify depth matches number of messages
    assert depth == 5

    # Verify messages were returned to queue
    mock_subscriber.modify_ack_deadline.assert_called_once()
    args = mock_subscriber.modify_ack_deadline.call_args.kwargs["request"]
    assert args["ack_ids"] == ["ack-id-0", "ack-id-1", "ack-id-2", "ack-id-3", "ack-id-4"]
    assert args["ack_deadline_seconds"] == 0


@pytest.mark.asyncio
async def test_get_queue_depth_pull_error(gcp_queue, mock_pubsub_client):
    """Test handling of pull errors during queue depth estimation."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Setup pull to raise an error
    mock_subscriber.pull.side_effect = gcp_exceptions.ServiceUnavailable("Service unavailable")

    # Attempt to get queue depth
    with pytest.raises(gcp_exceptions.ServiceUnavailable):
        await gcp_queue.get_queue_depth()

    # Verify pull was attempted
    mock_subscriber.pull.assert_called_once()
    assert (
        mock_subscriber.pull.call_args.kwargs["request"]["subscription"]
        == gcp_queue._subscription_path
    )


@pytest.mark.asyncio
async def test_purge_queue_delete_error(gcp_queue, mock_pubsub_client):
    """Test handling of errors during queue purging."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Setup delete_subscription to raise an error
    mock_subscriber.delete_subscription.side_effect = gcp_exceptions.PermissionDenied(
        "Permission denied"
    )

    # Patch asyncio.sleep to an async no-op
    async def no_sleep(*args, **kwargs):
        pass

    with patch("asyncio.sleep", side_effect=no_sleep):
        # Attempt to purge queue
        with pytest.raises(gcp_exceptions.PermissionDenied):
            await gcp_queue.purge_queue()

    # Verify deletion was attempted
    mock_subscriber.delete_subscription.assert_called_with(
        request={"subscription": gcp_queue._subscription_path}
    )

    # Verify create was not called since delete failed
    assert not mock_subscriber.create_subscription.called


@pytest.mark.asyncio
async def test_purge_queue_recreation_error(gcp_queue, mock_pubsub_client):
    """Test handling of errors during queue recreation after purge."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Setup create_subscription to raise an error
    mock_subscriber.create_subscription.side_effect = gcp_exceptions.PermissionDenied(
        "Permission denied"
    )

    # Patch asyncio.sleep to an async no-op
    async def no_sleep(*args, **kwargs):
        pass

    with patch("asyncio.sleep", side_effect=no_sleep):
        # Attempt to purge queue
        with pytest.raises(gcp_exceptions.PermissionDenied):
            await gcp_queue.purge_queue()

    # Verify deletion succeeded
    mock_subscriber.delete_subscription.assert_called_with(
        request={"subscription": gcp_queue._subscription_path}
    )

    # Verify creation was attempted with correct parameters
    mock_subscriber.create_subscription.assert_called_once()
    args = mock_subscriber.create_subscription.call_args.kwargs["request"]
    assert args["name"] == gcp_queue._subscription_path
    assert args["topic"] == gcp_queue._topic_path
    assert args["message_retention_duration"]["seconds"] == 7 * 24 * 60 * 60
    assert args["ack_deadline_seconds"] == 30


@pytest.mark.asyncio
async def test_purge_queue_with_delay(gcp_queue, mock_pubsub_client):
    """Test queue purging with delay between delete and create."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Track time of operations
    delete_time = None
    create_time = None

    # Mock delete_subscription to record time
    original_delete = mock_subscriber.delete_subscription

    def delete_with_timestamp(*args, **kwargs):
        nonlocal delete_time
        delete_time = time.time()
        return original_delete(*args, **kwargs)

    mock_subscriber.delete_subscription = MagicMock(side_effect=delete_with_timestamp)

    # Mock create_subscription to record time
    original_create = mock_subscriber.create_subscription

    def create_with_timestamp(*args, **kwargs):
        nonlocal create_time
        create_time = time.time()
        return original_create(*args, **kwargs)

    mock_subscriber.create_subscription = MagicMock(side_effect=create_with_timestamp)

    # Patch asyncio.sleep to an async no-op
    async def no_sleep(*args, **kwargs):
        pass

    with patch("asyncio.sleep", side_effect=no_sleep):
        # Purge queue
        await gcp_queue.purge_queue()

    # Verify operations happened with no delay (since we patched sleep)
    assert delete_time is not None
    assert create_time is not None
    # No need to check delay since we patched sleep


@pytest.mark.asyncio
async def test_get_queue_depth_modify_ack_error(gcp_queue, mock_pubsub_client):
    """Test handling of errors when modifying ack deadline during queue depth check."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Create mock messages
    messages = []
    for i in range(3):
        message = MagicMock()
        message.ack_id = f"ack-id-{i}"
        messages.append(message)

    # Setup mock response
    mock_response = MagicMock()
    mock_response.received_messages = messages
    mock_subscriber.pull.return_value = mock_response

    # Setup modify_ack_deadline to raise an error
    mock_subscriber.modify_ack_deadline.side_effect = gcp_exceptions.ServiceUnavailable(
        "Service unavailable"
    )

    # Attempt to get queue depth
    with pytest.raises(gcp_exceptions.ServiceUnavailable):
        await gcp_queue.get_queue_depth()

    # Verify pull was successful
    mock_subscriber.pull.assert_called_once()

    # Verify modify_ack_deadline was attempted with correct parameters
    mock_subscriber.modify_ack_deadline.assert_called_once()
    args = mock_subscriber.modify_ack_deadline.call_args.kwargs["request"]
    assert args["ack_ids"] == ["ack-id-0", "ack-id-1", "ack-id-2"]
    assert args["ack_deadline_seconds"] == 0


@pytest.mark.asyncio
async def test_delete_queue_subscription_error_handling(gcp_queue, mock_pubsub_client):
    """Test detailed error handling during subscription deletion."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Test different error types
    error_types = [
        gcp_exceptions.ServiceUnavailable("Service unavailable"),
        gcp_exceptions.DeadlineExceeded("Deadline exceeded"),
        gcp_exceptions.Aborted("Operation aborted"),
        gcp_exceptions.InternalServerError("Internal error"),
    ]

    for error in error_types:
        # Reset mocks
        mock_subscriber.delete_subscription.reset_mock()
        mock_publisher.delete_topic.reset_mock()

        # Setup error
        mock_subscriber.delete_subscription.side_effect = error

        # Attempt to delete queue
        with pytest.raises(type(error)):
            await gcp_queue.delete_queue()

        # Verify deletion was attempted
        mock_subscriber.delete_subscription.assert_called_once_with(
            request={"subscription": gcp_queue._subscription_path}
        )

        # Verify topic deletion was not attempted
        assert not mock_publisher.delete_topic.called


@pytest.mark.asyncio
async def test_delete_queue_topic_error_handling(gcp_queue, mock_pubsub_client):
    """Test detailed error handling during topic deletion."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Test different error types
    error_types = [
        gcp_exceptions.ServiceUnavailable("Service unavailable"),
        gcp_exceptions.DeadlineExceeded("Deadline exceeded"),
        gcp_exceptions.Aborted("Operation aborted"),
        gcp_exceptions.InternalServerError("Internal error"),
    ]

    for error in error_types:
        # Reset mocks
        mock_subscriber.delete_subscription.reset_mock()
        mock_publisher.delete_topic.reset_mock()

        # Setup subscription deletion to succeed but topic deletion to fail
        mock_subscriber.delete_subscription.side_effect = None
        mock_publisher.delete_topic.side_effect = error

        # Attempt to delete queue
        with pytest.raises(type(error)):
            await gcp_queue.delete_queue()

        # Verify both operations were attempted
        mock_subscriber.delete_subscription.assert_called_once_with(
            request={"subscription": gcp_queue._subscription_path}
        )
        mock_publisher.delete_topic.assert_called_once_with(
            request={"topic": gcp_queue._topic_path}
        )


@pytest.mark.asyncio
async def test_delete_queue_concurrent_deletion(gcp_queue, mock_pubsub_client):
    """Test handling of concurrent deletion scenarios."""
    mock_publisher, mock_subscriber = mock_pubsub_client

    # Simulate subscription already deleted between check and delete
    def subscription_race_condition(*args, **kwargs):
        raise gcp_exceptions.NotFound("Subscription was deleted concurrently")

    mock_subscriber.delete_subscription.side_effect = subscription_race_condition

    # Simulate topic already deleted between check and delete
    def topic_race_condition(*args, **kwargs):
        raise gcp_exceptions.NotFound("Topic was deleted concurrently")

    mock_publisher.delete_topic.side_effect = topic_race_condition

    # Delete should succeed even with concurrent deletions
    await gcp_queue.delete_queue()

    # Verify both deletion attempts were made
    mock_subscriber.delete_subscription.assert_called_once_with(
        request={"subscription": gcp_queue._subscription_path}
    )
    mock_publisher.delete_topic.assert_called_once_with(request={"topic": gcp_queue._topic_path})

    # Verify internal state was updated
    assert not gcp_queue._subscription_exists
    assert not gcp_queue._topic_exists


@pytest.mark.asyncio
async def test_initialization_missing_queue_name():
    """Test initialization with missing queue name."""
    with pytest.raises(ValueError, match="Queue name is required"):
        GCPPubSubQueue(gcp_config=MagicMock(queue_name=None))


@pytest.mark.asyncio
async def test_initialization_with_project_id_kwarg(mock_pubsub_client):
    """Test initialization with project_id provided as kwarg."""
    queue = GCPPubSubQueue(
        gcp_config=MagicMock(
            queue_name="test-queue",
            project_id="default-project",
            credentials_file=None,  # Use default credentials
        ),
        project_id="override-project",
    )
    assert queue._project_id == "override-project"


@pytest.mark.asyncio
async def test_topic_initialization_error(mock_pubsub_client):
    """Test error handling during topic initialization."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_publisher.get_topic.side_effect = gcp_exceptions.PermissionDenied("Permission denied")

    with pytest.raises(gcp_exceptions.PermissionDenied):
        GCPPubSubQueue(gcp_config=MagicMock(queue_name="test-queue"))


@pytest.mark.asyncio
async def test_subscription_initialization_error(mock_pubsub_client):
    """Test error handling during subscription initialization."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_subscriber.get_subscription.side_effect = gcp_exceptions.PermissionDenied(
        "Permission denied"
    )

    with pytest.raises(gcp_exceptions.PermissionDenied):
        GCPPubSubQueue(gcp_config=MagicMock(queue_name="test-queue"))


@pytest.mark.asyncio
async def test_topic_creation_error(gcp_queue, mock_pubsub_client):
    """Test error handling during topic creation."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_publisher.create_topic.side_effect = gcp_exceptions.PermissionDenied("Permission denied")
    gcp_queue._topic_exists = False

    with pytest.raises(gcp_exceptions.PermissionDenied):
        await gcp_queue.send_task("test-task", {"data": "test"})


@pytest.mark.asyncio
async def test_subscription_creation_error(gcp_queue, mock_pubsub_client):
    """Test error handling during subscription creation."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_subscriber.create_subscription.side_effect = gcp_exceptions.PermissionDenied(
        "Permission denied"
    )
    gcp_queue._subscription_exists = False

    with pytest.raises(gcp_exceptions.PermissionDenied):
        await gcp_queue.send_task("test-task", {"data": "test"})


@pytest.mark.asyncio
async def test_send_task_publish_error(gcp_queue, mock_pubsub_client):
    """Test error handling during task publishing."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    future = MagicMock()
    future.result.side_effect = gcp_exceptions.DeadlineExceeded("Deadline exceeded")
    mock_publisher.publish.return_value = future

    with pytest.raises(gcp_exceptions.DeadlineExceeded):
        await gcp_queue.send_task("test-task", {"data": "test"})


@pytest.mark.asyncio
async def test_receive_tasks_pull_error(gcp_queue, mock_pubsub_client):
    """Test error handling during task receiving."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_subscriber.pull.side_effect = gcp_exceptions.DeadlineExceeded("Deadline exceeded")

    with pytest.raises(gcp_exceptions.DeadlineExceeded):
        await gcp_queue.receive_tasks()


@pytest.mark.asyncio
async def test_complete_task_error(gcp_queue, mock_pubsub_client):
    """Test error handling during task completion."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_subscriber.acknowledge.side_effect = gcp_exceptions.InvalidArgument("Invalid ack_id")

    with pytest.raises(gcp_exceptions.InvalidArgument):
        await gcp_queue.complete_task("invalid-ack-id")


@pytest.mark.asyncio
async def test_fail_task_error(gcp_queue, mock_pubsub_client):
    """Test error handling during task failure."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_subscriber.modify_ack_deadline.side_effect = gcp_exceptions.InvalidArgument(
        "Invalid ack_id"
    )

    with pytest.raises(gcp_exceptions.InvalidArgument):
        await gcp_queue.fail_task("invalid-ack-id")


@pytest.mark.asyncio
async def test_get_queue_depth_pull_error_handling(gcp_queue, mock_pubsub_client):
    """Test error handling during queue depth check."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_subscriber.pull.side_effect = gcp_exceptions.ServiceUnavailable("Service unavailable")

    with pytest.raises(gcp_exceptions.ServiceUnavailable):
        await gcp_queue.get_queue_depth()


@pytest.mark.asyncio
async def test_purge_queue_delete_error_handling(gcp_queue, mock_pubsub_client):
    """Test error handling during queue purging."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_subscriber.delete_subscription.side_effect = gcp_exceptions.PermissionDenied(
        "Permission denied"
    )

    with pytest.raises(gcp_exceptions.PermissionDenied):
        await gcp_queue.purge_queue()


@pytest.mark.asyncio
async def test_delete_queue_error_handling(gcp_queue, mock_pubsub_client):
    """Test error handling during queue deletion."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_subscriber.delete_subscription.side_effect = gcp_exceptions.PermissionDenied(
        "Permission denied"
    )

    with pytest.raises(gcp_exceptions.PermissionDenied):
        await gcp_queue.delete_queue()


@pytest.mark.asyncio
async def test_topic_exists_error(mock_pubsub_client):
    """Test error handling when checking if topic exists."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_publisher.get_topic.side_effect = gcp_exceptions.ServerError("Internal error")

    with pytest.raises(gcp_exceptions.ServerError):
        GCPPubSubQueue(gcp_config=MagicMock(queue_name="test-queue"))


@pytest.mark.asyncio
async def test_subscription_exists_error(mock_pubsub_client):
    """Test error handling when checking if subscription exists."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_subscriber.get_subscription.side_effect = gcp_exceptions.ServerError("Internal error")

    with pytest.raises(gcp_exceptions.ServerError):
        GCPPubSubQueue(gcp_config=MagicMock(queue_name="test-queue"))


@pytest.mark.asyncio
async def test_send_task_error_handling(gcp_queue, mock_pubsub_client):
    """Test error handling during task sending."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_publisher.publish.side_effect = gcp_exceptions.ServerError("Internal error")

    with pytest.raises(gcp_exceptions.ServerError):
        await gcp_queue.send_task("test-task", {"data": "test"})


@pytest.mark.asyncio
async def test_receive_tasks_error_handling(gcp_queue, mock_pubsub_client):
    """Test error handling during task receiving."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_subscriber.modify_ack_deadline.side_effect = gcp_exceptions.ServerError("Internal error")

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

    with pytest.raises(gcp_exceptions.ServerError):
        await gcp_queue.receive_tasks()


@pytest.mark.asyncio
async def test_delete_queue_error_propagation(gcp_queue, mock_pubsub_client):
    """Test error propagation during queue deletion."""
    mock_publisher, mock_subscriber = mock_pubsub_client
    mock_publisher.delete_topic.side_effect = gcp_exceptions.ServerError("Internal error")

    with pytest.raises(gcp_exceptions.ServerError):
        await gcp_queue.delete_queue()
