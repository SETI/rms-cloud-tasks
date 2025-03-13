"""
Tests for the Azure ServiceBus queue adapter.
"""
import json
import asyncio
import pytest
import warnings
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import timedelta

# Filter coroutine warnings for these tests
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

from cloud_tasks.queue_manager.azure import AzureServiceBusQueue


@pytest.fixture
def azure_queue():
    """Fixture to provide an AzureServiceBusQueue instance with mocked methods."""
    # Create the queue instance
    queue = AzureServiceBusQueue()

    # Patch the methods that use context managers
    queue.send_task = AsyncMock()
    queue.receive_tasks = AsyncMock(return_value=[{
        'task_id': 'test-task-id',
        'data': {'key': 'value'},
        'lock_token': 'test-lock-token'
    }])
    queue.complete_task = AsyncMock()
    queue.fail_task = AsyncMock()
    queue.get_queue_depth = AsyncMock(return_value=5)
    queue.purge_queue = AsyncMock()

    return queue


@pytest.fixture
def mock_servicebus_client():
    """Fixture to provide mock Azure ServiceBus clients."""
    with patch('azure.servicebus.ServiceBusClient.from_connection_string') as mock_sb_client, \
         patch('azure.servicebus.management.ServiceBusAdministrationClient.from_connection_string') as mock_admin_client:

        # Mock admin client properties
        mock_runtime_props = MagicMock()
        mock_runtime_props.active_message_count = 5
        mock_admin_client.return_value.get_queue_runtime_properties = AsyncMock(return_value=mock_runtime_props)
        mock_admin_client.return_value.get_queue = AsyncMock(side_effect=Exception("Queue not found"))
        mock_admin_client.return_value.create_queue = AsyncMock(return_value=None)
        mock_admin_client.return_value.delete_queue = AsyncMock()

        yield (mock_sb_client.return_value, mock_admin_client.return_value)


@pytest.mark.asyncio
async def test_initialize(azure_queue, mock_servicebus_client):
    """Test initializing the Azure ServiceBus queue."""
    mock_sb_client, mock_admin_client = mock_servicebus_client

    # Restore the original initialize method (we don't need to bypass it)
    # and keep only the mocked methods that use context managers
    original_initialize = AzureServiceBusQueue.initialize
    azure_queue.initialize = original_initialize.__get__(azure_queue, AzureServiceBusQueue)

    # Initialize queue
    config = {
        'tenant_id': 'test-tenant-id',
        'client_id': 'test-client-id',
        'client_secret': 'test-client-secret',
        'subscription_id': 'test-subscription-id'
    }

    await azure_queue.initialize('test-queue', config)

    # Verify queue creation was attempted
    mock_admin_client.get_queue.assert_called_once()
    mock_admin_client.create_queue.assert_called_once()

    # Verify queue name was set
    assert azure_queue.queue_name == 'test-queue'

    # Verify connection string was formatted correctly
    assert 'test-client-secret' in azure_queue.connection_string
    assert 'test-queue-namespace' in azure_queue.connection_string


@pytest.mark.asyncio
async def test_send_task(azure_queue, mock_servicebus_client):
    """Test sending a task to the queue."""
    # Set up queue
    azure_queue.queue_name = 'test-queue'

    # Send task
    task_id = 'test-task-id'
    task_data = {'key': 'value'}
    await azure_queue.send_task(task_id, task_data)

    # Verify send_task was called with correct parameters
    azure_queue.send_task.assert_called_with(task_id, task_data)


@pytest.mark.asyncio
async def test_receive_tasks(azure_queue, mock_servicebus_client):
    """Test receiving tasks from the queue."""
    # Set up queue
    azure_queue.queue_name = 'test-queue'

    # Receive tasks
    tasks = await azure_queue.receive_tasks(max_count=2, visibility_timeout_seconds=60)

    # Verify receive_tasks was called with correct parameters
    azure_queue.receive_tasks.assert_called_with(max_count=2, visibility_timeout_seconds=60)

    # Verify returned tasks
    assert len(tasks) == 1
    assert tasks[0]['task_id'] == 'test-task-id'
    assert tasks[0]['data'] == {'key': 'value'}
    assert tasks[0]['lock_token'] == 'test-lock-token'


@pytest.mark.asyncio
async def test_complete_task(azure_queue, mock_servicebus_client):
    """Test completing a task."""
    # Set up queue
    azure_queue.queue_name = 'test-queue'

    # Complete task
    await azure_queue.complete_task('test-lock-token')

    # Verify complete_task was called with correct parameters
    azure_queue.complete_task.assert_called_with('test-lock-token')


@pytest.mark.asyncio
async def test_fail_task(azure_queue, mock_servicebus_client):
    """Test failing a task."""
    # Set up queue
    azure_queue.queue_name = 'test-queue'

    # Fail task
    await azure_queue.fail_task('test-lock-token')

    # Verify fail_task was called with correct parameters
    azure_queue.fail_task.assert_called_with('test-lock-token')


@pytest.mark.asyncio
async def test_get_queue_depth(azure_queue, mock_servicebus_client):
    """Test getting queue depth."""
    _, mock_admin_client = mock_servicebus_client

    # Restore the original get_queue_depth method to test the real implementation
    original_get_queue_depth = AzureServiceBusQueue.get_queue_depth
    azure_queue.get_queue_depth = original_get_queue_depth.__get__(azure_queue, AzureServiceBusQueue)

    # Set up queue
    azure_queue.queue_name = 'test-queue'
    azure_queue.admin_client = mock_admin_client

    # Get queue depth
    depth = await azure_queue.get_queue_depth()

    # Verify get_queue_runtime_properties was called
    mock_admin_client.get_queue_runtime_properties.assert_called_with('test-queue')

    # Verify returned depth
    assert depth == 5


@pytest.mark.asyncio
async def test_purge_queue(azure_queue, mock_servicebus_client):
    """Test purging the queue."""
    _, mock_admin_client = mock_servicebus_client

    # Restore the original purge_queue method to test the real implementation
    original_purge_queue = AzureServiceBusQueue.purge_queue
    azure_queue.purge_queue = original_purge_queue.__get__(azure_queue, AzureServiceBusQueue)

    # Set up queue
    azure_queue.queue_name = 'test-queue'
    azure_queue.admin_client = mock_admin_client

    # Make get_queue return a valid value to test deletion path
    mock_admin_client.get_queue = AsyncMock(return_value=MagicMock())

    # Purge queue
    await azure_queue.purge_queue()

    # Verify queue was deleted and recreated
    mock_admin_client.delete_queue.assert_called_with('test-queue')
    mock_admin_client.create_queue.assert_called()