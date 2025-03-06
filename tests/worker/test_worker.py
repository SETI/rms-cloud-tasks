"""
Tests for the worker module.
"""
import asyncio
import json
import os
import signal
import tempfile
import pytest
import warnings
from unittest.mock import patch, MagicMock, AsyncMock, call

# Filter coroutine warnings for these tests
warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

from cloud_tasks.worker.worker import Worker


@pytest.fixture
def mock_task_queue():
    """Fixture to provide a mock TaskQueue."""
    queue = AsyncMock()
    queue.get_queue_depth.return_value = 10
    return queue


@pytest.fixture
def config_file():
    """Fixture to provide a temporary configuration file."""
    config = {
        'provider': 'aws',
        'queue_name': 'test-queue',
        'job_id': 'test-job',
        'tasks_per_worker': 3,
        'config': {
            'access_key': 'test-key',
            'secret_key': 'test-secret',
            'region': 'us-east-1'
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        json.dump(config, f)
        config_path = f.name

    yield config_path

    # Clean up
    os.unlink(config_path)


@pytest.fixture
def mock_create_queue():
    """Fixture to mock the create_queue function."""
    with patch('cloud_tasks.queue_manager.create_queue') as mock:
        yield mock


@pytest.fixture
def worker(config_file, mock_create_queue, mock_task_queue):
    """Fixture to provide a Worker instance with a mock task queue."""
    # Mock create_queue to return our mock task queue
    mock_create_queue.return_value = mock_task_queue

    # Create worker
    worker = Worker(config_file)

    # Manually initialize task_queue since we're not calling initialize()
    worker.task_queue = mock_task_queue

    # Return worker
    return worker


def test_load_config(config_file):
    """Test loading configuration from a file."""
    # Create worker
    worker = Worker(config_file)

    # Verify configuration was loaded correctly
    assert worker.provider == 'aws'
    assert worker.queue_name == 'test-queue'
    assert worker.job_id == 'test-job'
    assert worker.tasks_per_worker == 3
    assert worker.provider_config == {
        'access_key': 'test-key',
        'secret_key': 'test-secret',
        'region': 'us-east-1'
    }


@pytest.mark.asyncio
async def test_initialize(worker, mock_create_queue, mock_task_queue):
    """Test initializing the worker."""
    # Initialize worker
    await worker.initialize()

    # Verify create_queue was called with correct parameters
    mock_create_queue.assert_called_once_with(
        provider='aws',
        queue_name='test-queue',
        config={
            'access_key': 'test-key',
            'secret_key': 'test-secret',
            'region': 'us-east-1'
        }
    )

    # Verify task queue was initialized
    assert worker.task_queue == mock_task_queue

    # Verify get_queue_depth was called
    mock_task_queue.get_queue_depth.assert_called_once()


@pytest.mark.asyncio
async def test_signal_handler(worker):
    """Test the signal handler."""
    # Mock the shutdown event
    worker.shutdown_event = MagicMock()
    worker.shutdown_event.is_set.return_value = False

    # Call the signal handler
    worker._signal_handler(signal.SIGTERM, None)

    # Verify termination_requested was set
    assert worker._termination_requested == True


@pytest.mark.asyncio
async def test_process_task_success(worker, mock_task_queue):
    """Test processing a task successfully."""
    # Mock receive_tasks to return a task
    mock_task_queue.receive_tasks.return_value = [{
        'task_id': 'task-1',
        'data': {'key': 'value'},
        'receipt_handle': 'receipt-1'
    }]

    # Mock _execute_task to return success
    worker._execute_task = AsyncMock(return_value=(True, {'status': 'completed'}))

    # Process task
    await worker._process_task()

    # Verify receive_tasks was called
    mock_task_queue.receive_tasks.assert_called_once_with(max_count=1)

    # Verify _execute_task was called with correct parameters
    worker._execute_task.assert_called_once_with('task-1', {'key': 'value'})

    # Verify complete_task was called
    mock_task_queue.complete_task.assert_called_once_with('receipt-1')

    # Verify tasks_processed was incremented
    assert worker.tasks_processed == 1
    assert worker.tasks_failed == 0


@pytest.mark.asyncio
async def test_process_task_failure(worker, mock_task_queue):
    """Test processing a task that fails."""
    # Mock receive_tasks to return a task
    mock_task_queue.receive_tasks.return_value = [{
        'task_id': 'task-1',
        'data': {'key': 'value'},
        'receipt_handle': 'receipt-1'
    }]

    # Mock _execute_task to return failure
    worker._execute_task = AsyncMock(return_value=(False, 'Error processing task'))

    # Process task
    await worker._process_task()

    # Verify receive_tasks was called
    mock_task_queue.receive_tasks.assert_called_once_with(max_count=1)

    # Verify _execute_task was called
    worker._execute_task.assert_called_once_with('task-1', {'key': 'value'})

    # Verify fail_task was called
    mock_task_queue.fail_task.assert_called_once_with('receipt-1')

    # Verify tasks_failed was incremented
    assert worker.tasks_processed == 0
    assert worker.tasks_failed == 1


@pytest.mark.asyncio
async def test_execute_task(worker):
    """Test executing a task."""
    # Execute a normal task
    task_id = 'test-task'
    task_data = {'size': 0.001}  # Very small size to make test fast

    success, result = await worker._execute_task(task_id, task_data)

    # Verify success
    assert success == True
    assert result['status'] == 'completed'
    assert result['task_id'] == task_id

    # Execute a failing task
    task_data = {'should_fail': True}

    success, result = await worker._execute_task(task_id, task_data)

    # Verify failure
    assert success == False
    assert result == 'Task was configured to fail'


@pytest.mark.asyncio
async def test_check_termination_notice(worker):
    """Test checking for termination notice."""
    # On AWS should return False (simplified implementation)
    worker.provider = 'aws'
    assert await worker._check_termination_notice() == False

    # On GCP should return False (simplified implementation)
    worker.provider = 'gcp'
    assert await worker._check_termination_notice() == False

    # On Azure should return False (simplified implementation)
    worker.provider = 'azure'
    assert await worker._check_termination_notice() == False


class MockTaskSet:
    """A mock class that behaves like a set but allows mocking methods."""

    def __init__(self):
        self.items = set()

    def add(self, item):
        self.items.add(item)

    def discard(self, item):
        self.items.discard(item)

    def __len__(self):
        return len(self.items)


@pytest.mark.asyncio
async def test_processing_loop(worker):
    """Test the main processing loop."""
    # Set worker as running for the loop to execute
    worker.running = True

    # Mock required methods
    worker._cleanup_worker_tasks = MagicMock()
    worker._process_task = AsyncMock()
    worker.shutdown_event = MagicMock()

    # Replace worker tasks with our custom mockable set
    mock_tasks = MockTaskSet()
    worker.worker_tasks = mock_tasks

    # Spy on add and discard methods
    original_add = mock_tasks.add
    original_discard = mock_tasks.discard
    mock_tasks.add = MagicMock(wraps=original_add)
    mock_tasks.discard = MagicMock(wraps=original_discard)

    # Set up sequence for shutdown event
    # First call returns False, second call returns True to stop the loop
    worker.shutdown_event.is_set.side_effect = [False, True]

    # Mock asyncio.create_task
    with patch('asyncio.create_task') as mock_create_task:
        mock_task = MagicMock()
        mock_create_task.return_value = mock_task

        # Run the processing loop
        await worker._processing_loop()

        # Verify _cleanup_worker_tasks was called
        worker._cleanup_worker_tasks.assert_called_once()

        # Verify create_task was called at least once
        assert mock_create_task.call_count >= 1

        # Verify the create_task was called with a coroutine
        args, kwargs = mock_create_task.call_args
        assert len(args) == 1
        assert asyncio.iscoroutine(args[0]), "create_task should be called with a coroutine"

        # Verify task was added to worker_tasks
        mock_tasks.add.assert_called_with(mock_task)


@pytest.mark.asyncio
async def test_check_termination_loop(worker, monkeypatch):
    """Test the termination check loop."""
    # Set worker as running
    worker.running = True

    # Create an async function to replace _check_termination_notice
    async def mock_check_termination():
        return True

    # Use monkeypatch which is more reliable for avoiding warnings
    monkeypatch.setattr(worker, '_check_termination_notice', mock_check_termination)

    # Mock the shutdown event
    worker.shutdown_event = MagicMock()
    worker.shutdown_event.is_set.side_effect = [False, True]  # First call False, then True to exit loop

    # Run the termination check loop
    await worker._check_termination_loop()

    # Verify shutdown_event.set was called
    worker.shutdown_event.set.assert_called_once()

    # Verify termination_requested was set
    assert worker._termination_requested == True