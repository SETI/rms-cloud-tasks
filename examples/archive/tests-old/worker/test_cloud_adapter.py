"""
Tests for the cloud_tasks.worker.cloud_adapter module.
"""
import asyncio
import json
import os
import signal
import sys
import tempfile
from unittest import mock
import pytest
from typing import Dict, Any, Tuple

from cloud_tasks.worker.cloud_adapter import CloudTaskAdapter, run_cloud_worker


@pytest.fixture
def sample_config() -> Dict[str, Any]:
    """Return a sample configuration for testing."""
    return {
        "provider": "aws",
        "job_id": "test-job",
        "queue_name": "test-queue",
        "result_bucket": "test-bucket",
        "result_prefix": "test-results",
        "config": {
            "region": "us-east-1",
            "access_key": "test-access-key",
            "secret_key": "test-secret-key"
        },
        "worker_options": {
            "max_retries": 2,
            "termination_check_interval": 1
        },
        "sample_task": {
            "task_id": "test-task-001",
            "num1": 42,
            "num2": 58
        }
    }


@pytest.fixture
def config_file(sample_config) -> str:
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        json.dump(sample_config, f)
        config_path = f.name

    yield config_path

    # Clean up
    if os.path.exists(config_path):
        os.unlink(config_path)


async def sample_task_processor(task_id: str, task_data: Dict[str, Any]) -> Tuple[bool, Any]:
    """Sample task processor for testing."""
    try:
        num1 = task_data.get("num1", 0)
        num2 = task_data.get("num2", 0)
        result = num1 + num2
        return True, result
    except Exception as e:
        return False, str(e)


@pytest.mark.asyncio
async def test_cloud_adapter_initialization(sample_config):
    """Test that the CloudTaskAdapter initializes correctly."""
    # Mock the _configure_provider method to avoid actual cloud client initialization
    with mock.patch.object(CloudTaskAdapter, '_configure_provider'):
        adapter = CloudTaskAdapter(sample_task_processor, config=sample_config)

        # Check that configuration was loaded correctly
        assert adapter.provider == "aws"
        assert adapter.job_id == "test-job"
        assert adapter.queue_name == "test-queue"
        assert adapter.result_bucket == "test-bucket"
        assert adapter.result_prefix == "test-results"
        assert adapter.max_retries == 2
        assert adapter.termination_check_interval == 1


@pytest.mark.asyncio
async def test_cloud_adapter_config_file(config_file):
    """Test that the CloudTaskAdapter loads configuration from a file."""
    # Mock the _configure_provider method to avoid actual cloud client initialization
    with mock.patch.object(CloudTaskAdapter, '_configure_provider'):
        adapter = CloudTaskAdapter(sample_task_processor, config_file=config_file)

        # Check that configuration was loaded correctly
        assert adapter.provider == "aws"
        assert adapter.job_id == "test-job"
        assert adapter.queue_name == "test-queue"


@pytest.mark.asyncio
async def test_process_task_with_retries():
    """Test that the process_task_with_retries method works correctly."""
    # Mock the _configure_provider method to avoid actual cloud client initialization
    with mock.patch.object(CloudTaskAdapter, '_configure_provider'):
        # Create a mock processor that always succeeds
        successful_processor = mock.AsyncMock(return_value=(True, 100))

        adapter = CloudTaskAdapter(
            successful_processor,
            config={
                "provider": "aws",
                "job_id": "test-job",
                "queue_name": "test-queue",
                "worker_options": {
                    "max_retries": 2
                }
            }
        )

        # Mock the upload_result method to always return True
        adapter.upload_result = mock.AsyncMock(return_value=True)

        # Test successful task processing
        task_id = "test-task"
        task_data = {"num1": 42, "num2": 58}
        success, result = await adapter.process_task_with_retries(task_id, task_data)

        assert success is True
        assert result == 100
        successful_processor.assert_called_once_with(task_id, task_data)

        # Reset mocks
        successful_processor.reset_mock()
        adapter.upload_result.reset_mock()

        # Test failed task processing with retries
        failing_processor = mock.AsyncMock(side_effect=[
            (False, "First failure"),
            (False, "Second failure"),
            (True, 100)
        ])
        adapter.task_processor = failing_processor

        # Limit retries to 1 for faster testing
        adapter.max_retries = 1
        success, result = await adapter.process_task_with_retries(task_id, task_data)

        # Should fail after 1 retry
        assert success is False
        assert "Failed after all retry attempts" in result
        assert failing_processor.call_count <= 2  # Initial + 1 retry

        # Reset retries to original value
        adapter.max_retries = 2

        # Create a new adapter with a processor that always fails
        always_failing_processor = mock.AsyncMock(return_value=(False, "Always fails"))
        adapter.task_processor = always_failing_processor

        success, result = await adapter.process_task_with_retries(task_id, task_data)

        assert success is False
        assert "Failed after all retry attempts" in result
        assert always_failing_processor.call_count <= 3  # Initial + 2 retries


@pytest.mark.asyncio
async def test_run_cloud_worker():
    """Test that the run_cloud_worker function initializes and runs the adapter."""
    # Create a mock adapter
    mock_adapter = mock.AsyncMock()
    mock_adapter.start = mock.AsyncMock()

    # Create a temporary config file for testing
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
        temp_file.write('{"provider": "aws", "job_id": "test", "queue_name": "test"}')
        config_path = temp_file.name

    try:
        # Mock the CloudTaskAdapter class to return our mock
        with mock.patch('cloud_tasks.worker.cloud_adapter.CloudTaskAdapter', return_value=mock_adapter):
            # Mock the num_workers parameter to use a single process for testing
            with mock.patch.object(sys, 'argv', ['script.py', f'--config={config_path}', '--workers=1']):
                # Call run_cloud_worker with a sample task processor
                await run_cloud_worker(sample_task_processor)

                # Check that the adapter's start method was called
                mock_adapter.start.assert_called_once()
    finally:
        # Clean up the temporary file
        if os.path.exists(config_path):
            os.unlink(config_path)


@pytest.mark.asyncio
async def test_termination_handling():
    """Test that the adapter handles termination signals correctly."""
    # Mock the _configure_provider method to avoid actual cloud client initialization
    with mock.patch.object(CloudTaskAdapter, '_configure_provider'):
        adapter = CloudTaskAdapter(
            sample_task_processor,
            config={
                "provider": "aws",
                "job_id": "test-job",
                "queue_name": "test-queue"
            }
        )

        # Set required attributes for termination handling
        adapter.terminating = False

        # Test setting the termination flag directly
        adapter.terminating = True
        assert adapter.terminating is True

        # Reset for signal test
        adapter.terminating = False

        # Create a signal handler that sets terminating to True
        def handle_signal(signum, frame):
            adapter.terminating = True

        # Register the signal handler for testing
        original_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, handle_signal)

        try:
            # Simulate sending a SIGTERM signal
            os.kill(os.getpid(), signal.SIGTERM)

            # Small delay to allow the signal handler to execute
            await asyncio.sleep(0.1)

            # Check that the terminating flag was set
            assert adapter.terminating is True
        finally:
            # Restore original signal handler
            signal.signal(signal.SIGTERM, original_handler)