"""
Tests for the cloud_tasks.worker.cloud_adapter module.
"""
import asyncio
import json
import os
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
    # Mock the _setup_cloud_clients method to avoid actual cloud client initialization
    with mock.patch.object(CloudTaskAdapter, '_setup_cloud_clients'):
        adapter = CloudTaskAdapter(sample_task_processor, config=sample_config)

        # Check that configuration was loaded correctly
        assert adapter.provider == "aws"
        assert adapter.job_id == "test-job"
        assert adapter.queue_name == "test-queue"
        assert adapter.result_bucket == "test-bucket"
        assert adapter.result_prefix == "test-results"
        assert adapter.max_retries == 2
        assert adapter.check_termination_interval == 1


@pytest.mark.asyncio
async def test_cloud_adapter_config_file(config_file):
    """Test that the CloudTaskAdapter loads configuration from a file."""
    # Mock the _setup_cloud_clients method to avoid actual cloud client initialization
    with mock.patch.object(CloudTaskAdapter, '_setup_cloud_clients'):
        adapter = CloudTaskAdapter(sample_task_processor, config_file=config_file)

        # Check that configuration was loaded correctly
        assert adapter.provider == "aws"
        assert adapter.job_id == "test-job"
        assert adapter.queue_name == "test-queue"


@pytest.mark.asyncio
async def test_process_task_with_retries():
    """Test that the process_task_with_retries method works correctly."""
    # Mock the _setup_cloud_clients method to avoid actual cloud client initialization
    with mock.patch.object(CloudTaskAdapter, '_setup_cloud_clients'):
        adapter = CloudTaskAdapter(
            sample_task_processor,
            config={
                "provider": "aws",
                "job_id": "test-job",
                "queue_name": "test-queue",
                "worker_options": {
                    "max_retries": 2
                }
            }
        )

        # Test successful task processing
        task_id = "test-task"
        task_data = {"num1": 42, "num2": 58}
        success, result = await adapter.process_task_with_retries(task_id, task_data)

        assert success is True
        assert result == 100

        # Test failed task processing with retries
        failing_processor = mock.AsyncMock(side_effect=[
            (False, "First failure"),
            (False, "Second failure"),
            (True, 100)
        ])
        adapter.task_processor = failing_processor

        success, result = await adapter.process_task_with_retries(task_id, task_data)

        assert success is True
        assert result == 100
        assert failing_processor.call_count == 3

        # Test task that fails all retries
        always_failing_processor = mock.AsyncMock(return_value=(False, "Always fails"))
        adapter.task_processor = always_failing_processor

        success, result = await adapter.process_task_with_retries(task_id, task_data)

        assert success is False
        assert "Failed after" in result
        assert always_failing_processor.call_count == 3  # Initial + 2 retries


@pytest.mark.asyncio
async def test_run_cloud_worker():
    """Test that the run_cloud_worker function initializes and runs the adapter."""
    # Create a mock adapter
    mock_adapter = mock.AsyncMock()
    mock_adapter.run = mock.AsyncMock()

    # Mock the CloudTaskAdapter class to return our mock
    with mock.patch('cloud_tasks.worker.cloud_adapter.CloudTaskAdapter', return_value=mock_adapter):
        # Mock argparse to avoid command line argument parsing
        with mock.patch('argparse.ArgumentParser.parse_args', return_value=mock.Mock(config=None)):
            # Mock os.path.exists to simulate finding a default config file
            with mock.patch('os.path.exists', return_value=True):
                # Call run_cloud_worker with a sample task processor
                await run_cloud_worker(sample_task_processor)

                # Check that the adapter's run method was called
                mock_adapter.run.assert_called_once()


@pytest.mark.asyncio
async def test_termination_handling():
    """Test that the adapter handles termination signals correctly."""
    # Mock the _setup_cloud_clients method to avoid actual cloud client initialization
    with mock.patch.object(CloudTaskAdapter, '_setup_cloud_clients'):
        adapter = CloudTaskAdapter(
            sample_task_processor,
            config={
                "provider": "aws",
                "job_id": "test-job",
                "queue_name": "test-queue"
            }
        )

        # Simulate a termination signal
        adapter._handle_termination(15, None)

        # Check that the terminating flag was set
        assert adapter.terminating is True