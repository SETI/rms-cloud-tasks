"""Shared fixtures for worker tests."""

import json
import os
import tempfile
from unittest.mock import AsyncMock, patch

import pytest
import yaml

from cloud_tasks.worker.worker import Worker


def _mock_worker_function(task_id, task_data, worker):
    """Default mock worker function returning success."""
    return False, "success"


@pytest.fixture
def mock_worker_function():
    """Fixture returning the shared mock worker function."""
    return _mock_worker_function


@pytest.fixture
def local_task_file_json():
    """Temporary JSON file containing two sample tasks."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(
            [
                {"task_id": "task1", "data": {"key": "value1"}},
                {"task_id": "task2", "data": {"key": "value2"}},
            ],
            f,
        )
    yield f.name
    os.unlink(f.name)


@pytest.fixture
def local_task_file_yaml():
    """Temporary YAML file containing two sample tasks."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(
            [
                {"task_id": "task3", "data": {"key": "value3"}},
                {"task_id": "task4", "data": {"key": "value4"}},
            ],
            f,
        )
    yield f.name
    os.unlink(f.name)


@pytest.fixture
def mock_queue():
    """Async mock task queue with receive/ack/retry methods."""
    queue = AsyncMock()
    queue.receive_tasks = AsyncMock()
    queue.acknowledge_task = AsyncMock()
    queue.retry_task = AsyncMock()
    return queue


@pytest.fixture
def sample_task():
    """Single sample task dict with task_id, data, and ack_id."""
    return {"task_id": "test-task-1", "data": {"key": "value"}, "ack_id": "test-ack-1"}


@pytest.fixture
def mock_task_factory():
    """Create a mock task factory that yields tasks."""

    def factory():
        tasks = [
            {"task_id": "factory-task-1", "data": {"key": "value1"}},
            {"task_id": "factory-task-2", "data": {"key": "value2"}},
            {"task_id": "factory-task-3", "data": {"key": "value3"}},
        ]
        yield from tasks

    return factory


@pytest.fixture
def mock_task_factory_empty():
    """Create a mock task factory that yields no tasks."""

    def factory():
        return
        yield  # This line is never reached

    return factory


@pytest.fixture
def worker(mock_worker_function):
    """Worker instance with provider and job-id from env (for tests that need a ready worker)."""
    os.environ["RMS_CLOUD_TASKS_PROVIDER"] = "AWS"
    os.environ["RMS_CLOUD_TASKS_JOB_ID"] = "test-job"
    with patch("sys.argv", ["worker.py"]):
        return Worker(mock_worker_function)


@pytest.fixture
def env_setup_teardown(monkeypatch):
    """Set env vars for Worker (true/truthy values); restore in teardown."""
    original_env = os.environ.copy()
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "AWS")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setenv("RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE", "true")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_TYPE", "t2.micro")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS", "2")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_MEM_GB", "4")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_SSD_GB", "100")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB", "20")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_IS_SPOT", "true")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_PRICE", "0.1")
    monkeypatch.setenv("RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE", "4")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_RUNTIME", "3600")
    monkeypatch.setenv("RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT", "true")
    monkeypatch.setenv("RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION", "true")
    monkeypatch.setenv("RMS_CLOUD_TASKS_RETRY_ON_EXIT", "true")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD", "300")
    monkeypatch.setenv("RMS_CLOUD_TASKS_TO_SKIP", "5")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_NUM_TASKS", "10")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER", "32")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY", "33")
    monkeypatch.setenv("RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE", "true")
    yield
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def env_setup_teardown_false(monkeypatch):
    """Set env vars for Worker (false/falsy values); restore in teardown."""
    original_env = os.environ.copy()
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "AWS")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setenv("RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE", "False")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_TYPE", "t2.micro")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS", "2")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_MEM_GB", "4")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_SSD_GB", "100")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB", "20")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_IS_SPOT", "false")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_PRICE", "0.1")
    monkeypatch.setenv("RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE", "4")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_RUNTIME", "3600")
    monkeypatch.setenv("RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT", "false")
    monkeypatch.setenv("RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION", "false")
    monkeypatch.setenv("RMS_CLOUD_TASKS_RETRY_ON_EXIT", "false")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD", "300")
    monkeypatch.setenv("RMS_CLOUD_TASKS_TO_SKIP", "5")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_NUM_TASKS", "10")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER", "32")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY", "33")
    monkeypatch.setenv("RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE", "false")
    yield
    os.environ.clear()
    os.environ.update(original_env)
