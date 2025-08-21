"""Tests for the worker module."""

import json
from filecache import FCPath
import os
import pytest
import tempfile
from unittest.mock import patch
from pathlib import Path

from cloud_tasks.worker.worker import Worker


@pytest.fixture
def sample_task():
    return {"task_id": "test-task-1", "data": {"key": "value"}, "ack_id": "test-ack-1"}


def _mock_worker_function(task_id, task_data, worker):
    return False, "success"


@pytest.fixture
def mock_worker_function():
    return _mock_worker_function


@pytest.fixture
def local_task_file_json():
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
def mock_task_factory():
    """Create a mock task factory that yields tasks."""

    def factory():
        tasks = [
            {"task_id": "factory-task-1", "data": {"key": "value1"}},
            {"task_id": "factory-task-2", "data": {"key": "value2"}},
            {"task_id": "factory-task-3", "data": {"key": "value3"}},
        ]
        for task in tasks:
            yield task

    return factory


# Task source argument tests


def test_worker_init_with_task_source_string(mock_worker_function, local_task_file_json):
    """Test Worker initialization with task_source as string."""
    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function, task_source=local_task_file_json)
        assert worker._task_source == FCPath(local_task_file_json)


def test_worker_init_with_task_source_path(mock_worker_function, local_task_file_json):
    """Test Worker initialization with task_source as Path."""
    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function, task_source=Path(local_task_file_json))
        assert worker._task_source == FCPath(local_task_file_json)


def test_worker_init_with_task_source_fcpath(mock_worker_function, local_task_file_json):
    """Test Worker initialization with task_source as FCPath."""
    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function, task_source=FCPath(local_task_file_json))
        assert worker._task_source == FCPath(local_task_file_json)


def test_worker_init_with_task_source_factory(mock_worker_function, mock_task_factory):
    """Test Worker initialization with task_source as factory function."""
    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function, task_source=mock_task_factory)
        assert worker._task_source == mock_task_factory


def test_worker_init_with_task_source_overrides_command_line(
    mock_worker_function, local_task_file_json
):
    """Test that task_source overrides command line arguments."""
    with patch("sys.argv", ["worker.py", "--task-file", "different_file.json"]):
        worker = Worker(mock_worker_function, task_source=local_task_file_json)
        assert worker._task_source == FCPath(local_task_file_json)
        assert worker._task_source != FCPath("different_file.json")


def test_worker_init_with_task_source_overrides_environment(
    mock_worker_function, local_task_file_json
):
    """Test that task_source overrides environment variables."""
    with patch("sys.argv", ["worker.py"]):
        with patch.dict(os.environ, {"RMS_CLOUD_TASKS_TASK_FILE": "env_file.json"}):
            worker = Worker(mock_worker_function, task_source=local_task_file_json)
            assert worker._task_source == FCPath(local_task_file_json)
            assert worker._task_source != FCPath("env_file.json")


def test_worker_init_with_task_source_factory_overrides_command_line(
    mock_worker_function, mock_task_factory
):
    """Test that task_source factory overrides command line arguments."""
    with patch("sys.argv", ["worker.py", "--task-file", "some_file.json"]):
        worker = Worker(mock_worker_function, task_source=mock_task_factory)
        assert worker._task_source == mock_task_factory


def test_worker_init_with_task_source_factory_overrides_environment(
    mock_worker_function, mock_task_factory
):
    """Test that task_source factory overrides environment variables."""
    with patch("sys.argv", ["worker.py"]):
        with patch.dict(os.environ, {"RMS_CLOUD_TASKS_TASK_FILE": "env_file.json"}):
            worker = Worker(mock_worker_function, task_source=mock_task_factory)
            assert worker._task_source == mock_task_factory


def test_worker_init_with_task_source_none_uses_command_line(mock_worker_function):
    """Test Worker initialization with task_source=None uses command line argument."""
    with patch("sys.argv", ["worker.py", "--task-file", "cmd_line_file.json"]):
        worker = Worker(mock_worker_function, task_source=None)
        assert str(worker._task_source) == "cmd_line_file.json"


def test_worker_init_with_task_source_none_uses_environment(mock_worker_function):
    """Test Worker initialization with task_source=None uses environment variable."""
    with patch("sys.argv", ["worker.py"]):
        with patch.dict(os.environ, {"RMS_CLOUD_TASKS_TASK_FILE": "env_file.json"}):
            worker = Worker(mock_worker_function, task_source=None)
            assert str(worker._task_source) == "env_file.json"


def test_worker_init_with_task_source_none_no_file_specified(mock_worker_function):
    """Test that when task_source is None and no file is specified, it exits."""
    with patch("sys.argv", ["worker.py"]):
        with patch.dict(os.environ, {}, clear=True):
            with patch("sys.exit") as mock_exit:
                Worker(mock_worker_function, task_source=None)
                # Multiple validation errors will cause multiple exit calls
                assert mock_exit.call_count >= 1


def test_worker_init_with_task_source_none_logging(mock_worker_function, caplog):
    """Test that Worker doesn't log task source when task_source is None."""
    with patch("sys.argv", ["worker.py"]):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(SystemExit) as exc_info:
                Worker(mock_worker_function, task_source=None)
            assert exc_info.value.code == 1
            assert "Provider not specified" in caplog.text


def test_worker_init_with_task_source_string_logging(
    mock_worker_function, local_task_file_json, caplog
):
    """Test that Worker logs when using task_source as string."""
    with patch("sys.argv", ["worker.py"]):
        _ = Worker(mock_worker_function, task_source=local_task_file_json)
        assert f'Using local tasks file: "{local_task_file_json}"' in caplog.text


def test_worker_init_with_task_source_factory_logging(
    mock_worker_function, mock_task_factory, caplog
):
    """Test that Worker logs when using task_source as factory."""
    with patch("sys.argv", ["worker.py"]):
        _ = Worker(mock_worker_function, task_source=mock_task_factory)
        assert "Using task factory function" in caplog.text
