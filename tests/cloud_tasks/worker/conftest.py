"""Shared fixtures for worker tests.

This module provides pytest fixtures used across worker tests: mock worker
functions, temporary task files (JSON/YAML), mock async task queues (receive_tasks,
acknowledge_task, retry_task), sample task dicts, task factories, a Worker instance
with env/argv set for tests, and env setup/teardown for config tests. Fixtures are
function-scoped unless otherwise noted. Use the worker fixture when a ready Worker
with provider and job_id is needed; use env_setup_teardown when testing config
parsing from environment.
"""

import json
import os
import sys
import tempfile
from collections.abc import Callable, Generator, Iterable
from typing import Any
from unittest.mock import AsyncMock

import pytest
import yaml

from cloud_tasks.worker.worker import Worker


def _mock_worker_function(task_id: str, task_data: dict[str, Any], worker: Any) -> tuple[bool, str]:
    """Default mock worker function for tests; returns (success, message)."""
    return False, "success"


@pytest.fixture
def mock_worker_function() -> Callable[[str, dict[str, Any], Any], tuple[bool, str]]:
    """Fixture returning a callable that accepts (task_id, task_data, worker) and returns (bool, str)."""
    return _mock_worker_function


@pytest.fixture
def local_task_file_json() -> Iterable[str]:
    """Create a temporary JSON file with two sample tasks; yield path then unlink."""
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
def local_task_file_yaml() -> Iterable[str]:
    """Create a temporary YAML file with two sample tasks; yield path then unlink."""
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
def mock_queue() -> AsyncMock:
    """Async mock task queue; receive_tasks, acknowledge_task, retry_task are AsyncMock instances."""
    queue = AsyncMock()
    queue.receive_tasks = AsyncMock()
    queue.acknowledge_task = AsyncMock()
    queue.retry_task = AsyncMock()
    return queue


@pytest.fixture
def sample_task() -> dict[str, object]:
    """Single sample task dict: task_id (str), data (dict), ack_id (str)."""
    return {"task_id": "test-task-1", "data": {"key": "value"}, "ack_id": "test-ack-1"}


@pytest.fixture
def mock_task_factory() -> Callable[[], Iterable[dict[str, Any]]]:
    """Mock task factory yielding a sequence of task dicts with task_id and data."""

    def factory() -> Iterable[dict[str, Any]]:
        tasks: list[dict[str, Any]] = [
            {"task_id": "factory-task-1", "data": {"key": "value1"}},
            {"task_id": "factory-task-2", "data": {"key": "value2"}},
            {"task_id": "factory-task-3", "data": {"key": "value3"}},
        ]
        yield from tasks

    return factory


@pytest.fixture
def mock_task_factory_empty() -> Callable[[], Iterable[dict[str, Any]]]:
    """Mock task factory that yields no tasks (empty iterator)."""

    def factory() -> Iterable[dict[str, Any]]:
        yield from ()

    return factory


@pytest.fixture
def worker(
    mock_worker_function: Callable[..., tuple[bool, str]], monkeypatch: pytest.MonkeyPatch
) -> Generator[Worker, None, None]:
    """Worker instance with provider and job-id from env; sys.argv patched for test scope."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "AWS")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setattr(sys, "argv", ["worker.py"])
    yield Worker(mock_worker_function)


_COMMON_ENV_VARS: list[tuple[str, str]] = [
    ("RMS_CLOUD_TASKS_PROVIDER", "AWS"),
    ("RMS_CLOUD_TASKS_JOB_ID", "test-job"),
    ("RMS_CLOUD_TASKS_INSTANCE_TYPE", "t2.micro"),
    ("RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS", "2"),
    ("RMS_CLOUD_TASKS_INSTANCE_MEM_GB", "4"),
    ("RMS_CLOUD_TASKS_INSTANCE_SSD_GB", "100"),
    ("RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB", "20"),
    ("RMS_CLOUD_TASKS_INSTANCE_PRICE", "0.1"),
    ("RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE", "4"),
    ("RMS_CLOUD_TASKS_MAX_RUNTIME", "3600"),
    ("RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD", "300"),
    ("RMS_CLOUD_TASKS_TO_SKIP", "5"),
    ("RMS_CLOUD_TASKS_MAX_NUM_TASKS", "10"),
    ("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER", "32"),
    ("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY", "33"),
]


def _apply_env_vars(monkeypatch: pytest.MonkeyPatch, overrides: dict[str, str]) -> None:
    """Set common env vars and overrides via monkeypatch.

    Parameters:
        monkeypatch: Pytest MonkeyPatch used to set environment variables.
        overrides: Dict of env var names to values to apply (merged with _COMMON_ENV_VARS).
    """
    for key, value in _COMMON_ENV_VARS:
        monkeypatch.setenv(key, value)
    for key, value in overrides.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def env_setup_teardown(monkeypatch: pytest.MonkeyPatch) -> Iterable[None]:
    """Set env vars for Worker (true/truthy values); monkeypatch restores after test."""
    _apply_env_vars(
        monkeypatch,
        {
            "RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE": "true",
            "RMS_CLOUD_TASKS_INSTANCE_IS_SPOT": "true",
            "RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT": "true",
            "RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION": "true",
            "RMS_CLOUD_TASKS_RETRY_ON_EXIT": "true",
            "RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE": "true",
        },
    )
    yield


@pytest.fixture
def env_setup_teardown_false(monkeypatch: pytest.MonkeyPatch) -> Iterable[None]:
    """Set env vars for Worker (false/falsy values); monkeypatch restores after test."""
    _apply_env_vars(
        monkeypatch,
        {
            "RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE": "False",
            "RMS_CLOUD_TASKS_INSTANCE_IS_SPOT": "false",
            "RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT": "false",
            "RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION": "false",
            "RMS_CLOUD_TASKS_RETRY_ON_EXIT": "false",
            "RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE": "false",
        },
    )
    yield
