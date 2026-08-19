"""Tests for the per-task memory limit support."""

import asyncio
import logging
import sys
import time
from collections.abc import Callable
from typing import Any
from unittest.mock import ANY, MagicMock, patch

import pytest

from cloud_tasks.worker.worker import Worker

#: Signature of the callable a Worker is constructed with, as built by the
#: mock_worker_function fixture: (task_id, task_data, worker) -> (retry, result).
WorkerFunction = Callable[[str, dict[str, Any], Any], tuple[bool, str]]


def test_max_memory_default_no_limit(
    mock_worker_function: WorkerFunction, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The maximum memory per task defaults to None (no limit)."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "GCP")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setattr(sys, "argv", ["worker.py"])
    worker = Worker(mock_worker_function)
    assert worker._data.max_memory_allowed_per_task is None


def test_max_memory_from_env(
    mock_worker_function: WorkerFunction, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The maximum memory per task can be set via the environment variable."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "GCP")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_MEMORY_ALLOWED_PER_TASK", "2.5")
    monkeypatch.setattr(sys, "argv", ["worker.py"])
    worker = Worker(mock_worker_function)
    assert worker._data.max_memory_allowed_per_task == 2.5


def test_max_memory_from_args(
    mock_worker_function: WorkerFunction, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--max-memory-allowed-per-task overrides the environment variable."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "GCP")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_MEMORY_ALLOWED_PER_TASK", "2.5")
    monkeypatch.setattr(sys, "argv", ["worker.py", "--max-memory-allowed-per-task", "4"])
    worker = Worker(mock_worker_function)
    assert worker._data.max_memory_allowed_per_task == 4.0


def _worker_data_mock(max_memory_gb: float | None) -> MagicMock:
    """Build a mock WorkerData with the given memory limit."""
    worker_data = MagicMock()
    worker_data.max_memory_allowed_per_task = max_memory_gb
    worker_data.received_shutdown_request = False
    worker_data.received_termination_notice = False
    return worker_data


def test_worker_process_main_sets_memory_limit(mock_worker_function: WorkerFunction) -> None:
    """_worker_process_main applies the memory limit via resource.setrlimit."""
    result_queue = MagicMock()
    worker_data = _worker_data_mock(2.0)
    mock_resource = MagicMock()

    with (
        patch("cloud_tasks.worker.worker.resource", mock_resource),
        patch("sys.exit"),
    ):
        Worker._worker_process_main(
            1, mock_worker_function, worker_data, "test-task", {"key": "value"}, result_queue
        )

    expected_bytes = int(2.0 * 1024**3)
    mock_resource.setrlimit.assert_called_once_with(
        mock_resource.RLIMIT_AS, (expected_bytes, expected_bytes)
    )
    result_queue.put.assert_called_once_with((1, False, "success"))


def test_worker_process_main_no_memory_limit(mock_worker_function: WorkerFunction) -> None:
    """_worker_process_main does not touch rlimits when no limit is configured."""
    result_queue = MagicMock()
    worker_data = _worker_data_mock(None)
    mock_resource = MagicMock()

    with (
        patch("cloud_tasks.worker.worker.resource", mock_resource),
        patch("sys.exit"),
    ):
        Worker._worker_process_main(
            1, mock_worker_function, worker_data, "test-task", {"key": "value"}, result_queue
        )

    mock_resource.setrlimit.assert_not_called()
    result_queue.put.assert_called_once_with((1, False, "success"))


def test_worker_process_main_memory_limit_without_resource_module(
    mock_worker_function: WorkerFunction, caplog: pytest.LogCaptureFixture
) -> None:
    """A configured memory limit is skipped with a warning when resource is unavailable."""
    result_queue = MagicMock()
    worker_data = _worker_data_mock(2.0)

    with (
        patch("cloud_tasks.worker.worker.resource", None),
        patch("sys.exit"),
        caplog.at_level(logging.WARNING, logger="cloud_tasks.worker.worker"),
    ):
        Worker._worker_process_main(
            1, mock_worker_function, worker_data, "test-task", {"key": "value"}, result_queue
        )

    # The caller must be told the limit they asked for is not in effect
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "'resource' module is not available" in m and "no limit will be applied" in m
        for m in warnings
    ), warnings
    # The task still runs normally
    result_queue.put.assert_called_once_with((1, False, "success"))


def test_worker_process_main_setrlimit_failure(
    mock_worker_function: WorkerFunction, caplog: pytest.LogCaptureFixture
) -> None:
    """A setrlimit failure is logged as a warning and the task still runs."""
    result_queue = MagicMock()
    worker_data = _worker_data_mock(2.0)
    mock_resource = MagicMock()
    mock_resource.setrlimit.side_effect = ValueError("cannot raise hard limit")

    with (
        patch("cloud_tasks.worker.worker.resource", mock_resource),
        patch("sys.exit"),
        caplog.at_level(logging.WARNING, logger="cloud_tasks.worker.worker"),
    ):
        Worker._worker_process_main(
            1, mock_worker_function, worker_data, "test-task", {"key": "value"}, result_queue
        )

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "Failed to set memory limit" in m and "cannot raise hard limit" in m for m in warnings
    ), warnings
    result_queue.put.assert_called_once_with((1, False, "success"))


def test_worker_process_main_memory_error(mock_worker_function: WorkerFunction) -> None:
    """A MemoryError from the task is reported as a memory_error result."""

    def oom_worker_function(task_id: str, task_data: dict[str, Any], worker: Any):
        raise MemoryError("out of memory")

    result_queue = MagicMock()
    worker_data = _worker_data_mock(2.0)
    mock_resource = MagicMock()

    with (
        patch("cloud_tasks.worker.worker.resource", mock_resource),
        patch("sys.exit"),
    ):
        Worker._worker_process_main(
            1, oom_worker_function, worker_data, "test-task", {"key": "value"}, result_queue
        )

    result_queue.put.assert_called_once_with((1, "memory_error", ANY))
    assert "MemoryError" in result_queue.put.call_args.args[0][2]


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_on_exception", [True, False])
async def test_handle_results_memory_error_never_retried(
    worker: Any, mock_queue: Any, retry_on_exception: bool
) -> None:
    """A memory_error result is acknowledged and never retried, even with retry-on-exception."""
    mock_proc = MagicMock()
    mock_proc.is_alive.return_value = True
    mock_proc.pid = 1234

    worker._task_queue = mock_queue
    worker._running = True
    worker._data.retry_on_exception = retry_on_exception
    worker._data.max_memory_allowed_per_task = 2.0
    worker._event_logger_queue = None
    worker._event_logger_fp = None

    worker_id = 1
    task = {"task_id": "task1", "ack_id": "ack1"}
    worker._processes = {
        worker_id: {
            "process": mock_proc,
            "task": task,
            "start_time": time.time(),
        }
    }
    worker._result_queue.put((worker_id, "memory_error", "MemoryError traceback"))

    async def stop_when_done() -> None:
        """Wait for the task to be processed, then stop the worker."""
        start_time = time.time()
        while time.time() - start_time < 2.0:
            if worker._num_tasks_not_retried == 1 or worker._num_tasks_retried == 1:
                break
            await asyncio.sleep(0.01)
        worker._running = False

    handler_task = asyncio.create_task(worker._handle_results())
    stop_task = asyncio.create_task(stop_when_done())
    await asyncio.wait_for(asyncio.gather(handler_task, stop_task), timeout=3.0)

    assert worker._num_tasks_not_retried == 1
    assert worker._num_tasks_retried == 0
    assert worker._num_tasks_memory_exceeded == 1
    mock_queue.acknowledge_task.assert_called_once_with("ack1")
    mock_queue.retry_task.assert_not_called()


@pytest.mark.asyncio
async def test_handle_results_memory_error_logs_nonretriable_exception_event(
    worker: Any, mock_queue: Any
) -> None:
    """A memory_error result is logged as a task_exception event with retry False."""
    from unittest.mock import AsyncMock

    mock_proc = MagicMock()
    mock_proc.is_alive.return_value = True
    mock_proc.pid = 1234

    worker._task_queue = mock_queue
    worker._running = True
    worker._data.retry_on_exception = True
    worker._data.max_memory_allowed_per_task = 2.0

    worker_id = 1
    task = {"task_id": "task1", "ack_id": "ack1"}
    worker._processes = {
        worker_id: {
            "process": mock_proc,
            "task": task,
            "start_time": time.time(),
        }
    }
    worker._result_queue.put((worker_id, "memory_error", "MemoryError traceback"))

    with patch.object(worker, "_log_task_exception", new_callable=AsyncMock) as mock_log:

        async def stop_when_done() -> None:
            """Wait for the task to be processed, then stop the worker."""
            start_time = time.time()
            while time.time() - start_time < 2.0:
                if worker._num_tasks_not_retried == 1:
                    break
                await asyncio.sleep(0.01)
            worker._running = False

        handler_task = asyncio.create_task(worker._handle_results())
        stop_task = asyncio.create_task(stop_when_done())
        await asyncio.wait_for(asyncio.gather(handler_task, stop_task), timeout=3.0)

    mock_log.assert_awaited_once_with(
        "task1", retry=False, elapsed_time=ANY, exception="MemoryError traceback"
    )


@pytest.mark.parametrize("bad_value", ["-1", "0", "nan", "inf"])
def test_max_memory_rejects_invalid_values(
    mock_worker_function: WorkerFunction, monkeypatch: pytest.MonkeyPatch, bad_value: str
) -> None:
    """A non-positive or non-finite memory limit is rejected during worker startup.

    A worker configured from the command line or the environment doesn't go through the
    manager's RunConfig validation. Left unchecked, a non-finite value raises while being
    converted to bytes in every task process, and a negative one is rejected by setrlimit
    and leaves the task running with no limit at all despite one having been requested.
    """
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "GCP")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setattr(sys, "argv", ["worker.py", "--max-memory-allowed-per-task", bad_value])

    with patch("cloud_tasks.worker.worker.sys.exit", side_effect=SystemExit(1)) as mock_exit:
        with pytest.raises(SystemExit):
            Worker(mock_worker_function)

    mock_exit.assert_called_once_with(1)


def test_max_memory_accepts_small_positive_value(
    mock_worker_function: WorkerFunction, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A small but positive memory limit is accepted."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "GCP")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setattr(sys, "argv", ["worker.py", "--max-memory-allowed-per-task", "0.25"])
    worker = Worker(mock_worker_function)
    assert worker._data.max_memory_allowed_per_task == 0.25
