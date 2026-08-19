"""Tests for how the worker reports tasks that exceed max_runtime."""

import sys
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cloud_tasks.worker.worker import Worker


@pytest.fixture
def timeout_worker(mock_worker_function, monkeypatch) -> Worker:
    """Worker with one process that has already exceeded max_runtime."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "GCP")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setattr(sys, "argv", ["worker.py"])
    worker = Worker(mock_worker_function)
    worker._data.max_runtime = 10
    worker._running = True
    process = MagicMock()
    process.pid = 1234
    process.is_alive.return_value = False
    worker._processes = {
        0: {
            "process": process,
            "start_time": time.time() - 100,
            "last_renewal_time": time.time(),
            "task": {"task_id": "slow-task", "ack_id": "ack-1"},
        }
    }
    return worker


async def _run_one_pass(worker: Worker) -> None:
    """Run _monitor_process_runtimes for a single pass and stop it."""

    async def stop_after_first_sleep(_seconds: float) -> None:
        worker._running = False

    with patch("cloud_tasks.worker.worker.asyncio.sleep", side_effect=stop_after_first_sleep):
        await worker._monitor_process_runtimes()


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_on_timeout", [True, False])
async def test_timed_out_event_retry_flag_matches_queue_action(
    timeout_worker: Worker, retry_on_timeout: bool
) -> None:
    """The retry flag on a task_timed_out event matches what happens to the message.

    A mismatch is not cosmetic: the task database derives its status from this flag, so
    reporting retry=False while returning the message to the queue marks a task
    terminally timed out even though it is about to run again. The manager then counts
    it as finished and can tear down instances with retries still in flight.
    """
    timeout_worker._data.retry_on_timeout = retry_on_timeout
    events: list[dict[str, Any]] = []
    timeout_worker._log_event = AsyncMock(side_effect=lambda event, **kw: events.append(event))
    timeout_worker._queue_retry_task_with_logging = AsyncMock()
    timeout_worker._queue_acknowledge_task_with_logging = AsyncMock()

    await _run_one_pass(timeout_worker)

    timed_out = [e for e in events if e["event_type"] == "task_timed_out"]
    assert len(timed_out) == 1
    assert timed_out[0]["task_id"] == "slow-task"
    assert timed_out[0]["retry"] is retry_on_timeout

    if retry_on_timeout:
        timeout_worker._queue_retry_task_with_logging.assert_awaited_once()
        timeout_worker._queue_acknowledge_task_with_logging.assert_not_awaited()
    else:
        timeout_worker._queue_acknowledge_task_with_logging.assert_awaited_once()
        timeout_worker._queue_retry_task_with_logging.assert_not_awaited()


@pytest.mark.asyncio
async def test_timed_out_task_is_killed_and_untracked(timeout_worker: Worker) -> None:
    """A timed-out task's process is terminated and dropped from the process table."""
    timeout_worker._data.retry_on_timeout = False
    timeout_worker._log_event = AsyncMock()
    timeout_worker._queue_acknowledge_task_with_logging = AsyncMock()
    process = timeout_worker._processes[0]["process"]

    await _run_one_pass(timeout_worker)

    process.terminate.assert_called_once()
    assert timeout_worker._processes == {}
    assert timeout_worker._num_tasks_timed_out == 1


@pytest.mark.asyncio
async def test_task_within_max_runtime_is_left_alone(timeout_worker: Worker) -> None:
    """A task that hasn't exceeded max_runtime is not reported or killed."""
    timeout_worker._processes[0]["start_time"] = time.time()
    timeout_worker._log_event = AsyncMock()
    timeout_worker._queue_retry_task_with_logging = AsyncMock()
    timeout_worker._queue_acknowledge_task_with_logging = AsyncMock()

    await _run_one_pass(timeout_worker)

    timeout_worker._log_event.assert_not_awaited()
    assert timeout_worker._processes[0]["process"].terminate.call_count == 0
    assert timeout_worker._num_tasks_timed_out == 0
