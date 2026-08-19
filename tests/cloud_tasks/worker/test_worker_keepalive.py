"""Tests for the worker keep-alive event support."""

import asyncio
import sys
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cloud_tasks.worker.worker import Worker, WorkerData

#: Signature of the callable a Worker is constructed with, as built by the
#: mock_worker_function fixture: (task_id, task_data, worker_data) -> (retry, result).
WorkerFunction = Callable[[str, dict[str, Any], WorkerData], tuple[bool, str]]


@pytest.fixture
def keepalive_worker(
    mock_worker_function: WorkerFunction, monkeypatch: pytest.MonkeyPatch
) -> Worker:
    """Worker instance with provider and job-id from env; sys.argv patched for test scope."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "GCP")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setattr(sys, "argv", ["worker.py"])
    return Worker(mock_worker_function)


def test_keepalive_interval_default(keepalive_worker: Worker) -> None:
    """The keep-alive interval defaults to 60 seconds."""
    assert keepalive_worker._data.keepalive_interval == 60.0


def test_keepalive_interval_from_env(
    mock_worker_function: WorkerFunction, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The keep-alive interval can be set via RMS_CLOUD_TASKS_KEEPALIVE_INTERVAL."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "GCP")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setenv("RMS_CLOUD_TASKS_KEEPALIVE_INTERVAL", "120")
    monkeypatch.setattr(sys, "argv", ["worker.py"])
    worker = Worker(mock_worker_function)
    assert worker._data.keepalive_interval == 120.0


def test_keepalive_interval_from_args(
    mock_worker_function: WorkerFunction, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The keep-alive interval can be set via --keepalive-interval, overriding the env var."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "GCP")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setenv("RMS_CLOUD_TASKS_KEEPALIVE_INTERVAL", "120")
    monkeypatch.setattr(sys, "argv", ["worker.py", "--keepalive-interval", "30"])
    worker = Worker(mock_worker_function)
    assert worker._data.keepalive_interval == 30.0


@pytest.mark.asyncio
async def test_log_keep_alive_sends_to_queue_only(keepalive_worker: Worker) -> None:
    """Keep-alive events go to the event queue with instance_id and timestamp, never to file."""
    keepalive_worker._instance_identity = "test-instance-1"
    keepalive_worker._event_logger_queue = AsyncMock()
    keepalive_worker._event_logger_fp = MagicMock()

    await keepalive_worker._log_keep_alive()

    keepalive_worker._event_logger_fp.write.assert_not_called()
    keepalive_worker._event_logger_queue.send_message.assert_awaited_once()
    event = keepalive_worker._event_logger_queue.send_message.await_args.args[0]
    assert event["event_type"] == "keep_alive"
    assert event["instance_id"] == "test-instance-1"
    assert event["timestamp"]
    assert event["hostname"] == keepalive_worker._hostname


def test_get_instance_identity_gcp(keepalive_worker: Worker) -> None:
    """The instance identity is fetched from the GCP metadata server and cached."""
    response = MagicMock(status_code=200, text="gcp-instance-name\n")
    with patch("cloud_tasks.worker.worker.requests.get", return_value=response) as mock_get:
        assert keepalive_worker._get_instance_identity() == "gcp-instance-name"
        # Second call uses the cached value
        assert keepalive_worker._get_instance_identity() == "gcp-instance-name"
        assert mock_get.call_count == 1
        assert "metadata.google.internal" in mock_get.call_args.args[0]


def test_get_instance_identity_aws(
    mock_worker_function: WorkerFunction, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The instance identity is fetched from the AWS metadata server."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "AWS")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setattr(sys, "argv", ["worker.py"])
    worker = Worker(mock_worker_function)

    token_response = MagicMock(status_code=200, text="imds-token")
    id_response = MagicMock(status_code=200, text="i-0123456789abcdef0")
    with (
        patch("cloud_tasks.worker.worker.requests.put", return_value=token_response),
        patch("cloud_tasks.worker.worker.requests.get", return_value=id_response) as mock_get,
    ):
        assert worker._get_instance_identity() == "i-0123456789abcdef0"
        assert mock_get.call_args.kwargs["headers"] == {"X-aws-ec2-metadata-token": "imds-token"}


def test_get_instance_identity_fallback_to_hostname(keepalive_worker: Worker) -> None:
    """The hostname fallback is not cached, so the metadata server is retried later."""
    with patch(
        "cloud_tasks.worker.worker.requests.get", side_effect=ConnectionError("no metadata")
    ) as mock_get:
        assert keepalive_worker._get_instance_identity() == keepalive_worker._hostname
        assert keepalive_worker._get_instance_identity() == keepalive_worker._hostname
        # The fallback must not be cached; each call retries the metadata server
        assert mock_get.call_count == 2

    # Once the metadata server recovers, its answer is used and cached
    response = MagicMock(status_code=200, text="gcp-instance-name\n")
    with patch("cloud_tasks.worker.worker.requests.get", return_value=response):
        assert keepalive_worker._get_instance_identity() == "gcp-instance-name"
    assert keepalive_worker._get_instance_identity() == "gcp-instance-name"


@pytest.mark.asyncio
async def test_keepalive_worker_sends_and_stops(keepalive_worker: Worker) -> None:
    """The keep-alive worker sends an event immediately and stops when the worker stops."""
    keepalive_worker._running = True
    keepalive_worker._data.keepalive_interval = 60.0
    with patch.object(keepalive_worker, "_log_keep_alive", new_callable=AsyncMock) as mock_log:
        task = asyncio.create_task(keepalive_worker._keepalive_worker())
        await asyncio.sleep(0.1)
        keepalive_worker._running = False
        await task
    mock_log.assert_awaited_once()


@pytest.mark.asyncio
async def test_keepalive_worker_survives_send_errors(keepalive_worker: Worker) -> None:
    """The keep-alive worker keeps running if sending an event fails."""
    keepalive_worker._running = True
    keepalive_worker._data.keepalive_interval = 60.0
    with patch.object(
        keepalive_worker,
        "_log_keep_alive",
        new_callable=AsyncMock,
        side_effect=RuntimeError("queue unavailable"),
    ) as mock_log:
        task = asyncio.create_task(keepalive_worker._keepalive_worker())
        await asyncio.sleep(0.1)
        keepalive_worker._running = False
        await task
    mock_log.assert_awaited_once()


@pytest.mark.asyncio
async def test_keepalive_worker_started_only_with_event_queue(
    mock_worker_function: WorkerFunction, monkeypatch: pytest.MonkeyPatch
) -> None:
    """start() launches the keep-alive worker only when logging events to a queue."""
    monkeypatch.setenv("RMS_CLOUD_TASKS_PROVIDER", "GCP")
    monkeypatch.setenv("RMS_CLOUD_TASKS_JOB_ID", "test-job")
    monkeypatch.setattr(sys, "argv", ["worker.py", "--no-event-log-to-queue"])
    worker = Worker(mock_worker_function)
    assert worker._data.event_log_to_queue is False

    started: list[Any] = []

    async def fake_feed() -> None:
        worker._running = False

    with (
        patch.object(worker, "_keepalive_worker", new_callable=AsyncMock) as mock_keepalive,
        patch.object(worker, "_handle_results", new_callable=AsyncMock),
        patch.object(worker, "_feed_tasks_to_workers", side_effect=fake_feed),
        patch.object(worker, "_monitor_process_runtimes", new_callable=AsyncMock),
        patch.object(worker, "_visibility_renewal_worker", new_callable=AsyncMock),
        patch.object(worker, "_wait_for_shutdown", new_callable=AsyncMock),
        patch("cloud_tasks.worker.worker.create_queue", new_callable=AsyncMock),
    ):
        await worker.start()
        started = [mock_keepalive.call_count]

    assert started == [0]
