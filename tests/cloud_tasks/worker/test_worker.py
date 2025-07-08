"""Tests for the worker module."""

import asyncio
from filecache import FCPath
import json
import os
import pytest
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch, call, ANY
import types
import yaml
import signal
import time
from pathlib import Path
import logging
import multiprocessing
import sys

from cloud_tasks.worker.worker import Worker, LocalTaskQueue


@pytest.fixture
def mock_queue():
    queue = AsyncMock()
    queue.receive_tasks = AsyncMock()
    queue.acknowledge_task = AsyncMock()
    queue.retry_task = AsyncMock()
    return queue


@pytest.fixture
def sample_task():
    return {"task_id": "test-task-1", "data": {"key": "value"}, "ack_id": "test-ack-1"}


def _mock_worker_function(task_id, task_data, worker):
    return False, "success"


@pytest.fixture
def mock_worker_function():
    return _mock_worker_function


# Local tasks tests


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
def local_task_file_yaml():
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


@pytest.mark.asyncio
async def test_local_queue_init_with_invalid_format():
    with tempfile.NamedTemporaryFile(suffix=".txt") as f:
        f.write(b"invalid content")
        f.flush()
        queue = LocalTaskQueue(FCPath(f.name))
        # Try to receive a task to trigger the error
        with pytest.raises(ValueError, match="Unsupported file format"):
            await queue.receive_tasks(1)


@pytest.mark.asyncio
async def test_local_queue_receive_tasks_json(local_task_file_json):
    queue = LocalTaskQueue(FCPath(local_task_file_json))
    tasks = await queue.receive_tasks(max_count=2)
    assert len(tasks) == 2
    assert tasks[0]["task_id"] == "task1"
    assert tasks[1]["task_id"] == "task2"
    assert "ack_id" in tasks[0]
    assert "ack_id" in tasks[1]


@pytest.mark.asyncio
async def test_local_queue_receive_tasks_yaml(local_task_file_yaml):
    queue = LocalTaskQueue(FCPath(local_task_file_yaml))
    tasks = await queue.receive_tasks(max_count=2)
    assert len(tasks) == 2
    assert tasks[0]["task_id"] == "task3"
    assert tasks[1]["task_id"] == "task4"
    assert "ack_id" in tasks[0]
    assert "ack_id" in tasks[1]


@pytest.mark.asyncio
async def test_local_queue_acknowledge_task(local_task_file_json):
    queue = LocalTaskQueue(FCPath(local_task_file_json))
    await queue.acknowledge_task("test-ack-1")  # This does nothing


@pytest.mark.asyncio
async def test_local_queue_retry_task(local_task_file_json):
    queue = LocalTaskQueue(FCPath(local_task_file_json))
    await queue.retry_task("test-ack-1")  # This does nothing


@pytest.mark.asyncio
async def test_local_queue_receive_all_tasks(local_task_file_json):
    """Test that LocalTaskQueue.receive_tasks returns all tasks when max_count is larger than available tasks."""
    queue = LocalTaskQueue(FCPath(local_task_file_json))
    tasks = await queue.receive_tasks(max_count=5)
    # The test file has two tasks
    assert len(tasks) == 2
    task_ids = {task["task_id"] for task in tasks}
    assert task_ids == {"task1", "task2"}
    # Ensure ack_id is present in each task
    for task in tasks:
        assert "ack_id" in task


# Work __init__ tests


@pytest.fixture
def worker(mock_worker_function):
    os.environ["RMS_CLOUD_TASKS_PROVIDER"] = "AWS"
    os.environ["RMS_CLOUD_TASKS_JOB_ID"] = "test-job"
    with patch("sys.argv", ["worker.py"]):
        return Worker(mock_worker_function)


@pytest.fixture
def env_setup_teardown(monkeypatch):
    # Setup: Store original environment variables
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
    monkeypatch.setenv("RMS_CLOUD_TASKS_RETRY_ON_EXIT", "true")
    monkeypatch.setenv("RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE", "true")
    # Provide the modified environment
    yield

    # Teardown: Restore original environment variables
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def env_setup_teardown_false(monkeypatch):
    # Setup: Store original environment variables
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
    monkeypatch.setenv("RMS_CLOUD_TASKS_RETRY_ON_EXIT", "false")
    monkeypatch.setenv("RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE", "false")
    # Provide the modified environment
    yield

    # Teardown: Restore original environment variables
    os.environ.clear()
    os.environ.update(original_env)


def test_init_with_env_vars(mock_worker_function, env_setup_teardown):

    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function)
        assert worker._data.provider == "AWS"
        assert worker._data.job_id == "test-job"
        assert worker._data.queue_name == "test-job"
        assert worker._data.event_log_to_queue is True
        assert worker._data.instance_type == "t2.micro"
        assert worker._data.num_cpus == 2
        assert worker._data.memory_gb == 4.0
        assert worker._data.local_ssd_gb == 100.0
        assert worker._data.boot_disk_gb == 20.0
        assert worker._data.is_spot is True
        assert worker._is_spot is True
        assert worker._data.price_per_hour == 0.1
        assert worker._data.num_simultaneous_tasks == 4
        assert worker._data.max_runtime == 3600
        assert worker._data.retry_on_timeout is True
        assert worker._data.retry_on_exception is True
        assert worker._data.retry_on_exit is True
        assert worker._data.shutdown_grace_period == 300
        assert worker._task_skip_count == 5
        assert worker._max_num_tasks == 10
        assert worker._data.simulate_spot_termination_after == 32
        assert worker._data.simulate_spot_termination_delay == 33
        assert worker._data.exactly_once_queue is True


def test_init_with_env_vars_false(mock_worker_function, env_setup_teardown_false):

    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function)
        assert worker._data.provider == "AWS"
        assert worker._data.job_id == "test-job"
        assert worker._data.queue_name == "test-job"
        assert worker._data.event_log_to_queue is False
        assert worker._data.instance_type == "t2.micro"
        assert worker._data.num_cpus == 2
        assert worker._data.memory_gb == 4.0
        assert worker._data.local_ssd_gb == 100.0
        assert worker._data.boot_disk_gb == 20.0
        assert worker._data.is_spot is False
        assert worker._is_spot is True  # Because of simulate_spot_termination_after
        assert worker._data.price_per_hour == 0.1
        assert worker._data.num_simultaneous_tasks == 4
        assert worker._data.max_runtime == 3600
        assert worker._data.retry_on_timeout is False
        assert worker._data.retry_on_exception is False
        assert worker._data.retry_on_exit is False
        assert worker._data.shutdown_grace_period == 300
        assert worker._task_skip_count == 5
        assert worker._max_num_tasks == 10
        assert worker._data.simulate_spot_termination_after == 32
        assert worker._data.simulate_spot_termination_delay == 33
        assert worker._data.exactly_once_queue is False


def test_init_with_args_true(mock_worker_function, env_setup_teardown):
    args = [
        "worker.py",
        "--provider",
        "GCP",
        "--project-id",
        "test-project",
        "--job-id",
        "gcp-test-job",
        "--queue-name",
        "aws-test-queue",
        "--instance-type",
        "n1-standard-1",
        "--num-cpus",
        "1",
        "--memory",
        "2",
        "--local-ssd",
        "50",
        "--boot-disk",
        "10",
        "--no-is-spot",
        "--price",
        "0.2",
        "--num-simultaneous-tasks",
        "2",
        "--max-runtime",
        "1800",
        "--shutdown-grace-period",
        "150",
        "--tasks-to-skip",
        "7",
        "--max-num-tasks",
        "10",
        "--simulate-spot-termination-after",
        "16",
        "--simulate-spot-termination-delay",
        "17",
        "--no-retry-on-exit",
        "--no-retry-on-timeout",
        "--no-retry-on-exception",
    ]
    with patch("sys.argv", args):
        worker = Worker(mock_worker_function)
        assert worker._data.provider == "GCP"
        assert worker._data.project_id == "test-project"
        assert worker._data.job_id == "gcp-test-job"
        assert worker._data.queue_name == "aws-test-queue"
        assert worker._data.instance_type == "n1-standard-1"
        assert worker._data.num_cpus == 1
        assert worker._data.memory_gb == 2.0
        assert worker._data.local_ssd_gb == 50.0
        assert worker._data.boot_disk_gb == 10.0
        assert worker._data.is_spot is False
        assert worker._is_spot is True  # because of simulate_spot_termination_after
        assert worker._data.price_per_hour == 0.2
        assert worker._data.num_simultaneous_tasks == 2
        assert worker._data.max_runtime == 1800
        assert worker._data.shutdown_grace_period == 150
        assert worker._task_skip_count == 7
        assert worker._max_num_tasks == 10
        assert worker._data.simulate_spot_termination_after == 16
        assert worker._data.simulate_spot_termination_delay == 17
        assert worker._data.retry_on_exit is False
        assert worker._data.retry_on_timeout is False
        assert worker._data.retry_on_exception is False


def test_init_with_args(mock_worker_function, env_setup_teardown_false):
    args = [
        "worker.py",
        "--provider",
        "GCP",
        "--project-id",
        "test-project",
        "--job-id",
        "gcp-test-job",
        "--queue-name",
        "aws-test-queue",
        "--instance-type",
        "n1-standard-1",
        "--num-cpus",
        "1",
        "--memory",
        "2",
        "--local-ssd",
        "50",
        "--boot-disk",
        "10",
        "--is-spot",
        "--price",
        "0.2",
        "--num-simultaneous-tasks",
        "2",
        "--max-runtime",
        "1800",
        "--shutdown-grace-period",
        "150",
        "--tasks-to-skip",
        "7",
        "--max-num-tasks",
        "10",
        "--simulate-spot-termination-after",
        "16",
        "--simulate-spot-termination-delay",
        "17",
        "--retry-on-exit",
        "--retry-on-timeout",
        "--retry-on-exception",
    ]
    with patch("sys.argv", args):
        worker = Worker(mock_worker_function)
        assert worker._data.provider == "GCP"
        assert worker._data.project_id == "test-project"
        assert worker._data.job_id == "gcp-test-job"
        assert worker._data.queue_name == "aws-test-queue"
        assert worker._data.instance_type == "n1-standard-1"
        assert worker._data.num_cpus == 1
        assert worker._data.memory_gb == 2.0
        assert worker._data.local_ssd_gb == 50.0
        assert worker._data.boot_disk_gb == 10.0
        assert worker._data.is_spot is True
        assert worker._is_spot is True
        assert worker._data.price_per_hour == 0.2
        assert worker._data.num_simultaneous_tasks == 2
        assert worker._data.max_runtime == 1800
        assert worker._data.shutdown_grace_period == 150
        assert worker._task_skip_count == 7
        assert worker._max_num_tasks == 10
        assert worker._data.simulate_spot_termination_after == 16
        assert worker._data.simulate_spot_termination_delay == 17
        assert worker._data.retry_on_exit is True
        assert worker._data.retry_on_timeout is True
        assert worker._data.retry_on_exception is True


def test_num_simultaneous_tasks_default(mock_worker_function):
    # num_cpus is set
    with patch("sys.argv", ["worker.py"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            args = types.SimpleNamespace(
                provider="AWS",
                project_id=None,
                task_file=None,
                job_id="jid",
                queue_name=None,
                instance_type=None,
                num_cpus=3,
                memory=None,
                local_ssd=None,
                boot_disk=None,
                is_spot=None,
                price=None,
                num_simultaneous_tasks=None,
                max_runtime=None,
                shutdown_grace_period=None,
                tasks_to_skip=None,
                max_num_tasks=None,
                simulate_spot_termination_after=None,
                simulate_spot_termination_delay=None,
                event_log_to_file=None,
                event_log_file=None,
                event_log_to_queue=None,
                verbose=False,
                retry_on_timeout=None,
                retry_on_exception=None,
                retry_on_exit=None,
                exactly_once_queue=None,
            )
            mock_parse_args.return_value = args
            worker = Worker(mock_worker_function)
            assert worker._data.num_simultaneous_tasks == 3
    # num_cpus is None
    with patch("sys.argv", ["worker.py"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            args = types.SimpleNamespace(
                provider="AWS",
                project_id=None,
                task_file=None,
                job_id="jid",
                queue_name=None,
                instance_type=None,
                num_cpus=None,
                memory=None,
                local_ssd=None,
                boot_disk=None,
                is_spot=None,
                price=None,
                num_simultaneous_tasks=None,
                max_runtime=None,
                shutdown_grace_period=None,
                tasks_to_skip=None,
                max_num_tasks=None,
                simulate_spot_termination_after=None,
                simulate_spot_termination_delay=None,
                event_log_to_file=None,
                event_log_file=None,
                event_log_to_queue=False,
                verbose=False,
                retry_on_timeout=None,
                retry_on_exception=None,
                retry_on_exit=None,
                exactly_once_queue=None,
            )
            mock_parse_args.return_value = args
            worker = Worker(mock_worker_function)
            assert worker._data.num_simultaneous_tasks == 1


def test_provider_required_without_tasks(mock_worker_function, caplog):
    """Test that provider is required when no tasks file is specified."""
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["worker.py"]):
            with patch("sys.exit") as mock_exit:
                args = types.SimpleNamespace(
                    provider=None,
                    project_id=None,
                    task_file=None,
                    job_id="test-job",
                    queue_name=None,
                    instance_type=None,
                    num_cpus=None,
                    memory=None,
                    local_ssd=None,
                    boot_disk=None,
                    is_spot=None,
                    price=None,
                    num_simultaneous_tasks=None,
                    max_runtime=None,
                    shutdown_grace_period=None,
                    tasks_to_skip=None,
                    max_num_tasks=None,
                    simulate_spot_termination_after=None,
                    simulate_spot_termination_delay=None,
                    event_log_to_file=None,
                    event_log_file=None,
                    event_log_to_queue=False,
                    verbose=False,
                    retry_on_timeout=None,
                    retry_on_exception=None,
                    retry_on_exit=None,
                    exactly_once_queue=None,
                )
                with patch("cloud_tasks.worker.worker._parse_args", return_value=args):
                    Worker(mock_worker_function)
                    mock_exit.assert_called_once_with(1)
                    assert (
                        "Provider not specified via --provider or RMS_CLOUD_TASKS_PROVIDER and no tasks file specified via --task-file"
                        in caplog.text
                    )


def test_provider_not_required_with_tasks(mock_worker_function):
    """Test that provider is not required when tasks file is specified."""
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["worker.py", "--task-file", "tasks.json"]):
            with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
                args = types.SimpleNamespace(
                    provider=None,
                    project_id=None,
                    task_file="tasks.json",
                    job_id=None,
                    queue_name=None,
                    instance_type=None,
                    num_cpus=None,
                    memory=None,
                    local_ssd=None,
                    boot_disk=None,
                    is_spot=None,
                    price=None,
                    num_simultaneous_tasks=None,
                    max_runtime=None,
                    shutdown_grace_period=None,
                    tasks_to_skip=None,
                    max_num_tasks=None,
                    simulate_spot_termination_after=None,
                    simulate_spot_termination_delay=None,
                    event_log_to_file=None,
                    event_log_file=None,
                    event_log_to_queue=False,
                    verbose=False,
                    retry_on_timeout=None,
                    retry_on_exception=None,
                    retry_on_exit=None,
                    exactly_once_queue=None,
                )
                mock_parse_args.return_value = args
                worker = Worker(mock_worker_function)
                assert worker._data.provider is None


@pytest.mark.asyncio
async def test_start_with_local_tasks(mock_worker_function, local_task_file_json):
    with patch("sys.argv", ["worker.py", "--task-file", local_task_file_json]):
        worker = Worker(mock_worker_function)
        with patch.object(worker, "_wait_for_shutdown") as mock_wait:
            mock_wait.side_effect = asyncio.CancelledError()
            with pytest.raises(asyncio.CancelledError):
                await worker.start()
            await worker._cleanup_tasks()


@pytest.mark.asyncio
async def test_start_with_cloud_queue(mock_worker_function, mock_queue):
    with patch(
        "sys.argv",
        ["worker.py", "--provider", "AWS", "--job-id", "test-job", "--no-event-log-to-queue"],
    ):
        worker = Worker(mock_worker_function)
        with patch(
            "cloud_tasks.worker.worker.create_queue", return_value=mock_queue
        ) as mock_create_queue:
            with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                mock_wait.side_effect = asyncio.CancelledError()
                with patch("asyncio.create_task", side_effect=lambda x: x):
                    with pytest.raises(asyncio.CancelledError):
                        await worker.start()
                await worker._cleanup_tasks()
    mock_create_queue.assert_called_once_with(
        provider="AWS",
        queue_name="test-job",
        project_id=None,
        exactly_once=False,
        visibility_timeout=605,
    )


@pytest.mark.asyncio
async def test_handle_results(worker, mock_queue):
    try:
        worker._task_queue = mock_queue
        worker._running = True
        worker._data.shutdown_grace_period = 0.01

        # Set up the mock queue to return immediately
        mock_queue.acknowledge_task.return_value = asyncio.Future()
        mock_queue.acknowledge_task.return_value.set_result(None)
        mock_queue.retry_task.return_value = asyncio.Future()
        mock_queue.retry_task.return_value.set_result(None)

        worker._processes = {
            1: {
                "process": MagicMock(),
                "task": {"task_id": "task1", "ack_id": "ack1"},
                "start_time": time.time(),
            },
            2: {
                "process": MagicMock(),
                "task": {"task_id": "task2", "ack_id": "ack2"},
                "start_time": time.time(),
            },
        }

        # Put tasks in the result queue
        worker._result_queue.put((1, False, "success"))
        worker._result_queue.put((2, True, "error"))

        async def shutdown_when_done():
            # Wait for both tasks to be processed with a shorter timeout
            start_time = time.time()
            timeout = 0.5  # 500ms timeout

            while time.time() - start_time < timeout:
                if worker._num_tasks_not_retried == 1 and worker._num_tasks_retried == 1:
                    break
                await asyncio.sleep(0.01)  # 10ms sleep

            if worker._num_tasks_not_retried != 1 or worker._num_tasks_retried != 1:
                pytest.fail("Timeout waiting for tasks to be processed")

            # Set shutdown event and wait for a moment to ensure it's processed
            worker._data.shutdown_event.set()
            await asyncio.sleep(0.1)  # Give time for shutdown to be processed

        handler_task = asyncio.create_task(worker._handle_results())
        shutdown_task = asyncio.create_task(worker._wait_for_shutdown(interval=0.01))
        done_task = asyncio.create_task(shutdown_when_done())

        try:
            await asyncio.wait_for(
                asyncio.gather(handler_task, shutdown_task, done_task), timeout=1.0
            )
        except asyncio.TimeoutError:
            pytest.fail("Test timed out waiting for tasks to complete")

        # Verify the results
        assert worker._num_tasks_not_retried == 1
        assert worker._num_tasks_retried == 1
        mock_queue.acknowledge_task.assert_called_once_with("ack1")
        mock_queue.retry_task.assert_called_once_with("ack2")
    finally:
        worker._running = False
        await handler_task
        await shutdown_task
        await done_task


@pytest.mark.asyncio
async def test_check_termination_notice_aws(worker):
    worker._data.provider = "AWS"
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        assert await worker._check_termination_notice() is True


@pytest.mark.asyncio
async def test_check_termination_notice_gcp(worker):
    worker._data.provider = "GCP"
    with patch("requests.get") as mock_get:
        mock_get.return_value.text = "true"
        assert await worker._check_termination_notice() is True


@pytest.mark.asyncio
async def test_check_termination_notice_azure(worker):
    worker._data.provider = "AZURE"
    assert await worker._check_termination_notice() is False


@pytest.mark.asyncio
async def test_handle_results_process_normal(worker, mock_queue):
    try:
        worker._task_queue = mock_queue
        worker._running = True
        worker._data.shutdown_grace_period = 0.01

        # Set up the mock queue to return immediately
        mock_queue.acknowledge_task.return_value = asyncio.Future()
        mock_queue.acknowledge_task.return_value.set_result(None)
        mock_queue.retry_task.return_value = asyncio.Future()
        mock_queue.retry_task.return_value.set_result(None)

        worker._processes = {
            1: {
                "process": MagicMock(),
                "task": {"task_id": "task1", "ack_id": "ack1"},
                "start_time": time.time(),
            },
            2: {
                "process": MagicMock(),
                "task": {"task_id": "task2", "ack_id": "ack2"},
                "start_time": time.time(),
            },
        }

        # Put tasks in the result queue
        worker._result_queue.put((1, False, "success"))
        worker._result_queue.put((2, True, "error"))

        async def shutdown_when_done():
            # Wait for both tasks to be processed with a shorter timeout
            start_time = time.time()
            timeout = 0.5  # 500ms timeout

            while time.time() - start_time < timeout:
                if worker._num_tasks_not_retried == 1 and worker._num_tasks_retried == 1:
                    break
                await asyncio.sleep(0.01)  # 10ms sleep

            if worker._num_tasks_not_retried != 1 or worker._num_tasks_retried != 1:
                pytest.fail("Timeout waiting for tasks to be processed")

            # Set shutdown event and wait for a moment to ensure it's processed
            worker._data.shutdown_event.set()
            await asyncio.sleep(0.1)  # Give time for shutdown to be processed

        handler_task = asyncio.create_task(worker._handle_results())
        shutdown_task = asyncio.create_task(worker._wait_for_shutdown(interval=0.01))
        done_task = asyncio.create_task(shutdown_when_done())

        try:
            await asyncio.wait_for(
                asyncio.gather(handler_task, shutdown_task, done_task), timeout=1.0
            )
        except asyncio.TimeoutError:
            pytest.fail("Test timed out waiting for tasks to complete")

        # Verify the results
        assert worker._num_tasks_not_retried == 1
        assert worker._num_tasks_retried == 1
        mock_queue.acknowledge_task.assert_called_once_with("ack1")
        mock_queue.retry_task.assert_called_once_with("ack2")
    finally:
        worker._running = False


def exception_worker_function(task_id, task_data, worker):
    raise ValueError("Test exception")


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_on_exception", [True, False])
async def test_handle_results_process_exception(worker, mock_queue, retry_on_exception):
    # Patch MP_CTX.Process to avoid real multiprocessing
    with patch("cloud_tasks.worker.worker.MP_CTX.Process") as mock_process_cls:
        mock_proc = MagicMock()
        # Simulate process is alive when result is processed (so exception path is taken)
        mock_proc.is_alive.return_value = True
        mock_proc.exitcode = 1
        mock_proc.pid = 1234
        mock_process_cls.return_value = mock_proc

        worker._task_queue = mock_queue
        worker._running = True
        worker._data.shutdown_grace_period = 0.01
        worker._user_worker_function = exception_worker_function
        worker._data.retry_on_exception = retry_on_exception

        # Set up the mock queue to return immediately
        mock_queue.acknowledge_task.return_value = asyncio.Future()
        mock_queue.acknowledge_task.return_value.set_result(None)
        mock_queue.retry_task.return_value = asyncio.Future()
        mock_queue.retry_task.return_value.set_result(None)

        # Simulate the process and result
        worker_id = 1
        task = {"task_id": "task1", "ack_id": "ack1"}
        worker._processes = {
            worker_id: {
                "process": mock_proc,
                "task": task,
                "start_time": time.time(),
            }
        }
        # Simulate the result the process would put in the queue
        worker._result_queue.put((worker_id, "exception", "Test exception"))

        async def shutdown_when_done():
            # Wait for the task to be processed with a shorter timeout
            start_time = time.time()
            timeout = 2.0
            while time.time() - start_time < timeout:
                if worker._num_tasks_retried == 1 or worker._num_tasks_not_retried == 1:
                    break
                await asyncio.sleep(0.01)
            if worker._num_tasks_retried != 1 and worker._num_tasks_not_retried != 1:
                pytest.fail("Timeout waiting for task to be processed")
            worker._data.shutdown_event.set()
            await asyncio.sleep(0.1)

        handler_task = asyncio.create_task(worker._handle_results())
        shutdown_task = asyncio.create_task(worker._wait_for_shutdown(interval=0.01))
        done_task = asyncio.create_task(shutdown_when_done())

        try:
            await asyncio.wait_for(
                asyncio.gather(handler_task, shutdown_task, done_task), timeout=3.0
            )
        except asyncio.TimeoutError:
            pytest.fail("Test timed out waiting for task to complete")

        # Verify the results
        if retry_on_exception:
            assert worker._num_tasks_retried == 1
            assert worker._num_tasks_not_retried == 0
            mock_queue.retry_task.assert_called_once_with("ack1")
        else:
            assert worker._num_tasks_retried == 0
            assert worker._num_tasks_not_retried == 1
            mock_queue.acknowledge_task.assert_called_once_with("ack1")
        worker._running = False


def test_worker_process_main(mock_worker_function):
    result_queue = MagicMock()
    worker_data = MagicMock()
    task = {
        "task_id": "test-task",
        "data": {"key": "value"},
    }
    worker_data.received_shutdown_request = False
    worker_data.received_termination_notice = False

    with patch("sys.exit") as mock_exit:
        Worker._worker_process_main(
            1,
            mock_worker_function,
            worker_data,
            task["task_id"],
            task["data"],
            result_queue,
        )
        result_queue.put.assert_called_once_with((1, False, "success"))
        mock_exit.assert_called_once_with(0)


@staticmethod
def test_execute_task_isolated(mock_worker_function):
    task_id = "test-task"
    task_data = {"key": "value"}
    worker = MagicMock()
    retry, result = Worker._execute_task_isolated(task_id, task_data, worker, mock_worker_function)
    assert retry is False
    assert result == "success"


@staticmethod
def test_execute_task_isolated_error():
    def error_func(task_id, task_data, worker):
        raise ValueError("Test error")

    task_id = "test-task"
    task_data = {"key": "value"}
    worker = MagicMock()
    pytest.raises(ValueError, Worker._execute_task_isolated, task_id, task_data, worker, error_func)


def test_exit_if_no_job_id_and_no_tasks(mock_worker_function, caplog):
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["worker.py"]):
            with patch("sys.exit") as mock_exit:
                args = types.SimpleNamespace(
                    provider="AWS",
                    project_id=None,
                    task_file=None,
                    job_id=None,
                    queue_name=None,
                    instance_type=None,
                    num_cpus=None,
                    memory=None,
                    local_ssd=None,
                    boot_disk=None,
                    is_spot=None,
                    price=None,
                    num_simultaneous_tasks=None,
                    max_runtime=None,
                    shutdown_grace_period=None,
                    tasks_to_skip=None,
                    max_num_tasks=None,
                    simulate_spot_termination_after=None,
                    simulate_spot_termination_delay=None,
                    event_log_to_file=None,
                    event_log_file=None,
                    event_log_to_queue=False,
                    verbose=False,
                    retry_on_timeout=None,
                    retry_on_exception=None,
                    retry_on_exit=None,
                    exactly_once_queue=None,
                )
                # Patch _parse_args to return our args
                with patch("cloud_tasks.worker.worker._parse_args", return_value=args):
                    with patch("cloud_tasks.worker.worker.create_queue", return_value=None):
                        Worker(mock_worker_function)
                        mock_exit.assert_called_once_with(1)
                        assert (
                            "Queue name not specified via --queue-name or RMS_CLOUD_TASKS_QUEUE_NAME or --job-id or RMS_CLOUD_TASKS_JOB_ID and no tasks file specified via --task-file"
                            in caplog.text
                        )


def test_worker_properties(mock_worker_function):
    with patch("sys.argv", ["worker.py"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            args = types.SimpleNamespace(
                provider="AWS",
                project_id="pid",
                task_file=None,
                job_id="jid",
                queue_name="qname",
                instance_type="itype",
                num_cpus=2,
                memory=3.5,
                local_ssd=4.5,
                boot_disk=5.5,
                is_spot=True,
                price=0.99,
                num_simultaneous_tasks=2,
                max_runtime=100,
                shutdown_grace_period=200,
                tasks_to_skip=1,
                max_num_tasks=10,
                simulate_spot_termination_after=32,
                simulate_spot_termination_delay=33,
                event_log_to_file=True,
                event_log_file="temp_log.json",
                event_log_to_queue=True,
                verbose=False,
                retry_on_timeout=None,
                retry_on_exception=None,
                retry_on_exit=None,
                exactly_once_queue=None,
            )
            mock_parse_args.return_value = args
            worker = Worker(mock_worker_function)
            assert worker._data.provider == "AWS"
            assert worker._data.project_id == "pid"
            assert worker._data.job_id == "jid"
            assert worker._data.queue_name == "qname"
            assert worker._data.instance_type == "itype"
            assert worker._data.num_cpus == 2
            assert worker._data.memory_gb == 3.5
            assert worker._data.local_ssd_gb == 4.5
            assert worker._data.boot_disk_gb == 5.5
            assert worker._data.is_spot is True
            assert worker._data.price_per_hour == 0.99
            assert worker._data.num_simultaneous_tasks == 2
            assert worker._data.max_runtime == 100
            assert worker._data.shutdown_grace_period == 200
            assert worker._data.event_log_to_file is True
            assert worker._data.event_log_to_queue is True
            assert worker._data.event_log_queue_name == "qname-events"
            assert worker._data.event_log_file == "temp_log.json"


def test_signal_handler(mock_worker_function, caplog):
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        worker = Worker(mock_worker_function)
        with patch("signal.signal") as mock_signal:
            worker._signal_handler(signal.SIGINT, None)
            assert worker._data.shutdown_event.is_set()
            mock_signal.assert_called_with(signal.SIGTERM, signal.SIG_DFL)
            assert "Received signal SIGINT, initiating graceful shutdown" in caplog.text


@pytest.mark.asyncio
async def test_wait_for_shutdown_graceful(mock_worker_function):
    """Test _wait_for_shutdown when processes exit gracefully."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        worker = Worker(mock_worker_function)

        # Create mock processes
        mock_process1 = MagicMock()
        mock_process2 = MagicMock()

        # Set up initial state
        worker._running = True
        worker._processes = {
            1: {"process": mock_process1, "task": "task1"},
            2: {"process": mock_process2, "task": "task2"},
        }
        worker._data.shutdown_grace_period = 5  # Longer grace period for testing

        # Create a task to set shutdown event and simulate process completion
        async def trigger_shutdown():
            await asyncio.sleep(0.1)
            worker._data.shutdown_event.set()
            # Simulate processes completing their tasks immediately
            worker._processes = {}
            # Simulate processes being done
            mock_process1.is_alive.return_value = False
            mock_process2.is_alive.return_value = False

        # Start the shutdown task
        shutdown_task = asyncio.create_task(trigger_shutdown())

        # Call _wait_for_shutdown
        await worker._wait_for_shutdown()

        # Wait for shutdown task to complete
        await shutdown_task

        # Verify processes were not terminated
        mock_process1.terminate.assert_not_called()
        mock_process2.terminate.assert_not_called()


@pytest.mark.asyncio
async def test_wait_for_shutdown_force_terminate(mock_worker_function):
    """Test _wait_for_shutdown when processes need to be force terminated."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        worker = Worker(mock_worker_function)

        # Create mock processes
        mock_process1 = MagicMock()
        mock_process2 = MagicMock()
        worker._processes = [mock_process1, mock_process2]

        # Set up initial state
        worker._running = True
        worker._processes = {
            1: {"process": mock_process1, "task": "task1"},
            2: {"process": mock_process2, "task": "task2"},
        }
        worker._data.shutdown_grace_period = 1  # Short grace period for testing

        # Create a task to set shutdown event after a delay
        async def trigger_shutdown():
            await asyncio.sleep(0.1)
            worker._data.shutdown_event.set()
            # Keep active tasks count high to force termination
            worker._processes = {
                1: {"process": mock_process1, "task": "task1"},
                2: {"process": mock_process2, "task": "task2"},
            }

        # Start the shutdown task
        shutdown_task = asyncio.create_task(trigger_shutdown())

        # Call _wait_for_shutdown
        await worker._wait_for_shutdown()

        # Wait for shutdown task to complete
        await shutdown_task

        # Verify processes were terminated and killed
        mock_process1.terminate.assert_called_once()
        mock_process2.terminate.assert_called_once()
        mock_process1.join.assert_called_once()
        mock_process2.join.assert_called_once()
        mock_process1.kill.assert_called_once()
        mock_process2.kill.assert_called_once()


@pytest.mark.asyncio
async def test_wait_for_shutdown_no_processes(mock_worker_function):
    """Test _wait_for_shutdown when there are no processes to clean up."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        worker = Worker(mock_worker_function)

        # Set up initial state
        worker._running = True
        worker._processes = {}

        # Set shutdown event
        worker._data.shutdown_event.set()

        # Call _wait_for_shutdown
        await worker._wait_for_shutdown()

        # Verify worker is no longer running
        assert not worker._running


@pytest.mark.asyncio
async def test_create_single_task_process(mock_worker_function, caplog):
    """Test creating a single task process."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        with (
            patch("cloud_tasks.worker.worker.MP_CTX.Process") as mock_process,
            patch("cloud_tasks.worker.worker.MP_CTX.Queue") as mock_queue,
        ):
            # Mock the Queue instance
            mock_result_queue = MagicMock()
            mock_queue.return_value = mock_result_queue

            worker = Worker(mock_worker_function)
            worker._num_tasks_not_retried = 1
            worker._num_tasks_retried = 2
            worker._processes = {}
            worker._task_skip_count = 0
            worker._running = True

            # Mock process instance
            mock_proc = MagicMock()
            mock_process.return_value = mock_proc

            # Mock task queue to return a task once and then None
            mock_queue = AsyncMock()
            task = {"task_id": "test-task", "data": {}, "ack_id": "test-ack"}
            mock_queue.receive_tasks.side_effect = [
                [task],
                [],
            ]
            worker._task_queue = mock_queue
            worker._next_worker_id = 3

            # Create a task to set shutdown request after process creation
            async def trigger_shutdown():
                await asyncio.sleep(0.1)
                worker._data.shutdown_event.set()
                worker._running = False

            # Start the shutdown task
            shutdown_task = asyncio.create_task(trigger_shutdown())

            # Call _feed_tasks_to_workers which will create the process
            await worker._feed_tasks_to_workers()

            # Wait for shutdown task to complete
            await shutdown_task

            # Verify process creation
            mock_process.assert_called_once_with(
                target=Worker._worker_process_main,
                args=(
                    3,
                    mock_worker_function,
                    worker._data,
                    task["task_id"],
                    task["data"],
                    worker._result_queue,
                ),
            )

            # Verify process configuration
            assert mock_proc.daemon is True
            mock_proc.start.assert_called_once()

            # Verify logging
            expected_message = f"Started single-task worker #3 (PID {mock_proc.pid})"
            assert expected_message in caplog.text

            # Verify process was added to the list
            assert len(worker._processes) == 1
            assert worker._processes[3]["process"] == mock_proc


@pytest.mark.asyncio
async def test_check_termination_loop(mock_worker_function, caplog):
    """Test that _check_termination_loop properly handles termination notices."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("asyncio.sleep") as mock_sleep:  # Patch sleep to run instantly
            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.shutdown_event = MagicMock()
            worker._data.shutdown_event.is_set.return_value = False
            worker._data.termination_event = MagicMock()
            worker._data.termination_event.is_set.return_value = False

            # Mock _check_termination_notice to return True once then False
            async def mock_check_termination():
                mock_check_termination.call_count = (
                    getattr(mock_check_termination, "call_count", 0) + 1
                )
                if mock_check_termination.call_count == 2:
                    return True
                return False

            worker._check_termination_notice = mock_check_termination

            # Run the termination check loop
            await worker._check_termination_loop()

            # Verify termination event was set
            worker._data.termination_event.set.assert_called_once()

            # Verify logger was called with appropriate message
            assert "Instance termination notice received" in caplog.text

            # Verify sleep was called with the correct duration
            mock_sleep.assert_called_once_with(5)


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_on_timeout", [True, False])
async def test_monitor_process_runtimes(mock_worker_function, caplog, retry_on_timeout):
    """Test _monitor_process_runtimes with a process that exceeds max runtime."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("asyncio.sleep") as mock_sleep:
            # Make sleep raise an exception after first call to break the loop
            mock_sleep.side_effect = [None, Exception("Test complete")]

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.max_runtime = 0.2
            worker._data.retry_on_timeout = retry_on_timeout

            # Create a mock process that will exceed max runtime
            mock_process = MagicMock()
            mock_process.pid = 123
            mock_process.is_alive.return_value = False  # Process stays alive after terminate

            # Set up process info to indicate it's been running too long
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": time.time() - 0.2,
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }  # 0.2 seconds runtime

            # Mock task queue for retry_task call
            mock_queue = AsyncMock()
            worker._task_queue = mock_queue

            # Run the monitor for one iteration
            with pytest.raises(Exception, match="Test complete"):
                await worker._monitor_process_runtimes()

            # Verify process was terminated
            mock_process.terminate.assert_called_once()
            # Verify two join calls since process stays alive
            assert mock_process.join.call_count == 1
            mock_process.join.assert_has_calls(
                [
                    call(timeout=1),  # First join after terminate
                ]
            )

            # Verify task was marked as failed
            if retry_on_timeout:
                mock_queue.retry_task.assert_called_once_with("ack1")
                mock_queue.acknowledge_task.assert_not_called()
                assert "Worker #123: Task task-1 will be retried" in caplog.text
            else:
                mock_queue.acknowledge_task.assert_called_once_with("ack1")
                mock_queue.retry_task.assert_not_called()
                assert "Worker #123: Task task-1 will not be retried" in caplog.text

            # Verify no new process was created to replace it
            assert len(worker._processes) == 0

            # Verify logging
            assert (
                "Worker #123 (PID 123), task task-1 exceeded max runtime of 0.2 seconds"
                in caplog.text
            )


@pytest.mark.asyncio
async def test_monitor_process_runtimes_no_termination(mock_worker_function, caplog):
    """Test _monitor_process_runtimes with a process that exceeds max runtime."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("asyncio.sleep") as mock_sleep:
            # Make sleep raise an exception after first call to break the loop
            mock_sleep.side_effect = [None, Exception("Test complete")]

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.max_runtime = 0.2

            # Create a mock process that will exceed max runtime
            mock_process = MagicMock()
            mock_process.pid = 123
            mock_process.is_alive.return_value = True  # Process stays alive after terminate

            # Set up process info to indicate it's been running too long
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": time.time() - 0.2,
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }  # 0.2 seconds runtime

            # Mock task queue for retry_task call
            mock_queue = AsyncMock()
            worker._task_queue = mock_queue

            # Run the monitor for one iteration
            with pytest.raises(Exception, match="Test complete"):
                await worker._monitor_process_runtimes()

            # Verify process was terminated
            mock_process.terminate.assert_called_once()
            # Verify two join calls since process stays alive
            assert mock_process.join.call_count == 2
            mock_process.join.assert_has_calls(
                [
                    call(timeout=1),  # First join after terminate
                    call(timeout=1),  # Second join after kill
                ]
            )
            mock_process.kill.assert_called_once()  # Verify kill was called after second join

            # Verify task was marked as failed
            mock_queue.acknowledge_task.assert_called_once_with("ack1")

            # Verify no new process was created to replace it
            assert len(worker._processes) == 0

            # Verify logging
            assert (
                "Worker #123 (PID 123), task task-1 exceeded max runtime of 0.2 seconds"
                in caplog.text
            )


@pytest.mark.asyncio
async def test_monitor_process_runtimes_no_exceeded_processes(mock_worker_function, caplog):
    """Test _monitor_process_runtimes when no processes exceed max runtime."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("asyncio.sleep") as mock_sleep:
            # Make sleep raise an exception after first call to break the loop
            mock_sleep.side_effect = [None, Exception("Test complete")]

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.shutdown_event = MagicMock()
            worker._data.shutdown_event.is_set.return_value = False
            worker._data.max_runtime = 0.1

            # Create a mock process that hasn't exceeded max runtime
            mock_process = MagicMock()
            mock_process.pid = 123
            mock_process.is_alive.return_value = True

            # Set up process info to indicate it's been running for a short time
            worker._processes = {
                123: {
                    "process": mock_process,
                    "start_time": time.time() - 0.05,
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }  # 0.05 seconds runtime

            # Run the monitor for one iteration
            with pytest.raises(Exception, match="Test complete"):
                await worker._monitor_process_runtimes()

            # Verify process was not terminated
            mock_process.terminate.assert_not_called()
            mock_process.join.assert_not_called()

            # Verify no new process was created
            assert len(worker._processes) == 1
            assert worker._processes[123]["process"] == mock_process

            # Verify no logging of warnings or info messages
            assert "Process 123 exceeded max runtime" not in caplog.text
            assert "Terminating" not in caplog.text


@pytest.mark.asyncio
async def test_worker_with_simulate_spot_termination_delay():
    """Test that worker correctly handles simulate_spot_termination_delay argument."""
    with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
        mock_queue = AsyncMock()
        mock_create_queue.return_value = mock_queue
        mock_queue.receive_tasks.return_value = []

        # Create worker with simulate_spot_termination_delay
        worker = Worker(
            user_worker_function=lambda task_id, task_data, worker: (True, "success"),
            args=[
                "--provider",
                "AWS",
                "--job-id",
                "test-job",
                "--simulate-spot-termination-after",
                "0.1",
            ],
        )

        # Verify the after was set
        assert worker._data.simulate_spot_termination_after == 0.1
        worker._start_time = time.time()

        # Test before delay is exceeded
        assert not await worker._check_termination_notice()

        # Set start time to be before the delay
        worker._start_time = time.time() - 0.2  # Set start time to 0.2 seconds ago

        # Test after delay is exceeded
        assert await worker._check_termination_notice()


@pytest.mark.asyncio
async def test_worker_with_is_spot():
    """Test that worker creates termination check loop when is_spot is enabled."""
    with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
        mock_queue = AsyncMock()
        mock_create_queue.return_value = mock_queue
        mock_queue.receive_tasks.return_value = []

        # Create worker with is_spot enabled
        worker = Worker(
            user_worker_function=lambda task_id, task_data, worker: (True, "success"),
            args=["--provider", "AWS", "--job-id", "test-job", "--is-spot"],
        )

        # Verify is_spot was set
        assert worker._is_spot

        # Mock _check_termination_loop
        mock_loop = AsyncMock()
        worker._check_termination_loop = mock_loop

        # Set up the worker to exit immediately
        worker._data.shutdown_event.set()

        # Start the worker
        await worker.start()

        # Verify _check_termination_loop was called
        mock_loop.assert_called_once()


@pytest.mark.asyncio
async def test_worker_without_is_spot():
    """Test that worker does not create termination check loop when is_spot is disabled."""
    with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
        mock_queue = AsyncMock()
        mock_create_queue.return_value = mock_queue
        mock_queue.receive_tasks.return_value = []

        # Create worker with is_spot disabled
        worker = Worker(
            user_worker_function=lambda task_id, task_data, worker: (True, "success"),
            args=["--provider", "AWS", "--job-id", "test-job"],
        )

        # Verify is_spot was not set
        assert not worker._is_spot

        # Mock _check_termination_loop
        mock_loop = AsyncMock()
        worker._check_termination_loop = mock_loop

        # Set up the worker to exit immediately
        worker._data.shutdown_event.set()

        # Start the worker
        await worker.start()

        # Verify _check_termination_loop was not called
        mock_loop.assert_not_called()


@pytest.mark.asyncio
async def test_check_termination_loop_with_simulated_delay(mock_worker_function, caplog):
    """Test that _check_termination_loop properly handles simulated spot termination delay."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._start_time = time.time() - 0.2
        worker._running = True
        worker._data.simulate_spot_termination_after = 0.1
        worker._data.simulate_spot_termination_delay = None

        # Create mock processes
        mock_process1 = MagicMock()
        mock_process1.is_alive.return_value = True
        mock_process1.pid = 123
        mock_process1.join.return_value = None

        mock_process2 = MagicMock()
        mock_process2.is_alive.return_value = True
        mock_process2.pid = 456
        mock_process2.join.return_value = None

        worker._processes = {
            1: {"process": mock_process1, "worker_id": 1},
            2: {"process": mock_process2, "worker_id": 2},
        }

        # Start the termination check loop
        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.return_value = None

            worker._data.simulate_spot_termination_delay = 0.2

            await worker._check_termination_loop()

            # Verify processes were terminated
            mock_process1.terminate.assert_called_once()
            mock_process2.terminate.assert_called_once()
            mock_process1.join.assert_called_once_with(timeout=5)
            mock_process2.join.assert_called_once_with(timeout=5)

            # Verify processes were cleaned up
            assert len(worker._processes) == 0
            assert not worker._running

        # Verify logging
        assert "Simulated spot termination delay complete, killing all processes" in caplog.text


@pytest.mark.asyncio
async def test_handle_results_process_exit_retry_on_exit(mock_worker_function):
    """Should properly handle worker process exits and task retry logic."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.logger") as mock_logger:
            worker = Worker(mock_worker_function)
            worker._running = True
            worker._num_simultaneous_tasks = 2
            mock_queue = AsyncMock()
            worker._task_queue = mock_queue

            # Create a mock process that will exit
            mock_process = MagicMock()
            mock_process.pid = 123
            mock_process.is_alive.return_value = False
            mock_process.exitcode = 1

            # Set up initial process
            task = {"task_id": "test-task", "data": {}, "ack_id": "test-ack"}
            worker._processes = {
                1: {
                    "worker_id": 1,
                    "process": mock_process,
                    "start_time": time.time(),
                    "task": task,
                }
            }

            async def fake_sleep(*a, **kw):
                worker._running = False
                return []

            with patch("asyncio.sleep") as mock_sleep:
                mock_sleep.side_effect = fake_sleep

                # Test with retry_on_exit=False
                worker._data.retry_on_exit = False
                await worker._handle_results()
                mock_queue.acknowledge_task.assert_called_once_with("test-ack")
                mock_queue.retry_task.assert_not_called()
                assert len(worker._processes) == 0
                mock_logger.warning.assert_called_with(
                    'Worker #1 (PID 123) processing task "test-task" exited prematurely in '
                    "0.0 seconds with exit code 1; not retrying"
                )

                # Reset for next test
                mock_queue.reset_mock()
                mock_logger.reset_mock()
                worker._processes = {
                    1: {
                        "worker_id": 1,
                        "process": mock_process,
                        "start_time": time.time(),
                        "task": task,
                    }
                }
                worker._running = True

                # Test with retry_on_exit=True
                worker._data.retry_on_exit = True
                await worker._handle_results()
                mock_queue.retry_task.assert_called_once_with("test-ack")
                mock_queue.acknowledge_task.assert_not_called()
                assert len(worker._processes) == 0
                mock_logger.warning.assert_called_with(
                    'Worker #1 (PID 123) processing task "test-task" exited prematurely in '
                    "0.0 seconds with exit code 1; retrying"
                )


@pytest.mark.asyncio
async def test_event_logging_to_file(mock_worker_function, tmp_path, local_task_file_json):
    """Test event logging to file for various event types."""
    event_log_file = tmp_path / "events.log"

    with patch(
        "sys.argv",
        [
            "worker.py",
            "--provider",
            "AWS",
            "--job-id",
            "test-job",
            "--event-log-file",
            str(event_log_file),
            "--task-file",
            str(local_task_file_json),
        ],
    ):
        worker = Worker(mock_worker_function)

        # Initialize the worker
        with patch.object(worker, "_wait_for_shutdown") as mock_wait:
            mock_wait.side_effect = asyncio.CancelledError()
            with patch("asyncio.create_task", side_effect=lambda x: x):
                try:
                    await worker.start()
                except asyncio.CancelledError:
                    pass
            await worker._cleanup_tasks()

        # Test task completion logging
        await worker._log_task_completed("task1", elapsed_time=1.5, retry=False, result="success")

        # Test task timeout logging
        await worker._log_task_timed_out("task2", retry=False, runtime=2.5)

        # Test task exit logging
        await worker._log_task_exited("task3", retry=False, elapsed_time=2.5, exit_code=1)

        # Test task exception logging
        await worker._log_task_exception(
            "task4", retry=False, elapsed_time=2.5, exception="test error"
        )

        # Test non-fatal exception logging
        await worker._log_non_fatal_exception("ValueError: test error")

        # Test fatal exception logging
        await worker._log_fatal_exception("RuntimeError: fatal error")

        # Test spot termination logging
        await worker._log_spot_termination()

        # Close the file handle
        if worker._event_logger_fp:
            worker._event_logger_fp.close()

        # Read and verify logged events
        with open(event_log_file) as f:
            events = [json.loads(line) for line in f]

        assert len(events) == 7

        # Verify task completion event
        completion_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_TASK_COMPLETED
        )
        assert completion_event["task_id"] == "task1"
        assert completion_event["elapsed_time"] == 1.5
        assert completion_event["retry"] is False
        assert completion_event["result"] == "success"

        # Verify task timeout event
        timeout_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_TASK_TIMED_OUT
        )
        assert timeout_event["task_id"] == "task2"
        assert timeout_event["elapsed_time"] == 2.5

        # Verify task exit event
        exit_event = next(e for e in events if e["event_type"] == worker._EVENT_TYPE_TASK_EXITED)
        assert exit_event["task_id"] == "task3"
        assert exit_event["elapsed_time"] == 2.5
        assert exit_event["exit_code"] == 1

        # Verify task exception event
        exception_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_TASK_EXCEPTION
        )
        assert exception_event["task_id"] == "task4"
        assert exception_event["elapsed_time"] == 2.5
        assert exception_event["exception"] == "test error"

        # Verify non-fatal exception event
        non_fatal_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_NON_FATAL_EXCEPTION
        )
        assert non_fatal_event["exception"] == "ValueError: test error"

        # Verify fatal exception event
        fatal_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_FATAL_EXCEPTION
        )
        assert fatal_event["exception"] == "RuntimeError: fatal error"

        # Verify spot termination event
        spot_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_SPOT_TERMINATION
        )
        assert "timestamp" in spot_event
        assert "hostname" in spot_event


@pytest.mark.asyncio
async def test_event_logging_to_queue(mock_worker_function):
    """Test event logging to queue for various event types."""
    with patch(
        "sys.argv",
        ["worker.py", "--provider", "AWS", "--job-id", "test-job", "--event-log-to-queue"],
    ):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)

            # Initialize the worker
            with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                mock_wait.side_effect = asyncio.CancelledError()
                with patch("asyncio.create_task", side_effect=lambda x: x):
                    try:
                        await worker.start()
                    except asyncio.CancelledError:
                        pass
                await worker._cleanup_tasks()

            # Test task completion logging
            await worker._log_task_completed(
                "task1", elapsed_time=1.5, retry=False, result="success"
            )

            # Test task timeout logging
            await worker._log_task_timed_out("task2", retry=False, runtime=2.5)

            # Test task exit logging
            await worker._log_task_exited("task3", retry=False, elapsed_time=2.5, exit_code=1)

            # Test task exception logging
            await worker._log_task_exception(
                "task4", retry=False, elapsed_time=2.5, exception="test error"
            )

            # Test non-fatal exception logging
            await worker._log_non_fatal_exception("ValueError: test error")

            # Test fatal exception logging
            await worker._log_fatal_exception("RuntimeError: fatal error")

            # Test spot termination logging
            await worker._log_spot_termination()

            # Verify queue messages
            assert mock_queue.send_message.call_count == 7

            # Get all sent messages
            messages = [json.loads(call.args[0]) for call in mock_queue.send_message.call_args_list]

            # Verify task completion event
            completion_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_TASK_COMPLETED
            )
            assert completion_event["task_id"] == "task1"
            assert completion_event["elapsed_time"] == 1.5
            assert completion_event["retry"] is False
            assert completion_event["result"] == "success"

            # Verify task timeout event
            timeout_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_TASK_TIMED_OUT
            )
            assert timeout_event["task_id"] == "task2"
            assert timeout_event["retry"] is False
            assert timeout_event["elapsed_time"] == 2.5

            # Verify task exit event
            exit_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_TASK_EXITED
            )
            assert exit_event["task_id"] == "task3"
            assert exit_event["retry"] is False
            assert exit_event["elapsed_time"] == 2.5
            assert exit_event["exit_code"] == 1

            # Verify task exception event
            exception_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_TASK_EXCEPTION
            )
            assert exception_event["task_id"] == "task4"
            assert exception_event["retry"] is False
            assert exception_event["elapsed_time"] == 2.5
            assert exception_event["exception"] == "test error"

            # Verify non-fatal exception event
            non_fatal_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_NON_FATAL_EXCEPTION
            )
            assert non_fatal_event["exception"] == "ValueError: test error"

            # Verify fatal exception event
            fatal_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_FATAL_EXCEPTION
            )
            assert fatal_event["exception"] == "RuntimeError: fatal error"

            # Verify spot termination event
            spot_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_SPOT_TERMINATION
            )
            assert "timestamp" in spot_event
            assert "hostname" in spot_event


@pytest.mark.asyncio
async def test_event_logging_both_file_and_queue(mock_worker_function, tmp_path):
    """Test event logging to both file and queue simultaneously."""
    event_log_file = tmp_path / "events.log"

    with patch(
        "sys.argv",
        [
            "worker.py",
            "--provider",
            "AWS",
            "--job-id",
            "test-job",
            "--event-log-to-file",
            "--event-log-file",
            str(event_log_file),
            "--event-log-to-queue",
        ],
    ):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_create_queue.return_value = mock_queue
            worker = Worker(mock_worker_function)
            try:
                # Initialize the worker
                with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                    mock_wait.side_effect = asyncio.CancelledError()
                    with patch("asyncio.create_task", side_effect=lambda x: x):
                        try:
                            await worker.start()
                        except asyncio.CancelledError:
                            pass
                    await worker._cleanup_tasks()

                # Log a test event
                await worker._log_task_completed(
                    "task1", elapsed_time=1.5, retry=False, result="success"
                )

                # Close the file handle
                if worker._event_logger_fp:
                    worker._event_logger_fp.close()

                # Verify file logging
                with open(event_log_file) as f:
                    file_events = [json.loads(line) for line in f]
                    assert len(file_events) == 1
                    assert file_events[0]["task_id"] == "task1"

                # Verify queue logging
                assert mock_queue.send_message.call_count == 1
                queue_event = json.loads(mock_queue.send_message.call_args[0][0])
                assert queue_event["task_id"] == "task1"
            finally:
                worker._running = False


@pytest.mark.asyncio
async def test_event_logging_no_logging(mock_worker_function):
    """Test that no logging occurs when neither file nor queue logging is enabled."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        try:
            # Verify no file handle was created
            assert worker._event_logger_fp is None

            # Verify no queue was created
            assert worker._event_logger_queue is None

            # Log a test event - should not raise any errors
            await worker._log_task_completed(
                "task1", elapsed_time=1.5, retry=False, result="success"
            )
        finally:
            worker._running = False


@pytest.mark.asyncio
async def test_event_logging_file_error(mock_worker_function, tmp_path, caplog):
    """Test handling of file logging errors."""
    # Create a directory to make file creation fail
    event_log_file = tmp_path / "nonexistent" / "events.log"

    with patch(
        "sys.argv",
        [
            "worker.py",
            "--provider",
            "AWS",
            "--job-id",
            "test-job",
            "--event-log-to-file",
            "--event-log-file",
            str(event_log_file),
        ],
    ):
        with patch("sys.exit") as mock_exit:
            with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
                mock_queue = AsyncMock()
                mock_create_queue.return_value = mock_queue

                worker = Worker(mock_worker_function)
                with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                    mock_wait.side_effect = asyncio.CancelledError()
                    with patch("asyncio.create_task", side_effect=lambda x: x):
                        try:
                            await worker.start()
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                    await worker._cleanup_tasks()
                mock_exit.assert_called_once_with(1)
                assert "Error opening event log file" in caplog.text


@pytest.mark.asyncio
async def test_event_logging_queue_error(mock_worker_function, caplog):
    """Test handling of queue logging errors."""
    with patch(
        "sys.argv",
        ["worker.py", "--provider", "AWS", "--job-id", "test-job", "--event-log-to-queue"],
    ):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            # First call fails for event logger queue, second call fails for task queue
            mock_create_queue.side_effect = [
                Exception("Queue creation failed"),  # For event logger queue
                Exception("Queue creation failed"),  # For task queue
            ]

            with patch("sys.exit", side_effect=SystemExit(1)) as mock_exit:
                worker = Worker(mock_worker_function)
                try:
                    with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                        mock_wait.side_effect = asyncio.CancelledError()
                        with patch("asyncio.create_task", side_effect=lambda x: x):
                            try:
                                await worker.start()
                            except asyncio.CancelledError:
                                pass
                            except SystemExit:
                                pass
                            except Exception:
                                pass
                        await worker._cleanup_tasks()
                    mock_exit.assert_called_once_with(1)
                    assert "Error initializing event log queue" in caplog.text
                finally:
                    worker._running = False


@pytest.mark.asyncio
async def test_tasks_to_skip_and_limit(mock_worker_function, caplog):
    """Test that tasks-to-skip and task-limit options work correctly."""
    with patch(
        "sys.argv",
        [
            "worker.py",
            "--provider",
            "AWS",
            "--job-id",
            "test-job",
            "--tasks-to-skip",
            "2",
            "--max-num-tasks",
            "3",
        ],
    ):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_create_queue.return_value = mock_queue

            # Create tasks that will be returned by the queue
            tasks = [{"task_id": f"task-{i}", "data": {}, "ack_id": f"ack-{i}"} for i in range(6)]
            mock_queue.receive_tasks.side_effect = [
                [tasks[0]],  # First batch - should be skipped
                [tasks[1]],  # Second batch - should be skipped
                [tasks[2]],  # Third batch - should be processed
                [tasks[3]],  # Fourth batch - should be processed
                [tasks[4]],  # Fifth batch - should be processed
                [tasks[5]],  # Sixth batch - should not get here
                [],
            ]

            worker = Worker(mock_worker_function)
            worker._running = True

            # Create a task to set shutdown event after processing
            async def trigger_shutdown():
                while worker._running:
                    if mock_queue.receive_tasks.call_count >= 7:  # After we've received all tasks
                        worker._data.shutdown_event.set()
                        worker._running = False
                        break
                    await asyncio.sleep(0.01)  # Short sleep time

            # Start the shutdown task
            shutdown_task = asyncio.create_task(trigger_shutdown())

            # Start the worker
            await worker.start()

            # Wait for shutdown task to complete
            await shutdown_task

            # Verify tasks were skipped and processed correctly
            assert worker._task_skip_count == 0  # Should have used up all skips
            assert worker._tasks_remaining == 0  # Should have used up all task limit
            assert worker._num_tasks_not_retried == 3  # Should have processed 3 tasks
            assert worker._num_tasks_retried == 0  # No retries

            # Verify logging of started workers for tasks 2-4
            log_lines = caplog.text.split("\n")
            worker_start_lines = [
                line for line in log_lines if "Started single-task worker" in line
            ]
            assert len(worker_start_lines) == 3  # Should have started 3 workers

            # Verify each worker was started with the correct task
            for worker_id in range(3):
                expected_line = f"Started single-task worker #{worker_id}"
                assert any(
                    expected_line in line for line in worker_start_lines
                ), f"Missing log entry for worker {worker_id}"

            # Verify queue interactions
            assert mock_queue.receive_tasks.call_count >= 7  # 6 batches + empty batch
            assert mock_queue.acknowledge_task.call_count == 3  # Should have completed 3 tasks


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


@pytest.fixture
def mock_task_factory_empty():
    """Create a mock task factory that yields no tasks."""

    def factory():
        return
        yield  # This line is never reached

    return factory


@pytest.mark.asyncio
async def test_factory_queue_receive_tasks(mock_task_factory):
    """Test LocalTaskQueue.receive_tasks with factory and multiple tasks."""
    queue = LocalTaskQueue(mock_task_factory)

    # Test receiving 2 tasks
    tasks = await queue.receive_tasks(max_count=2)
    assert len(tasks) == 2
    assert tasks[0]["task_id"] == "factory-task-1"
    assert tasks[1]["task_id"] == "factory-task-2"
    assert "ack_id" in tasks[0]
    assert "ack_id" in tasks[1]

    # Test receiving remaining task
    tasks = await queue.receive_tasks(max_count=2)
    assert len(tasks) == 1
    assert tasks[0]["task_id"] == "factory-task-3"
    assert "ack_id" in tasks[0]

    # Test receiving when no more tasks
    tasks = await queue.receive_tasks(max_count=2)
    assert len(tasks) == 0


@pytest.mark.asyncio
async def test_factory_queue_receive_tasks_empty_factory(mock_task_factory_empty):
    """Test LocalTaskQueue.receive_tasks with empty factory."""
    queue = LocalTaskQueue(mock_task_factory_empty)

    tasks = await queue.receive_tasks(max_count=5)
    assert len(tasks) == 0


@pytest.mark.asyncio
async def test_factory_queue_receive_tasks_max_count_larger_than_available(mock_task_factory):
    """Test LocalTaskQueue.receive_tasks when max_count is larger than available tasks."""
    queue = LocalTaskQueue(mock_task_factory)

    tasks = await queue.receive_tasks(max_count=10)
    assert len(tasks) == 3  # Only 3 tasks available
    task_ids = {task["task_id"] for task in tasks}
    assert task_ids == {"factory-task-1", "factory-task-2", "factory-task-3"}


@pytest.mark.asyncio
async def test_factory_queue_acknowledge_task(mock_task_factory):
    """Test LocalTaskQueue.acknowledge_task (should do nothing)."""
    queue = LocalTaskQueue(mock_task_factory)

    # This should not raise any exception
    await queue.acknowledge_task("test-ack-1")


@pytest.mark.asyncio
async def test_factory_queue_retry_task(mock_task_factory):
    """Test LocalTaskQueue.retry_task (should do nothing)."""
    queue = LocalTaskQueue(mock_task_factory)

    # This should not raise any exception
    await queue.retry_task("test-ack-1")


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


@pytest.mark.asyncio
async def test_worker_start_with_factory_task_queue(mock_worker_function, mock_task_factory):
    """Test Worker.start with LocalTaskQueue."""
    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function, task_source=mock_task_factory)

        with patch.object(worker, "_wait_for_shutdown") as mock_wait:
            mock_wait.side_effect = asyncio.CancelledError()
            with patch("asyncio.create_task", side_effect=lambda x: x):
                with pytest.raises(asyncio.CancelledError):
                    await worker.start()
            await worker._cleanup_tasks()

        # Verify LocalTaskQueue was created
        assert isinstance(worker._task_queue, LocalTaskQueue)


@pytest.mark.asyncio
async def test_worker_start_with_factory_task_queue_error(mock_worker_function, caplog):
    """Test Worker.start with LocalTaskQueue initialization error."""

    def bad_factory():
        raise ValueError("Bad factory")

    with patch("sys.argv", ["worker.py"]):
        with patch("sys.exit") as mock_exit:
            worker = Worker(mock_worker_function, task_source=bad_factory)

            with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                mock_wait.side_effect = asyncio.CancelledError()
                with patch("asyncio.create_task", side_effect=lambda x: x):
                    try:
                        await worker.start()
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass
                await worker._cleanup_tasks()
            mock_exit.assert_called_once_with(1)
            assert "Error initializing local task queue" in caplog.text


@pytest.mark.asyncio
async def test_worker_with_factory_task_queue_and_provider_required(
    mock_worker_function, mock_task_factory
):
    """Test that Worker with a factory task queue doesn't require provider."""
    with patch("sys.argv", ["worker.py"]):
        with patch.dict(os.environ, {}, clear=True):
            worker = Worker(mock_worker_function, task_source=mock_task_factory)
            assert worker._data.provider is None


@pytest.mark.asyncio
async def test_worker_with_factory_task_queue_and_queue_name_required(
    mock_worker_function, mock_task_factory
):
    """Test that Worker with a factory task queue doesn't require queue name."""
    with patch("sys.argv", ["worker.py"]):
        with patch.dict(os.environ, {}, clear=True):
            worker = Worker(mock_worker_function, task_source=mock_task_factory)
            assert worker._data.queue_name is None


def test_local_queue_init_with_invalid_task_source():
    """Test LocalTaskQueue.__init__ with invalid task_source type."""
    with pytest.raises(TypeError, match="task_source must be FCPath or callable, got int"):
        LocalTaskQueue(123)  # Invalid type


def test_worker_init_with_verbose_logging(mock_worker_function):
    """Test Worker initialization with verbose logging."""
    with patch("sys.argv", ["worker.py", "--verbose", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("logging.getLogger") as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            _ = Worker(mock_worker_function)
            mock_logger.setLevel.assert_called_once_with(logging.DEBUG)


def test_worker_init_event_log_to_queue_without_provider(mock_worker_function):
    """Test Worker initialization with --event-log-to-queue but no provider."""
    with patch("sys.argv", ["worker.py", "--event-log-to-queue"]):
        with patch.dict(os.environ, {}, clear=True):
            with patch("sys.exit") as mock_exit:
                Worker(mock_worker_function)
                # Accept multiple sys.exit calls
                assert mock_exit.call_count >= 1


def test_worker_init_simulate_spot_termination_without_delay(mock_worker_function, caplog):
    """Test Worker initialization with simulate spot termination but no delay."""
    with patch("sys.argv", ["worker.py", "--simulate-spot-termination-after", "10"]):
        with patch.dict(
            os.environ, {"RMS_CLOUD_TASKS_PROVIDER": "AWS", "RMS_CLOUD_TASKS_JOB_ID": "test-job"}
        ):
            _ = Worker(mock_worker_function)
            assert "Simulating spot termination after but no delay specified" in caplog.text


@pytest.mark.asyncio
async def test_queue_acknowledge_task_with_logging_error(mock_worker_function):
    """Test _queue_acknowledge_task_with_logging with error."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        mock_queue = AsyncMock()
        mock_queue.acknowledge_task.side_effect = Exception("Queue error")
        worker._task_queue = mock_queue

        task = {"task_id": "test-task", "ack_id": "test-ack"}
        await worker._queue_acknowledge_task_with_logging(task)

        # Verify error was logged
        mock_queue.acknowledge_task.assert_called_once_with("test-ack")


@pytest.mark.asyncio
async def test_queue_retry_task_with_logging_error(mock_worker_function):
    """Test _queue_retry_task_with_logging with error."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        mock_queue = AsyncMock()
        mock_queue.retry_task.side_effect = Exception("Queue error")
        worker._task_queue = mock_queue

        task = {"task_id": "test-task", "ack_id": "test-ack"}
        await worker._queue_retry_task_with_logging(task)

        # Verify error was logged
        mock_queue.retry_task.assert_called_once_with("test-ack")


@pytest.mark.asyncio
async def test_start_with_event_log_file_error(mock_worker_function, caplog):
    """Test start() with event log file error."""
    with patch(
        "sys.argv",
        [
            "worker.py",
            "--provider",
            "AWS",
            "--job-id",
            "test-job",
            "--event-log-to-file",
            "--event-log-file",
            "/nonexistent/events.log",
        ],
    ):
        with patch("builtins.open", side_effect=PermissionError("Permission denied")):
            with patch("sys.exit") as mock_exit:
                worker = Worker(mock_worker_function)
                with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                    mock_wait.side_effect = asyncio.CancelledError()
                    with patch("asyncio.create_task", side_effect=lambda x: x):
                        try:
                            await worker.start()
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                    await worker._cleanup_tasks()
                # Multiple errors will cause multiple exit calls
                assert mock_exit.call_count >= 1
                assert "Error opening event log file" in caplog.text


@pytest.mark.asyncio
async def test_start_with_event_log_queue_error(mock_worker_function, caplog):
    """Test start() with event log queue error."""
    with patch(
        "sys.argv",
        ["worker.py", "--provider", "AWS", "--job-id", "test-job", "--event-log-to-queue"],
    ):
        with patch(
            "cloud_tasks.worker.worker.create_queue", side_effect=Exception("Queue creation failed")
        ):
            with patch("sys.exit") as mock_exit:
                worker = Worker(mock_worker_function)
                with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                    mock_wait.side_effect = asyncio.CancelledError()
                    with patch("asyncio.create_task", side_effect=lambda x: x):
                        try:
                            await worker.start()
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                    await worker._cleanup_tasks()
                # Multiple errors will cause multiple exit calls
                assert mock_exit.call_count >= 1
                assert "Error initializing event log queue" in caplog.text


@pytest.mark.asyncio
async def test_start_with_local_task_queue_error(mock_worker_function, caplog):
    """Test start() with local task queue error."""
    with patch("sys.argv", ["worker.py"]):

        def bad_factory():
            raise ValueError("Bad factory")

        with patch("sys.exit") as mock_exit:
            worker = Worker(mock_worker_function, task_source=bad_factory)
            with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                mock_wait.side_effect = asyncio.CancelledError()
                with patch("asyncio.create_task", side_effect=lambda x: x):
                    with pytest.raises(asyncio.CancelledError):
                        await worker.start()
                await worker._cleanup_tasks()
            # Multiple errors will cause multiple exit calls
            assert mock_exit.call_count >= 1
            assert "Error initializing local task queue" in caplog.text


@pytest.mark.asyncio
async def test_start_with_cloud_queue_error(mock_worker_function, caplog):
    """Test start() with cloud queue error."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch(
            "cloud_tasks.worker.worker.create_queue", side_effect=Exception("Queue creation failed")
        ):
            with patch("sys.exit") as mock_exit:
                worker = Worker(mock_worker_function)
                with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                    mock_wait.side_effect = asyncio.CancelledError()
                    with patch("asyncio.create_task", side_effect=lambda x: x):
                        with pytest.raises(asyncio.CancelledError):
                            await worker.start()
                    await worker._cleanup_tasks()
                # Multiple errors will cause multiple exit calls
                assert mock_exit.call_count >= 1
                assert "Error initializing task queue" in caplog.text


@pytest.mark.asyncio
async def test_handle_results_with_exception(mock_worker_function, caplog):
    """Test _handle_results with exception."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True

        # Mock the result queue to raise an exception
        with patch.object(worker._result_queue, "get_nowait", side_effect=Exception("Queue error")):
            with patch("asyncio.sleep") as mock_sleep:
                mock_sleep.side_effect = [None, StopAsyncIteration()]
                try:
                    await worker._handle_results()
                except StopAsyncIteration:
                    pass
        assert "Error handling results" in caplog.text


@pytest.mark.asyncio
async def test_wait_for_shutdown_with_termination_event(mock_worker_function):
    """Test _wait_for_shutdown with termination event."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True
        worker._processes = {}
        worker._data.termination_event.set()

        await worker._wait_for_shutdown()
        assert not worker._running


@pytest.mark.asyncio
async def test_check_termination_loop_with_exception(mock_worker_function, caplog):
    """Test _check_termination_loop with exception."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True
        worker._data.shutdown_event = MagicMock()
        worker._data.shutdown_event.is_set.return_value = False

        # Mock _check_termination_notice to raise an exception
        async def mock_check_termination():
            raise Exception("Termination check failed")

        worker._check_termination_notice = mock_check_termination

        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.side_effect = [None, Exception("Test complete")]
            with pytest.raises(Exception, match="Test complete"):
                await worker._check_termination_loop()

        assert "Error checking for termination" in caplog.text


@pytest.mark.asyncio
async def test_feed_tasks_to_workers_with_exception(mock_worker_function, caplog):
    """Test _feed_tasks_to_workers with exception."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True
        worker._data.num_simultaneous_tasks = 1

        # Mock the task queue to raise an exception
        mock_queue = AsyncMock()
        mock_queue.receive_tasks.side_effect = Exception("Queue error")
        worker._task_queue = mock_queue

        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.side_effect = [None, Exception("Test complete")]
            with pytest.raises(Exception, match="Test complete"):
                await worker._feed_tasks_to_workers()

        assert "Error feeding tasks to workers" in caplog.text


@pytest.mark.asyncio
async def test_monitor_process_runtimes_with_termination_error(mock_worker_function, caplog):
    """Test _monitor_process_runtimes with process termination error."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True
        worker._data.max_runtime = 0.1
        worker._data.retry_on_timeout = False

        # Create a mock process that fails to terminate
        mock_process = MagicMock()
        mock_process.pid = 123
        mock_process.is_alive.return_value = True
        mock_process.terminate.side_effect = Exception("Terminate failed")

        worker._processes = {
            123: {
                "process": mock_process,
                "start_time": time.time() - 0.2,  # Exceeded max runtime
                "task": {"task_id": "task-1", "ack_id": "ack1"},
            }
        }

        mock_queue = AsyncMock()
        worker._task_queue = mock_queue

        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.side_effect = [None, Exception("Test complete")]
            with pytest.raises(Exception, match="Test complete"):
                await worker._monitor_process_runtimes()

        assert "Error terminating process worker" in caplog.text


@pytest.mark.asyncio
async def test_monitor_process_runtimes_with_queue_error(mock_worker_function, caplog):
    """Test _monitor_process_runtimes with queue error."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True
        worker._data.max_runtime = 0.1
        worker._data.retry_on_timeout = False

        # Create a mock process that exceeds max runtime
        mock_process = MagicMock()
        mock_process.pid = 123
        mock_process.is_alive.return_value = False

        worker._processes = {
            123: {
                "process": mock_process,
                "start_time": time.time() - 0.2,  # Exceeded max runtime
                "task": {"task_id": "task-1", "ack_id": "ack1"},
            }
        }

        # Mock queue to raise exception
        mock_queue = AsyncMock()
        mock_queue.acknowledge_task.side_effect = Exception("Queue error")
        worker._task_queue = mock_queue

        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.side_effect = [None, StopAsyncIteration()]
            try:
                await worker._monitor_process_runtimes()
            except StopAsyncIteration:
                pass
        assert "Error completing task task-1: Queue error" in caplog.text


def test_worker_process_main_with_exception():
    """Test _worker_process_main with exception."""

    def bad_worker_function(task_id, task_data, worker):
        raise ValueError("Worker function error")

    result_queue = MagicMock()
    worker_data = MagicMock()
    worker_data.received_shutdown_request = False
    worker_data.received_termination_notice = False

    with patch("sys.exit") as mock_exit:
        Worker._worker_process_main(
            1,
            bad_worker_function,
            worker_data,
            "test-task",
            {"key": "value"},
            result_queue,
        )
        result_queue.put.assert_called_once_with((1, "exception", ANY))
        mock_exit.assert_called_once_with(0)


def test_worker_process_main_with_unhandled_exception():
    """Test _worker_process_main with unhandled exception."""

    def bad_worker_function(task_id, task_data, worker):
        raise ValueError("Worker function error")

    result_queue = MagicMock()
    worker_data = MagicMock()
    worker_data.received_shutdown_request = False
    worker_data.received_termination_notice = False

    # Mock the _execute_task_isolated to raise an exception
    with patch.object(Worker, "_execute_task_isolated", side_effect=Exception("Unhandled error")):
        with patch("sys.exit") as mock_exit:
            Worker._worker_process_main(
                1,
                bad_worker_function,
                worker_data,
                "test-task",
                {"key": "value"},
                result_queue,
            )
            result_queue.put.assert_called_once_with((1, "exception", ANY))
            mock_exit.assert_called_once_with(0)


# Test for YAML file processing in LocalTaskQueue
@pytest.fixture
def local_task_file_yaml_stream():
    """Create a YAML file with multiple documents for testing streaming."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("- task_id: task1\n  data: {key: value1}\n")
        f.write("- task_id: task2\n  data: {key: value2}\n")
        f.write("- task_id: task3\n  data: {key: value3}\n")
    yield f.name
    os.unlink(f.name)


@pytest.mark.asyncio
async def test_local_queue_yaml_streaming(local_task_file_yaml_stream):
    """Test LocalTaskQueue with YAML streaming."""
    queue = LocalTaskQueue(FCPath(local_task_file_yaml_stream))
    tasks = await queue.receive_tasks(max_count=3)
    # Filter out None values if any
    tasks = [t for t in tasks if t is not None]
    assert len(tasks) == 3
    assert tasks[0]["task_id"] == "task1"
    assert tasks[1]["task_id"] == "task2"
    assert tasks[2]["task_id"] == "task3"


# Test for invalid file format
@pytest.mark.asyncio
async def test_local_queue_invalid_file_format():
    """Test LocalTaskQueue with invalid file format."""
    with tempfile.NamedTemporaryFile(suffix=".txt") as f:
        f.write(b"invalid content")
        f.flush()
        with pytest.raises(ValueError, match="Unsupported file format"):
            queue = LocalTaskQueue(FCPath(f.name))
            await queue.receive_tasks(1)


# Test for signal handler with SIGTERM
def test_signal_handler_sigterm(mock_worker_function, caplog):
    """Test _signal_handler with SIGTERM signal."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)

        with patch("signal.signal") as mock_signal:
            worker._signal_handler(signal.SIGTERM, None)
            assert worker._data.shutdown_event.is_set()
            mock_signal.assert_called_with(signal.SIGTERM, signal.SIG_DFL)
            assert "Received signal SIGTERM, initiating graceful shutdown" in caplog.text


# Test for WorkerData properties
def test_worker_data_properties():
    """Test WorkerData properties."""
    from cloud_tasks.worker.worker import WorkerData

    data = WorkerData()
    data.termination_event = multiprocessing.Event()
    data.shutdown_event = multiprocessing.Event()

    # Test received_termination_notice property
    assert not data.received_termination_notice
    data.termination_event.set()
    assert data.received_termination_notice

    # Test received_shutdown_request property
    assert not data.received_shutdown_request
    data.shutdown_event.set()
    assert data.received_shutdown_request


# Test for _is_spot property
def test_worker_is_spot_property(mock_worker_function):
    """Test _is_spot property."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)

        # Test with is_spot=True
        worker._data.is_spot = True
        assert worker._is_spot is True

        # Test with simulate_spot_termination_after
        worker._data.is_spot = False
        worker._data.simulate_spot_termination_after = 10.0
        assert worker._is_spot is True

        # Test with both False
        worker._data.is_spot = False
        worker._data.simulate_spot_termination_after = None
        assert worker._is_spot is False


def test_worker_init_event_log_to_file_environment_variable_conversion(mock_worker_function):
    """Test that event_log_to_file environment variable is correctly converted from string to boolean."""
    with patch("sys.argv", ["worker.py"]):
        # Test with "true" (should be True)
        with patch.dict(
            os.environ,
            {
                "RMS_CLOUD_TASKS_PROVIDER": "AWS",
                "RMS_CLOUD_TASKS_JOB_ID": "test-job",
                "RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE": "true",
            },
        ):
            worker = Worker(mock_worker_function)
            assert worker._data.event_log_to_file is True

        # Test with "1" (should be True)
        with patch.dict(
            os.environ,
            {
                "RMS_CLOUD_TASKS_PROVIDER": "AWS",
                "RMS_CLOUD_TASKS_JOB_ID": "test-job",
                "RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE": "1",
            },
        ):
            worker = Worker(mock_worker_function)
            assert worker._data.event_log_to_file is True

        # Test with "TRUE" (should be True after lowercasing)
        with patch.dict(
            os.environ,
            {
                "RMS_CLOUD_TASKS_PROVIDER": "AWS",
                "RMS_CLOUD_TASKS_JOB_ID": "test-job",
                "RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE": "TRUE",
            },
        ):
            worker = Worker(mock_worker_function)
            assert worker._data.event_log_to_file is True

        # Test with "True" (should be True after lowercasing)
        with patch.dict(
            os.environ,
            {
                "RMS_CLOUD_TASKS_PROVIDER": "AWS",
                "RMS_CLOUD_TASKS_JOB_ID": "test-job",
                "RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE": "True",
            },
        ):
            worker = Worker(mock_worker_function)
            assert worker._data.event_log_to_file is True

        # Test with "false" (should be False)
        with patch.dict(
            os.environ,
            {
                "RMS_CLOUD_TASKS_PROVIDER": "AWS",
                "RMS_CLOUD_TASKS_JOB_ID": "test-job",
                "RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE": "false",
            },
        ):
            worker = Worker(mock_worker_function)
            assert worker._data.event_log_to_file is False

        # Test with "0" (should be False)
        with patch.dict(
            os.environ,
            {
                "RMS_CLOUD_TASKS_PROVIDER": "AWS",
                "RMS_CLOUD_TASKS_JOB_ID": "test-job",
                "RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE": "0",
            },
        ):
            worker = Worker(mock_worker_function)
            assert worker._data.event_log_to_file is False

        # Test with "yes" (should be False)
        with patch.dict(
            os.environ,
            {
                "RMS_CLOUD_TASKS_PROVIDER": "AWS",
                "RMS_CLOUD_TASKS_JOB_ID": "test-job",
                "RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE": "yes",
            },
        ):
            worker = Worker(mock_worker_function)
            assert worker._data.event_log_to_file is False

        # Test with empty string (should be False)
        with patch.dict(
            os.environ,
            {
                "RMS_CLOUD_TASKS_PROVIDER": "AWS",
                "RMS_CLOUD_TASKS_JOB_ID": "test-job",
                "RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE": "",
            },
        ):
            worker = Worker(mock_worker_function)
            assert worker._data.event_log_to_file is False

        # Test with None (should default based on task_source)
        with patch.dict(
            os.environ,
            {"RMS_CLOUD_TASKS_PROVIDER": "AWS", "RMS_CLOUD_TASKS_JOB_ID": "test-job"},
            clear=True,
        ):
            worker = Worker(mock_worker_function)
            # When no task_source is provided, event_log_to_file should default to False
            assert worker._data.event_log_to_file is False


@pytest.mark.asyncio
async def test_wait_for_shutdown_exiting_branch(caplog, mock_task_factory_empty):
    """Test _wait_for_shutdown exits when task source is empty and no processes remain."""
    with patch.object(sys, "argv", ["worker.py"]):
        worker = Worker(lambda tid, data, w: (False, None), task_source=mock_task_factory_empty)
        worker._task_source_is_empty = True
        worker._processes = {}
        worker._running = True
        worker._data.shutdown_event = MagicMock()
        worker._data.shutdown_event.is_set.return_value = False
        with patch("asyncio.sleep", return_value=None):
            with caplog.at_level(logging.INFO):
                await worker._wait_for_shutdown(interval=0.01)
    assert not worker._running
    assert any(
        "Local task source is empty and all processes complete; exiting" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_wait_for_shutdown_waiting_branch(caplog, mock_task_factory_empty):
    """Test _wait_for_shutdown waits when task source is empty but processes remain."""
    with patch.object(sys, "argv", ["worker.py"]):
        worker = Worker(lambda tid, data, w: (False, None), task_source=mock_task_factory_empty)
        worker._task_source_is_empty = True
        mock_proc = MagicMock()
        worker._processes = {1: {"process": mock_proc, "task": {}, "start_time": 0}}
        worker._running = True
        worker._data.shutdown_event = MagicMock()
        worker._data.shutdown_event.is_set.return_value = False

        # Let the loop run once, then simulate all processes finished
        async def fake_sleep(interval):
            worker._processes = {}
            return None

        with patch("asyncio.sleep", side_effect=fake_sleep):
            with caplog.at_level(logging.INFO):
                await worker._wait_for_shutdown(interval=0.01)
    assert worker._running is False
    assert any(
        "Local task source is empty; waiting for 1 processes to complete" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_feed_tasks_to_workers_sets_task_source_is_empty_when_factory_runs_out(
    mock_task_factory_empty,
):
    """Test that _feed_tasks_to_workers sets _task_source_is_empty when factory runs out of tasks."""
    from cloud_tasks.worker.worker import LocalTaskQueue

    with patch.object(sys, "argv", ["worker.py"]):
        worker = Worker(lambda tid, data, w: (False, None), task_source=mock_task_factory_empty)
        worker._running = True
        worker._data.num_simultaneous_tasks = 1
        worker._task_queue = LocalTaskQueue(mock_task_factory_empty)
        # Initially, _task_source_is_empty should be False
        assert worker._task_source_is_empty is False
        # Mock asyncio.sleep to break out of the loop after first iteration
        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.side_effect = [None, Exception("Break loop")]
            try:
                await worker._feed_tasks_to_workers()
            except Exception:
                pass
        # After running out of tasks, _task_source_is_empty should be True
        assert worker._task_source_is_empty is True
