"""Tests for the worker module."""

import asyncio
import json
import os
import pytest
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch, call
import types
import yaml
import signal
import time

from cloud_tasks.worker.worker import Worker, LocalTaskQueue


@pytest.fixture
def mock_queue():
    queue = AsyncMock()
    queue.receive_tasks = AsyncMock()
    queue.complete_task = AsyncMock()
    queue.fail_task = AsyncMock()
    return queue


@pytest.fixture
def sample_task():
    return {"task_id": "test-task-1", "data": {"key": "value"}, "ack_id": "test-ack-1"}


@pytest.fixture
def mock_worker_function():
    def worker_func(task_id, task_data, worker):
        return True, "success"

    return worker_func


# Local tasks tests


@pytest.fixture
def local_tasks_file_json():
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
def local_tasks_file_yaml():
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


def test_local_queue_init_with_json(local_tasks_file_json):
    queue = LocalTaskQueue(local_tasks_file_json)
    assert queue._tasks_file == local_tasks_file_json


def test_local_queue_init_with_yaml(local_tasks_file_yaml):
    queue = LocalTaskQueue(local_tasks_file_yaml)
    assert queue._tasks_file == local_tasks_file_yaml


def test_local_queue_init_with_invalid_format():
    with tempfile.NamedTemporaryFile(suffix=".txt") as f:
        f.write(b"invalid content")
        f.flush()
        queue = LocalTaskQueue(f.name)
        # Try to receive a task to trigger the error
        with pytest.raises(ValueError, match="Unsupported file format"):
            asyncio.get_event_loop().run_until_complete(queue.receive_tasks(1, 10))


@pytest.mark.asyncio
async def test_local_queue_receive_tasks_json(local_tasks_file_json):
    queue = LocalTaskQueue(local_tasks_file_json)
    tasks = await queue.receive_tasks(max_count=2, visibility_timeout=30)
    assert len(tasks) == 2
    assert tasks[0]["task_id"] == "task1"
    assert tasks[1]["task_id"] == "task2"
    assert "ack_id" in tasks[0]
    assert "ack_id" in tasks[1]


@pytest.mark.asyncio
async def test_local_queue_receive_tasks_yaml(local_tasks_file_yaml):
    queue = LocalTaskQueue(local_tasks_file_yaml)
    tasks = await queue.receive_tasks(max_count=2, visibility_timeout=30)
    assert len(tasks) == 2
    assert tasks[0]["task_id"] == "task3"
    assert tasks[1]["task_id"] == "task4"
    assert "ack_id" in tasks[0]
    assert "ack_id" in tasks[1]


@pytest.mark.asyncio
async def test_local_queue_complete_task(local_tasks_file_json):
    queue = LocalTaskQueue(local_tasks_file_json)
    await queue.complete_task("test-ack-1")  # This does nothing


@pytest.mark.asyncio
async def test_local_queue_fail_task(local_tasks_file_json):
    queue = LocalTaskQueue(local_tasks_file_json)
    await queue.fail_task("test-ack-1")  # This does nothing


@pytest.mark.asyncio
async def test_local_queue_receive_all_tasks(local_tasks_file_json):
    """Test that LocalTaskQueue.receive_tasks returns all tasks when max_count is larger than available tasks."""
    queue = LocalTaskQueue(local_tasks_file_json)
    tasks = await queue.receive_tasks(max_count=5, visibility_timeout=30)
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
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_TYPE", "t2.micro")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS", "2")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_MEM_GB", "4")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_SSD_GB", "100")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB", "20")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_IS_SPOT", "true")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_PRICE", "0.1")
    monkeypatch.setenv("RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE", "4")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_RUNTIME", "3600")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD", "300")
    monkeypatch.setenv("RMS_CLOUD_TASKS_TO_SKIP", "5")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_NUM_TASKS", "10")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER", "32")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY", "33")
    monkeypatch.setenv("RMS_CLOUD_TASKS_NO_RETRY_ON_CRASH", "true")
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
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_TYPE", "t2.micro")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS", "2")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_MEM_GB", "4")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_SSD_GB", "100")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB", "20")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_IS_SPOT", "false")
    monkeypatch.setenv("RMS_CLOUD_TASKS_INSTANCE_PRICE", "0.1")
    monkeypatch.setenv("RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE", "4")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_RUNTIME", "3600")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD", "300")
    monkeypatch.setenv("RMS_CLOUD_TASKS_TO_SKIP", "5")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_NUM_TASKS", "10")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER", "32")
    monkeypatch.setenv("RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY", "33")
    monkeypatch.setenv("RMS_CLOUD_TASKS_NO_RETRY_ON_CRASH", "false")
    # Provide the modified environment
    yield

    # Teardown: Restore original environment variables
    os.environ.clear()
    os.environ.update(original_env)


def test_init_with_env_vars(mock_worker_function, env_setup_teardown):

    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function)
        assert worker.provider == "AWS"
        assert worker.job_id == "test-job"
        assert worker.queue_name == "test-job"
        assert worker.instance_type == "t2.micro"
        assert worker.num_cpus == 2
        assert worker.memory_gb == 4.0
        assert worker.local_ssd_gb == 100.0
        assert worker.boot_disk_gb == 20.0
        assert worker._is_spot is True
        assert worker.is_spot is True
        assert worker.price_per_hour == 0.1
        assert worker.num_simultaneous_tasks == 4
        assert worker.max_runtime == 3600
        assert worker.shutdown_grace_period == 300
        assert worker._task_skip_count == 5
        assert worker._max_num_tasks == 10
        assert worker._simulate_spot_termination_after == 32
        assert worker._simulate_spot_termination_delay == 33
        assert worker._no_retry_on_crash is True


def test_init_with_env_vars_false(mock_worker_function, env_setup_teardown_false):

    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function)
        assert worker.provider == "AWS"
        assert worker.job_id == "test-job"
        assert worker.queue_name == "test-job"
        assert worker.instance_type == "t2.micro"
        assert worker.num_cpus == 2
        assert worker.memory_gb == 4.0
        assert worker.local_ssd_gb == 100.0
        assert worker.boot_disk_gb == 20.0
        assert worker._is_spot is False
        assert worker.is_spot is True  # Because of simulate_spot_termination_after
        assert worker.price_per_hour == 0.1
        assert worker.num_simultaneous_tasks == 4
        assert worker.max_runtime == 3600
        assert worker.shutdown_grace_period == 300
        assert worker._task_skip_count == 5
        assert worker._max_num_tasks == 10
        assert worker._simulate_spot_termination_after == 32
        assert worker._simulate_spot_termination_delay == 33
        assert worker._no_retry_on_crash is False


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
        "--no-retry-on-crash",
    ]
    with patch("sys.argv", args):
        worker = Worker(mock_worker_function)
        assert worker.provider == "GCP"
        assert worker.project_id == "test-project"
        assert worker.job_id == "gcp-test-job"
        assert worker.queue_name == "aws-test-queue"
        assert worker.instance_type == "n1-standard-1"
        assert worker.num_cpus == 1
        assert worker.memory_gb == 2.0
        assert worker.local_ssd_gb == 50.0
        assert worker.boot_disk_gb == 10.0
        assert worker._is_spot is True
        assert worker.is_spot is True
        assert worker.price_per_hour == 0.2
        assert worker.num_simultaneous_tasks == 2
        assert worker.max_runtime == 1800
        assert worker.shutdown_grace_period == 150
        assert worker._task_skip_count == 7
        assert worker._max_num_tasks == 10
        assert worker._simulate_spot_termination_after == 16
        assert worker._simulate_spot_termination_delay == 17
        assert worker._no_retry_on_crash is True


def test_num_simultaneous_tasks_default(mock_worker_function):
    # num_cpus is set
    with patch("sys.argv", ["worker.py"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            args = types.SimpleNamespace(
                provider="AWS",
                project_id=None,
                tasks=None,
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
                no_retry_on_crash=None,
            )
            mock_parse_args.return_value = args
            worker = Worker(mock_worker_function)
            assert worker.num_simultaneous_tasks == 3
    # num_cpus is None
    with patch("sys.argv", ["worker.py"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            args = types.SimpleNamespace(
                provider="AWS",
                project_id=None,
                tasks=None,
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
                no_retry_on_crash=None,
            )
            mock_parse_args.return_value = args
            worker = Worker(mock_worker_function)
            assert worker.num_simultaneous_tasks == 1


def test_provider_required_without_tasks(mock_worker_function):
    """Test that provider is required when no tasks file is specified."""
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["worker.py"]):
            with patch("sys.exit") as mock_exit:
                with patch("cloud_tasks.worker.worker.logger") as mock_logger:
                    args = types.SimpleNamespace(
                        provider=None,
                        project_id=None,
                        tasks=None,
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
                        no_retry_on_crash=None,
                    )
                    with patch("cloud_tasks.worker.worker._parse_args", return_value=args):
                        Worker(mock_worker_function)
                        mock_exit.assert_called_once_with(1)
                        mock_logger.error.assert_called_once_with(
                            "Provider not specified via --provider or RMS_CLOUD_TASKS_PROVIDER and no tasks file specified via --tasks"
                        )


def test_provider_not_required_with_tasks(mock_worker_function):
    """Test that provider is not required when tasks file is specified."""
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["worker.py", "--tasks", "tasks.json"]):
            with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
                args = types.SimpleNamespace(
                    provider=None,
                    project_id=None,
                    tasks="tasks.json",
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
                    no_retry_on_crash=None,
                )
                mock_parse_args.return_value = args
                worker = Worker(mock_worker_function)
                assert worker.provider is None


@pytest.mark.asyncio
async def test_start_with_local_tasks(mock_worker_function, local_tasks_file_json):
    with patch("sys.argv", ["worker.py", "--tasks", local_tasks_file_json]):
        worker = Worker(mock_worker_function)
        with patch.object(worker, "_wait_for_shutdown") as mock_wait:
            mock_wait.side_effect = asyncio.CancelledError()
            with pytest.raises(asyncio.CancelledError):
                await worker.start()


@pytest.mark.asyncio
async def test_start_with_cloud_queue(mock_worker_function, mock_queue):
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        with patch(
            "cloud_tasks.worker.worker.create_queue", return_value=mock_queue
        ) as mock_create_queue:
            with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                mock_wait.side_effect = asyncio.CancelledError()
                with patch("asyncio.create_task", return_value=MagicMock()):
                    with pytest.raises(asyncio.CancelledError):
                        await worker.start()
    mock_create_queue.assert_called_once_with(
        provider="AWS",
        queue_name="test-job",
        project_id=None,
    )


@pytest.mark.asyncio
async def test_handle_results(worker, mock_queue):
    worker._task_queue = mock_queue
    worker._running = True
    worker._shutdown_grace_period = 0.01

    # Set up the mock queue to return immediately
    mock_queue.complete_task.return_value = asyncio.Future()
    mock_queue.complete_task.return_value.set_result(None)
    mock_queue.fail_task.return_value = asyncio.Future()
    mock_queue.fail_task.return_value.set_result(None)

    worker._processes = {
        1: {"process": MagicMock(), "task": {"task_id": "task1", "ack_id": "ack1"}},
        2: {"process": MagicMock(), "task": {"task_id": "task2", "ack_id": "ack2"}},
    }

    # Put tasks in the result queue
    worker._result_queue.put((1, True, "success"))
    worker._result_queue.put((2, False, "error"))

    async def shutdown_when_done():
        # Wait for both tasks to be processed with a shorter timeout
        start_time = time.time()
        timeout = 0.5  # 500ms timeout

        while time.time() - start_time < timeout:
            if worker._num_tasks_succeeded == 1 and worker._num_tasks_failed == 1:
                break
            await asyncio.sleep(0.01)  # 10ms sleep

        if worker._num_tasks_succeeded != 1 or worker._num_tasks_failed != 1:
            pytest.fail("Timeout waiting for tasks to be processed")

        # Set shutdown event and wait for a moment to ensure it's processed
        worker._shutdown_event.set()
        await asyncio.sleep(0.1)  # Give time for shutdown to be processed

    handler_task = asyncio.create_task(worker._handle_results())
    shutdown_task = asyncio.create_task(worker._wait_for_shutdown(interval=0.01))
    done_task = asyncio.create_task(shutdown_when_done())

    try:
        await asyncio.wait_for(asyncio.gather(handler_task, shutdown_task, done_task), timeout=1.0)
    except asyncio.TimeoutError:
        pytest.fail("Test timed out waiting for tasks to complete")

    # Verify the results
    assert worker._num_tasks_succeeded == 1
    assert worker._num_tasks_failed == 1
    mock_queue.complete_task.assert_called_once_with("ack1")
    mock_queue.fail_task.assert_called_once_with("ack2")


@pytest.mark.asyncio
async def test_check_termination_notice_aws(worker):
    worker._provider = "AWS"
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        assert await worker._check_termination_notice() is True


@pytest.mark.asyncio
async def test_check_termination_notice_gcp(worker):
    worker._provider = "GCP"
    with patch("requests.get") as mock_get:
        mock_get.return_value.text = "true"
        assert await worker._check_termination_notice() is True


@pytest.mark.asyncio
async def test_check_termination_notice_azure(worker):
    worker._provider = "AZURE"
    assert await worker._check_termination_notice() is False


@pytest.mark.asyncio
async def test_feed_tasks_to_workers(worker, mock_queue, sample_task):
    worker._task_queue = mock_queue
    worker._running = True
    worker._task_skip_count = 0
    mock_queue.receive_tasks.return_value = [sample_task]

    # Directly call the coroutine instead of relying on background task
    async def run_once():
        # Set running to False after one iteration to break the loop
        async def fake_receive_tasks(*a, **kw):
            worker._running = False
            return [sample_task]

        mock_queue.receive_tasks.side_effect = fake_receive_tasks
        await worker._feed_tasks_to_workers()

    await asyncio.wait_for(run_once(), timeout=0.2)
    # The value should be incremented by 1
    assert len(worker._processes) == 1


def test_worker_process_main(mock_worker_function):
    task_queue = MagicMock()
    result_queue = MagicMock()
    shutdown_event = MagicMock()
    termination_event = MagicMock()
    active_tasks = MagicMock()
    task = {
        "task_id": "test-task",
        "data": {"key": "value"},
        "ack_id": "test-ack",
    }
    shutdown_event.is_set.side_effect = [False, True]
    termination_event.is_set.return_value = False
    Worker._worker_process_main(
        1,
        mock_worker_function,
        MagicMock(),
        task,
        result_queue,
        shutdown_event,
        termination_event,
    )
    result_queue.put.assert_called_once()


@staticmethod
def test_execute_task_isolated(mock_worker_function):
    task_id = "test-task"
    task_data = {"key": "value"}
    worker = MagicMock()
    success, result = Worker._execute_task_isolated(
        task_id, task_data, worker, mock_worker_function
    )
    assert success is True
    assert result == "success"


@staticmethod
def test_execute_task_isolated_error():
    def error_func(task_id, task_data, worker):
        raise ValueError("Test error")

    task_id = "test-task"
    task_data = {"key": "value"}
    worker = MagicMock()
    success, result = Worker._execute_task_isolated(task_id, task_data, worker, error_func)
    assert success is False
    assert "Test error" in result


def test_exit_if_no_job_id_and_no_tasks(mock_worker_function):
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["worker.py"]):
            with patch("sys.exit") as mock_exit:
                with patch("cloud_tasks.worker.worker.logger") as mock_logger:
                    args = types.SimpleNamespace(
                        provider="AWS",
                        project_id=None,
                        tasks=None,
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
                        no_retry_on_crash=None,
                    )
                    # Patch _parse_args to return our args
                    with patch("cloud_tasks.worker.worker._parse_args", return_value=args):
                        with patch("cloud_tasks.worker.worker.create_queue", return_value=None):
                            Worker(mock_worker_function)
                            mock_exit.assert_called_once_with(1)
                            mock_logger.error.assert_called_once_with(
                                "Queue name not specified via --queue-name or "
                                "RMS_CLOUD_TASKS_QUEUE_NAME or --job-id or RMS_CLOUD_TASKS_JOB_ID "
                                "and no tasks file specified via --tasks"
                            )


def test_worker_properties(mock_worker_function):
    with patch("sys.argv", ["worker.py"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            args = types.SimpleNamespace(
                provider="AWS",
                project_id="pid",
                tasks=None,
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
                no_retry_on_crash=None,
            )
            mock_parse_args.return_value = args
            worker = Worker(mock_worker_function)
            assert worker.provider == "AWS"
            assert worker.project_id == "pid"
            assert worker.job_id == "jid"
            assert worker.queue_name == "qname"
            assert worker.instance_type == "itype"
            assert worker.num_cpus == 2
            assert worker.memory_gb == 3.5
            assert worker.local_ssd_gb == 4.5
            assert worker.boot_disk_gb == 5.5
            assert worker.is_spot is True
            assert worker.price_per_hour == 0.99
            assert worker.num_simultaneous_tasks == 2
            assert worker.max_runtime == 100
            assert worker.shutdown_grace_period == 200


def test_signal_handler(mock_worker_function):
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            args = types.SimpleNamespace(
                provider="AWS",
                project_id=None,
                tasks=None,
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
                no_retry_on_crash=None,
            )
            mock_parse_args.return_value = args
            worker = Worker(mock_worker_function)

            # Test SIGINT
            with patch("signal.signal") as mock_signal:
                with patch("cloud_tasks.worker.worker.logger") as mock_logger:
                    worker._signal_handler(signal.SIGINT, None)
                    assert worker._shutdown_event.is_set()
                    mock_signal.assert_called_with(signal.SIGTERM, signal.SIG_DFL)
                    mock_logger.info.assert_called_with(
                        "Received signal SIGINT, initiating graceful shutdown"
                    )


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
        worker._shutdown_grace_period = 5  # Longer grace period for testing

        # Create a task to set shutdown event and simulate process completion
        async def trigger_shutdown():
            await asyncio.sleep(0.1)
            worker._shutdown_event.set()
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
        worker._shutdown_grace_period = 1  # Short grace period for testing

        # Create a task to set shutdown event after a delay
        async def trigger_shutdown():
            await asyncio.sleep(0.1)
            worker._shutdown_event.set()
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
        worker._shutdown_event.set()

        # Call _wait_for_shutdown
        await worker._wait_for_shutdown()

        # Verify worker is no longer running
        assert not worker._running


@pytest.mark.asyncio
async def test_create_single_task_process(mock_worker_function):
    """Test creating a single task process."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        with patch("cloud_tasks.worker.worker.Process") as mock_process:
            with patch("cloud_tasks.worker.worker.logger") as mock_logger:
                worker = Worker(mock_worker_function)
                worker._num_tasks_succeeded = 1
                worker._num_tasks_failed = 2
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

                # Create a task to set shutdown event after process creation
                async def trigger_shutdown():
                    await asyncio.sleep(0.1)
                    worker._shutdown_event.set()
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
                        worker,
                        task,
                        worker._result_queue,
                        worker._shutdown_event,
                        worker._termination_event,
                    ),
                )

                # Verify process configuration
                assert mock_proc.daemon is True
                mock_proc.start.assert_called_once()

                # Verify logging
                expected_message = f"Started single-task worker #3 (PID {mock_proc.pid})"
                mock_logger.info.assert_any_call(expected_message)

                # Verify process was added to the list
                assert len(worker._processes) == 1
                assert worker._processes[3]["process"] == mock_proc


@pytest.mark.asyncio
async def test_check_termination_loop(mock_worker_function):
    """Test that _check_termination_loop properly handles termination notices."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.logger") as mock_logger:
            with patch("asyncio.sleep") as mock_sleep:  # Patch sleep to run instantly
                worker = Worker(mock_worker_function)
                worker._running = True
                worker._shutdown_event = MagicMock()
                worker._shutdown_event.is_set.return_value = False
                worker._termination_event = MagicMock()
                worker._termination_event.is_set.return_value = False

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
                worker._termination_event.set.assert_called_once()

                # Verify logger was called with appropriate message
                mock_logger.warning.assert_called_once_with("Instance termination notice received")

                # Verify sleep was called with the correct duration
                mock_sleep.assert_called_once_with(5)


@pytest.mark.asyncio
async def test_monitor_process_runtimes(mock_worker_function, caplog):
    """Test _monitor_process_runtimes with a process that exceeds max runtime."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("asyncio.sleep") as mock_sleep:
            # Make sleep raise an exception after first call to break the loop
            mock_sleep.side_effect = [None, Exception("Test complete")]

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._max_runtime = 0.2

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

            # Mock task queue for fail_task call
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
            mock_queue.complete_task.assert_called_once_with("ack1")

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
            worker._max_runtime = 0.2

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

            # Mock task queue for fail_task call
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
            mock_queue.complete_task.assert_called_once_with("ack1")

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
            worker._shutdown_event = MagicMock()
            worker._shutdown_event.is_set.return_value = False
            worker._max_runtime = 0.1

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
        assert worker._simulate_spot_termination_after == 0.1
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
        assert worker.is_spot

        # Mock _check_termination_loop
        mock_loop = AsyncMock()
        worker._check_termination_loop = mock_loop

        # Set up the worker to exit immediately
        worker._shutdown_event.set()

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
        assert not worker.is_spot

        # Mock _check_termination_loop
        mock_loop = AsyncMock()
        worker._check_termination_loop = mock_loop

        # Set up the worker to exit immediately
        worker._shutdown_event.set()

        # Start the worker
        await worker.start()

        # Verify _check_termination_loop was not called
        mock_loop.assert_not_called()
