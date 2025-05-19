"""Tests for the worker module."""

import asyncio
import json
import os
import pytest
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch
import sys
import types
import yaml
import signal
import multiprocessing
from multiprocessing import Process

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
    monkeypatch.setenv("RMS_CLOUD_WORKER_USE_NEW_PROCESS", "false")
    monkeypatch.setenv("RMS_CLOUD_TASKS_TO_SKIP", "5")
    monkeypatch.setenv("RMS_CLOUD_TASKS_MAX_NUM_TASKS", "10")

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
        assert worker.is_spot is True
        assert worker.price_per_hour == 0.1
        assert worker.num_simultaneous_tasks == 4
        assert worker.max_runtime == 3600
        assert worker.shutdown_grace_period == 300
        assert worker._use_new_process is False
        assert worker._task_skip_count == 5
        assert worker._max_num_tasks == 10


def test_init_with_args(mock_worker_function, env_setup_teardown):
    args = [
        "worker.py",
        "--provider",
        "GCP",
        "--project-id",
        "test-project",
        "--job-id",
        "test-job",
        "--queue-name",
        "test-queue",
        "--instance-type",
        "n1-standard-1",
        "--num-cpus",
        "2",
        "--memory",
        "4",
        "--local-ssd",
        "100",
        "--boot-disk",
        "20",
        "--is-spot",
        "--price",
        "0.1",
        "--num-simultaneous-tasks",
        "4",
        "--max-runtime",
        "3600",
        "--shutdown-grace-period",
        "300",
        "--use-new-process",
        "--tasks-to-skip",
        "15",
        "--max-num-tasks",
        "20",
    ]
    with patch("sys.argv", args):
        worker = Worker(mock_worker_function)
        assert worker.provider == "GCP"
        assert worker.project_id == "test-project"
        assert worker.job_id == "test-job"
        assert worker.queue_name == "test-queue"
        assert worker.instance_type == "n1-standard-1"
        assert worker.num_cpus == 2
        assert worker.memory_gb == 4.0
        assert worker.local_ssd_gb == 100.0
        assert worker.boot_disk_gb == 20.0
        assert worker.is_spot is True
        assert worker.price_per_hour == 0.1
        assert worker.num_simultaneous_tasks == 4
        assert worker.max_runtime == 3600
        assert worker.shutdown_grace_period == 300
        assert worker._use_new_process is True
        assert worker._task_skip_count == 15
        assert worker._max_num_tasks == 20


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
    assert mock_create_queue.call_count == 1


@pytest.mark.asyncio
async def test_handle_results(worker, mock_queue):
    worker._task_queue = mock_queue
    worker._running = True
    worker._result_queue.put((1, "task1", "ack1", True, "success"))
    worker._result_queue.put((2, "task2", "ack2", False, "error"))

    async def shutdown_when_done():
        # Wait until both counters are incremented or timeout
        for _ in range(20):
            if worker._num_tasks_processed.value == 1 and worker._num_tasks_failed.value == 1:
                break
            await asyncio.sleep(0.02)
        worker._shutdown_event.set()

    handler_task = asyncio.create_task(worker._handle_results())
    shutdown_task = asyncio.create_task(shutdown_when_done())
    await asyncio.gather(handler_task, shutdown_task)

    assert worker._num_tasks_processed.value == 1
    assert worker._num_tasks_failed.value == 1
    mock_queue.complete_task.assert_called_once_with("ack1")
    mock_queue.fail_task.assert_called_once_with("ack2")


@pytest.mark.asyncio
async def test_check_termination_notice_aws(worker):
    worker._provider = "aws"
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        assert await worker._check_termination_notice() is True


@pytest.mark.asyncio
async def test_check_termination_notice_gcp(worker):
    worker._provider = "gcp"
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
        # Set shutdown after one iteration to break the loop
        async def fake_receive_tasks(*a, **kw):
            worker._shutdown_event.set()
            return [sample_task]

        mock_queue.receive_tasks.side_effect = fake_receive_tasks
        await worker._feed_tasks_to_workers()

    await asyncio.wait_for(run_once(), timeout=0.2)
    # The value should be incremented by 1
    assert worker._num_active_tasks.value == 1


def test_worker_process_main(mock_worker_function):
    task_queue = MagicMock()
    result_queue = MagicMock()
    shutdown_event = MagicMock()
    termination_event = MagicMock()
    active_tasks = MagicMock()
    task_queue.get.return_value = {
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
        task_queue,
        result_queue,
        shutdown_event,
        termination_event,
        active_tasks,
        False,
    )
    result_queue.put.assert_called_once()
    active_tasks.get_lock.assert_called_once()


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
                        use_new_process=None,
                        tasks_to_skip=None,
                        max_num_tasks=None,
                    )
                    # Patch _parse_args to return our args
                    with patch("cloud_tasks.worker.worker._parse_args", return_value=args):
                        Worker(mock_worker_function)
                        mock_exit.assert_called_once_with(1)


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
                use_new_process=None,
                tasks_to_skip=None,
                max_num_tasks=None,
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
                use_new_process=None,
                tasks_to_skip=None,
                max_num_tasks=None,
            )
            mock_parse_args.return_value = args
            worker = Worker(mock_worker_function)
            assert worker.num_simultaneous_tasks == 1


def test_max_runtime_from_cli_env(mock_worker_function):
    # From CLI
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
                max_runtime=123,
                shutdown_grace_period=None,
                use_new_process=None,
                tasks_to_skip=None,
                max_num_tasks=None,
            )
            mock_parse_args.return_value = args
            worker = Worker(mock_worker_function)
            assert worker.max_runtime == 123
    # From ENV
    with patch("sys.argv", ["worker.py"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            with patch.dict(os.environ, {"RMS_CLOUD_TASKS_MAX_RUNTIME": "456"}):
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
                    use_new_process=None,
                    tasks_to_skip=None,
                    max_num_tasks=None,
                )
                mock_parse_args.return_value = args
                worker = Worker(mock_worker_function)
                assert worker.max_runtime == 456


def test_task_skip_count_and_running_init(mock_worker_function):
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
                use_new_process=None,
                tasks_to_skip=7,
                max_num_tasks=None,
            )
            mock_parse_args.return_value = args
            worker = Worker(mock_worker_function)
            assert worker._task_skip_count == 7
            assert worker._running is False


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
                use_new_process=None,
                tasks_to_skip=1,
                max_num_tasks=10,
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
                use_new_process=None,
                tasks_to_skip=None,
                max_num_tasks=None,
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
        worker._processes = [mock_process1, mock_process2]

        # Set up initial state
        worker._running = True
        worker._num_active_tasks.value = 2
        worker._shutdown_grace_period = 5  # Longer grace period for testing

        # Create a task to set shutdown event and simulate process completion
        async def trigger_shutdown():
            await asyncio.sleep(0.1)
            worker._shutdown_event.set()
            # Simulate processes completing their tasks immediately
            worker._num_active_tasks.value = 0
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
        mock_process1.join.assert_called_once()
        mock_process2.join.assert_called_once()


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
        worker._num_active_tasks.value = 2
        worker._shutdown_grace_period = 1  # Short grace period for testing

        # Create a task to set shutdown event after a delay
        async def trigger_shutdown():
            await asyncio.sleep(0.1)
            worker._shutdown_event.set()
            # Keep active tasks count high to force termination
            worker._num_active_tasks.value = 2

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
        worker._processes = []
        worker._num_active_tasks.value = 0

        # Set shutdown event
        worker._shutdown_event.set()

        # Call _wait_for_shutdown
        await worker._wait_for_shutdown()

        # Verify worker is no longer running
        assert not worker._running


def test_start_worker_processes(mock_worker_function):
    """Test starting worker processes in process pool mode."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        with patch("cloud_tasks.worker.worker.Process") as mock_process:
            with patch("cloud_tasks.worker.worker.logger") as mock_logger:
                worker = Worker(mock_worker_function)
                worker._num_simultaneous_tasks = 3
                worker._use_new_process = False

                # Mock process instances
                mock_procs = [MagicMock() for _ in range(3)]
                mock_process.side_effect = mock_procs

                # Start worker processes
                worker._start_worker_processes()

                # Verify processes were created and started
                assert mock_process.call_count == 3
                for i, proc in enumerate(mock_procs):
                    # Verify process creation
                    mock_process.assert_any_call(
                        target=Worker._worker_process_main,
                        args=(
                            i,
                            mock_worker_function,
                            worker,
                            worker._task_queue_mp,
                            worker._result_queue,
                            worker._shutdown_event,
                            worker._termination_event,
                            worker._num_active_tasks,
                            False,  # is_single_task
                        ),
                    )
                    # Verify process configuration
                    assert proc.daemon is True
                    proc.start.assert_called_once()
                    # Verify logging
                    mock_logger.info.assert_any_call(
                        f"Started worker process #{i} (PID: {proc.pid})"
                    )

                # Verify processes were added to the list
                assert len(worker._processes) == 3
                assert worker._processes == mock_procs


@pytest.mark.asyncio
async def test_create_single_task_process(mock_worker_function):
    """Test creating a single task process."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        with patch("cloud_tasks.worker.worker.Process") as mock_process:
            with patch("cloud_tasks.worker.worker.logger") as mock_logger:
                worker = Worker(mock_worker_function)
                worker._use_new_process = True
                worker._num_tasks_processed.value = 1
                worker._num_tasks_failed.value = 2
                worker._num_active_tasks.value = 0
                worker._task_skip_count = 0
                worker._running = True

                # Mock process instance
                mock_proc = MagicMock()
                mock_process.return_value = mock_proc

                # Mock task queue to return a task once and then None
                mock_queue = AsyncMock()
                mock_queue.receive_tasks.side_effect = [
                    [{"task_id": "test-task", "data": {}, "ack_id": "test-ack"}],
                    [],
                ]
                worker._task_queue = mock_queue

                # Create a task to set shutdown event after process creation
                async def trigger_shutdown():
                    await asyncio.sleep(0.1)
                    worker._shutdown_event.set()

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
                        worker._task_queue_mp,
                        worker._result_queue,
                        worker._shutdown_event,
                        worker._termination_event,
                        worker._num_active_tasks,
                        True,  # is_single_task
                    ),
                )

                # Verify process configuration
                assert mock_proc.daemon is True
                mock_proc.start.assert_called_once()

                # Verify logging
                expected_message = f"Started single-task process #3 (PID: {mock_proc.pid})"
                mock_logger.info.assert_any_call(expected_message)

                # Verify process was added to the list
                assert len(worker._processes) == 1
                assert worker._processes[0] == mock_proc
