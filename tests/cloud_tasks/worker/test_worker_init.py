"""Tests for Worker initialization and configuration."""

import logging
import multiprocessing
import os
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from filecache import FCPath

from cloud_tasks.worker.worker import Worker

_DEFAULT_ARGS = {
    "provider": "AWS",
    "project_id": None,
    "task_file": None,
    "job_id": "jid",
    "queue_name": None,
    "instance_type": None,
    "num_cpus": None,
    "memory": None,
    "local_ssd": None,
    "boot_disk": None,
    "is_spot": None,
    "price": None,
    "num_simultaneous_tasks": None,
    "max_runtime": None,
    "shutdown_grace_period": None,
    "tasks_to_skip": None,
    "max_num_tasks": None,
    "simulate_spot_termination_after": None,
    "simulate_spot_termination_delay": None,
    "event_log_to_file": None,
    "event_log_file": None,
    "event_log_to_queue": False,
    "verbose": False,
    "retry_on_timeout": None,
    "retry_on_exception": None,
    "retry_on_exit": None,
    "exactly_once_queue": None,
}


def default_args_namespace(**overrides: object) -> types.SimpleNamespace:
    """Build a SimpleNamespace from default args with optional overrides for Worker tests."""
    d = dict(_DEFAULT_ARGS)
    d.update(overrides)
    return types.SimpleNamespace(**d)


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


def test_worker_init_with_task_source_string_logging(mock_worker_function, local_task_file_json):
    """Test that Worker logs when using task_source as string."""
    with patch("sys.argv", ["worker.py"]), patch("cloud_tasks.worker.worker.logger") as mock_logger:
        _ = Worker(mock_worker_function, task_source=local_task_file_json)
        info_calls = [str(c) for c in mock_logger.info.call_args_list]
        assert any("Using local tasks file" in c and local_task_file_json in c for c in info_calls)


def test_worker_init_with_task_source_factory_logging(mock_worker_function, mock_task_factory):
    """Test that Worker logs when using task_source as factory."""
    with patch("sys.argv", ["worker.py"]), patch("cloud_tasks.worker.worker.logger") as mock_logger:
        _ = Worker(mock_worker_function, task_source=mock_task_factory)
        info_calls = [str(c) for c in mock_logger.info.call_args_list]
        assert any("Using task factory function" in c for c in info_calls)


# Worker __init__ from env/args and validation


def test_init_with_env_vars(mock_worker_function, env_setup_teardown):
    """Test Worker initialization from environment variables (truthy values)."""
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
    """Test Worker initialization from environment variables (falsy values)."""
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
    """Test Worker initialization with command-line args overriding env (no-* flags)."""
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
    """Test Worker initialization with command-line args (truthy flags)."""
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


def test_num_simultaneous_tasks_default(mock_worker_function) -> None:
    """Test default num_simultaneous_tasks from num_cpus or 1."""
    with patch("sys.argv", ["worker.py"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            mock_parse_args.return_value = default_args_namespace(num_cpus=3)
            worker = Worker(mock_worker_function)
            assert worker._data.num_simultaneous_tasks == 3
    with patch("sys.argv", ["worker.py"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            mock_parse_args.return_value = default_args_namespace()
            worker = Worker(mock_worker_function)
            assert worker._data.num_simultaneous_tasks == 1


def test_provider_required_without_tasks(mock_worker_function, caplog) -> None:
    """Test that provider is required when no tasks file is specified."""
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["worker.py"]):
            with patch("sys.exit") as mock_exit:
                with patch(
                    "cloud_tasks.worker.worker._parse_args",
                    return_value=default_args_namespace(provider=None, job_id="test-job"),
                ):
                    Worker(mock_worker_function)
                    mock_exit.assert_called_once_with(1)
                    assert (
                        "Provider not specified via --provider or RMS_CLOUD_TASKS_PROVIDER and no tasks file specified via --task-file"
                        in caplog.text
                    )


def test_provider_not_required_with_tasks(mock_worker_function) -> None:
    """Test that provider is not required when tasks file is specified."""
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["worker.py", "--task-file", "tasks.json"]):
            with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
                mock_parse_args.return_value = default_args_namespace(
                    provider=None, task_file="tasks.json"
                )
                worker = Worker(mock_worker_function)
                assert worker._data.provider is None


def test_exit_if_no_job_id_and_no_tasks(mock_worker_function, caplog) -> None:
    """Test Worker exits when job_id and task file are both missing."""
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["worker.py"]):
            with patch("sys.exit") as mock_exit:
                with patch(
                    "cloud_tasks.worker.worker._parse_args",
                    return_value=default_args_namespace(job_id=None),
                ):
                    with patch("cloud_tasks.worker.worker.create_queue", return_value=None):
                        Worker(mock_worker_function)
                        mock_exit.assert_called_once_with(1)
                        assert (
                            "Queue name not specified via --queue-name or RMS_CLOUD_TASKS_QUEUE_NAME or --job-id or RMS_CLOUD_TASKS_JOB_ID and no tasks file specified via --task-file"
                            in caplog.text
                        )


def test_worker_properties(mock_worker_function) -> None:
    """Test Worker properties reflect parsed args."""
    with patch("sys.argv", ["worker.py"]):
        with patch("cloud_tasks.worker.worker._parse_args") as mock_parse_args:
            mock_parse_args.return_value = default_args_namespace(
                provider="AWS",
                project_id="pid",
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
            )
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
            assert worker._task_skip_count == 1
            assert worker._max_num_tasks == 10
            assert worker._data.simulate_spot_termination_after == 32
            assert worker._data.simulate_spot_termination_delay == 33
            assert worker._data.event_log_to_file is True
            assert worker._data.event_log_to_queue is True
            assert worker._data.event_log_queue_name == "qname-events"
            assert worker._data.event_log_file == "temp_log.json"


def test_worker_init_with_verbose_logging(mock_worker_function) -> None:
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
                assert mock_exit.call_count >= 1


def test_worker_init_simulate_spot_termination_without_delay(mock_worker_function, caplog):
    """Test Worker initialization with simulate spot termination but no delay."""
    with patch("sys.argv", ["worker.py", "--simulate-spot-termination-after", "10"]):
        with patch.dict(
            os.environ, {"RMS_CLOUD_TASKS_PROVIDER": "AWS", "RMS_CLOUD_TASKS_JOB_ID": "test-job"}
        ):
            _ = Worker(mock_worker_function)
            assert "Simulating spot termination after but no delay specified" in caplog.text


def test_worker_data_properties():
    """Test WorkerData properties."""
    from cloud_tasks.worker.worker import WorkerData

    data = WorkerData()
    data.termination_event = multiprocessing.Event()
    data.shutdown_event = multiprocessing.Event()

    assert not data.received_termination_notice
    data.termination_event.set()
    assert data.received_termination_notice

    assert not data.received_shutdown_request
    data.shutdown_event.set()
    assert data.received_shutdown_request


def test_worker_is_spot_property(mock_worker_function):
    """Test _is_spot property."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)

        worker._data.is_spot = True
        assert worker._is_spot is True

        worker._data.is_spot = False
        worker._data.simulate_spot_termination_after = 10.0
        assert worker._is_spot is True

        worker._data.is_spot = False
        worker._data.simulate_spot_termination_after = None
        assert worker._is_spot is False


@pytest.mark.parametrize(
    "env_value,expected",
    [
        ("true", True),
        ("1", True),
        ("TRUE", True),
        ("True", True),
        ("false", False),
        ("0", False),
        ("yes", False),
        ("", False),
    ],
)
def test_worker_init_event_log_to_file_environment_variable_conversion(
    mock_worker_function, env_value: str, expected: bool
) -> None:
    """Test that event_log_to_file env var is converted from string to boolean."""
    with patch("sys.argv", ["worker.py"]):
        with patch.dict(
            os.environ,
            {
                "RMS_CLOUD_TASKS_PROVIDER": "AWS",
                "RMS_CLOUD_TASKS_JOB_ID": "test-job",
                "RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE": env_value,
            },
        ):
            worker = Worker(mock_worker_function)
            assert worker._data.event_log_to_file is expected


def test_worker_init_event_log_to_file_env_unset(mock_worker_function) -> None:
    """Test that event_log_to_file is False when env var is not set."""
    with patch("sys.argv", ["worker.py"]):
        with patch.dict(
            os.environ,
            {"RMS_CLOUD_TASKS_PROVIDER": "AWS", "RMS_CLOUD_TASKS_JOB_ID": "test-job"},
            clear=True,
        ):
            worker = Worker(mock_worker_function)
            assert worker._data.event_log_to_file is False
