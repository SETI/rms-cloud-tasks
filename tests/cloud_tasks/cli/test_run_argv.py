"""Tests for cloud_tasks.cli: run_argv, build_parser, dump_tasks_by_status, log_task_stats, print_final_report."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cloud_tasks.cli import (
    build_parser,
    dump_tasks_by_status,
    log_task_stats,
    print_final_report,
    run_argv,
)
from cloud_tasks.common.task_db import TaskDatabase


def test_build_parser_returns_parser() -> None:
    """build_parser returns an ArgumentParser with subparsers and expected options.

    Parameters:
        None.

    Returns:
        None. Asserts parser is not None and parses show_queue with config/provider.
    """
    parser = build_parser()
    assert parser is not None
    args = parser.parse_args(["show_queue", "--config", "/nonexistent", "--provider", "gcp"])
    assert args.command == "show_queue"
    assert args.config == "/nonexistent"
    assert args.func is not None


def test_run_argv_help_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    """run_argv with --help exits with code 0.

    Parameters:
        capsys: Pytest fixture capturing stdout/stderr.

    Returns:
        None. Asserts exit code is 0.
    """
    code = run_argv(["--help"])
    assert code == 0


def test_run_argv_no_args_exits_nonzero(capsys: pytest.CaptureFixture[str]) -> None:
    """run_argv with no arguments returns non-zero (parse error).

    Parameters:
        capsys: Pytest fixture capturing stdout/stderr.

    Returns:
        None. Asserts exit code is 2.
    """
    code = run_argv([])
    assert code == 2


def test_run_argv_invalid_config_exits_one(tmp_path: Path) -> None:
    """run_argv with invalid or missing config file returns exit code 1.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts exit code is 1 when config file is missing.
    """
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    # Missing --config with a subcommand that requires it: use invalid path so load fails
    code = run_argv(
        ["show_queue", "--config", str(tmp_path / "nonexistent.yaml"), "--provider", "gcp"]
    )
    assert code == 1


def test_run_argv_show_queue_success(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """show_queue with mocked create_queue returns 0 and prints queue depth.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.
        capsys: Pytest fixture capturing stdout/stderr.

    Returns:
        None. Asserts exit code 0 and output contains queue depth (42) and 'queue'.
    """
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_queue = AsyncMock()
    mock_queue.get_queue_depth = AsyncMock(return_value=42)
    mock_queue.receive_tasks = AsyncMock(return_value=[])

    with patch("cloud_tasks.cli.create_queue", new_callable=AsyncMock, return_value=mock_queue):
        code = run_argv(["show_queue", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    out = capsys.readouterr().out
    assert "42" in out
    assert "queue" in out.lower()


def test_run_argv_show_queue_detail_success(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """show_queue --detail with mocked queue and one message."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_queue = AsyncMock()
    mock_queue.get_queue_depth = AsyncMock(return_value=1)
    mock_queue.receive_tasks = AsyncMock(
        return_value=[{"ack_id": "a1", "task_id": "task-1", "data": {}}]
    )
    mock_queue.retry_task = AsyncMock()

    with patch("cloud_tasks.cli.create_queue", new_callable=AsyncMock, return_value=mock_queue):
        code = run_argv(
            ["show_queue", "--config", str(config_path), "--provider", "gcp", "--detail"]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "1" in out
    mock_queue.retry_task.assert_called_once()


def test_run_argv_status_success(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """status with mocked InstanceOrchestrator returns 0."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_orch = AsyncMock()
    mock_orch.get_job_instances = AsyncMock(
        return_value=(2, 4, 0.5, "2 instances, 4 vCPUs, $0.50/hr")
    )
    mock_orch._task_queue = AsyncMock()
    mock_orch._task_queue.get_queue_depth = AsyncMock(return_value=10)

    with patch("cloud_tasks.cli.InstanceOrchestrator", return_value=mock_orch):
        mock_orch.initialize = AsyncMock()
        code = run_argv(["status", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    out = capsys.readouterr().out
    assert "2 instances" in out or "instances" in out.lower()
    assert "queue" in out.lower() or "depth" in out.lower()


def test_run_argv_list_regions_success(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """list_regions with mocked create_instance_manager returns 0."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.get_available_regions = AsyncMock(
        return_value={
            "us-central1": {
                "name": "us-central1",
                "description": "Iowa",
                "zones": ["us-central1-a"],
            }
        }
    )

    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(["list_regions", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    out = capsys.readouterr().out
    assert "us-central1" in out


def test_run_argv_list_images_success(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """list_images with mocked create_instance_manager returns 0."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_available_images = AsyncMock(
        return_value=[{"name": "img-1", "family": "debian", "source": "google"}]
    )

    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(["list_images", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    out = capsys.readouterr().out
    assert any(val in out for val in ["img-1", "debian"]) or "image" in out.lower()


def test_run_argv_list_instance_types_success(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_instance_types with mocked create_instance_manager returns 0."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    instances = {"n1-standard-1": {"name": "n1-standard-1", "vcpu": 1, "mem_gb": 3.75}}
    # pricing_data: {zone: {instance_name: {boot_disk_type: price_info}}}; CLI expects many keys per price_info
    price_info = {
        "name": "n1-standard-1",
        "vcpu": 1,
        "mem_gb": 3.75,
        "zone": "us-central1-a",
        "architecture": "x86_64",
        "local_ssd_gb": 0.0,
        "boot_disk_gb": 10.0,
        "boot_disk_type": "pd-standard",
        "per_cpu_price": 0.01,
        "mem_per_gb_price": 0.001,
        "total_price": 0.05,
        "total_price_per_cpu": 0.05,
        "local_ssd_per_gb_price": 0.0,
        "boot_disk_per_gb_price": 0.0001,
        "boot_disk_iops_price": 0.0,
        "boot_disk_throughput_price": 0.0,
        "cpu_rank": 1,
        "cpu_family": "Intel",
        "description": "Test type",
    }
    pricing_data = {"us-central1-a": {"n1-standard-1": {"pd-standard": price_info}}}
    mock_im = AsyncMock()
    mock_im.get_available_instance_types = AsyncMock(return_value=instances)
    mock_im.get_instance_pricing = AsyncMock(return_value=pricing_data)

    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(["list_instance_types", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    out = capsys.readouterr().out
    assert "n1-standard-1" in out


def test_run_argv_list_running_instances_success(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_running_instances with mocked create_instance_manager returns 0."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_running_instances = AsyncMock(return_value=[])

    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            ["list_running_instances", "--config", str(config_path), "--provider", "gcp"]
        )
    assert code == 0


def test_run_argv_purge_queue_abort(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """purge_queue without --force prompts; user cancels so returns 0."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_queue = AsyncMock()
    mock_queue.get_queue_depth = AsyncMock(return_value=1)
    mock_queue.purge_queue = AsyncMock()

    with patch("cloud_tasks.cli.create_queue", new_callable=AsyncMock, return_value=mock_queue):
        with patch("builtins.input", return_value="n"):
            code = run_argv(["purge_queue", "--config", str(config_path), "--provider", "gcp"])
    # User typed 'n' so confirm != 'EMPTY test-job', operation cancelled, returns 0
    assert code == 0


def test_run_argv_purge_queue_force_success(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """purge_queue --force with mocked queues returns 0."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_task_queue = AsyncMock()
    mock_task_queue.get_queue_depth = AsyncMock(return_value=0)
    mock_task_queue.purge_queue = AsyncMock()
    mock_event_queue = AsyncMock()
    mock_event_queue.get_queue_depth = AsyncMock(return_value=0)
    mock_event_queue.purge_queue = AsyncMock()

    async def create_queue_side_effect(config, queue_name=None):
        if queue_name and "event" in queue_name.lower():
            return mock_event_queue
        return mock_task_queue

    with patch(
        "cloud_tasks.cli.create_queue", new_callable=AsyncMock, side_effect=create_queue_side_effect
    ):
        code = run_argv(
            ["purge_queue", "--config", str(config_path), "--provider", "gcp", "--force"]
        )
    assert code == 0
    mock_task_queue.purge_queue.assert_called_once()
    mock_event_queue.purge_queue.assert_called_once()


def test_run_argv_delete_queue_force_success(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """delete_queue --force with mocked queues returns 0."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_task_queue = AsyncMock()
    mock_task_queue.delete_queue = AsyncMock()
    mock_event_queue = AsyncMock()
    mock_event_queue.delete_queue = AsyncMock()

    async def create_queue_side_effect(config, queue_name=None):
        if queue_name and "event" in queue_name.lower():
            return mock_event_queue
        return mock_task_queue

    with patch(
        "cloud_tasks.cli.create_queue", new_callable=AsyncMock, side_effect=create_queue_side_effect
    ):
        code = run_argv(
            ["delete_queue", "--config", str(config_path), "--provider", "gcp", "--force"]
        )
    assert code == 0


def test_run_argv_stop_success(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """stop with mocked InstanceOrchestrator returns 0."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_orch = AsyncMock()
    mock_orch.job_id = "test-job"
    mock_orch.initialize = AsyncMock()
    mock_orch.stop = AsyncMock()
    mock_orch.task_queue = AsyncMock()
    mock_orch.task_queue.purge_queue = AsyncMock()

    with patch("cloud_tasks.cli.InstanceOrchestrator", return_value=mock_orch):
        code = run_argv(["stop", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    mock_orch.stop.assert_called_once()


def test_run_argv_load_queue_no_task_file_exits_one(tmp_path: Path) -> None:
    """load_queue without --task-file and without --continue exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    code = run_argv(["load_queue", "--config", str(config_path), "--provider", "gcp"])
    assert code == 1


def test_run_argv_load_queue_continue_no_db_exits_one(tmp_path: Path) -> None:
    """load_queue --continue when db file does not exist exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    code = run_argv(
        [
            "load_queue",
            "--config",
            str(config_path),
            "--provider",
            "gcp",
            "--continue",
            "--db-file",
            str(tmp_path / "nonexistent.db"),
        ]
    )
    assert code == 1


def test_run_argv_monitor_event_queue_no_db_exits_one(tmp_path: Path) -> None:
    """monitor_event_queue when database file does not exist exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    code = run_argv(
        [
            "monitor_event_queue",
            "--config",
            str(config_path),
            "--provider",
            "gcp",
            "--db-file",
            str(tmp_path / "nonexistent.db"),
        ]
    )
    assert code == 1


def test_run_argv_run_dry_run(tmp_path: Path) -> None:
    """run --dry-run skips queue load and returns 0."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    task_file = tmp_path / "tasks.json"
    task_file.write_text('[{"task_id": "t1", "data": {}}]')
    code = run_argv(
        [
            "run",
            "--config",
            str(config_path),
            "--provider",
            "gcp",
            "--task-file",
            str(task_file),
            "--dry-run",
        ]
    )
    assert code == 0


def test_run_argv_run_no_task_file_exits_one(tmp_path: Path) -> None:
    """run without --task-file and without --continue exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    code = run_argv(["run", "--config", str(config_path), "--provider", "gcp"])
    assert code == 1


# --- show_queue error and edge paths ---


def test_run_argv_show_queue_create_queue_raises_exits_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """show_queue when create_queue raises exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    with patch(
        "cloud_tasks.cli.create_queue",
        new_callable=AsyncMock,
        side_effect=RuntimeError("connection failed"),
    ):
        code = run_argv(["show_queue", "--config", str(config_path), "--provider", "gcp"])
    assert code == 1
    out = capsys.readouterr()
    assert "connection failed" in out.out


def test_run_argv_show_queue_get_queue_depth_raises_exits_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """show_queue when get_queue_depth raises exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_queue = AsyncMock()
    mock_queue.get_queue_depth = AsyncMock(side_effect=RuntimeError("permission denied"))
    with patch("cloud_tasks.cli.create_queue", new_callable=AsyncMock, return_value=mock_queue):
        code = run_argv(["show_queue", "--config", str(config_path), "--provider", "gcp"])
    assert code == 1
    out = capsys.readouterr()
    assert "permission" in out.out or "Error" in out.out


def test_run_argv_show_queue_depth_none_exits_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """show_queue when get_queue_depth returns None exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_queue = AsyncMock()
    mock_queue.get_queue_depth = AsyncMock(return_value=None)
    with patch("cloud_tasks.cli.create_queue", new_callable=AsyncMock, return_value=mock_queue):
        code = run_argv(["show_queue", "--config", str(config_path), "--provider", "gcp"])
    assert code == 1
    out = capsys.readouterr().out
    assert "Failed to get queue depth" in out


def test_run_argv_show_queue_empty_queue(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """show_queue when queue depth is 0 prints empty message."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_queue = AsyncMock()
    mock_queue.get_queue_depth = AsyncMock(return_value=0)
    with patch("cloud_tasks.cli.create_queue", new_callable=AsyncMock, return_value=mock_queue):
        code = run_argv(["show_queue", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    out = capsys.readouterr().out
    assert "Queue depth: 0" in out
    assert "empty" in out.lower() or "No messages" in out


def test_run_argv_show_queue_detail_empty_messages(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """show_queue --detail when receive_tasks returns empty prints fallback message."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_queue = AsyncMock()
    mock_queue.get_queue_depth = AsyncMock(return_value=1)
    mock_queue.receive_tasks = AsyncMock(return_value=[])
    with patch("cloud_tasks.cli.create_queue", new_callable=AsyncMock, return_value=mock_queue):
        code = run_argv(
            ["show_queue", "--config", str(config_path), "--provider", "gcp", "--detail"]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "Could not retrieve" in out


def test_run_argv_show_queue_detail_receipt_handle_aws(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """show_queue --detail with message containing receipt_handle (AWS-style) prints it."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_queue = AsyncMock()
    mock_queue.get_queue_depth = AsyncMock(return_value=1)
    mock_queue.receive_tasks = AsyncMock(
        return_value=[
            {
                "ack_id": "a1",
                "receipt_handle": "x" * 60,
                "task_id": "t1",
                "data": {"k": "v"},
            }
        ]
    )
    mock_queue.retry_task = AsyncMock()
    with patch("cloud_tasks.cli.create_queue", new_callable=AsyncMock, return_value=mock_queue):
        code = run_argv(
            ["show_queue", "--config", str(config_path), "--provider", "gcp", "--detail"]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "Receipt Handle" in out or "..." in out


def test_run_argv_show_queue_detail_lock_token_azure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """show_queue --detail with message containing lock_token (Azure-style) and ack_id for retry."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_queue = AsyncMock()
    mock_queue.get_queue_depth = AsyncMock(return_value=1)
    mock_queue.receive_tasks = AsyncMock(
        return_value=[
            {
                "ack_id": "a1",
                "lock_token": "lock-" + "x" * 60,
                "task_id": "t1",
                "data": {"k": "v"},
            }
        ]
    )
    mock_queue.retry_task = AsyncMock()
    with patch("cloud_tasks.cli.create_queue", new_callable=AsyncMock, return_value=mock_queue):
        code = run_argv(
            ["show_queue", "--config", str(config_path), "--provider", "gcp", "--detail"]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "Task ID" in out
    assert "t1" in out
    assert "Data:" in out


def test_run_argv_show_queue_detail_data_not_dict(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """show_queue --detail with message data not a dict still prints (non-dict branch)."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_queue = AsyncMock()
    mock_queue.get_queue_depth = AsyncMock(return_value=1)
    mock_queue.receive_tasks = AsyncMock(
        return_value=[{"ack_id": "a1", "task_id": "t1", "data": "raw string"}]
    )
    mock_queue.retry_task = AsyncMock()
    with patch("cloud_tasks.cli.create_queue", new_callable=AsyncMock, return_value=mock_queue):
        code = run_argv(
            ["show_queue", "--config", str(config_path), "--provider", "gcp", "--detail"]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "Data:" in out or "raw" in out


# --- list_running_instances extra paths ---


def test_run_argv_list_running_instances_with_instances_table(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_running_instances with instances returned prints table."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_running_instances = AsyncMock(
        return_value=[
            {
                "id": "i-1",
                "type": "n1-standard-1",
                "state": "running",
                "zone": "us-central1-a",
                "creation_time": "2024-01-01",
                "job_id": "test-job",
            }
        ]
    )
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            ["list_running_instances", "--config", str(config_path), "--provider", "gcp"]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "i-1" in out
    assert "n1-standard-1" in out
    assert "Summary" in out
    assert "1 total" in out


def test_run_argv_list_running_instances_with_job_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_running_instances --job-id prints job filter message."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_running_instances = AsyncMock(return_value=[])
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            [
                "list_running_instances",
                "--config",
                str(config_path),
                "--provider",
                "gcp",
                "--job-id",
                "my-job",
            ]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "my-job" in out
    assert "No instances found" in out


def test_run_argv_list_running_instances_invalid_sort_exits_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_running_instances with invalid --sort-by exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_running_instances = AsyncMock(
        return_value=[
            {
                "id": "i-1",
                "type": "t",
                "state": "running",
                "zone": "z",
                "creation_time": "t",
                "job_id": "j",
            }
        ]
    )
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            [
                "list_running_instances",
                "--config",
                str(config_path),
                "--provider",
                "gcp",
                "--sort-by",
                "invalid_field",
            ]
        )
    assert code == 1
    out = capsys.readouterr().out
    assert "Invalid sort field" in out


def test_run_argv_list_running_instances_detail(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_running_instances --detail prints per-instance details."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_running_instances = AsyncMock(
        return_value=[
            {
                "id": "i-1",
                "type": "n1-standard-1",
                "state": "running",
                "zone": "us-central1-a",
                "creation_time": "2024-01-01",
                "job_id": "test-job",
                "private_ip": "10.0.0.1",
                "public_ip": "1.2.3.4",
            }
        ]
    )
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            [
                "list_running_instances",
                "--config",
                str(config_path),
                "--provider",
                "gcp",
                "--detail",
            ]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "Instance ID" in out or "i-1" in out
    assert "10.0.0.1" in out or "1.2.3.4" in out


def test_run_argv_list_running_instances_raises_exits_one(tmp_path: Path) -> None:
    """list_running_instances when create_instance_manager/list_running_instances raises exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_running_instances = AsyncMock(side_effect=RuntimeError("api error"))
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            ["list_running_instances", "--config", str(config_path), "--provider", "gcp"]
        )
    assert code == 1


# --- list_regions extra paths ---


def test_run_argv_list_regions_empty(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """list_regions when no regions returned prints No regions found."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.get_available_regions = AsyncMock(return_value={})
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(["list_regions", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    out = capsys.readouterr().out
    assert "No regions found" in out


def test_run_argv_list_regions_with_prefix(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_regions --prefix prints filtered count."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.get_available_regions = AsyncMock(
        return_value={"us-central1": {"name": "us-central1", "description": "Iowa", "zones": []}}
    )
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            ["list_regions", "--config", str(config_path), "--provider", "gcp", "--prefix", "us-"]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "us-central1" in out or "Found" in out


# --- list_images extra paths ---


def test_run_argv_list_images_empty(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """list_images when no images returned prints No images found."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_available_images = AsyncMock(return_value=[])
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(["list_images", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    out = capsys.readouterr().out
    assert "No images found" in out


def test_run_argv_list_images_with_filter(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_images --filter filters by text."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_available_images = AsyncMock(
        return_value=[
            {"name": "debian-11", "family": "debian", "source": "google"},
            {"name": "ubuntu-22", "family": "ubuntu", "source": "google"},
        ]
    )
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            ["list_images", "--config", str(config_path), "--provider", "gcp", "--filter", "debian"]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "debian" in out


def test_run_argv_list_images_invalid_sort_exits_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_images with invalid --sort-by exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_available_images = AsyncMock(
        return_value=[{"name": "img1", "family": "f", "source": "google"}]
    )
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            [
                "list_images",
                "--config",
                str(config_path),
                "--provider",
                "gcp",
                "--sort-by",
                "invalid_field",
            ]
        )
    assert code == 1
    out = capsys.readouterr().out
    assert "Invalid sort field" in out


def test_run_argv_list_images_with_detail(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_images --detail prints table with detail columns."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.list_available_images = AsyncMock(
        return_value=[
            {
                "name": "debian-11",
                "family": "debian",
                "source": "google",
                "creation_date": "2024-01-01",
            }
        ]
    )
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            ["list_images", "--config", str(config_path), "--provider", "gcp", "--detail"]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "debian" in out


def test_run_argv_list_regions_with_zones(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_regions --zones shows zones in table."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.get_available_regions = AsyncMock(
        return_value={
            "us-central1": {
                "name": "us-central1",
                "description": "Iowa",
                "zones": ["us-central1-a", "us-central1-b"],
            }
        }
    )
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(
            ["list_regions", "--config", str(config_path), "--provider", "gcp", "--zones"]
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "us-central1" in out
    assert "Zones" in out or "zone" in out.lower()


# --- list_instance_types extra paths ---


def test_run_argv_list_instance_types_empty(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """list_instance_types when no instance types returned prints No instance types found."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_im = AsyncMock()
    mock_im.get_available_instance_types = AsyncMock(return_value={})
    mock_im.get_instance_pricing = AsyncMock(return_value={})
    with patch(
        "cloud_tasks.cli.create_instance_manager", new_callable=AsyncMock, return_value=mock_im
    ):
        code = run_argv(["list_instance_types", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    out = capsys.readouterr().out
    assert "No instance types found" in out


# --- status and stop error paths ---


def test_run_argv_status_queue_depth_none(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """status when get_queue_depth returns None prints failure message."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_orch = AsyncMock()
    mock_orch.get_job_instances = AsyncMock(return_value=(0, 0, 0.0, "0 instances"))
    mock_orch.get_queue_depth = AsyncMock(return_value=None)
    with patch("cloud_tasks.cli.InstanceOrchestrator", return_value=mock_orch):
        mock_orch.initialize = AsyncMock()
        code = run_argv(["status", "--config", str(config_path), "--provider", "gcp"])
    assert code == 0
    out = capsys.readouterr().out
    assert "queue depth" in out.lower() or "not initialized" in out.lower()


def test_run_argv_status_raises_exits_one(tmp_path: Path) -> None:
    """status when orchestrator.initialize or get_job_instances raises exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    with patch("cloud_tasks.cli.InstanceOrchestrator") as mock_class:
        mock_orch = MagicMock()
        mock_orch.initialize = AsyncMock(side_effect=RuntimeError("init failed"))
        mock_class.return_value = mock_orch
        code = run_argv(["status", "--config", str(config_path), "--provider", "gcp"])
    assert code == 1


def test_run_argv_stop_with_purge_queue(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """stop --purge-queue purges queue after stopping."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    mock_orch = AsyncMock()
    mock_orch.job_id = "test-job"
    mock_orch.queue_name = "test-job"
    mock_orch.initialize = AsyncMock()
    mock_orch.stop = AsyncMock()
    mock_task_queue = AsyncMock()
    mock_task_queue.purge_queue = AsyncMock()
    mock_orch.task_queue = mock_task_queue
    mock_orch._task_queue = mock_task_queue  # CLI uses _task_queue for purge
    with patch("cloud_tasks.cli.InstanceOrchestrator", return_value=mock_orch):
        code = run_argv(
            ["stop", "--config", str(config_path), "--provider", "gcp", "--purge-queue"]
        )
    assert code == 0
    mock_task_queue.purge_queue.assert_called_once()


def test_run_argv_stop_raises_exits_one(tmp_path: Path) -> None:
    """stop when orchestrator.initialize or stop raises exits 1."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("provider: gcp\ngcp:\n  job_id: test-job\n  project_id: test-project\n")
    with patch("cloud_tasks.cli.InstanceOrchestrator") as mock_class:
        mock_orch = MagicMock()
        mock_orch.initialize = AsyncMock(side_effect=RuntimeError("init failed"))
        mock_class.return_value = mock_orch
        code = run_argv(["stop", "--config", str(config_path), "--provider", "gcp"])
    assert code == 1


# --- CLI helper functions (dump_tasks_by_status, log_task_stats, print_final_report) ---


def test_dump_tasks_by_status_empty_db(tmp_path: Path) -> None:
    """dump_tasks_by_status with no tasks does not write files."""
    db_path = tmp_path / "test.db"
    task_db = TaskDatabase(str(db_path))
    dump_tasks_by_status(task_db, str(tmp_path / "out"))
    task_db.close()
    assert list(tmp_path.glob("*.json")) == []


def test_dump_tasks_by_status_writes_files(tmp_path: Path) -> None:
    """dump_tasks_by_status writes one JSON file per status."""
    db_path = tmp_path / "test.db"
    task_db = TaskDatabase(str(db_path))
    task_db.insert_task("t1", {"x": 1})
    task_db.insert_task("t2", {"x": 2})
    task_db.update_task_enqueued("t1")
    task_db.update_task_from_event(
        {
            "event_type": "task_completed",
            "task_id": "t1",
            "timestamp": "2025-01-01T12:00:00Z",
            "result": {},
        }
    )
    dump_tasks_by_status(task_db, str(tmp_path / "out"))
    task_db.close()
    completed_file = tmp_path / "out_completed.json"
    pending_file = tmp_path / "out_pending.json"
    assert completed_file.exists()
    content = completed_file.read_text()
    assert "t1" in content
    assert pending_file.exists()
    pending_content = pending_file.read_text()
    assert "t2" in pending_content


def test_log_task_stats_smoke(tmp_path: Path) -> None:
    """log_task_stats runs without error."""
    db_path = tmp_path / "test.db"
    task_db = TaskDatabase(str(db_path))
    task_db.insert_task("t1", {"x": 1})
    log_task_stats(task_db, header="Test summary:", include_remaining_ids=True)
    task_db.close()


def test_print_final_report_smoke(tmp_path: Path) -> None:
    """print_final_report runs without error."""
    db_path = tmp_path / "test.db"
    task_db = TaskDatabase(str(db_path))
    task_db.insert_task("t1", {"x": 1})
    print_final_report(task_db)
    task_db.close()
