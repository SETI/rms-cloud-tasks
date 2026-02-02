"""Tests for cloud_tasks.cli.EventMonitor and run_event_monitoring_loop."""

import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from cloud_tasks.cli import EventMonitor, run_event_monitoring_loop
from cloud_tasks.common.task_db import TaskDatabase


@pytest.mark.asyncio
async def test_event_monitor_process_events_batch_empty(tmp_path: Path) -> None:
    """EventMonitor.process_events_batch returns 0 when receive_messages returns empty."""
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(return_value=[])
    monitor = EventMonitor(mock_queue, task_db, print_events=False, print_summary=False)
    count = await monitor.process_events_batch()
    task_db.close()
    assert count == 0


@pytest.mark.asyncio
async def test_event_monitor_process_events_batch_with_messages(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """EventMonitor.process_events_batch processes dict and str payloads, writes file, prints."""
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    task_db.insert_task("t1", {})
    out_file = tmp_path / "events.txt"
    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(
        return_value=[
            {"data": {"task_id": "t1", "status": "completed"}},
            {"data": '{"task_id":"t2","status":"done"}'},
        ]
    )
    monitor = EventMonitor(
        mock_queue,
        task_db,
        output_file_path=str(out_file),
        print_events=True,
        print_summary=False,
    )
    await monitor.start()
    count = await monitor.process_events_batch()
    monitor.close()
    task_db.close()
    assert count == 2
    assert out_file.exists()
    file_text = out_file.read_text()
    assert "completed" in file_text
    assert "done" in file_text
    out = capsys.readouterr().out
    assert "completed" in out
    assert "done" in out


@pytest.mark.asyncio
async def test_event_monitor_process_events_batch_json_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """EventMonitor.process_events_batch logs and skips on JSONDecodeError."""
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(return_value=[{"data": "not valid json {"}])
    monitor = EventMonitor(mock_queue, task_db, print_events=False, print_summary=False)
    with caplog.at_level(logging.ERROR):
        count = await monitor.process_events_batch()
    task_db.close()
    assert count == 1
    assert any("JSON" in rec.message or "json" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_event_monitor_process_events_batch_exception(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """EventMonitor.process_events_batch logs on generic Exception in message processing."""
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(return_value=[{"data": {"task_id": "t1"}}])
    monitor = EventMonitor(mock_queue, task_db, print_events=False, print_summary=False)
    with caplog.at_level(logging.ERROR):
        with patch.object(monitor.task_db, "insert_event", side_effect=RuntimeError("db error")):
            count = await monitor.process_events_batch()
    task_db.close()
    assert count == 1
    assert any("db error" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_event_monitor_print_status_summary(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """EventMonitor.print_status_summary with force=True logs summary even when nothing changed."""
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    task_db.insert_task("t1", {})
    mock_queue = AsyncMock()
    monitor = EventMonitor(mock_queue, task_db, print_events=False, print_summary=True)
    monitor.something_changed = False
    with caplog.at_level(logging.INFO):
        monitor.print_status_summary(force=True)
    task_db.close()
    assert "Summary" in caplog.text
    assert "Total tasks" in caplog.text


@pytest.mark.asyncio
async def test_event_monitor_start_open_file_raises(tmp_path: Path) -> None:
    """EventMonitor.start exits 1 when opening output file raises."""
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    mock_queue = AsyncMock()
    monitor = EventMonitor(
        mock_queue,
        task_db,
        output_file_path="/nonexistent/invalid/path/events.txt",
        print_events=False,
        print_summary=False,
    )
    with patch("cloud_tasks.cli.open", side_effect=OSError("Permission denied")):
        with patch("cloud_tasks.cli.sys.exit") as mock_exit:
            await monitor.start()
    mock_exit.assert_called_once_with(1)
    task_db.close()


@pytest.mark.asyncio
async def test_event_monitor_close_with_file(tmp_path: Path) -> None:
    """EventMonitor.close closes output file when open."""
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    out_file = tmp_path / "out.txt"
    mock_queue = AsyncMock()
    monitor = EventMonitor(
        mock_queue,
        task_db,
        output_file_path=str(out_file),
        print_events=False,
        print_summary=False,
    )
    await monitor.start()
    assert monitor.output_file is not None
    monitor.close()
    assert monitor.output_file.closed
    task_db.close()


@pytest.mark.asyncio
async def test_run_event_monitoring_loop_stop_signal(tmp_path: Path) -> None:
    """run_event_monitoring_loop exits when stop_signal is set."""
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    task_db.insert_task("t1", {})
    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(return_value=[])
    monitor = EventMonitor(mock_queue, task_db, print_events=False, print_summary=False)
    stop_signal = asyncio.Event()
    stop_signal.set()
    await run_event_monitoring_loop(
        monitor, task_db, check_completion=False, stop_signal=stop_signal
    )
    assert mock_queue.receive_messages.call_count <= 1
    task_db.close()


@pytest.mark.asyncio
async def test_run_event_monitoring_loop_check_completion(tmp_path: Path) -> None:
    """run_event_monitoring_loop exits when check_completion and all tasks complete."""
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    task_db.insert_task("t1", {})
    call_count = 0

    async def receive_messages(*, max_count: int):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [{"data": {"task_id": "t1", "event_type": "task_completed"}}]
        return []

    mock_queue = AsyncMock()
    mock_queue.receive_messages = receive_messages
    monitor = EventMonitor(mock_queue, task_db, print_events=False, print_summary=False)
    await run_event_monitoring_loop(monitor, task_db, check_completion=True)
    completed = task_db.get_tasks_by_status("completed")
    assert any(t["task_id"] == "t1" for t in completed)
    task_db.close()


@pytest.mark.asyncio
async def test_run_event_monitoring_loop_process_events_raises(tmp_path: Path) -> None:
    """run_event_monitoring_loop catches Exception from process_events_batch and continues."""
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    task_db.insert_task("t1", {})
    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(return_value=[])
    monitor = EventMonitor(mock_queue, task_db, print_events=False, print_summary=False)
    stop_signal = asyncio.Event()
    call_count = 0

    async def process_events_that_raises():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("receive failed")
        stop_signal.set()
        return 0

    with patch.object(monitor, "process_events_batch", side_effect=process_events_that_raises):
        with patch("cloud_tasks.cli.asyncio.sleep", new_callable=AsyncMock):
            await run_event_monitoring_loop(
                monitor, task_db, check_completion=False, stop_signal=stop_signal
            )
    task_db.close()
    assert call_count >= 2
