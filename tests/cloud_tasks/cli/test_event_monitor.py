"""Tests for cloud_tasks.cli.EventMonitor and run_event_monitoring_loop.

This module exercises EventMonitor.process_events_batch, start, close,
print_status_summary, and run_event_monitoring_loop. Common fixtures used:
tmp_path (Path): Temporary filesystem path for DB and output files.
capsys (pytest.CaptureFixture[str]): Captured stdout/stderr.
caplog (pytest.LogCaptureFixture): Captured log records.
"""

import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from cloud_tasks.cli import EventMonitor, run_event_monitoring_loop
from cloud_tasks.common.task_db import TaskDatabase


@pytest.mark.asyncio
async def test_event_monitor_process_events_batch_empty(tmp_path: Path) -> None:
    """EventMonitor.process_events_batch returns 0 when receive_messages returns empty.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for the task DB.

    Returns:
        None. Asserts count == 0.
    """
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
    """EventMonitor.process_events_batch processes dict and str payloads, writes file, prints.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for DB and output file.
        capsys: Pytest fixture; captured stdout/stderr.

    Returns:
        None. Asserts count 2, file content, and stdout contain expected strings.
    """
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
    """EventMonitor.process_events_batch logs and skips on JSONDecodeError.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for the task DB.
        caplog: Pytest fixture; captured log records.

    Returns:
        None. Asserts count 1 and error log message.
    """
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(return_value=[{"data": "not valid json {"}])
    monitor = EventMonitor(mock_queue, task_db, print_events=False, print_summary=False)
    with caplog.at_level(logging.ERROR):
        count = await monitor.process_events_batch()
    task_db.close()
    assert count == 1
    assert any("decoding" in rec.message or "Expecting" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_event_monitor_process_events_batch_exception(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """EventMonitor.process_events_batch logs on generic Exception in message processing.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for the task DB.
        caplog: Pytest fixture; captured log records.

    Returns:
        None. Asserts count 1 and log contains 'db error'.
    """
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
    """EventMonitor.print_status_summary with force=True logs summary when nothing changed.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for the task DB.
        caplog: Pytest fixture; captured log records.

    Returns:
        None. Asserts log contains Summary and Total tasks.
    """
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
    """EventMonitor.start calls sys.exit(1) when opening output file raises.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for the task DB.

    Returns:
        None. Asserts sys.exit(1) was called.
    """
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
    """EventMonitor.close closes the output file when open.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for DB and output file.

    Returns:
        None. Asserts output_file is closed after close().
    """
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
    """run_event_monitoring_loop exits when stop_signal is set.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for the task DB.

    Returns:
        None. Asserts receive_messages call count is at most 1.
    """
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
    """run_event_monitoring_loop exits when check_completion and all tasks complete.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for the task DB.

    Returns:
        None. Asserts task t1 is in completed status.
    """
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
    """run_event_monitoring_loop catches Exception from process_events_batch and continues.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for the task DB.

    Returns:
        None. Asserts process_events_batch was called at least twice.
    """
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


@pytest.mark.asyncio
async def test_event_monitor_intercepts_keepalive_events(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Keep-alive events invoke the callback and are not printed, filed, or stored.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for DB and output file.
        capsys: Pytest fixture; captured stdout/stderr.

    Returns:
        None. Asserts callback invocation and absence of keep_alive in outputs.
    """
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    task_db.insert_task("t1", {})
    out_file = tmp_path / "events.txt"
    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(
        return_value=[
            {
                "data": {
                    "event_type": "keep_alive",
                    "instance_id": "instance-1",
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "hostname": "instance-1",
                }
            },
            {"data": {"task_id": "t1", "event_type": "task_completed", "retry": False}},
        ]
    )
    keepalives: list[tuple[str, str | None]] = []
    monitor = EventMonitor(
        mock_queue,
        task_db,
        output_file_path=str(out_file),
        print_events=True,
        print_summary=False,
        keepalive_callback=lambda instance_id, timestamp: keepalives.append(
            (instance_id, timestamp)
        ),
    )
    await monitor.start()
    count = await monitor.process_events_batch()
    monitor.close()

    assert count == 2
    assert keepalives == [("instance-1", "2026-01-01T00:00:00+00:00")]
    assert "keep_alive" not in out_file.read_text()
    assert "keep_alive" not in capsys.readouterr().out
    # The keep-alive event must not be stored in the events table
    cursor = task_db._get_conn().cursor()
    cursor.execute("SELECT COUNT(*) FROM events WHERE event_type = 'keep_alive'")
    assert cursor.fetchone()[0] == 0
    cursor.execute("SELECT COUNT(*) FROM events")
    assert cursor.fetchone()[0] == 1
    task_db.close()


@pytest.mark.asyncio
async def test_event_monitor_keepalive_without_callback(tmp_path: Path) -> None:
    """Keep-alive events are skipped harmlessly when no callback is registered.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for the task DB.

    Returns:
        None. Asserts the batch is processed without errors or DB writes.
    """
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(
        return_value=[{"data": {"event_type": "keep_alive", "instance_id": "instance-1"}}]
    )
    monitor = EventMonitor(mock_queue, task_db, print_events=False, print_summary=False)
    count = await monitor.process_events_batch()
    cursor = task_db._get_conn().cursor()
    cursor.execute("SELECT COUNT(*) FROM events")
    assert cursor.fetchone()[0] == 0
    task_db.close()
    assert count == 1


@pytest.mark.asyncio
async def test_event_monitor_keepalive_falls_back_to_hostname(tmp_path: Path) -> None:
    """Keep-alive events without an instance_id use the hostname for the callback.

    Parameters:
        tmp_path: Pytest fixture; temporary directory for the task DB.

    Returns:
        None. Asserts the callback receives the hostname.
    """
    db_path = tmp_path / "events.db"
    task_db = TaskDatabase(str(db_path))
    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(
        return_value=[{"data": {"event_type": "keep_alive", "hostname": "host-7"}}]
    )
    keepalives: list[tuple[str, str | None]] = []
    monitor = EventMonitor(
        mock_queue,
        task_db,
        print_events=False,
        print_summary=False,
        keepalive_callback=lambda instance_id, timestamp: keepalives.append(
            (instance_id, timestamp)
        ),
    )
    await monitor.process_events_batch()
    task_db.close()
    assert keepalives == [("host-7", None)]
