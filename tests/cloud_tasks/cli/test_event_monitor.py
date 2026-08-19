"""Tests for cloud_tasks.cli.EventMonitor and run_event_monitoring_loop.

This module exercises EventMonitor.process_events_batch, start, close,
print_status_summary, and run_event_monitoring_loop. Common fixtures used:
tmp_path (Path): Temporary filesystem path for DB and output files.
capsys (pytest.CaptureFixture[str]): Captured stdout/stderr.
caplog (pytest.LogCaptureFixture): Captured log records.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

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


def _stored_event(task_db: TaskDatabase, task_id: str, event_type: str = "task_completed") -> dict:
    """Insert one event into the database and return it."""
    event = {
        "timestamp": "2026-08-18T12:00:00+00:00",
        "hostname": "host-1",
        "event_type": event_type,
        "task_id": task_id,
        "retry": False,
        "elapsed_time": 1.5,
    }
    task_db.insert_event(event)
    return event


def test_iter_raw_events_streams_in_insertion_order(tmp_path: Path) -> None:
    """iter_raw_events yields each stored event's raw JSON, oldest first."""
    task_db = TaskDatabase(str(tmp_path / "events.db"))
    for task_id in ("t1", "t2", "t3"):
        _stored_event(task_db, task_id)

    raw = list(task_db.iter_raw_events())
    task_db.close()

    assert [json.loads(line)["task_id"] for line in raw] == ["t1", "t2", "t3"]


def test_iter_raw_events_empty_database(tmp_path: Path) -> None:
    """iter_raw_events yields nothing when no events have been recorded."""
    task_db = TaskDatabase(str(tmp_path / "events.db"))
    assert list(task_db.iter_raw_events()) == []
    task_db.close()


@pytest.mark.asyncio
async def test_start_backfills_new_output_file(tmp_path: Path, caplog) -> None:
    """A new output file is seeded with the events already in the database."""
    task_db = TaskDatabase(str(tmp_path / "events.db"))
    for task_id in ("t1", "t2"):
        _stored_event(task_db, task_id)
    out_file = tmp_path / "events.jsonl"

    monitor = EventMonitor(
        AsyncMock(),
        task_db,
        output_file_path=str(out_file),
        print_events=False,
        print_summary=False,
        backfill_output_file=True,
    )
    with caplog.at_level(logging.INFO, logger="cloud_tasks.cli"):
        await monitor.start()
    monitor.close()
    task_db.close()

    lines = out_file.read_text().splitlines()
    assert [json.loads(line)["task_id"] for line in lines] == ["t1", "t2"]
    assert "Wrote 2 events already in the database" in caplog.text


@pytest.mark.asyncio
async def test_start_does_not_backfill_existing_output_file(tmp_path: Path) -> None:
    """An output file that already exists is appended to, not re-seeded.

    Its contents are presumed to already cover the events in the database, so
    re-writing them would duplicate every line on each resume.
    """
    task_db = TaskDatabase(str(tmp_path / "events.db"))
    _stored_event(task_db, "t1")
    out_file = tmp_path / "events.jsonl"
    out_file.write_text('{"event_type": "pre-existing"}\n')

    monitor = EventMonitor(
        AsyncMock(),
        task_db,
        output_file_path=str(out_file),
        print_events=False,
        print_summary=False,
        backfill_output_file=True,
    )
    await monitor.start()
    monitor.close()
    task_db.close()

    assert out_file.read_text() == '{"event_type": "pre-existing"}\n'


@pytest.mark.asyncio
async def test_start_does_not_backfill_when_disabled(tmp_path: Path) -> None:
    """A fresh run leaves the output file empty even though the database has events."""
    task_db = TaskDatabase(str(tmp_path / "events.db"))
    _stored_event(task_db, "t1")
    out_file = tmp_path / "events.jsonl"

    monitor = EventMonitor(
        AsyncMock(),
        task_db,
        output_file_path=str(out_file),
        print_events=False,
        print_summary=False,
    )
    await monitor.start()
    monitor.close()
    task_db.close()

    assert out_file.read_text() == ""


@pytest.mark.asyncio
async def test_backfilled_events_are_followed_by_live_events(tmp_path: Path) -> None:
    """Events received after the backfill append to it, giving one continuous log."""
    task_db = TaskDatabase(str(tmp_path / "events.db"))
    task_db.insert_task("t2", {})
    _stored_event(task_db, "t1")
    out_file = tmp_path / "events.jsonl"

    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(
        return_value=[{"data": {"event_type": "task_completed", "task_id": "t2", "retry": False}}]
    )
    monitor = EventMonitor(
        mock_queue,
        task_db,
        output_file_path=str(out_file),
        print_events=False,
        print_summary=False,
        backfill_output_file=True,
    )
    await monitor.start()
    await monitor.process_events_batch()
    monitor.close()
    task_db.close()

    lines = out_file.read_text().splitlines()
    assert [json.loads(line)["task_id"] for line in lines] == ["t1", "t2"]


@pytest.mark.asyncio
async def test_backfill_write_failure_drops_the_output_file(tmp_path: Path, caplog) -> None:
    """An output file that can't be written during the backfill is dropped.

    The job may already be running, so refusing to monitor it would be worse than losing
    its log file. The stream is disabled rather than kept, because a stream that has
    already failed would keep failing on every live event.
    """
    task_db = TaskDatabase(str(tmp_path / "events.db"))
    task_db.insert_task("t2", {})
    _stored_event(task_db, "t1")
    out_file = tmp_path / "events.jsonl"

    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(
        return_value=[{"data": {"event_type": "task_completed", "task_id": "t2", "retry": False}}]
    )
    monitor = EventMonitor(
        mock_queue,
        task_db,
        output_file_path=str(out_file),
        print_events=False,
        print_summary=False,
        backfill_output_file=True,
    )
    real_open = open

    def open_with_failing_write(path: str, mode: str) -> Any:
        """Open the file for real but make writing to it fail."""
        handle = real_open(path, mode)
        handle.write = MagicMock(side_effect=OSError("No space left on device"))
        return handle

    with patch("cloud_tasks.cli.open", side_effect=open_with_failing_write):
        with caplog.at_level(logging.ERROR, logger="cloud_tasks.cli"):
            await monitor.start()

    assert monitor.output_file is None
    assert "No space left on device" in caplog.text

    # Monitoring still works and the database is still updated
    assert await monitor.process_events_batch() == 1
    monitor.close()
    counts = task_db.get_task_counts()
    task_db.close()
    assert counts == {"completed": 1}


@pytest.mark.asyncio
async def test_backfill_database_failure_keeps_the_output_file(tmp_path: Path, caplog) -> None:
    """A database read failure during the backfill says nothing about the output file.

    The file is left open so live events are still logged to it, and the history it is
    missing is reported rather than silently dropped.
    """
    task_db = TaskDatabase(str(tmp_path / "events.db"))
    task_db.insert_task("t2", {})
    _stored_event(task_db, "t1")
    out_file = tmp_path / "events.jsonl"

    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(
        return_value=[{"data": {"event_type": "task_completed", "task_id": "t2", "retry": False}}]
    )
    monitor = EventMonitor(
        mock_queue,
        task_db,
        output_file_path=str(out_file),
        print_events=False,
        print_summary=False,
        backfill_output_file=True,
    )
    with patch.object(task_db, "iter_raw_events", side_effect=RuntimeError("db gone")):
        with caplog.at_level(logging.ERROR, logger="cloud_tasks.cli"):
            await monitor.start()

    assert monitor.output_file is not None
    assert "Error reading stored events from the database" in caplog.text

    assert await monitor.process_events_batch() == 1
    monitor.close()
    task_db.close()
    # The backfilled history is missing, but live events were still logged
    lines = out_file.read_text().splitlines()
    assert [json.loads(line)["task_id"] for line in lines] == ["t2"]


@pytest.mark.asyncio
async def test_output_file_write_failure_does_not_block_database(tmp_path: Path, caplog) -> None:
    """A broken output stream is dropped and the database is still updated.

    The database is the authoritative record. If a write error on the optional log file
    aborted event processing, a full disk would silently stop all task tracking.
    """
    task_db = TaskDatabase(str(tmp_path / "events.db"))
    task_db.insert_task("t1", {})
    task_db.insert_task("t2", {})
    out_file = tmp_path / "events.jsonl"

    mock_queue = AsyncMock()
    mock_queue.receive_messages = AsyncMock(
        return_value=[{"data": {"event_type": "task_completed", "task_id": "t1", "retry": False}}]
    )
    monitor = EventMonitor(
        mock_queue,
        task_db,
        output_file_path=str(out_file),
        print_events=False,
        print_summary=False,
    )
    await monitor.start()
    assert monitor.output_file is not None
    monitor.output_file.write = MagicMock(side_effect=OSError("No space left on device"))

    with caplog.at_level(logging.ERROR, logger="cloud_tasks.cli"):
        assert await monitor.process_events_batch() == 1

    assert monitor.output_file is None
    assert "No space left on device" in caplog.text
    assert task_db.get_task_counts() == {"completed": 1, "pending": 1}

    # A later event is still recorded even though the file is gone
    mock_queue.receive_messages = AsyncMock(
        return_value=[{"data": {"event_type": "task_completed", "task_id": "t2", "retry": False}}]
    )
    assert await monitor.process_events_batch() == 1
    counts = task_db.get_task_counts()
    monitor.close()
    task_db.close()
    assert counts == {"completed": 2}


@pytest.mark.asyncio
async def test_start_logs_nothing_when_database_has_no_events(tmp_path: Path, caplog) -> None:
    """Backfilling an empty database creates the file without a misleading log line."""
    task_db = TaskDatabase(str(tmp_path / "events.db"))
    out_file = tmp_path / "events.jsonl"

    monitor = EventMonitor(
        AsyncMock(),
        task_db,
        output_file_path=str(out_file),
        print_events=False,
        print_summary=False,
        backfill_output_file=True,
    )
    with caplog.at_level(logging.INFO, logger="cloud_tasks.cli"):
        await monitor.start()
    monitor.close()
    task_db.close()

    assert out_file.exists()
    assert "already in the database" not in caplog.text
