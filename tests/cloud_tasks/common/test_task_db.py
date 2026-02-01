"""Tests for cloud_tasks.common.task_db."""

from pathlib import Path

import pytest

from cloud_tasks.common.task_db import TaskDatabase


def test_task_database_init_creates_tables(tmp_path: Path) -> None:
    """TaskDatabase creates DB file and tables on init."""
    db_path = tmp_path / "test.db"
    db = TaskDatabase(str(db_path))
    db.insert_task("t1", {"k": "v"})
    counts = db.get_task_counts()
    assert counts.get("pending", 0) == 1
    db.close()


def test_insert_task_and_get_task_counts(tmp_path: Path) -> None:
    """insert_task adds task; get_task_counts reflects status."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("task-1", {"a": 1})
    db.insert_task("task-2", {"b": 2}, status="pending")
    counts = db.get_task_counts()
    assert counts["pending"] == 2
    db.close()


def test_update_task_enqueued(tmp_path: Path) -> None:
    """update_task_enqueued sets status and started_at."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    db.update_task_enqueued("t1")
    tasks = db.get_tasks_by_status("in_queue_original")
    assert len(tasks) == 1
    assert tasks[0]["task_id"] == "t1"
    assert tasks[0]["started_at"] is not None
    db.close()


def test_update_task_from_event_no_task_id(tmp_path: Path) -> None:
    """update_task_from_event with no task_id returns without updating."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    db.update_task_from_event({"event_type": "spot_termination"})
    counts = db.get_task_counts()
    assert counts.get("pending", 0) == 1
    db.close()


def test_update_task_from_event_task_completed(tmp_path: Path) -> None:
    """update_task_from_event with task_completed sets completed status."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    db.update_task_enqueued("t1")
    db.update_task_from_event(
        {
            "event_type": "task_completed",
            "task_id": "t1",
            "timestamp": "2025-01-01T12:00:00Z",
            "result": {"x": 1},
        }
    )
    tasks = db.get_tasks_by_status("completed")
    assert len(tasks) == 1
    assert tasks[0]["task_id"] == "t1"
    db.close()


def test_update_task_from_event_task_exception_with_retry(tmp_path: Path) -> None:
    """update_task_from_event task_exception with retry sets exception_with_retry."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    db.update_task_from_event(
        {
            "event_type": "task_exception",
            "task_id": "t1",
            "retry": True,
            "exception": "ValueError",
        }
    )
    tasks = db.get_tasks_by_status("exception_with_retry")
    assert len(tasks) == 1
    db.close()


def test_update_task_from_event_task_exception_no_retry(tmp_path: Path) -> None:
    """update_task_from_event task_exception without retry sets exception."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    db.update_task_from_event({"event_type": "task_exception", "task_id": "t1", "retry": False})
    tasks = db.get_tasks_by_status("exception")
    assert len(tasks) == 1
    db.close()


def test_update_task_from_event_task_timed_out(tmp_path: Path) -> None:
    """update_task_from_event task_timed_out sets timed_out or timed_out_with_retry."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    db.update_task_from_event({"event_type": "task_timed_out", "task_id": "t1", "retry": False})
    tasks = db.get_tasks_by_status("timed_out")
    assert len(tasks) == 1
    db.close()


def test_update_task_from_event_task_exited(tmp_path: Path) -> None:
    """update_task_from_event task_exited sets exited_without_status."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    db.update_task_from_event(
        {"event_type": "task_exited", "task_id": "t1", "retry": False, "exit_code": 1}
    )
    tasks = db.get_tasks_by_status("exited_without_status")
    assert len(tasks) == 1
    assert tasks[0].get("exit_code") == 1
    db.close()


def test_insert_event(tmp_path: Path) -> None:
    """insert_event stores event in events table."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_event(
        {
            "timestamp": "2025-01-01T12:00:00Z",
            "hostname": "host1",
            "event_type": "task_completed",
            "task_id": "t1",
        }
    )
    stats = db.get_task_statistics()
    assert "spot_terminations" in stats
    db.close()


def test_get_total_tasks(tmp_path: Path) -> None:
    """get_total_tasks returns correct count."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    assert db.get_total_tasks() == 0
    db.insert_task("t1", {})
    db.insert_task("t2", {})
    assert db.get_total_tasks() == 2
    db.close()


def test_is_all_tasks_complete(tmp_path: Path) -> None:
    """is_all_tasks_complete True when all terminal, False otherwise."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    assert db.is_all_tasks_complete() is False
    db.update_task_from_event({"event_type": "task_completed", "task_id": "t1", "result": {}})
    assert db.is_all_tasks_complete() is True
    db.close()


def test_get_tasks_by_status(tmp_path: Path) -> None:
    """get_tasks_by_status returns tasks with given status."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    db.insert_task("t2", {})
    db.update_task_enqueued("t1")
    pending = db.get_tasks_by_status("pending")
    assert len(pending) == 1
    assert pending[0]["task_id"] == "t2"
    db.close()


def test_get_task_statistics_with_elapsed_times(tmp_path: Path) -> None:
    """get_task_statistics includes percentiles when elapsed_time present."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    db.update_task_from_event(
        {
            "event_type": "task_completed",
            "task_id": "t1",
            "elapsed_time": 10.5,
            "result": {},
        }
    )
    stats = db.get_task_statistics()
    assert "time_stats" in stats
    assert "percentiles" in stats
    assert "median" in stats["percentiles"]
    db.close()


def test_get_remaining_task_ids(tmp_path: Path) -> None:
    """get_remaining_task_ids returns only non-terminal task IDs."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.insert_task("t1", {})
    db.insert_task("t2", {})
    db.update_task_from_event({"event_type": "task_completed", "task_id": "t1", "result": {}})
    remaining = db.get_remaining_task_ids()
    assert remaining == ["t2"]
    db.close()


def test_close_prevents_further_use(tmp_path: Path) -> None:
    """close() closes connection; subsequent use raises."""
    db = TaskDatabase(str(tmp_path / "test.db"))
    db.close()
    with pytest.raises(AssertionError) as exc_info:
        db.get_total_tasks()
    assert "closed" in str(exc_info.value).lower()


def test_context_manager_closes_on_exit(tmp_path: Path) -> None:
    """Context manager closes DB on exit."""
    db_path = tmp_path / "test.db"
    with TaskDatabase(str(db_path)) as db:
        db.insert_task("t1", {})
        assert db.get_total_tasks() == 1
    with pytest.raises(AssertionError) as exc_info:
        db.get_total_tasks()
    assert "closed" in str(exc_info.value).lower()
