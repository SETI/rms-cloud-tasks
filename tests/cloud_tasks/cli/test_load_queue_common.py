"""Tests for cloud_tasks.cli.load_queue_common: the fresh-run queue reset and enqueue."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cloud_tasks.cli import load_queue_common


class RecordingQueue:
    """A queue that records the order of the operations performed on it."""

    def __init__(self, name: str, log: list[tuple[str, str]]) -> None:
        """Initialize the queue.

        Parameters:
            name: Name used in the log entries, to tell the queues apart.
            log: List shared by all the queues of a test, appended to as operations happen.
        """
        self._name = name
        self._log = log

    async def get_queue_depth(self) -> int | None:
        """Record the depth check and report that the depth is unknown."""
        self._log.append((self._name, "get_queue_depth"))
        return None

    async def delete_queue(self) -> None:
        """Record the deletion."""
        self._log.append((self._name, "delete_queue"))

    async def ensure_queue_ready(self) -> None:
        """Record that the queue was made ready."""
        self._log.append((self._name, "ensure_queue_ready"))

    async def send_task(self, task_id: str, task_data: dict[str, Any]) -> None:
        """Record the enqueueing of one task."""
        self._log.append((self._name, f"send_task:{task_id}"))


@pytest.mark.asyncio
async def test_load_queue_common_readies_queues_before_enqueueing(tmp_path: Path) -> None:
    """No task is published before both queues have been deleted and made ready again.

    A task published before its queue is live is dropped by the provider without an error
    (issue #56), so the fresh-run path must delete, recreate and confirm the queues before it
    enqueues anything.
    """
    task_file = tmp_path / "tasks.json"
    task_file.write_text(
        '[{"task_id": "t1", "data": {"x": 1}}, {"task_id": "t2", "data": {"x": 2}}]'
    )
    db_file = tmp_path / "tasks.db"

    config = MagicMock()
    config.provider = "GCP"
    config.get_provider_config.return_value = MagicMock(queue_name="test-queue")

    log: list[tuple[str, str]] = []

    async def fake_create_queue(cfg: Any, queue_name: str | None = None, **kwargs: Any) -> Any:
        return RecordingQueue("events" if queue_name else "tasks", log)

    with patch("cloud_tasks.cli.create_queue", side_effect=fake_create_queue) as mock_create_queue:
        task_db, _task_queue, _events_queue, num_tasks = await load_queue_common(
            config=config,
            db_file=str(db_file),
            task_file=str(task_file),
            start_task=None,
            limit=None,
            max_concurrent_queue_operations=2,
        )
    task_db.close()

    assert num_tasks == 2

    sends = [i for i, entry in enumerate(log) if entry[1].startswith("send_task")]
    assert len(sends) == 2
    for queue in ("tasks", "events"):
        assert log.index((queue, "delete_queue")) < log.index((queue, "ensure_queue_ready"))
        assert log.index((queue, "ensure_queue_ready")) < sends[0]

    # One object per queue, so the object that deleted a queue is the one that created it
    # again: an object made after the deletion could still be told the old queue is there
    assert mock_create_queue.call_count == 2


def _fresh_run_setup(
    tmp_path: Path, depth: int | None = None
) -> tuple[Any, Path, Path, Any, list[tuple[str, str]]]:
    """Build the arguments a fresh run needs, with a database already on disk.

    Parameters:
        tmp_path: Directory to put the task file and database in.
        depth: Depth the task queue reports, or None for unknown.

    Returns:
        tuple: (config, task_file, db_file, create_queue stand-in, operation log).
    """
    task_file = tmp_path / "tasks.json"
    task_file.write_text('[{"task_id": "t1", "data": {"x": 1}}]')
    db_file = tmp_path / "tasks.db"
    db_file.write_text("an existing job's record of what has already run")

    config = MagicMock()
    config.provider = "GCP"
    config.get_provider_config.return_value = MagicMock(queue_name="test-queue")

    log: list[tuple[str, str]] = []

    class Queue(RecordingQueue):
        async def get_queue_depth(self) -> int | None:
            """Report the depth this test was set up with."""
            self._log.append((self._name, "get_queue_depth"))
            return depth

    async def fake_create_queue(cfg: Any, queue_name: str | None = None, **kwargs: Any) -> Any:
        return Queue("events" if queue_name else "tasks", log)

    return config, task_file, db_file, fake_create_queue, log


@pytest.mark.asyncio
async def test_declining_the_confirmation_leaves_the_database_alone(tmp_path: Path) -> None:
    """Saying no has to still be worth saying: nothing is deleted before the question."""
    config, task_file, db_file, fake_create_queue, _log = _fresh_run_setup(tmp_path, depth=51455)
    original = db_file.read_text()

    with (
        patch("cloud_tasks.cli.create_queue", side_effect=fake_create_queue),
        patch("sys.stdin.isatty", return_value=True),
        patch("builtins.input", return_value="no"),
        pytest.raises(SystemExit) as exit_info,
    ):
        await load_queue_common(
            config=config,
            db_file=str(db_file),
            task_file=str(task_file),
            start_task=None,
            limit=None,
            max_concurrent_queue_operations=2,
        )

    assert exit_info.value.code == 0
    assert db_file.exists()
    assert db_file.read_text() == original


@pytest.mark.asyncio
async def test_the_warning_names_the_database_and_the_queue(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The user can only weigh the answer if the question lists what is at stake."""
    config, task_file, db_file, fake_create_queue, _log = _fresh_run_setup(tmp_path, depth=51455)

    with (
        caplog.at_level("INFO"),
        patch("cloud_tasks.cli.create_queue", side_effect=fake_create_queue),
        patch("sys.stdin.isatty", return_value=True),
        patch("builtins.input", return_value="no"),
        pytest.raises(SystemExit),
    ):
        await load_queue_common(
            config=config,
            db_file=str(db_file),
            task_file=str(task_file),
            start_task=None,
            limit=None,
            max_concurrent_queue_operations=2,
        )

    warning = "\n".join(caplog.messages)
    assert str(db_file) in warning
    assert "51455" in warning
    assert "--continue" in warning


@pytest.mark.asyncio
async def test_an_existing_database_is_confirmed_even_with_an_empty_queue(tmp_path: Path) -> None:
    """The database is the thing --continue protects, whatever the queue happens to hold."""
    config, task_file, db_file, fake_create_queue, _log = _fresh_run_setup(tmp_path, depth=0)

    with (
        patch("cloud_tasks.cli.create_queue", side_effect=fake_create_queue),
        patch("sys.stdin.isatty", return_value=True),
        patch("builtins.input", return_value="no") as mock_input,
        pytest.raises(SystemExit),
    ):
        await load_queue_common(
            config=config,
            db_file=str(db_file),
            task_file=str(task_file),
            start_task=None,
            limit=None,
            max_concurrent_queue_operations=2,
        )

    mock_input.assert_called_once()
    assert db_file.exists()


@pytest.mark.asyncio
async def test_a_non_interactive_run_refuses_rather_than_deleting(tmp_path: Path) -> None:
    """With nobody there to answer, the safe reading of silence is "don't"."""
    config, task_file, db_file, fake_create_queue, _log = _fresh_run_setup(tmp_path, depth=51455)

    with (
        patch("cloud_tasks.cli.create_queue", side_effect=fake_create_queue),
        patch("sys.stdin.isatty", return_value=False),
        patch("builtins.input", side_effect=AssertionError("must not prompt")),
        pytest.raises(SystemExit) as exit_info,
    ):
        await load_queue_common(
            config=config,
            db_file=str(db_file),
            task_file=str(task_file),
            start_task=None,
            limit=None,
            max_concurrent_queue_operations=2,
        )

    assert exit_info.value.code == 1
    assert db_file.exists()


@pytest.mark.asyncio
async def test_force_deletes_without_asking(tmp_path: Path) -> None:
    """--force is the caller saying they know, so it doesn't stop for anything."""
    config, task_file, db_file, fake_create_queue, log = _fresh_run_setup(tmp_path, depth=51455)

    with (
        patch("cloud_tasks.cli.create_queue", side_effect=fake_create_queue),
        patch("builtins.input", side_effect=AssertionError("must not prompt")),
    ):
        task_db, _task_queue, _events_queue, num_tasks = await load_queue_common(
            config=config,
            db_file=str(db_file),
            task_file=str(task_file),
            start_task=None,
            limit=None,
            max_concurrent_queue_operations=2,
            force=True,
        )
    task_db.close()

    assert num_tasks == 1
    assert ("tasks", "delete_queue") in log
