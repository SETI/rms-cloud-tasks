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
