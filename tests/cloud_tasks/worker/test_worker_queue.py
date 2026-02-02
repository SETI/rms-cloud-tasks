"""Tests for LocalTaskQueue and factory task queue behavior."""

import asyncio
import os
import sys
import tempfile
from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest
from filecache import FCPath

from cloud_tasks.worker.worker import LocalTaskQueue, Worker


class BreakLoopError(Exception):
    """Raised to break _feed_tasks_to_workers loop in tests."""


@pytest.fixture
def local_task_file_yaml_stream() -> Generator[str, None, None]:
    """Create a YAML file with multiple documents for testing streaming."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("- task_id: task1\n  data: {key: value1}\n")
        f.write("- task_id: task2\n  data: {key: value2}\n")
        f.write("- task_id: task3\n  data: {key: value3}\n")
    yield f.name
    os.unlink(f.name)


@pytest.mark.asyncio
async def test_local_queue_init_with_invalid_format() -> None:
    """LocalTaskQueue with invalid file format raises on receive_tasks."""
    with tempfile.NamedTemporaryFile(suffix=".txt") as f:
        f.write(b"invalid content")
        f.flush()
        queue = LocalTaskQueue(FCPath(f.name))
        with pytest.raises(ValueError, match="Unsupported file format"):
            await queue.receive_tasks(1)


@pytest.mark.asyncio
async def test_local_queue_receive_tasks_json(local_task_file_json: str) -> None:
    """LocalTaskQueue receives tasks from JSON file."""
    queue = LocalTaskQueue(FCPath(local_task_file_json))
    tasks = await queue.receive_tasks(max_count=2)
    assert len(tasks) == 2
    assert tasks[0]["task_id"] == "task1"
    assert tasks[1]["task_id"] == "task2"
    assert "ack_id" in tasks[0]
    assert "ack_id" in tasks[1]


@pytest.mark.asyncio
async def test_local_queue_receive_tasks_yaml(local_task_file_yaml: str) -> None:
    """LocalTaskQueue receives tasks from YAML file."""
    queue = LocalTaskQueue(FCPath(local_task_file_yaml))
    tasks = await queue.receive_tasks(max_count=2)
    assert len(tasks) == 2
    assert tasks[0]["task_id"] == "task3"
    assert tasks[1]["task_id"] == "task4"
    assert "ack_id" in tasks[0]
    assert "ack_id" in tasks[1]


@pytest.mark.asyncio
async def test_local_queue_acknowledge_task(local_task_file_json: str) -> None:
    """LocalTaskQueue.acknowledge_task is a no-op (does not raise)."""
    queue = LocalTaskQueue(FCPath(local_task_file_json))
    tasks_first = await queue.receive_tasks(max_count=5)
    await queue.acknowledge_task("test-ack-1")
    # Recreate queue and receive again; acknowledge is no-op so file unchanged
    queue2 = LocalTaskQueue(FCPath(local_task_file_json))
    tasks_after = await queue2.receive_tasks(max_count=5)
    assert len(tasks_after) == len(tasks_first)


@pytest.mark.asyncio
async def test_local_queue_retry_task(local_task_file_json: str) -> None:
    """LocalTaskQueue.retry_task is a no-op (does not raise)."""
    queue = LocalTaskQueue(FCPath(local_task_file_json))
    tasks_first = await queue.receive_tasks(max_count=5)
    await queue.retry_task("test-ack-1")
    # Recreate queue and receive again; retry is no-op so file unchanged
    queue2 = LocalTaskQueue(FCPath(local_task_file_json))
    tasks_after = await queue2.receive_tasks(max_count=5)
    assert len(tasks_after) == len(tasks_first)


@pytest.mark.asyncio
async def test_local_queue_receive_all_tasks(local_task_file_json: str) -> None:
    """LocalTaskQueue.receive_tasks returns all tasks when max_count is larger than available."""
    queue = LocalTaskQueue(FCPath(local_task_file_json))
    tasks = await queue.receive_tasks(max_count=5)
    assert len(tasks) == 2
    task_ids = {task["task_id"] for task in tasks}
    assert task_ids == {"task1", "task2"}
    for task in tasks:
        assert "ack_id" in task


@pytest.mark.asyncio
async def test_local_queue_yaml_streaming(local_task_file_yaml_stream: str) -> None:
    """LocalTaskQueue with YAML streaming yields all documents."""
    queue = LocalTaskQueue(FCPath(local_task_file_yaml_stream))
    tasks = await queue.receive_tasks(max_count=3)
    tasks = [t for t in tasks if t is not None]
    assert len(tasks) == 3
    assert tasks[0]["task_id"] == "task1"
    assert tasks[1]["task_id"] == "task2"
    assert tasks[2]["task_id"] == "task3"


@pytest.mark.asyncio
async def test_local_queue_invalid_file_format() -> None:
    """LocalTaskQueue with invalid file format raises on receive_tasks."""
    with tempfile.NamedTemporaryFile(suffix=".txt") as f:
        f.write(b"invalid content")
        f.flush()
        with pytest.raises(ValueError, match="Unsupported file format"):
            queue = LocalTaskQueue(FCPath(f.name))
            await queue.receive_tasks(1)


def test_local_queue_init_with_invalid_task_source() -> None:
    """LocalTaskQueue.__init__ with invalid task_source type raises TypeError."""
    with pytest.raises(TypeError, match="task_source must be FCPath or callable, got int"):
        LocalTaskQueue(123)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_factory_queue_receive_tasks(mock_task_factory):
    """LocalTaskQueue with factory yields tasks from factory."""
    queue = LocalTaskQueue(mock_task_factory)
    tasks = await queue.receive_tasks(max_count=2)
    assert len(tasks) == 2
    assert tasks[0]["task_id"] == "factory-task-1"
    assert tasks[1]["task_id"] == "factory-task-2"
    assert "ack_id" in tasks[0]
    assert "ack_id" in tasks[1]
    tasks = await queue.receive_tasks(max_count=2)
    assert len(tasks) == 1
    assert tasks[0]["task_id"] == "factory-task-3"
    tasks = await queue.receive_tasks(max_count=2)
    assert len(tasks) == 0


@pytest.mark.asyncio
async def test_factory_queue_receive_tasks_empty_factory(mock_task_factory_empty) -> None:
    """LocalTaskQueue with empty factory returns no tasks."""
    queue = LocalTaskQueue(mock_task_factory_empty)
    tasks = await queue.receive_tasks(max_count=5)
    assert len(tasks) == 0


@pytest.mark.asyncio
async def test_factory_queue_receive_tasks_max_count_larger_than_available(mock_task_factory):
    """LocalTaskQueue with factory returns only available tasks when max_count is larger."""
    queue = LocalTaskQueue(mock_task_factory)
    tasks = await queue.receive_tasks(max_count=10)
    assert len(tasks) == 3
    task_ids = {task["task_id"] for task in tasks}
    assert task_ids == {"factory-task-1", "factory-task-2", "factory-task-3"}


@pytest.mark.asyncio
async def test_factory_queue_acknowledge_task(mock_task_factory) -> None:
    """LocalTaskQueue.acknowledge_task with factory is a no-op."""
    queue = LocalTaskQueue(mock_task_factory)
    tasks_snapshot = await queue.receive_tasks(max_count=10)
    task_ids_snapshot = {t["task_id"] for t in tasks_snapshot}
    # acknowledge_task is only checked for not raising; snapshot verifies factory output.
    await queue.acknowledge_task("test-ack-1")
    assert task_ids_snapshot == {"factory-task-1", "factory-task-2", "factory-task-3"}


@pytest.mark.asyncio
async def test_factory_queue_retry_task(mock_task_factory):
    """LocalTaskQueue.retry_task with factory is a no-op."""
    queue = LocalTaskQueue(mock_task_factory)
    await queue.retry_task("test-ack-1")


@pytest.mark.asyncio
async def test_worker_start_with_factory_task_queue(
    mock_worker_function, mock_task_factory
) -> None:
    """Worker.start with factory task source uses LocalTaskQueue."""
    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function, task_source=mock_task_factory)
        with patch.object(worker, "_wait_for_shutdown") as mock_wait:
            mock_wait.side_effect = asyncio.CancelledError()
            loop = asyncio.get_running_loop()
            with patch(
                "asyncio.create_task",
                side_effect=lambda coro: loop.create_task(coro),
            ):
                with pytest.raises(asyncio.CancelledError):
                    await worker.start()
            await worker._cleanup_tasks()
        assert isinstance(worker._task_queue, LocalTaskQueue)


@pytest.mark.asyncio
async def test_worker_start_with_factory_task_queue_error(mock_worker_function, caplog):
    """Worker.start with bad factory exits with code 1."""

    def bad_factory():
        raise ValueError("Bad factory")

    with patch("sys.argv", ["worker.py"]):
        with patch("sys.exit") as mock_exit:
            worker = Worker(mock_worker_function, task_source=bad_factory)
            with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                mock_wait.side_effect = asyncio.CancelledError()
                loop = asyncio.get_running_loop()
                with patch(
                    "asyncio.create_task",
                    side_effect=lambda coro: loop.create_task(coro),
                ):
                    try:
                        await worker.start()
                    except asyncio.CancelledError:
                        pass
                    await worker._cleanup_tasks()
            mock_exit.assert_called_once_with(1)
    assert "Error initializing local task queue" in caplog.text


@pytest.mark.asyncio
async def test_worker_with_factory_task_queue_no_provider_or_queue_name(
    mock_worker_function, mock_task_factory
) -> None:
    """Worker with factory task queue does not require provider or queue name."""
    with patch("sys.argv", ["worker.py"]):
        with patch.dict(os.environ, {}, clear=True):
            worker = Worker(mock_worker_function, task_source=mock_task_factory)
            assert worker._data.provider is None
            assert worker._data.queue_name is None


@pytest.mark.asyncio
async def test_queue_acknowledge_task_with_logging_error(
    mock_worker_function, caplog: pytest.LogCaptureFixture
) -> None:
    """_queue_acknowledge_task_with_logging logs on queue error."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        mock_queue = AsyncMock()
        mock_queue.acknowledge_task.side_effect = Exception("Queue error")
        worker._task_queue = mock_queue
        task = {"task_id": "test-task", "ack_id": "test-ack"}
        await worker._queue_acknowledge_task_with_logging(task)
        mock_queue.acknowledge_task.assert_called_once_with("test-ack")
        assert "Queue error" in caplog.text
        assert any(rec.levelname == "ERROR" for rec in caplog.records)


@pytest.mark.asyncio
async def test_queue_retry_task_with_logging_error(
    mock_worker_function, caplog: pytest.LogCaptureFixture
) -> None:
    """_queue_retry_task_with_logging logs on queue error."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        mock_queue = AsyncMock()
        mock_queue.retry_task.side_effect = Exception("Queue error")
        worker._task_queue = mock_queue
        task = {"task_id": "test-task", "ack_id": "test-ack"}
        await worker._queue_retry_task_with_logging(task)
        mock_queue.retry_task.assert_called_once_with("test-ack")
        assert "Queue error" in caplog.text
        assert any(rec.levelname == "ERROR" for rec in caplog.records)


@pytest.mark.asyncio
async def test_feed_tasks_to_workers_sets_task_source_is_empty_when_factory_runs_out(
    mock_task_factory_empty,
) -> None:
    """_feed_tasks_to_workers sets _task_source_is_empty when factory runs out of tasks."""
    with patch.object(sys, "argv", ["worker.py"]):
        worker = Worker(lambda tid, data, w: (False, "retry"), task_source=mock_task_factory_empty)
        worker._running = True
        worker._data.num_simultaneous_tasks = 1
        worker._task_queue = LocalTaskQueue(mock_task_factory_empty)
        assert worker._task_source_is_empty is False
        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.side_effect = BreakLoopError("Break loop")
            with pytest.raises(BreakLoopError):
                await worker._feed_tasks_to_workers()
        assert worker._task_source_is_empty is True
