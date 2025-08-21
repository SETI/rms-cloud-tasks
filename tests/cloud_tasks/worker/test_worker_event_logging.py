"""Tests for the worker module."""

import asyncio
import json
import os
import pytest
import tempfile
from unittest.mock import AsyncMock, patch

from cloud_tasks.worker.worker import Worker


def _mock_worker_function(task_id, task_data, worker):
    return False, "success"


@pytest.fixture
def mock_worker_function():
    return _mock_worker_function


@pytest.fixture
def local_task_file_json():
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


@pytest.mark.asyncio
async def test_event_logging_to_file(mock_worker_function, tmp_path, local_task_file_json):
    """Test event logging to file for various event types."""
    event_log_file = tmp_path / "events.log"

    with patch(
        "sys.argv",
        [
            "worker.py",
            "--provider",
            "AWS",
            "--job-id",
            "test-job",
            "--event-log-file",
            str(event_log_file),
            "--task-file",
            str(local_task_file_json),
        ],
    ):
        worker = Worker(mock_worker_function)

        # Initialize the worker
        with patch.object(worker, "_wait_for_shutdown") as mock_wait:
            mock_wait.side_effect = asyncio.CancelledError()
            with patch("asyncio.create_task", side_effect=lambda x: x):
                try:
                    await worker.start()
                except asyncio.CancelledError:
                    pass
            await worker._cleanup_tasks()

        # Test task completion logging
        await worker._log_task_completed("task1", elapsed_time=1.5, retry=False, result="success")

        # Test task timeout logging
        await worker._log_task_timed_out("task2", retry=False, runtime=2.5)

        # Test task exit logging
        await worker._log_task_exited("task3", retry=False, elapsed_time=2.5, exit_code=1)

        # Test task exception logging
        await worker._log_task_exception(
            "task4", retry=False, elapsed_time=2.5, exception="test error"
        )

        # Test non-fatal exception logging
        await worker._log_non_fatal_exception("ValueError: test error")

        # Test fatal exception logging
        await worker._log_fatal_exception("RuntimeError: fatal error")

        # Test spot termination logging
        await worker._log_spot_termination()

        # Close the file handle
        if worker._event_logger_fp:
            worker._event_logger_fp.close()

        # Read and verify logged events
        with open(event_log_file) as f:
            events = [json.loads(line) for line in f]

        assert len(events) == 7

        # Verify task completion event
        completion_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_TASK_COMPLETED
        )
        assert completion_event["task_id"] == "task1"
        assert completion_event["elapsed_time"] == 1.5
        assert completion_event["retry"] is False
        assert completion_event["result"] == "success"

        # Verify task timeout event
        timeout_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_TASK_TIMED_OUT
        )
        assert timeout_event["task_id"] == "task2"
        assert timeout_event["elapsed_time"] == 2.5

        # Verify task exit event
        exit_event = next(e for e in events if e["event_type"] == worker._EVENT_TYPE_TASK_EXITED)
        assert exit_event["task_id"] == "task3"
        assert exit_event["elapsed_time"] == 2.5
        assert exit_event["exit_code"] == 1

        # Verify task exception event
        exception_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_TASK_EXCEPTION
        )
        assert exception_event["task_id"] == "task4"
        assert exception_event["elapsed_time"] == 2.5
        assert exception_event["exception"] == "test error"

        # Verify non-fatal exception event
        non_fatal_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_NON_FATAL_EXCEPTION
        )
        assert non_fatal_event["exception"] == "ValueError: test error"

        # Verify fatal exception event
        fatal_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_FATAL_EXCEPTION
        )
        assert fatal_event["exception"] == "RuntimeError: fatal error"

        # Verify spot termination event
        spot_event = next(
            e for e in events if e["event_type"] == worker._EVENT_TYPE_SPOT_TERMINATION
        )
        assert "timestamp" in spot_event
        assert "hostname" in spot_event


@pytest.mark.asyncio
async def test_event_logging_to_queue(mock_worker_function):
    """Test event logging to queue for various event types."""
    with patch(
        "sys.argv",
        ["worker.py", "--provider", "AWS", "--job-id", "test-job", "--event-log-to-queue"],
    ):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)

            # Initialize the worker
            with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                mock_wait.side_effect = asyncio.CancelledError()
                with patch("asyncio.create_task", side_effect=lambda x: x):
                    try:
                        await worker.start()
                    except asyncio.CancelledError:
                        pass
                await worker._cleanup_tasks()

            # Test task completion logging
            await worker._log_task_completed(
                "task1", elapsed_time=1.5, retry=False, result="success"
            )

            # Test task timeout logging
            await worker._log_task_timed_out("task2", retry=False, runtime=2.5)

            # Test task exit logging
            await worker._log_task_exited("task3", retry=False, elapsed_time=2.5, exit_code=1)

            # Test task exception logging
            await worker._log_task_exception(
                "task4", retry=False, elapsed_time=2.5, exception="test error"
            )

            # Test non-fatal exception logging
            await worker._log_non_fatal_exception("ValueError: test error")

            # Test fatal exception logging
            await worker._log_fatal_exception("RuntimeError: fatal error")

            # Test spot termination logging
            await worker._log_spot_termination()

            # Verify queue messages
            assert mock_queue.send_message.call_count == 7

            # Get all sent messages
            messages = [json.loads(call.args[0]) for call in mock_queue.send_message.call_args_list]

            # Verify task completion event
            completion_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_TASK_COMPLETED
            )
            assert completion_event["task_id"] == "task1"
            assert completion_event["elapsed_time"] == 1.5
            assert completion_event["retry"] is False
            assert completion_event["result"] == "success"

            # Verify task timeout event
            timeout_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_TASK_TIMED_OUT
            )
            assert timeout_event["task_id"] == "task2"
            assert timeout_event["retry"] is False
            assert timeout_event["elapsed_time"] == 2.5

            # Verify task exit event
            exit_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_TASK_EXITED
            )
            assert exit_event["task_id"] == "task3"
            assert exit_event["retry"] is False
            assert exit_event["elapsed_time"] == 2.5
            assert exit_event["exit_code"] == 1

            # Verify task exception event
            exception_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_TASK_EXCEPTION
            )
            assert exception_event["task_id"] == "task4"
            assert exception_event["retry"] is False
            assert exception_event["elapsed_time"] == 2.5
            assert exception_event["exception"] == "test error"

            # Verify non-fatal exception event
            non_fatal_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_NON_FATAL_EXCEPTION
            )
            assert non_fatal_event["exception"] == "ValueError: test error"

            # Verify fatal exception event
            fatal_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_FATAL_EXCEPTION
            )
            assert fatal_event["exception"] == "RuntimeError: fatal error"

            # Verify spot termination event
            spot_event = next(
                e for e in messages if e["event_type"] == worker._EVENT_TYPE_SPOT_TERMINATION
            )
            assert "timestamp" in spot_event
            assert "hostname" in spot_event


@pytest.mark.asyncio
async def test_event_logging_both_file_and_queue(mock_worker_function, tmp_path):
    """Test event logging to both file and queue simultaneously."""
    event_log_file = tmp_path / "events.log"

    with patch(
        "sys.argv",
        [
            "worker.py",
            "--provider",
            "AWS",
            "--job-id",
            "test-job",
            "--event-log-to-file",
            "--event-log-file",
            str(event_log_file),
            "--event-log-to-queue",
        ],
    ):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_create_queue.return_value = mock_queue
            worker = Worker(mock_worker_function)
            try:
                # Initialize the worker
                with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                    mock_wait.side_effect = asyncio.CancelledError()
                    with patch("asyncio.create_task", side_effect=lambda x: x):
                        try:
                            await worker.start()
                        except asyncio.CancelledError:
                            pass
                    await worker._cleanup_tasks()

                # Log a test event
                await worker._log_task_completed(
                    "task1", elapsed_time=1.5, retry=False, result="success"
                )

                # Close the file handle
                if worker._event_logger_fp:
                    worker._event_logger_fp.close()

                # Verify file logging
                with open(event_log_file) as f:
                    file_events = [json.loads(line) for line in f]
                    assert len(file_events) == 1
                    assert file_events[0]["task_id"] == "task1"

                # Verify queue logging
                assert mock_queue.send_message.call_count == 1
                queue_event = json.loads(mock_queue.send_message.call_args[0][0])
                assert queue_event["task_id"] == "task1"
            finally:
                worker._running = False


@pytest.mark.asyncio
async def test_event_logging_no_logging(mock_worker_function):
    """Test that no logging occurs when neither file nor queue logging is enabled."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        try:
            # Verify no file handle was created
            assert worker._event_logger_fp is None

            # Verify no queue was created
            assert worker._event_logger_queue is None

            # Log a test event - should not raise any errors
            await worker._log_task_completed(
                "task1", elapsed_time=1.5, retry=False, result="success"
            )
        finally:
            worker._running = False


@pytest.mark.asyncio
async def test_event_logging_file_error(mock_worker_function, tmp_path, caplog):
    """Test handling of file logging errors."""
    # Create a directory to make file creation fail
    event_log_file = tmp_path / "nonexistent" / "events.log"

    with patch(
        "sys.argv",
        [
            "worker.py",
            "--provider",
            "AWS",
            "--job-id",
            "test-job",
            "--event-log-to-file",
            "--event-log-file",
            str(event_log_file),
        ],
    ):
        with patch("sys.exit") as mock_exit:
            with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
                mock_queue = AsyncMock()
                mock_create_queue.return_value = mock_queue

                worker = Worker(mock_worker_function)
                with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                    mock_wait.side_effect = asyncio.CancelledError()
                    with patch("asyncio.create_task", side_effect=lambda x: x):
                        try:
                            await worker.start()
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                    await worker._cleanup_tasks()
                mock_exit.assert_called_once_with(1)
                assert "Error opening event log file" in caplog.text


@pytest.mark.asyncio
async def test_event_logging_queue_error(mock_worker_function, caplog):
    """Test handling of queue logging errors."""
    with patch(
        "sys.argv",
        ["worker.py", "--provider", "AWS", "--job-id", "test-job", "--event-log-to-queue"],
    ):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            # First call fails for event logger queue, second call fails for task queue
            mock_create_queue.side_effect = [
                Exception("Queue creation failed"),  # For event logger queue
                Exception("Queue creation failed"),  # For task queue
            ]

            with patch("sys.exit", side_effect=SystemExit(1)) as mock_exit:
                worker = Worker(mock_worker_function)
                try:
                    with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                        mock_wait.side_effect = asyncio.CancelledError()
                        with patch("asyncio.create_task", side_effect=lambda x: x):
                            try:
                                await worker.start()
                            except asyncio.CancelledError:
                                pass
                            except SystemExit:
                                pass
                            except Exception:
                                pass
                        await worker._cleanup_tasks()
                    mock_exit.assert_called_once_with(1)
                    assert "Error initializing event log queue" in caplog.text
                finally:
                    worker._running = False
