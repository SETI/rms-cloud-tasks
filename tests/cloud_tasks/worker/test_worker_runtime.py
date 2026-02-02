"""Tests for Worker runtime behavior (start, handle_results, shutdown, etc.)."""

import asyncio
import logging
import signal
import sys
import time
from collections.abc import Iterable
from queue import Empty
from typing import Any, NoReturn
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

import pytest

from cloud_tasks.worker.worker import Worker, WorkerData


@pytest.mark.asyncio
async def test_start_with_local_tasks(mock_worker_function, local_task_file_json: str) -> None:
    """Worker started with local task file shuts down and cleans up when _wait_for_shutdown completes."""
    with patch("sys.argv", ["worker.py", "--task-file", local_task_file_json]):
        worker = Worker(mock_worker_function)
        with patch.object(worker, "_wait_for_shutdown") as mock_wait:
            mock_wait.side_effect = asyncio.CancelledError()
            with pytest.raises(asyncio.CancelledError):
                await worker.start()
            await worker._cleanup_tasks()


@pytest.mark.asyncio
async def test_start_with_cloud_queue(mock_worker_function: Any, mock_queue: Any) -> None:
    """Worker started with cloud queue calls create_queue and cleans up on CancelledError."""
    with patch(
        "sys.argv",
        ["worker.py", "--provider", "AWS", "--job-id", "test-job", "--no-event-log-to-queue"],
    ):
        worker = Worker(mock_worker_function)
        with patch(
            "cloud_tasks.worker.worker.create_queue", return_value=mock_queue
        ) as mock_create_queue:
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
    mock_create_queue.assert_called_once_with(
        provider="AWS",
        queue_name="test-job",
        project_id=None,
        exactly_once=False,
        visibility_timeout=3610,  # max_runtime (3600) + 10
    )


@pytest.mark.asyncio
async def test_handle_results(worker: Any, mock_queue: Any) -> None:
    try:
        worker._task_queue = mock_queue
        worker._running = True
        worker._data.shutdown_grace_period = 0.01

        # Set up the mock queue to return immediately
        mock_queue.acknowledge_task.return_value = asyncio.Future()
        mock_queue.acknowledge_task.return_value.set_result(None)
        mock_queue.retry_task.return_value = asyncio.Future()
        mock_queue.retry_task.return_value.set_result(None)

        worker._processes = {
            1: {
                "process": MagicMock(),
                "task": {"task_id": "task1", "ack_id": "ack1"},
                "start_time": time.time(),
            },
            2: {
                "process": MagicMock(),
                "task": {"task_id": "task2", "ack_id": "ack2"},
                "start_time": time.time(),
            },
        }

        # Put tasks in the result queue
        worker._result_queue.put((1, False, "success"))
        worker._result_queue.put((2, True, "error"))

        async def shutdown_when_done() -> None:
            """Wait for both tasks to be processed, then set shutdown event."""
            start_time = time.time()
            timeout = 0.5  # 500ms timeout

            while time.time() - start_time < timeout:
                if worker._num_tasks_not_retried == 1 and worker._num_tasks_retried == 1:
                    break
                await asyncio.sleep(0.01)  # 10ms sleep

            if worker._num_tasks_not_retried != 1 or worker._num_tasks_retried != 1:
                pytest.fail("Timeout waiting for tasks to be processed")

            # Set shutdown event and wait for a moment to ensure it's processed
            worker._data.shutdown_event.set()
            await asyncio.sleep(0.1)  # Give time for shutdown to be processed

        handler_task = asyncio.create_task(worker._handle_results())
        shutdown_task = asyncio.create_task(worker._wait_for_shutdown(interval=0.01))
        done_task = asyncio.create_task(shutdown_when_done())

        try:
            await asyncio.wait_for(
                asyncio.gather(handler_task, shutdown_task, done_task), timeout=1.0
            )
        except asyncio.TimeoutError:
            pytest.fail("Test timed out waiting for tasks to complete")

        # Verify the results
        assert worker._num_tasks_not_retried == 1
        assert worker._num_tasks_retried == 1
        mock_queue.acknowledge_task.assert_called_once_with("ack1")
        mock_queue.retry_task.assert_called_once_with("ack2")
    finally:
        worker._running = False
        await handler_task
        await shutdown_task
        await done_task


@pytest.mark.asyncio
async def test_check_termination_notice_aws(worker: Any) -> None:
    worker._data.provider = "AWS"
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        assert await worker._check_termination_notice() is True


@pytest.mark.asyncio
async def test_check_termination_notice_gcp(worker: Any) -> None:
    worker._data.provider = "GCP"
    with patch("requests.get") as mock_get:
        mock_get.return_value.text = "true"
        assert await worker._check_termination_notice() is True


@pytest.mark.asyncio
async def test_check_termination_notice_azure(worker: Any) -> None:
    worker._data.provider = "AZURE"
    assert await worker._check_termination_notice() is False


def exception_worker_function(task_id: str, task_data: dict[str, Any], worker: Worker) -> NoReturn:
    """Test worker that raises ValueError (used to exercise exception handling paths)."""
    raise ValueError("Test exception")


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_on_exception", [True, False])
async def test_handle_results_process_exception(
    worker: Any, mock_queue: Any, retry_on_exception: bool
) -> None:
    # Patch MP_CTX.Process to avoid real multiprocessing
    with patch("cloud_tasks.worker.worker.MP_CTX.Process") as mock_process_cls:
        mock_proc = MagicMock()
        # Simulate process is alive when result is processed (so exception path is taken)
        mock_proc.is_alive.return_value = True
        mock_proc.exitcode = 1
        mock_proc.pid = 1234
        mock_process_cls.return_value = mock_proc

        worker._task_queue = mock_queue
        worker._running = True
        worker._data.shutdown_grace_period = 0.01
        worker._user_worker_function = exception_worker_function
        worker._data.retry_on_exception = retry_on_exception

        # Set up the mock queue to return immediately
        mock_queue.acknowledge_task.return_value = asyncio.Future()
        mock_queue.acknowledge_task.return_value.set_result(None)
        mock_queue.retry_task.return_value = asyncio.Future()
        mock_queue.retry_task.return_value.set_result(None)

        # Simulate the process and result
        worker_id = 1
        task = {"task_id": "task1", "ack_id": "ack1"}
        worker._processes = {
            worker_id: {
                "process": mock_proc,
                "task": task,
                "start_time": time.time(),
            }
        }
        # Simulate the result the process would put in the queue
        worker._result_queue.put((worker_id, "exception", "Test exception"))

        async def shutdown_when_done() -> None:
            """Wait for the task to be processed, then set shutdown event."""
            start_time = time.time()
            timeout = 2.0
            while time.time() - start_time < timeout:
                if worker._num_tasks_retried == 1 or worker._num_tasks_not_retried == 1:
                    break
                await asyncio.sleep(0.01)
            if worker._num_tasks_retried != 1 and worker._num_tasks_not_retried != 1:
                pytest.fail("Timeout waiting for task to be processed")
            worker._data.shutdown_event.set()
            await asyncio.sleep(0.1)

        handler_task = asyncio.create_task(worker._handle_results())
        shutdown_task = asyncio.create_task(worker._wait_for_shutdown(interval=0.01))
        done_task = asyncio.create_task(shutdown_when_done())

        try:
            await asyncio.wait_for(
                asyncio.gather(handler_task, shutdown_task, done_task), timeout=3.0
            )
        except asyncio.TimeoutError:
            pytest.fail("Test timed out waiting for task to complete")

        # Verify the results
        if retry_on_exception:
            assert worker._num_tasks_retried == 1
            assert worker._num_tasks_not_retried == 0
            mock_queue.retry_task.assert_called_once_with("ack1")
        else:
            assert worker._num_tasks_retried == 0
            assert worker._num_tasks_not_retried == 1
            mock_queue.acknowledge_task.assert_called_once_with("ack1")
        worker._running = False


def test_worker_process_main(mock_worker_function):
    result_queue = MagicMock()
    worker_data = MagicMock()
    task = {
        "task_id": "test-task",
        "data": {"key": "value"},
    }
    worker_data.received_shutdown_request = False
    worker_data.received_termination_notice = False

    with patch("sys.exit") as mock_exit:
        Worker._worker_process_main(
            1,
            mock_worker_function,
            worker_data,
            task["task_id"],
            task["data"],
            result_queue,
        )
        result_queue.put.assert_called_once_with((1, False, "success"))
        mock_exit.assert_called_once_with(0)


def test_execute_task_isolated(mock_worker_function):
    task_id = "test-task"
    task_data = {"key": "value"}
    worker = MagicMock()
    retry, result = Worker._execute_task_isolated(task_id, task_data, worker, mock_worker_function)
    assert retry is False
    assert result == "success"


def test_execute_task_isolated_error():
    def error_func(task_id, task_data, worker):
        raise ValueError("Test error")

    task_id = "test-task"
    task_data = {"key": "value"}
    worker = MagicMock()
    with pytest.raises(ValueError, match="Test error"):
        Worker._execute_task_isolated(task_id, task_data, worker, error_func)


def test_signal_handler(mock_worker_function, caplog):
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        worker = Worker(mock_worker_function)
        with patch("signal.signal") as mock_signal:
            worker._signal_handler(signal.SIGINT, None)
            assert worker._data.shutdown_event.is_set()
            mock_signal.assert_called_with(signal.SIGTERM, signal.SIG_DFL)
            assert "Received signal SIGINT, initiating graceful shutdown" in caplog.text


@pytest.mark.asyncio
async def test_wait_for_shutdown_graceful(mock_worker_function: Any) -> None:
    """Test _wait_for_shutdown when processes exit gracefully."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        worker = Worker(mock_worker_function)

        # Create mock processes
        mock_process1 = MagicMock()
        mock_process2 = MagicMock()

        # Set up initial state
        worker._running = True
        worker._processes = {
            1: {"process": mock_process1, "task": "task1"},
            2: {"process": mock_process2, "task": "task2"},
        }
        worker._data.shutdown_grace_period = 5  # Longer grace period for testing

        # Create a task to set shutdown event and simulate process completion
        async def trigger_shutdown() -> None:
            await asyncio.sleep(0.1)
            if worker._data.shutdown_event is not None:
                worker._data.shutdown_event.set()
            # Simulate processes completing their tasks immediately
            worker._processes = {}
            # Simulate processes being done
            mock_process1.is_alive.return_value = False
            mock_process2.is_alive.return_value = False

        # Start the shutdown task
        shutdown_task = asyncio.create_task(trigger_shutdown())

        # Call _wait_for_shutdown
        await worker._wait_for_shutdown()

        # Wait for shutdown task to complete
        await shutdown_task

        # Verify processes were not terminated
        mock_process1.terminate.assert_not_called()
        mock_process2.terminate.assert_not_called()


@pytest.mark.asyncio
async def test_wait_for_shutdown_force_terminate(mock_worker_function: Any) -> None:
    """Test _wait_for_shutdown when processes need to be force terminated."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        worker = Worker(mock_worker_function)

        # Create mock processes and set up initial state
        mock_process1 = MagicMock()
        mock_process2 = MagicMock()
        worker._running = True
        worker._processes = {
            1: {"process": mock_process1, "task": "task1"},
            2: {"process": mock_process2, "task": "task2"},
        }
        worker._data.shutdown_grace_period = 1  # Short grace period for testing

        # Create a task to set shutdown event after a delay
        async def trigger_shutdown() -> None:
            """Set shutdown event and keep processes so _wait_for_shutdown force-terminates."""
            await asyncio.sleep(0.1)
            if worker._data.shutdown_event is not None:
                worker._data.shutdown_event.set()
            # Keep active tasks count high to force termination
            worker._processes = {
                1: {"process": mock_process1, "task": "task1"},
                2: {"process": mock_process2, "task": "task2"},
            }

        # Start the shutdown task
        shutdown_task = asyncio.create_task(trigger_shutdown())

        # Call _wait_for_shutdown
        await worker._wait_for_shutdown()

        # Wait for shutdown task to complete
        await shutdown_task

        # Verify processes were terminated and killed
        mock_process1.terminate.assert_called_once()
        mock_process2.terminate.assert_called_once()
        mock_process1.join.assert_called_once()
        mock_process2.join.assert_called_once()
        mock_process1.kill.assert_called_once()
        mock_process2.kill.assert_called_once()


@pytest.mark.asyncio
async def test_wait_for_shutdown_no_processes(mock_worker_function: Any) -> None:
    """Test _wait_for_shutdown when there are no processes to clean up."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        worker = Worker(mock_worker_function)

        # Set up initial state
        worker._running = True
        worker._processes = {}

        # Set shutdown event
        if worker._data.shutdown_event is not None:
            worker._data.shutdown_event.set()

        # Call _wait_for_shutdown
        await worker._wait_for_shutdown()

        # Verify worker is no longer running
        assert not worker._running


@pytest.mark.asyncio
async def test_create_single_task_process(mock_worker_function: Any) -> None:
    """Test creating a single task process."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "jid"]):
        with (
            patch("cloud_tasks.worker.worker.MP_CTX.Process") as mock_process,
            patch("cloud_tasks.worker.worker.MP_CTX.Queue") as mock_queue_cls,
            patch("cloud_tasks.worker.worker.logger") as mock_logger,
        ):
            # Mock the Queue instance (result queue)
            mock_result_queue = MagicMock()
            mock_queue_cls.return_value = mock_result_queue

            worker = Worker(mock_worker_function)
            worker._num_tasks_not_retried = 1
            worker._num_tasks_retried = 2
            worker._processes = {}
            worker._task_skip_count = 0
            worker._running = True

            # Mock process instance
            mock_proc = MagicMock()
            mock_process.return_value = mock_proc

            # Mock task queue to return a task once and then None
            mock_task_queue = AsyncMock()
            task = {"task_id": "test-task", "data": {}, "ack_id": "test-ack"}
            mock_task_queue.receive_tasks.side_effect = [
                [task],
                [],
            ]
            worker._task_queue = mock_task_queue
            worker._next_worker_id = 3

            # Create a task to set shutdown request after process creation
            async def trigger_shutdown() -> None:
                """Set shutdown event and stop running so _feed_tasks_to_workers exits."""
                await asyncio.sleep(0.1)
                if worker._data.shutdown_event is not None:
                    worker._data.shutdown_event.set()
                worker._running = False

            # Start the shutdown task
            shutdown_task = asyncio.create_task(trigger_shutdown())

            # Call _feed_tasks_to_workers which will create the process
            await worker._feed_tasks_to_workers()

            # Wait for shutdown task to complete
            await shutdown_task

            # Verify process creation
            mock_process.assert_called_once_with(
                target=Worker._worker_process_main,
                args=(
                    3,
                    mock_worker_function,
                    worker._data,
                    task["task_id"],
                    task["data"],
                    worker._result_queue,
                ),
            )

            # Verify process configuration
            assert mock_proc.daemon is True
            mock_proc.start.assert_called_once()

            # Verify logging (assert on patched logger; caplog can be empty if configure_logging cleared handlers)
            log_calls = [str(c) for c in mock_logger.info.call_args_list]
            assert any("Started single-task worker #3" in c for c in log_calls)
            assert any("PID" in c for c in log_calls)

            # Verify process was added to the list
            assert len(worker._processes) == 1
            assert worker._processes[3]["process"] == mock_proc


@pytest.mark.asyncio
async def test_check_termination_loop(mock_worker_function: Any) -> None:
    """Test that _check_termination_loop properly handles termination notices."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with (
            patch("asyncio.sleep") as mock_sleep,
            patch("cloud_tasks.worker.worker.logger") as mock_logger,
        ):
            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.shutdown_event = MagicMock()
            worker._data.shutdown_event.is_set.return_value = False
            worker._data.termination_event = MagicMock()
            worker._data.termination_event.is_set.return_value = False

            # Mock _check_termination_notice to return True once then False
            async def mock_check_termination():
                mock_check_termination.call_count = (
                    getattr(mock_check_termination, "call_count", 0) + 1
                )
                if mock_check_termination.call_count == 2:
                    return True
                return False

            worker._check_termination_notice = mock_check_termination

            # Run the termination check loop
            await worker._check_termination_loop()

            # Verify termination event was set
            worker._data.termination_event.set.assert_called_once()

            # Verify logger was called with appropriate message
            warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
            assert any("Instance termination notice received" in c for c in warning_calls)

            # Verify sleep was called with the correct duration
            mock_sleep.assert_called_once_with(5)


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_on_timeout", [True, False])
async def test_monitor_process_runtimes(mock_worker_function, retry_on_timeout):
    """Test _monitor_process_runtimes with a process that exceeds max runtime."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with (
            patch("asyncio.sleep") as mock_sleep,
            patch("cloud_tasks.worker.worker.logger") as mock_logger,
        ):
            # Make sleep raise an exception after first call to break the loop
            mock_sleep.side_effect = [None, Exception("Test complete")]

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.max_runtime = 0.2
            worker._data.retry_on_timeout = retry_on_timeout

            # Create a mock process that will exceed max runtime
            mock_process = MagicMock()
            mock_process.pid = 123
            mock_process.is_alive.return_value = False  # Process stays alive after terminate

            # Set up process info to indicate it's been running too long
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": time.time() - 0.2,
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }  # 0.2 seconds runtime

            # Mock task queue for retry_task call
            mock_queue = AsyncMock()
            worker._task_queue = mock_queue

            # Run the monitor for one iteration
            with pytest.raises(Exception, match="Test complete"):
                await worker._monitor_process_runtimes()

            # Verify process was terminated
            mock_process.terminate.assert_called_once()
            # Verify two join calls since process stays alive
            assert mock_process.join.call_count == 1
            mock_process.join.assert_has_calls(
                [
                    call(timeout=1),  # First join after terminate
                ]
            )

            # Verify task was marked as failed
            if retry_on_timeout:
                mock_queue.retry_task.assert_called_once_with("ack1")
                mock_queue.acknowledge_task.assert_not_called()
                info_calls = [str(c) for c in mock_logger.info.call_args_list]
                assert any("Worker #123" in c for c in info_calls)
                assert any("task-1 will be retried" in c for c in info_calls)
            else:
                mock_queue.acknowledge_task.assert_called_once_with("ack1")
                mock_queue.retry_task.assert_not_called()
                info_calls = [str(c) for c in mock_logger.info.call_args_list]
                assert any("Worker #123" in c for c in info_calls)
                assert any("task-1 will not be retried" in c for c in info_calls)

            # Verify no new process was created to replace it
            assert len(worker._processes) == 0

            # Verify warning logging (exceeded max runtime)
            warning_calls = [str(c) for c in mock_logger.warning.call_args_list]
            assert any("Worker #123" in c for c in warning_calls)
            assert any("task-1 exceeded max runtime" in c for c in warning_calls)


@pytest.mark.asyncio
async def test_monitor_process_runtimes_no_termination(mock_worker_function, caplog) -> None:
    """Test _monitor_process_runtimes when process does not exit after terminate (SIGTERM) and must be killed (SIGKILL)."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("asyncio.sleep") as mock_sleep:
            # Make sleep raise an exception after first call to break the loop
            mock_sleep.side_effect = [None, Exception("Test complete")]

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.max_runtime = 1

            # Create a mock process that will exceed max runtime
            mock_process = MagicMock()
            mock_process.pid = 123
            mock_process.is_alive.return_value = True  # Process stays alive after terminate

            # Set up process info to indicate it's been running too long
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": time.time() - 1.0,
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }  # 1 second runtime

            # Mock task queue for retry_task call
            mock_queue = AsyncMock()
            worker._task_queue = mock_queue

            # Run the monitor for one iteration
            with pytest.raises(Exception, match="Test complete"):
                await worker._monitor_process_runtimes()

            # Verify process was terminated
            mock_process.terminate.assert_called_once()
            # Verify two join calls since process stays alive
            assert mock_process.join.call_count == 2
            mock_process.join.assert_has_calls(
                [
                    call(timeout=1),  # First join after terminate
                    call(timeout=1),  # Second join after kill
                ]
            )
            mock_process.kill.assert_called_once()  # Verify kill was called after second join

            # Verify task was marked as failed
            mock_queue.acknowledge_task.assert_called_once_with("ack1")

            # Verify no new process was created to replace it
            assert len(worker._processes) == 0

            # Verify logging
            assert (
                "Worker #123 (PID 123), task task-1 exceeded max runtime of 1 second" in caplog.text
            )


@pytest.mark.asyncio
async def test_monitor_process_runtimes_no_exceeded_processes(mock_worker_function, caplog):
    """Test _monitor_process_runtimes when no processes exceed max runtime."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("asyncio.sleep") as mock_sleep:
            # Make sleep raise an exception after first call to break the loop
            mock_sleep.side_effect = [None, Exception("Test complete")]

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.shutdown_event = MagicMock()
            worker._data.shutdown_event.is_set.return_value = False
            worker._data.max_runtime = 0.1

            # Create a mock process that hasn't exceeded max runtime
            mock_process = MagicMock()
            mock_process.pid = 123
            mock_process.is_alive.return_value = True

            # Set up process info to indicate it's been running for a short time
            worker._processes = {
                123: {
                    "process": mock_process,
                    "start_time": time.time() - 0.05,
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }  # 0.05 seconds runtime

            # Run the monitor for one iteration
            with pytest.raises(Exception, match="Test complete"):
                await worker._monitor_process_runtimes()

            # Verify process was not terminated
            mock_process.terminate.assert_not_called()
            mock_process.join.assert_not_called()

            # Verify no new process was created
            assert len(worker._processes) == 1
            assert worker._processes[123]["process"] == mock_process

            # Verify no logging of warnings or info messages
            assert "Process 123 exceeded max runtime" not in caplog.text
            assert "Terminating" not in caplog.text


@pytest.mark.asyncio
async def test_worker_with_simulate_spot_termination_delay():
    """Test that worker correctly handles simulate_spot_termination_delay argument."""
    with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
        mock_queue = AsyncMock()
        mock_create_queue.return_value = mock_queue
        mock_queue.receive_tasks.return_value = []

        # Create worker with simulate_spot_termination_delay
        worker = Worker(
            user_worker_function=lambda task_id, task_data, worker: (True, "success"),
            args=[
                "--provider",
                "AWS",
                "--job-id",
                "test-job",
                "--simulate-spot-termination-after",
                "0.1",
            ],
        )

        # Verify the after was set
        assert worker._data.simulate_spot_termination_after == 0.1
        worker._start_time = time.time()

        # Test before delay is exceeded
        assert not await worker._check_termination_notice()

        # Set start time to be before the delay
        worker._start_time = time.time() - 0.2  # Set start time to 0.2 seconds ago

        # Test after delay is exceeded
        assert await worker._check_termination_notice()


@pytest.mark.asyncio
async def test_worker_with_is_spot():
    """Test that worker creates termination check loop when is_spot is enabled."""
    with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
        mock_queue = AsyncMock()
        mock_create_queue.return_value = mock_queue
        mock_queue.receive_tasks.return_value = []

        # Create worker with is_spot enabled
        worker = Worker(
            user_worker_function=lambda task_id, task_data, worker: (True, "success"),
            args=["--provider", "AWS", "--job-id", "test-job", "--is-spot"],
        )

        # Verify is_spot was set
        assert worker._is_spot

        # Mock _check_termination_loop
        mock_loop = AsyncMock()
        worker._check_termination_loop = mock_loop

        # Set up the worker to exit immediately
        worker._data.shutdown_event.set()

        # Start the worker
        await worker.start()

        # Verify _check_termination_loop was called
        mock_loop.assert_called_once()


@pytest.mark.asyncio
async def test_worker_without_is_spot():
    """Test that worker does not create termination check loop when is_spot is disabled."""
    with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
        mock_queue = AsyncMock()
        mock_create_queue.return_value = mock_queue
        mock_queue.receive_tasks.return_value = []

        # Create worker with is_spot disabled
        worker = Worker(
            user_worker_function=lambda task_id, task_data, worker: (True, "success"),
            args=["--provider", "AWS", "--job-id", "test-job"],
        )

        # Verify is_spot was not set
        assert not worker._is_spot

        # Mock _check_termination_loop
        mock_loop = AsyncMock()
        worker._check_termination_loop = mock_loop

        # Set up the worker to exit immediately
        worker._data.shutdown_event.set()

        # Start the worker
        await worker.start()

        # Verify _check_termination_loop was not called
        mock_loop.assert_not_called()


@pytest.mark.asyncio
async def test_check_termination_loop_with_simulated_delay(mock_worker_function):
    """Test that _check_termination_loop properly handles simulated spot termination delay."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.logger") as mock_logger:
            worker = Worker(mock_worker_function)
            worker._start_time = time.time() - 0.2
            worker._running = True
            worker._data.simulate_spot_termination_after = 0.1
            worker._data.simulate_spot_termination_delay = None

            # Create mock processes
            mock_process1 = MagicMock()
            mock_process1.is_alive.return_value = True
            mock_process1.pid = 123
            mock_process1.join.return_value = None

            mock_process2 = MagicMock()
            mock_process2.is_alive.return_value = True
            mock_process2.pid = 456
            mock_process2.join.return_value = None

            worker._processes = {
                1: {"process": mock_process1, "worker_id": 1},
                2: {"process": mock_process2, "worker_id": 2},
            }

            # Start the termination check loop
            with patch("asyncio.sleep") as mock_sleep:
                mock_sleep.return_value = None

                worker._data.simulate_spot_termination_delay = 0.2

                await worker._check_termination_loop()

                # Verify processes were terminated
                mock_process1.terminate.assert_called_once()
                mock_process2.terminate.assert_called_once()
                mock_process1.join.assert_called_once_with(timeout=5)
                mock_process2.join.assert_called_once_with(timeout=5)

                # Verify processes were cleaned up
                assert len(worker._processes) == 0
                assert not worker._running

            # Verify logging (assert on patched logger)
            info_calls = [str(c) for c in mock_logger.info.call_args_list]
            assert any(
                "Simulated spot termination delay complete" in c and "killing all processes" in c
                for c in info_calls
            )


@pytest.mark.asyncio
async def test_handle_results_process_exit_retry_on_exit(mock_worker_function):
    """Should properly handle worker process exits and task retry logic."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.logger") as mock_logger:
            worker = Worker(mock_worker_function)
            worker._running = True
            worker._num_simultaneous_tasks = 2
            mock_queue = AsyncMock()
            worker._task_queue = mock_queue

            # Create a mock process that will exit
            mock_process = MagicMock()
            mock_process.pid = 123
            mock_process.is_alive.return_value = False
            mock_process.exitcode = 1

            # Set up initial process
            task = {"task_id": "test-task", "data": {}, "ack_id": "test-ack"}
            worker._processes = {
                1: {
                    "worker_id": 1,
                    "process": mock_process,
                    "start_time": time.time(),
                    "task": task,
                }
            }

            async def fake_sleep(*a: Any, **kw: Any) -> None:
                """Stop worker and return None to break sleep loop (mocks asyncio.sleep)."""
                worker._running = False
                return None

            with patch("asyncio.sleep") as mock_sleep:
                mock_sleep.side_effect = fake_sleep

                # Test with retry_on_exit=False
                worker._data.retry_on_exit = False
                await worker._handle_results()
                mock_queue.acknowledge_task.assert_called_once_with("test-ack")
                mock_queue.retry_task.assert_not_called()
                assert len(worker._processes) == 0
                mock_logger.warning.assert_called_with(
                    'Worker #1 (PID 123) processing task "test-task" exited prematurely in '
                    "0.0 seconds with exit code 1; not retrying"
                )

                # Reset for next test
                mock_queue.reset_mock()
                mock_logger.reset_mock()
                worker._processes = {
                    1: {
                        "worker_id": 1,
                        "process": mock_process,
                        "start_time": time.time(),
                        "task": task,
                    }
                }
                worker._running = True

                # Test with retry_on_exit=True
                worker._data.retry_on_exit = True
                await worker._handle_results()
                mock_queue.retry_task.assert_called_once_with("test-ack")
                mock_queue.acknowledge_task.assert_not_called()
                assert len(worker._processes) == 0
                mock_logger.warning.assert_called_with(
                    'Worker #1 (PID 123) processing task "test-task" exited prematurely in '
                    "0.0 seconds with exit code 1; retrying"
                )


class _MockResultQueue:
    """Mock result queue that yields (worker_id, retry, result) so _handle_results clears processes."""

    def __init__(
        self,
        results: Iterable[tuple[int, bool, Any]] | None = None,
    ) -> None:
        """Initialize the mock queue with default or provided (worker_id, retry, result) tuples.

        Parameters:
            results: Optional iterable of (worker_id, retry, result); default is three ok results.
        """
        self.results: list[tuple[int, bool, Any]] = list(
            results or [(0, False, "ok"), (1, False, "ok"), (2, False, "ok")]
        )

    def empty(self) -> bool:
        """Return True if no results remain in the queue."""
        return len(self.results) == 0

    def get_nowait(self) -> tuple[int, bool, Any]:
        """Return and remove the next (worker_id, retry, result); raise Empty if empty."""
        if not self.results:
            raise Empty()
        return self.results.pop(0)


@pytest.mark.asyncio
async def test_tasks_to_skip_and_limit(mock_worker_function):
    """Test that tasks-to-skip and task-limit options work correctly."""
    with patch(
        "sys.argv",
        [
            "worker.py",
            "--provider",
            "AWS",
            "--job-id",
            "test-job",
            "--tasks-to-skip",
            "2",
            "--max-num-tasks",
            "3",
            "--num-simultaneous-tasks",
            "3",
        ],
    ):
        with (
            patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue,
            patch("cloud_tasks.worker.worker.MP_CTX.Queue") as mock_mp_queue_cls,
            patch("cloud_tasks.worker.worker.MP_CTX.Process") as mock_process_cls,
            patch("cloud_tasks.worker.worker.logger") as mock_logger,
            patch.object(Worker, "_wait_for_shutdown", new_callable=AsyncMock) as mock_wait,
        ):
            # Let feed/handle run for 3s then "shutdown" (mock returns, cleanup runs)
            async def _wait_impl(*args, **kwargs):
                await asyncio.sleep(3.0)

            mock_wait.side_effect = _wait_impl
            mock_mp_queue_cls.return_value = _MockResultQueue()
            mock_queue = AsyncMock()
            mock_create_queue.return_value = mock_queue
            # Avoid visibility renewal worker running (would need real queue semantics)
            mock_queue.get_max_visibility_timeout.return_value = None

            # Mock Process so we don't spawn real child processes (they'd need a real
            # result queue with put(); pre-populated mock is consumed by _handle_results).
            def make_mock_process(*args, **kwargs):
                p = MagicMock()
                p.is_alive.return_value = False  # So _handle_results treats as exited
                p.pid = id(p) % 100000
                p.join.return_value = None
                return p

            mock_process_cls.side_effect = make_mock_process

            # Create tasks that will be returned by the queue
            tasks = [{"task_id": f"task-{i}", "data": {}, "ack_id": f"ack-{i}"} for i in range(6)]
            mock_queue.receive_tasks.side_effect = [
                [tasks[0]],  # First batch - should be skipped
                [tasks[1]],  # Second batch - should be skipped
                [tasks[2]],  # Third batch - should be processed
                [tasks[3]],  # Fourth batch - should be processed
                [tasks[4]],  # Fifth batch - should be processed
                [tasks[5]],  # Sixth batch - should not get here
                [],
            ]

            worker = Worker(mock_worker_function)

            # _wait_for_shutdown is mocked to sleep 3s then return so feed/handle run
            await asyncio.wait_for(worker.start(), timeout=10.0)

            # Verify tasks were skipped and limit applied
            assert worker._task_skip_count == 0  # Should have used up all skips
            assert worker._tasks_remaining == 0  # Should have used up all task limit

            # Either normal completion (results from queue) or exited path (mock procs don't put)
            completed = worker._num_tasks_not_retried + worker._num_tasks_retried
            assert completed + worker._num_tasks_exited >= 3, (
                "Expected at least 3 tasks handled (completed or exited)"
            )
            if worker._num_tasks_exited == 0:
                assert worker._num_tasks_not_retried == 3 and worker._num_tasks_retried == 0
            else:
                assert worker._num_tasks_exited == 3  # Mock procs treated as exited prematurely

            # Verify logging of started workers for tasks 2-4 (assert on patched logger)
            info_calls = [str(c) for c in mock_logger.info.call_args_list]
            worker_start_calls = [c for c in info_calls if "Started single-task worker" in c]
            assert len(worker_start_calls) == 3  # Should have started 3 workers

            for worker_id in range(3):
                assert any(
                    f"Started single-task worker #{worker_id}" in c for c in worker_start_calls
                ), f"Missing log entry for worker {worker_id}"

            # Verify queue interactions (at least 5 calls: 2 skips + 3 dispatches)
            assert mock_queue.receive_tasks.call_count >= 5
            # Tasks are either acked or retried when completed/exited (counts verified above)


@pytest.mark.asyncio
async def test_start_with_event_log_file_error(mock_worker_function, caplog):
    """Test start() with event log file error."""
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
            "/nonexistent/events.log",
        ],
    ):
        with patch("builtins.open", side_effect=PermissionError("Permission denied")):
            with patch("sys.exit") as mock_exit:
                worker = Worker(mock_worker_function)
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
                # Multiple errors will cause multiple exit calls
                assert mock_exit.call_count >= 1
                assert "Error opening event log file" in caplog.text


@pytest.mark.asyncio
async def test_start_with_event_log_queue_error(mock_worker_function, caplog):
    """Test start() with event log queue error."""
    with patch(
        "sys.argv",
        ["worker.py", "--provider", "AWS", "--job-id", "test-job", "--event-log-to-queue"],
    ):
        with patch(
            "cloud_tasks.worker.worker.create_queue", side_effect=Exception("Queue creation failed")
        ):
            with patch("sys.exit") as mock_exit:
                worker = Worker(mock_worker_function)
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
                # Multiple errors will cause multiple exit calls
                assert mock_exit.call_count >= 1
                assert "Error initializing event log queue" in caplog.text


@pytest.mark.asyncio
async def test_start_with_local_task_queue_error(mock_worker_function, caplog):
    """Test start() with local task queue error."""
    with patch("sys.argv", ["worker.py"]):

        def bad_factory() -> NoReturn:
            """Raise to simulate task source failure."""
            raise ValueError("Bad factory")

        with patch("sys.exit") as mock_exit:
            worker = Worker(mock_worker_function, task_source=bad_factory)
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
            # Multiple errors will cause multiple exit calls
            assert mock_exit.call_count >= 1
            assert "Error initializing local task queue" in caplog.text


@pytest.mark.asyncio
async def test_start_with_cloud_queue_error(mock_worker_function, caplog):
    """Test start() with cloud queue error."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch(
            "cloud_tasks.worker.worker.create_queue", side_effect=Exception("Queue creation failed")
        ):
            with patch("sys.exit") as mock_exit:
                worker = Worker(mock_worker_function)
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
                # Multiple errors will cause multiple exit calls
                assert mock_exit.call_count >= 1
                assert "Error initializing task queue" in caplog.text


@pytest.mark.asyncio
async def test_handle_results_with_exception(mock_worker_function, caplog):
    """Test _handle_results with exception."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True

        # Mock the result queue to raise an exception
        with patch.object(worker._result_queue, "get_nowait", side_effect=Exception("Queue error")):
            with patch("asyncio.sleep") as mock_sleep:
                mock_sleep.side_effect = [None, StopAsyncIteration()]
                try:
                    await worker._handle_results()
                except StopAsyncIteration:
                    pass
        assert "Error handling results" in caplog.text


@pytest.mark.asyncio
async def test_wait_for_shutdown_with_termination_event(mock_worker_function):
    """Test _wait_for_shutdown with termination event."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True
        worker._processes = {}
        worker._data.termination_event.set()

        await worker._wait_for_shutdown()
        assert not worker._running


@pytest.mark.asyncio
async def test_check_termination_loop_with_exception(mock_worker_function, caplog):
    """Test _check_termination_loop with exception."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True
        worker._data.shutdown_event = MagicMock()
        worker._data.shutdown_event.is_set.return_value = False

        # Mock _check_termination_notice to raise an exception
        async def mock_check_termination() -> bool:
            """Raise to simulate termination check failure."""
            raise Exception("Termination check failed")

        worker._check_termination_notice = mock_check_termination

        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.side_effect = [None, Exception("Test complete")]
            with pytest.raises(Exception, match="Test complete"):
                await worker._check_termination_loop()

        assert "Error checking for termination" in caplog.text


@pytest.mark.asyncio
async def test_feed_tasks_to_workers_with_exception(mock_worker_function, caplog):
    """Test _feed_tasks_to_workers with exception."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True
        worker._data.num_simultaneous_tasks = 1

        # Mock the task queue to raise an exception
        mock_queue = AsyncMock()
        mock_queue.receive_tasks.side_effect = Exception("Queue error")
        worker._task_queue = mock_queue

        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.side_effect = [None, Exception("Test complete")]
            with pytest.raises(Exception, match="Test complete"):
                await worker._feed_tasks_to_workers()

        assert "Error feeding tasks to workers" in caplog.text


@pytest.mark.asyncio
async def test_monitor_process_runtimes_with_termination_error(mock_worker_function, caplog):
    """Test _monitor_process_runtimes with process termination error."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True
        worker._data.max_runtime = 0.1
        worker._data.retry_on_timeout = False

        # Create a mock process that fails to terminate
        mock_process = MagicMock()
        mock_process.pid = 123
        mock_process.is_alive.return_value = True
        mock_process.terminate.side_effect = Exception("Terminate failed")

        worker._processes = {
            123: {
                "process": mock_process,
                "start_time": time.time() - 0.2,  # Exceeded max runtime
                "task": {"task_id": "task-1", "ack_id": "ack1"},
            }
        }

        mock_queue = AsyncMock()
        worker._task_queue = mock_queue

        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.side_effect = [None, Exception("Test complete")]
            with pytest.raises(Exception, match="Test complete"):
                await worker._monitor_process_runtimes()

        assert "Error terminating process worker" in caplog.text


@pytest.mark.asyncio
async def test_monitor_process_runtimes_with_queue_error(mock_worker_function, caplog):
    """Test _monitor_process_runtimes with queue error."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)
        worker._running = True
        worker._data.max_runtime = 0.1
        worker._data.retry_on_timeout = False

        # Create a mock process that exceeds max runtime
        mock_process = MagicMock()
        mock_process.pid = 123
        mock_process.is_alive.return_value = False

        worker._processes = {
            123: {
                "process": mock_process,
                "start_time": time.time() - 0.2,  # Exceeded max runtime
                "task": {"task_id": "task-1", "ack_id": "ack1"},
            }
        }

        # Mock queue to raise exception
        mock_queue = AsyncMock()
        mock_queue.acknowledge_task.side_effect = Exception("Queue error")
        worker._task_queue = mock_queue

        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.side_effect = [None, StopAsyncIteration()]
            try:
                await worker._monitor_process_runtimes()
            except StopAsyncIteration:
                pass
        assert "Error completing task task-1: Queue error" in caplog.text


def test_worker_process_main_with_exception():
    """Test _worker_process_main with exception."""

    def bad_worker_function(task_id, task_data, worker):
        raise ValueError("Worker function error")

    result_queue = MagicMock()
    worker_data = MagicMock()
    worker_data.received_shutdown_request = False
    worker_data.received_termination_notice = False

    with patch("sys.exit") as mock_exit:
        Worker._worker_process_main(
            1,
            bad_worker_function,
            worker_data,
            "test-task",
            {"key": "value"},
            result_queue,
        )
        result_queue.put.assert_called_once_with((1, "exception", ANY))
        mock_exit.assert_called_once_with(0)


def test_worker_process_main_with_unhandled_exception() -> None:
    """Test _worker_process_main with unhandled exception."""

    def bad_worker_function(
        task_id: str, task_data: dict[str, Any], worker_data: WorkerData
    ) -> tuple[bool, str]:
        """Raise to simulate worker function failure."""
        raise ValueError("Worker function error")

    result_queue = MagicMock()
    worker_data = MagicMock()
    worker_data.received_shutdown_request = False
    worker_data.received_termination_notice = False

    # Mock the _execute_task_isolated to raise an exception
    with patch.object(Worker, "_execute_task_isolated", side_effect=Exception("Unhandled error")):
        with patch("sys.exit") as mock_exit:
            Worker._worker_process_main(
                1,
                bad_worker_function,
                worker_data,
                "test-task",
                {"key": "value"},
                result_queue,
            )
            result_queue.put.assert_called_once_with((1, "exception", ANY))
            mock_exit.assert_called_once_with(0)


# Test for signal handler with SIGTERM
def test_signal_handler_sigterm(mock_worker_function, caplog):
    """Test _signal_handler with SIGTERM signal."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        worker = Worker(mock_worker_function)

        with patch("signal.signal") as mock_signal:
            worker._signal_handler(signal.SIGTERM, None)
            assert worker._data.shutdown_event.is_set()
            mock_signal.assert_called_with(signal.SIGTERM, signal.SIG_DFL)
            assert "Received signal SIGTERM, initiating graceful shutdown" in caplog.text


@pytest.mark.asyncio
async def test_wait_for_shutdown_exiting_branch(caplog, mock_task_factory_empty):
    """Test _wait_for_shutdown exits when task source is empty and no processes remain."""
    with patch.object(sys, "argv", ["worker.py"]):
        worker = Worker(lambda tid, data, w: (False, None), task_source=mock_task_factory_empty)
        worker._task_source_is_empty = True
        worker._processes = {}
        worker._running = True
        worker._data.shutdown_event = MagicMock()
        worker._data.shutdown_event.is_set.return_value = False
        with patch("asyncio.sleep", return_value=None):
            with caplog.at_level(logging.INFO):
                await worker._wait_for_shutdown(interval=0.01)
    assert not worker._running
    assert any(
        "Local task source is empty and all processes complete; exiting" in r.message
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_wait_for_shutdown_waiting_branch(caplog, mock_task_factory_empty):
    """Test _wait_for_shutdown waits when task source is empty but processes remain."""
    with patch.object(sys, "argv", ["worker.py"]):
        worker = Worker(lambda tid, data, w: (False, None), task_source=mock_task_factory_empty)
        worker._task_source_is_empty = True
        mock_proc = MagicMock()
        worker._processes = {1: {"process": mock_proc, "task": {}, "start_time": 0}}
        worker._running = True
        worker._data.shutdown_event = MagicMock()
        worker._data.shutdown_event.is_set.return_value = False

        # Let the loop run once, then simulate all processes finished
        async def fake_sleep(interval: float) -> None:
            """Clear processes so _wait_for_shutdown sees none left."""
            worker._processes = {}
            return None

        with patch("asyncio.sleep", side_effect=fake_sleep):
            with caplog.at_level(logging.INFO):
                await worker._wait_for_shutdown(interval=0.01)
    assert worker._running is False
    assert any(
        "Local task source is empty; waiting for 1 processes to complete" in r.message
        for r in caplog.records
    )
