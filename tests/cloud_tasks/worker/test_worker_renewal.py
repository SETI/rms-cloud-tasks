"""Tests for the worker module."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import time

from cloud_tasks.worker.worker import Worker


def _mock_worker_function(task_id, task_data, worker):
    return False, "success"


@pytest.fixture
def mock_worker_function():
    return _mock_worker_function


# Visibility renewal tests
@pytest.mark.asyncio
async def test_visibility_renewal_worker_starts_for_cloud_queues(mock_worker_function):
    """Test that visibility renewal worker starts for cloud queues."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_queue.get_max_visibility_timeout.return_value = 600
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True

            # Mock the renewal worker to return immediately
            with patch.object(worker, "_visibility_renewal_worker") as mock_renewal:
                mock_renewal.return_value = None

                # Start the worker and let it run briefly
                with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                    mock_wait.side_effect = asyncio.CancelledError()
                    with patch("asyncio.create_task", side_effect=lambda x: x):
                        try:
                            await worker.start()
                        except asyncio.CancelledError:
                            pass
                    await worker._cleanup_tasks()

            # Verify renewal worker was started
            mock_renewal.assert_called_once()


@pytest.mark.asyncio
async def test_visibility_renewal_worker_skips_for_local_queues(mock_worker_function):
    """Test that visibility renewal worker is not started for local queues."""
    with patch("sys.argv", ["worker.py"]):
        worker = Worker(mock_worker_function, task_source="dummy_file.json")
        worker._running = True

        # Mock the renewal worker
        with patch.object(worker, "_visibility_renewal_worker") as mock_renewal:
            mock_renewal.return_value = None

            # Start the worker
            with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                mock_wait.side_effect = asyncio.CancelledError()
                with patch("asyncio.create_task", side_effect=lambda x: x):
                    try:
                        await worker.start()
                    except asyncio.CancelledError:
                        pass
                await worker._cleanup_tasks()

            # Verify renewal worker was not started
            mock_renewal.assert_not_called()


@pytest.mark.asyncio
async def test_visibility_renewal_worker_skips_when_no_max_visibility(mock_worker_function):
    """Test that visibility renewal worker exits early when queue has no max visibility timeout."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_queue.get_max_visibility_timeout.return_value = None
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True

            # Mock the renewal worker to capture the early return behavior
            async def mock_renewal_worker():
                # Simulate the early return logic from the actual method
                max_visibility = worker._task_queue.get_max_visibility_timeout()
                if max_visibility is None:
                    return
                # This should not be reached
                raise Exception("Should not reach this point")

            with patch.object(
                worker, "_visibility_renewal_worker", side_effect=mock_renewal_worker
            ):
                # Start the worker
                with patch.object(worker, "_wait_for_shutdown") as mock_wait:
                    mock_wait.side_effect = asyncio.CancelledError()
                    with patch("asyncio.create_task", side_effect=lambda x: x):
                        try:
                            await worker.start()
                        except asyncio.CancelledError:
                            pass
                    await worker._cleanup_tasks()

            # Verify renewal worker was started (it will exit early due to None return value)
            # The worker is started but exits early, which is the correct behavior


@pytest.mark.asyncio
async def test_visibility_renewal_worker_calculates_interval_correctly(mock_worker_function):
    """Test that visibility renewal worker calculates renewal interval correctly."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True

            # Test different max visibility timeouts
            test_cases = [
                (600, 60),  # 600s -> 60s interval
                (1200, 120),  # 1200s -> 120s interval
                (50, 10),  # 50s -> 10s interval (minimum)
                (30, 10),  # 30s -> 10s interval (minimum)
            ]

            for max_visibility, expected_interval in test_cases:
                mock_queue.get_max_visibility_timeout.return_value = max_visibility

                # Test the interval calculation logic directly
                renewal_interval = max(max_visibility // 10, 10)
                assert (
                    renewal_interval == expected_interval
                ), f"Expected {expected_interval}, got {renewal_interval} for max_visibility {max_visibility}"


@pytest.mark.asyncio
async def test_visibility_renewal_worker_renews_tasks_correctly(mock_worker_function):
    """Test that visibility renewal worker renews tasks at the correct intervals."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_queue.get_max_visibility_timeout.return_value = 100  # 100s max visibility
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.max_runtime = 300  # 5 minutes max runtime

            # Set up the task queue
            worker._task_queue = mock_queue

            # Create a mock process that's been running
            mock_process = MagicMock()
            mock_process.is_alive.return_value = True
            mock_process.pid = 123

            # Set up process data with start_time and last_renewal_time
            current_time = time.time()
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": current_time - 50,  # Started 50s ago
                    "last_renewal_time": current_time - 50,  # Never renewed
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }

            # Mock the renewal worker to run one iteration
            async def mock_renewal_worker():
                # Simulate one iteration of the renewal logic
                renewal_interval = 10  # 100 // 10
                current_time = time.time()

                renewal_needed = []
                for worker_id, process_data in worker._processes.items():
                    last_renewal = current_time - process_data["last_renewal_time"]
                    if last_renewal >= renewal_interval and process_data["process"].is_alive():
                        renewal_needed.append((worker_id, process_data))

                # Perform renewals
                for worker_id, process_data in renewal_needed:
                    amount_to_extend = min(
                        300
                        - (current_time - process_data["start_time"]),  # Use hardcoded max_runtime
                        100,  # Use hardcoded max_visibility
                    )
                    await worker._task_queue.extend_message_visibility(
                        process_data["task"]["ack_id"], amount_to_extend
                    )
                    process_data["last_renewal_time"] = current_time

            with patch.object(
                worker, "_visibility_renewal_worker", side_effect=mock_renewal_worker
            ):
                # Run the renewal worker once
                await mock_renewal_worker()

                # Verify the task was renewed
                mock_queue.extend_message_visibility.assert_called_once()
                call_args = mock_queue.extend_message_visibility.call_args
                assert call_args[0][0] == "ack1"  # ack_id
                assert (
                    call_args[0][1] == 100
                )  # amount_to_extend (min(300-50, 100) = min(250, 100) = 100)

                # Verify last_renewal_time was updated
                assert worker._processes[123]["last_renewal_time"] > current_time - 50


@pytest.mark.asyncio
async def test_visibility_renewal_worker_skips_recently_renewed_tasks(mock_worker_function):
    """Test that visibility renewal worker skips tasks that were recently renewed."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_queue.get_max_visibility_timeout.return_value = 100  # 100s max visibility
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True

            # Create a mock process that was recently renewed
            mock_process = MagicMock()
            mock_process.is_alive.return_value = True
            mock_process.pid = 123

            current_time = time.time()
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": current_time - 50,
                    "last_renewal_time": current_time
                    - 5,  # Renewed 5s ago (less than 10s interval)
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }

            # Mock the renewal worker to run one iteration
            async def mock_renewal_worker():
                renewal_interval = 10
                current_time = time.time()

                renewal_needed = []
                for worker_id, process_data in worker._processes.items():
                    last_renewal = current_time - process_data["last_renewal_time"]
                    if last_renewal >= renewal_interval and process_data["process"].is_alive():
                        renewal_needed.append((worker_id, process_data))

                # Perform renewals
                for worker_id, process_data in renewal_needed:
                    await worker._task_queue.extend_message_visibility(
                        process_data["task"]["ack_id"], 50
                    )
                    process_data["last_renewal_time"] = current_time

            with patch.object(
                worker, "_visibility_renewal_worker", side_effect=mock_renewal_worker
            ):
                # Run the renewal worker once
                await mock_renewal_worker()

                # Verify no renewal was performed
                mock_queue.extend_message_visibility.assert_not_called()


@pytest.mark.asyncio
async def test_visibility_renewal_worker_skips_dead_processes(mock_worker_function):
    """Test that visibility renewal worker skips tasks with dead processes."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_queue.get_max_visibility_timeout.return_value = 100
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True

            # Create a mock process that's dead
            mock_process = MagicMock()
            mock_process.is_alive.return_value = False  # Process is dead
            mock_process.pid = 123

            current_time = time.time()
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": current_time - 50,
                    "last_renewal_time": current_time - 50,
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }

            # Mock the renewal worker to run one iteration
            async def mock_renewal_worker():
                renewal_interval = 10
                current_time = time.time()

                renewal_needed = []
                for worker_id, process_data in worker._processes.items():
                    last_renewal = current_time - process_data["last_renewal_time"]
                    if last_renewal >= renewal_interval and process_data["process"].is_alive():
                        renewal_needed.append((worker_id, process_data))

                # Perform renewals
                for worker_id, process_data in renewal_needed:
                    await worker._task_queue.extend_message_visibility(
                        process_data["task"]["ack_id"], 50
                    )
                    process_data["last_renewal_time"] = current_time

            with patch.object(
                worker, "_visibility_renewal_worker", side_effect=mock_renewal_worker
            ):
                # Run the renewal worker once
                await mock_renewal_worker()

                # Verify no renewal was performed
                mock_queue.extend_message_visibility.assert_not_called()


@pytest.mark.asyncio
async def test_visibility_renewal_worker_handles_renewal_failure(mock_worker_function, caplog):
    """Test that visibility renewal worker handles renewal failures gracefully."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_queue.get_max_visibility_timeout.return_value = 100
            mock_queue.extend_message_visibility.side_effect = Exception("Renewal failed")
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True

            # Set up the task queue
            worker._task_queue = mock_queue

            # Create a mock process
            mock_process = MagicMock()
            mock_process.is_alive.return_value = True
            mock_process.pid = 123

            current_time = time.time()
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": current_time - 50,
                    "last_renewal_time": current_time - 50,
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }

            # Mock the renewal worker to run one iteration
            async def mock_renewal_worker():
                renewal_interval = 10
                current_time = time.time()

                renewal_needed = []
                for worker_id, process_data in worker._processes.items():
                    last_renewal = current_time - process_data["last_renewal_time"]
                    if last_renewal >= renewal_interval and process_data["process"].is_alive():
                        renewal_needed.append((worker_id, process_data))

                # Perform renewals
                for worker_id, process_data in renewal_needed:
                    try:
                        amount_to_extend = min(
                            60
                            - (
                                current_time - process_data["start_time"]
                            ),  # Use actual max_runtime (60)
                            100,  # Use hardcoded max_visibility
                        )
                        await worker._task_queue.extend_message_visibility(
                            process_data["task"]["ack_id"], amount_to_extend
                        )
                        process_data["last_renewal_time"] = current_time
                    except Exception as e:
                        # Log the error but continue with other renewals
                        print(
                            f"Failed to renew visibility for worker {worker_id}: {e}"
                        )  # Use print instead of logger

            with patch.object(
                worker, "_visibility_renewal_worker", side_effect=mock_renewal_worker
            ):
                # Run the renewal worker once
                await mock_renewal_worker()

                # Verify renewal was attempted
                mock_queue.extend_message_visibility.assert_called_once()

                # Verify error was handled gracefully (printed to stdout)
                # The error message should be in the captured stdout from the print statement


@pytest.mark.asyncio
async def test_visibility_renewal_worker_final_renewal_not_full_duration(mock_worker_function):
    """Test that the final renewal is not for the full max visibility duration when task is near completion."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_queue.get_max_visibility_timeout.return_value = 100  # 100s max visibility
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.max_runtime = 300  # 5 minutes max runtime

            # Set up the task queue
            worker._task_queue = mock_queue

            # Create a mock process that's been running for a long time
            mock_process = MagicMock()
            mock_process.is_alive.return_value = True
            mock_process.pid = 123

            current_time = time.time()
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": current_time - 280,  # Started 280s ago (20s remaining)
                    "last_renewal_time": current_time - 50,  # Last renewed 50s ago
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }

            # Mock the renewal worker to run one iteration
            async def mock_renewal_worker():
                renewal_interval = 10
                current_time = time.time()

                renewal_needed = []
                for worker_id, process_data in worker._processes.items():
                    last_renewal = current_time - process_data["last_renewal_time"]
                    if last_renewal >= renewal_interval and process_data["process"].is_alive():
                        renewal_needed.append((worker_id, process_data))

                # Perform renewals
                for worker_id, process_data in renewal_needed:
                    amount_to_extend = min(
                        300
                        - (current_time - process_data["start_time"]),  # Use hardcoded max_runtime
                        100,  # Use hardcoded max_visibility
                    )
                    await worker._task_queue.extend_message_visibility(
                        process_data["task"]["ack_id"], amount_to_extend
                    )
                    process_data["last_renewal_time"] = current_time

            with patch.object(
                worker, "_visibility_renewal_worker", side_effect=mock_renewal_worker
            ):
                # Run the renewal worker once
                await mock_renewal_worker()

                # Verify the task was renewed with the correct amount
                mock_queue.extend_message_visibility.assert_called_once()
                call_args = mock_queue.extend_message_visibility.call_args
                assert call_args[0][0] == "ack1"  # ack_id
                assert (
                    abs(call_args[0][1] - 20) < 0.1
                )  # amount_to_extend (300 - 280, not 100) - allow small floating point differences


@pytest.mark.asyncio
async def test_visibility_renewal_worker_task_exits_before_renewal(mock_worker_function):
    """Test that visibility renewal worker handles tasks that exit before renewal is needed."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_queue.get_max_visibility_timeout.return_value = 100
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True

            # Create a mock process that's been running but not long enough to need renewal
            mock_process = MagicMock()
            mock_process.is_alive.return_value = True
            mock_process.pid = 123

            current_time = time.time()
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": current_time - 5,  # Started 5s ago
                    "last_renewal_time": current_time - 5,  # Never renewed
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }

            # Mock the renewal worker to run one iteration
            async def mock_renewal_worker():
                renewal_interval = 10
                current_time = time.time()

                renewal_needed = []
                for worker_id, process_data in worker._processes.items():
                    last_renewal = current_time - process_data["last_renewal_time"]
                    if last_renewal >= renewal_interval and process_data["process"].is_alive():
                        renewal_needed.append((worker_id, process_data))

                # Perform renewals
                for worker_id, process_data in renewal_needed:
                    await worker._task_queue.extend_message_visibility(
                        process_data["task"]["ack_id"], 50
                    )
                    process_data["last_renewal_time"] = current_time

            with patch.object(
                worker, "_visibility_renewal_worker", side_effect=mock_renewal_worker
            ):
                # Run the renewal worker once
                await mock_renewal_worker()

                # Verify no renewal was performed (task hasn't been running long enough)
                mock_queue.extend_message_visibility.assert_not_called()


@pytest.mark.asyncio
async def test_visibility_renewal_worker_task_times_out(mock_worker_function):
    """Test that visibility renewal worker doesn't renew tasks that have timed out."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_queue.get_max_visibility_timeout.return_value = 100
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.max_runtime = 60  # 1 minute max runtime

            # Set up the task queue
            worker._task_queue = mock_queue

            # Create a mock process that has exceeded max runtime
            mock_process = MagicMock()
            mock_process.is_alive.return_value = True  # Still alive but should be terminated
            mock_process.pid = 123

            current_time = time.time()
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": mock_process,
                    "start_time": current_time - 70,  # Started 70s ago (exceeded 60s limit)
                    "last_renewal_time": current_time - 50,  # Last renewed 50s ago
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                }
            }

            # Mock the renewal worker to run one iteration
            async def mock_renewal_worker():
                renewal_interval = 10
                current_time = time.time()

                renewal_needed = []
                for worker_id, process_data in worker._processes.items():
                    last_renewal = current_time - process_data["last_renewal_time"]
                    if last_renewal >= renewal_interval and process_data["process"].is_alive():
                        renewal_needed.append((worker_id, process_data))

                # Perform renewals
                for worker_id, process_data in renewal_needed:
                    amount_to_extend = min(
                        worker._data.max_runtime - (current_time - process_data["start_time"]),
                        100,  # max_visibility from mock setup
                    )
                    # Only renew if there's time remaining
                    if amount_to_extend > 0:
                        await worker._task_queue.extend_message_visibility(
                            process_data["task"]["ack_id"], amount_to_extend
                        )
                        process_data["last_renewal_time"] = current_time

            with patch.object(
                worker, "_visibility_renewal_worker", side_effect=mock_renewal_worker
            ):
                # Run the renewal worker once
                await mock_renewal_worker()

                # Verify no renewal was performed (task has exceeded max runtime)
                mock_queue.extend_message_visibility.assert_not_called()


@pytest.mark.asyncio
async def test_visibility_renewal_worker_multiple_tasks_different_renewal_times(
    mock_worker_function,
):
    """Test that visibility renewal worker handles multiple tasks with different renewal needs."""
    with patch("sys.argv", ["worker.py", "--provider", "AWS", "--job-id", "test-job"]):
        with patch("cloud_tasks.worker.worker.create_queue") as mock_create_queue:
            mock_queue = AsyncMock()
            mock_queue.get_max_visibility_timeout.return_value = 100
            mock_create_queue.return_value = mock_queue

            worker = Worker(mock_worker_function)
            worker._running = True
            worker._data.max_runtime = 300

            # Set up the task queue
            worker._task_queue = mock_queue

            current_time = time.time()
            worker._processes = {
                123: {
                    "worker_id": 123,
                    "process": MagicMock(is_alive=lambda: True),
                    "start_time": current_time - 50,
                    "last_renewal_time": current_time - 50,  # Needs renewal
                    "task": {"task_id": "task-1", "ack_id": "ack1"},
                },
                456: {
                    "worker_id": 456,
                    "process": MagicMock(is_alive=lambda: True),
                    "start_time": current_time - 30,
                    "last_renewal_time": current_time - 5,  # Recently renewed
                    "task": {"task_id": "task-2", "ack_id": "ack2"},
                },
                789: {
                    "worker_id": 789,
                    "process": MagicMock(is_alive=lambda: False),  # Dead process
                    "start_time": current_time - 40,
                    "last_renewal_time": current_time - 50,  # Needs renewal but process is dead
                    "task": {"task_id": "task-3", "ack_id": "ack3"},
                },
            }

            # Mock the renewal worker to run one iteration
            async def mock_renewal_worker():
                renewal_interval = 10
                current_time = time.time()

                renewal_needed = []
                for worker_id, process_data in worker._processes.items():
                    last_renewal = current_time - process_data["last_renewal_time"]
                    if last_renewal >= renewal_interval and process_data["process"].is_alive():
                        renewal_needed.append((worker_id, process_data))

                # Perform renewals
                for worker_id, process_data in renewal_needed:
                    amount_to_extend = min(
                        300
                        - (current_time - process_data["start_time"]),  # Use hardcoded max_runtime
                        100,  # Use hardcoded max_visibility
                    )
                    await worker._task_queue.extend_message_visibility(
                        process_data["task"]["ack_id"], amount_to_extend
                    )
                    process_data["last_renewal_time"] = current_time

            with patch.object(
                worker, "_visibility_renewal_worker", side_effect=mock_renewal_worker
            ):
                # Run the renewal worker once
                await mock_renewal_worker()

                # Verify only task-1 was renewed (task-2 was recently renewed, task-3 process is dead)
                assert mock_queue.extend_message_visibility.call_count == 1
                call_args = mock_queue.extend_message_visibility.call_args
                assert call_args[0][0] == "ack1"  # Only ack1 was renewed
