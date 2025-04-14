"""Unit tests for the InstanceOrchestrator."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from cloud_tasks.instance_manager.orchestrator import InstanceOrchestrator
from cloud_tasks.common.config import Config


@pytest.fixture
def mock_config():
    """Create a mock configuration object."""
    config = MagicMock(spec=Config)

    # Mock provider config
    provider_config = MagicMock()
    provider_config.job_id = "test-job"
    provider_config.queue_name = "test-queue"
    provider_config.region = "us-central1"
    provider_config.zone = "us-central1-a"
    provider_config.startup_script = "#!/bin/bash\necho 'Hello World'"

    # Mock run config
    run_config = MagicMock()
    run_config.min_instances = 1
    run_config.max_instances = 10
    run_config.cpus_per_task = 2
    run_config.min_tasks_per_instance = 1
    run_config.max_tasks_per_instance = 4
    run_config.min_cpu = 2
    run_config.max_cpu = 8
    run_config.min_total_memory = 4
    run_config.max_total_memory = 32
    run_config.min_memory_per_cpu = 2
    run_config.max_memory_per_cpu = 4
    run_config.min_local_ssd = 0
    run_config.max_local_ssd = 375
    run_config.min_local_ssd_per_cpu = 0
    run_config.max_local_ssd_per_cpu = 100
    run_config.use_spot = False
    run_config.instance_types = ["n1-standard-2", "n1-standard-4"]
    run_config.image = "ubuntu-2404-lts"
    run_config.startup_script = "#!/bin/bash\necho 'Hello World'"

    # Setup config return values
    config.provider = "gcp"
    config.get_provider_config.return_value = provider_config
    config.run = run_config

    return config


@pytest.fixture
def orchestrator(mock_config):
    """Create an InstanceOrchestrator with mocked dependencies."""
    with patch(
        "cloud_tasks.instance_manager.create_instance_manager"
    ) as mock_create_instance_manager:
        with patch("cloud_tasks.queue_manager.create_queue") as mock_create_queue:
            # Create orchestrator
            orchestrator = InstanceOrchestrator(mock_config)

            # Setup orchestrator for testing
            orchestrator._instance_manager = AsyncMock()
            orchestrator._task_queue = AsyncMock()
            orchestrator._optimal_instance_type = "n1-standard-2"
            orchestrator._optimal_instance_zone = "us-central1-a"
            orchestrator._optimal_instance_price = 5.75

            # Override start_instance_max_threads for testing
            orchestrator._start_instance_max_threads = 3

            yield orchestrator


@pytest.mark.asyncio
async def test_provision_instances_parallel(orchestrator):
    """Test that instances are provisioned in parallel with a maximum concurrency limit."""
    # Arrange
    instance_count = 5

    # Track concurrency metrics
    concurrent_starts = 0
    max_concurrent_starts = 0
    start_times = []
    original_semaphore_enter = asyncio.Semaphore.__aenter__
    original_semaphore_exit = asyncio.Semaphore.__aexit__

    # Create a delayed mocked start_instance method to simulate real-world behavior
    async def delayed_start_instance(*args, **kwargs):
        nonlocal concurrent_starts, max_concurrent_starts
        concurrent_starts += 1
        max_concurrent_starts = max(max_concurrent_starts, concurrent_starts)

        # Add a timestamp to track when this instance was started
        start_times.append(asyncio.get_event_loop().time())

        # Simulate API call time
        await asyncio.sleep(0.2)

        concurrent_starts -= 1
        return f"instance-{len(start_times)}"

    orchestrator._instance_manager.start_instance = AsyncMock(side_effect=delayed_start_instance)

    # Mock the generate_worker_startup_script method
    orchestrator._generate_worker_startup_script = MagicMock(
        return_value="#!/bin/bash\necho 'Mocked script'"
    )

    # Act
    instance_ids = await orchestrator.provision_instances(instance_count)

    # Assert
    assert len(instance_ids) == instance_count
    assert max_concurrent_starts <= orchestrator._start_instance_max_threads

    # Verify that the instance manager's start_instance was called the correct number of times
    assert orchestrator._instance_manager.start_instance.call_count == instance_count

    # Calculate time metrics
    # First instance start time
    first_start = min(start_times) if start_times else 0
    # Last instance start time
    last_start = max(start_times) if start_times else 0

    # If execution was sequential, time between first and last start would be (count-1) * delay
    sequential_time = 0.2 * (instance_count - 1)
    actual_time = last_start - first_start

    # In parallel execution, the time between first and last start should be less
    # We check against 0.8 * sequential time to allow for some test timing variability
    assert (
        actual_time < 0.8 * sequential_time
    ), f"Expected parallel execution to be faster than sequential (actual: {actual_time}, sequential: {sequential_time})"


@pytest.mark.asyncio
async def test_provision_instances_handles_failures(orchestrator):
    """Test that provision_instances correctly handles instance creation failures."""
    # Arrange
    instance_count = 5

    # Mock start_instance to fail for every other instance
    call_count = 0

    async def mock_start_instance(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count % 2 == 0:
            raise RuntimeError("Instance creation failed")
        return f"instance-{call_count}"

    orchestrator._instance_manager.start_instance = AsyncMock(side_effect=mock_start_instance)

    # Mock the generate_worker_startup_script method
    orchestrator._generate_worker_startup_script = MagicMock(
        return_value="#!/bin/bash\necho 'Mocked script'"
    )

    # Act
    instance_ids = await orchestrator.provision_instances(instance_count)

    # Assert
    # Should have 3 successful instances (odd-numbered calls: 1, 3, 5)
    assert len(instance_ids) == 3

    # Verify that start_instance was called 5 times (even if some failed)
    assert orchestrator._instance_manager.start_instance.call_count == instance_count
