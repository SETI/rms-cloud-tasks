"""Unit tests for the InstanceOrchestrator."""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from cloud_tasks.common.config import Config
from cloud_tasks.instance_manager.orchestrator import InstanceOrchestrator


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

    # Add price-related attributes
    run_config.min_total_price_per_hour = 0.0
    run_config.max_total_price_per_hour = 100.0
    run_config.min_boot_disk = 10
    run_config.max_boot_disk = 100
    run_config.min_boot_disk_per_cpu = 5
    run_config.max_boot_disk_per_cpu = 20
    run_config.min_total_cpus = 2
    run_config.max_total_cpus = 16
    run_config.min_simultaneous_tasks = 1
    run_config.max_simultaneous_tasks = 8
    run_config.instance_termination_delay = 300
    run_config.scaling_check_interval = 60
    run_config.worker_use_new_process = True

    # Setup config return values
    config.provider = "gcp"
    config.get_provider_config.return_value = provider_config
    config.run = run_config

    return config


@pytest.fixture
def orchestrator(mock_config):
    """Create an InstanceOrchestrator with mocked dependencies."""
    # Create orchestrator
    orchestrator = InstanceOrchestrator(mock_config)

    # Setup orchestrator for testing
    orchestrator._instance_manager = AsyncMock()
    orchestrator._task_queue = AsyncMock()
    orchestrator._optimal_instance_info = {
        "name": "n1-standard-2",
        "vcpu": 2,
        "mem_gb": 8,
        "local_ssd_gb": 0,
        "total_price": 5.75,
        "zone": "us-central1-a",
        "boot_disk_type": "pd-balanced",
        "boot_disk_iops": None,
        "boot_disk_throughput": None,
    }
    orchestrator._optimal_instance_boot_disk_size = 20
    orchestrator._optimal_instance_num_tasks = 2

    # Override start_instance_max_threads for testing
    orchestrator._start_instance_max_threads = 3

    yield orchestrator


def test_orchestrator_init_missing_job_id(mock_config: Mock) -> None:
    """InstanceOrchestrator.__init__ raises ValueError when provider_config.job_id is missing."""
    mock_config.get_provider_config.return_value.job_id = None
    with pytest.raises(ValueError) as exc_info:
        InstanceOrchestrator(mock_config)
    assert "job_id" in str(exc_info.value).lower()


def test_orchestrator_init_missing_queue_name(mock_config: Mock) -> None:
    """InstanceOrchestrator.__init__ raises ValueError when provider_config.queue_name is missing."""
    mock_config.get_provider_config.return_value.queue_name = None
    with pytest.raises(ValueError) as exc_info:
        InstanceOrchestrator(mock_config)
    assert "queue_name" in str(exc_info.value).lower() or "queue" in str(exc_info.value).lower()


def test_orchestrator_init_missing_run_config(mock_config: Mock) -> None:
    """InstanceOrchestrator.__init__ raises ValueError when config.run is missing."""
    mock_config.run = None
    with pytest.raises(ValueError) as exc_info:
        InstanceOrchestrator(mock_config)
    assert "run" in str(exc_info.value).lower() or "configuration" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_get_job_instances_list_raises(orchestrator: InstanceOrchestrator) -> None:
    """get_job_instances returns error tuple when list_job_instances raises."""
    assert orchestrator._instance_manager is not None
    with patch.object(orchestrator, "_initialize_pricing_info", new_callable=AsyncMock):
        orchestrator._instance_manager.list_running_instances = AsyncMock(
            side_effect=RuntimeError("api error")
        )
        num_running, running_cpus, running_price, summary = await orchestrator.get_job_instances()
    assert num_running == 0
    assert running_cpus == 0
    assert running_price == 0.0
    assert "error" in summary.lower()


@pytest.mark.asyncio
async def test_get_job_instances_empty_running(orchestrator: InstanceOrchestrator) -> None:
    """get_job_instances returns zero counts and 'No running instances' when list is empty."""
    assert orchestrator._instance_manager is not None
    with patch.object(orchestrator, "_initialize_pricing_info", new_callable=AsyncMock):
        orchestrator._instance_manager.list_running_instances = AsyncMock(return_value=[])
        num_running, running_cpus, running_price, summary = await orchestrator.get_job_instances()
    assert num_running == 0
    assert running_cpus == 0
    assert running_price == 0.0
    assert "no running" in summary.lower()


@pytest.mark.asyncio
async def test_provision_instances_parallel(orchestrator):
    """Test that instances are provisioned in parallel with a maximum concurrency limit."""
    # Arrange
    instance_count = 5

    # Track concurrency metrics
    concurrent_starts = 0
    max_concurrent_starts = 0
    start_times = []

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
        instance_id = f"instance-{len(start_times)}"
        return instance_id, "us-central1-a"

    orchestrator._instance_manager.start_instance = AsyncMock(side_effect=delayed_start_instance)

    # Mock the generate_worker_startup_script method
    orchestrator._generate_worker_startup_script = MagicMock(
        return_value="#!/bin/bash\necho 'Mocked script'"
    )

    # Act
    instance_ids = await orchestrator._provision_instances(instance_count)

    # Assert
    assert len(instance_ids) == instance_count
    assert max_concurrent_starts <= orchestrator._start_instance_max_threads

    # Verify that the instance manager's start_instance was called the correct number of times
    assert orchestrator._instance_manager.start_instance.call_count == instance_count

    # Assert parallelism: more than one start was in flight at once
    assert max_concurrent_starts > 1, "Expected parallel starts (max_concurrent_starts > 1)"


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
        instance_id = f"instance-{call_count}"
        return instance_id, "us-central1-a"

    orchestrator._instance_manager.start_instance = AsyncMock(side_effect=mock_start_instance)

    # Mock the generate_worker_startup_script method
    orchestrator._generate_worker_startup_script = MagicMock(
        return_value="#!/bin/bash\necho 'Mocked script'"
    )

    # Act
    instance_ids = await orchestrator._provision_instances(instance_count)

    # Assert
    # Should have 3 successful instances (odd-numbered calls: 1, 3, 5)
    assert len(instance_ids) == 3

    # Verify that start_instance was called 5 times (even if some failed)
    assert orchestrator._instance_manager.start_instance.call_count == instance_count


@pytest.mark.asyncio
async def test_dry_run_prevents_instance_creation(orchestrator):
    """Test that dry_run mode prevents instance creation and sets running to False."""
    # Arrange
    orchestrator._dry_run = True
    orchestrator._instance_manager.start_instance = AsyncMock()
    orchestrator._task_queue.get_queue_depth = AsyncMock(return_value=10)  # Non-empty queue

    # Mock the optimal instance info
    optimal_instance_info = {
        "name": "n1-standard-2",
        "vcpu": 2,
        "mem_gb": 8,
        "local_ssd_gb": 0,
        "total_price": 5.75,
        "zone": "us-central1-a",
        "boot_disk_type": "pd-balanced",
        "boot_disk_iops": None,
        "boot_disk_throughput": None,
        "boot_disk_gb": 20,
    }
    orchestrator._instance_manager.get_optimal_instance_type = AsyncMock(
        return_value=optimal_instance_info
    )

    # Act
    await orchestrator.start()

    # Assert
    # Verify that no instances were started
    orchestrator._instance_manager.start_instance.assert_not_called()

    # Verify that running is set to False after start()
    assert not orchestrator.is_running

    # Verify that scaling task was not created
    assert orchestrator._scaling_task is None


def _make_instance(instance_id: str, state: str = "running") -> dict:
    """Build a minimal instance dict as returned by list_job_instances."""
    return {
        "id": instance_id,
        "type": "n1-standard-2",
        "state": state,
        "zone": "us-central1-a",
        "creation_time": "2026-01-01T00:00:00+00:00",
    }


def test_record_keepalive(orchestrator):
    """record_keepalive tracks the last-heard time and that any instance was heard."""
    assert not orchestrator._keepalive_ever_heard
    orchestrator.record_keepalive("instance-1", "2026-01-01T00:00:00+00:00")
    assert "instance-1" in orchestrator._keepalive_last_heard
    assert orchestrator._keepalive_ever_heard
    assert orchestrator.keepalive_abort_reason is None


@pytest.mark.asyncio
async def test_check_keepalives_terminates_silent_instance(orchestrator):
    """An instance that stops sending keep-alives is terminated."""
    import time

    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0
    orchestrator._running = True
    orchestrator._instance_manager.list_running_instances = AsyncMock(
        return_value=[_make_instance("instance-1"), _make_instance("instance-2")]
    )
    orchestrator.record_keepalive("instance-1")
    orchestrator.record_keepalive("instance-2")
    # instance-2 has been silent for longer than the keep-alive timeout
    orchestrator._keepalive_last_heard["instance-2"] = time.time() - 400

    await orchestrator._check_keepalives()

    orchestrator._instance_manager.terminate_instance.assert_awaited_once_with(
        "instance-2", "us-central1-a"
    )
    assert "instance-2" not in orchestrator._keepalive_last_heard
    assert orchestrator.keepalive_abort_reason is None
    # Terminating a single crashed instance must not stop the whole job
    assert orchestrator._running is True


@pytest.mark.asyncio
async def test_check_keepalives_aborts_when_no_instance_ever_heard(orchestrator):
    """If every instance misses the startup timeout and none was ever heard, abort the job."""
    import time

    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0
    orchestrator._running = True
    orchestrator._instance_manager.list_running_instances = AsyncMock(
        return_value=[_make_instance("instance-1"), _make_instance("instance-2")]
    )
    orchestrator.terminate_all_instances = AsyncMock()
    # Both instances were first seen long ago and never sent a keep-alive
    orchestrator._keepalive_first_seen = {
        "instance-1": time.time() - 700,
        "instance-2": time.time() - 700,
    }

    await orchestrator._check_keepalives()

    orchestrator.terminate_all_instances.assert_awaited_once()
    assert orchestrator.keepalive_abort_reason is not None
    assert orchestrator._running is False
    orchestrator._instance_manager.terminate_instance.assert_not_awaited()


@pytest.mark.asyncio
async def test_check_keepalives_waits_for_young_instances(orchestrator):
    """If some instances are still within the startup window, nothing is terminated yet."""
    import time

    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0
    orchestrator._running = True
    orchestrator._instance_manager.list_running_instances = AsyncMock(
        return_value=[_make_instance("instance-1"), _make_instance("instance-2")]
    )
    orchestrator.terminate_all_instances = AsyncMock()
    # instance-1 is overdue but instance-2 was just seen; nothing was ever heard
    orchestrator._keepalive_first_seen = {"instance-1": time.time() - 700}

    await orchestrator._check_keepalives()

    orchestrator.terminate_all_instances.assert_not_awaited()
    orchestrator._instance_manager.terminate_instance.assert_not_awaited()
    assert orchestrator.keepalive_abort_reason is None
    assert orchestrator._running is True


@pytest.mark.asyncio
async def test_check_keepalives_terminates_startup_failure_when_others_alive(orchestrator):
    """If other instances are alive, a startup-timeout instance is terminated individually."""
    import time

    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0
    orchestrator._running = True
    orchestrator._instance_manager.list_running_instances = AsyncMock(
        return_value=[_make_instance("instance-1"), _make_instance("instance-2")]
    )
    orchestrator.terminate_all_instances = AsyncMock()
    orchestrator.record_keepalive("instance-1")
    orchestrator._keepalive_first_seen = {
        "instance-1": time.time() - 700,
        "instance-2": time.time() - 700,
    }

    await orchestrator._check_keepalives()

    orchestrator.terminate_all_instances.assert_not_awaited()
    orchestrator._instance_manager.terminate_instance.assert_awaited_once_with(
        "instance-2", "us-central1-a"
    )
    assert orchestrator.keepalive_abort_reason is None
    assert orchestrator._running is True


@pytest.mark.asyncio
async def test_check_keepalives_disabled_timeouts(orchestrator):
    """Timeouts of 0 disable the keep-alive checks."""
    import time

    orchestrator._keepalive_startup_timeout = 0.0
    orchestrator._keepalive_timeout = 0.0
    orchestrator._running = True
    orchestrator._instance_manager.list_running_instances = AsyncMock(
        return_value=[_make_instance("instance-1")]
    )
    orchestrator.terminate_all_instances = AsyncMock()
    orchestrator._keepalive_first_seen = {"instance-1": time.time() - 100000}
    orchestrator.record_keepalive("instance-1")
    orchestrator._keepalive_last_heard["instance-1"] = time.time() - 100000

    await orchestrator._check_keepalives()

    orchestrator.terminate_all_instances.assert_not_awaited()
    orchestrator._instance_manager.terminate_instance.assert_not_awaited()
    assert orchestrator._running is True


@pytest.mark.asyncio
async def test_check_keepalives_cleans_up_gone_instances(orchestrator):
    """Tracking data is dropped for instances that no longer exist."""
    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0
    orchestrator._instance_manager.list_running_instances = AsyncMock(
        return_value=[_make_instance("instance-1")]
    )
    orchestrator.record_keepalive("instance-1")
    orchestrator.record_keepalive("instance-gone")
    orchestrator._keepalive_first_seen["instance-gone"] = 1.0

    await orchestrator._check_keepalives()

    assert "instance-gone" not in orchestrator._keepalive_last_heard
    assert "instance-gone" not in orchestrator._keepalive_first_seen
    assert "instance-1" in orchestrator._keepalive_last_heard


def test_startup_script_exports_keepalive_interval(orchestrator, mock_config):
    """The generated startup script exports the configured keep-alive interval."""
    mock_config.run.keepalive_interval = 45
    mock_config.run.startup_script = "#!/bin/bash\necho 'Hello World'"
    script = orchestrator._generate_worker_startup_script()
    assert "export RMS_CLOUD_TASKS_KEEPALIVE_INTERVAL=45" in script


def test_startup_script_omits_keepalive_interval_when_unset(orchestrator, mock_config):
    """The generated startup script omits the keep-alive interval when not configured."""
    mock_config.run.keepalive_interval = None
    mock_config.run.startup_script = "#!/bin/bash\necho 'Hello World'"
    script = orchestrator._generate_worker_startup_script()
    assert "RMS_CLOUD_TASKS_KEEPALIVE_INTERVAL" not in script


def test_startup_script_exports_max_memory(orchestrator, mock_config):
    """The generated startup script exports the configured memory limit."""
    mock_config.run.keepalive_interval = None
    mock_config.run.max_memory_allowed_per_task = 2.5
    mock_config.run.startup_script = "#!/bin/bash\necho 'Hello World'"
    script = orchestrator._generate_worker_startup_script()
    assert "export RMS_CLOUD_TASKS_MAX_MEMORY_ALLOWED_PER_TASK=2.5" in script


def test_startup_script_omits_max_memory_when_unset(orchestrator, mock_config):
    """The generated startup script omits the memory limit when not configured."""
    mock_config.run.keepalive_interval = None
    mock_config.run.max_memory_allowed_per_task = None
    mock_config.run.startup_script = "#!/bin/bash\necho 'Hello World'"
    script = orchestrator._generate_worker_startup_script()
    assert "RMS_CLOUD_TASKS_MAX_MEMORY_ALLOWED_PER_TASK" not in script


@pytest.mark.asyncio
async def test_check_keepalives_abort_terminates_starting_instances(orchestrator):
    """A full abort terminates instances still in the 'starting' state, not just 'running'."""
    import time

    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0
    orchestrator._running = True
    orchestrator._instance_manager.list_running_instances = AsyncMock(
        return_value=[
            _make_instance("instance-1", state="running"),
            _make_instance("instance-2", state="starting"),
        ]
    )
    orchestrator._keepalive_first_seen = {
        "instance-1": time.time() - 700,
        "instance-2": time.time() - 700,
    }

    # Use the real terminate_all_instances so the state filter is exercised
    await orchestrator._check_keepalives()

    assert orchestrator.keepalive_abort_reason is not None
    assert orchestrator._running is False
    terminated = {
        call.args[0] for call in orchestrator._instance_manager.terminate_instance.await_args_list
    }
    assert terminated == {"instance-1", "instance-2"}


def _capture_instance_details(orchestrator, instances, caplog) -> list[str]:
    """Run _log_instance_details at DEBUG level and return the emitted lines."""
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="cloud_tasks.instance_manager.orchestrator"):
        orchestrator._log_instance_details(instances)
    return [record.getMessage() for record in caplog.records]


def test_log_instance_details_skipped_without_debug(orchestrator, caplog) -> None:
    """The instance table costs a line per instance, so it is only emitted at DEBUG."""
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="cloud_tasks.instance_manager.orchestrator"):
        orchestrator._log_instance_details([_make_instance("instance-1")])
    assert caplog.records == []


def test_log_instance_details_no_instances(orchestrator, caplog) -> None:
    """With no instances the table collapses to a single line, with no header."""
    lines = _capture_instance_details(orchestrator, [], caplog)
    assert lines == ["Instance details: no instances"]


def test_log_instance_details_rows_sorted_with_details(orchestrator, caplog) -> None:
    """One row per instance, sorted by ID, carrying the instance's details."""
    orchestrator._running = True
    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0
    instances = [_make_instance("instance-2"), _make_instance("instance-1", state="starting")]

    lines = _capture_instance_details(orchestrator, instances, caplog)

    assert lines[0] == "Instance details:"
    assert "Instance ID" in lines[1] and "Keep-Alive" in lines[1] and "Status" in lines[1]
    # The separator spans the whole table, including rows wider than the header
    assert set(lines[2].strip()) == {"-"}
    assert len(lines[2]) == max(len(line) for line in lines[1:])
    assert len(lines) == 5
    assert lines[3].split() == [
        "instance-1",
        "n1-standard-2",
        "starting",
        "us-central1-a",
        "2026-01-01T00:00:00",
        "never",
        "awaiting",
        "first",
        "keep-alive",
    ]
    assert lines[4].startswith("  instance-2 ")


def test_log_instance_details_columns_size_to_content(orchestrator, caplog) -> None:
    """Columns widen to fit their contents so long GCP instance names stay aligned."""
    orchestrator._running = True
    long_id = "rmscr-parallel-addition-job-1riovtucuu1o1dx9lotafw5pb"
    instances = [
        _make_instance(long_id),
        _make_instance("short"),
    ]

    lines = _capture_instance_details(orchestrator, instances, caplog)

    header, rows = lines[1], lines[3:]
    assert long_id in rows[0]
    # Every column of every row starts at the same offset as its header
    for column in ("Type", "State", "Zone", "Created", "Keep-Alive", "Status"):
        offset = header.index(column)
        for row in rows:
            assert row[offset - 1] == " "
            assert row[offset] != " "


def test_log_instance_details_columns_never_narrower_than_headers(orchestrator, caplog) -> None:
    """Short values don't squeeze a column below the width of its own header."""
    orchestrator._running = True

    lines = _capture_instance_details(orchestrator, [_make_instance("i-0abc")], caplog)

    header, row = lines[1], lines[3]
    assert header.index("Type") == row.index("n1-standard-2")
    assert header.index("State") == row.index("running")


def test_log_instance_details_keepalive_states(orchestrator, caplog) -> None:
    """Each keep-alive state - healthy, overdue, and never heard from - is reported."""
    import time

    orchestrator._running = True
    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0
    now = time.time()
    instances = [
        _make_instance("healthy"),
        _make_instance("silent"),
        _make_instance("young", state="starting"),
        _make_instance("never-started"),
        _make_instance("gone", state="terminated"),
    ]
    orchestrator._keepalive_last_heard = {"healthy": now - 60, "silent": now - 400}
    orchestrator._keepalive_first_seen = {
        "healthy": now - 1000,
        "silent": now - 1000,
        "young": now - 120,
        "never-started": now - 900,
    }

    rows = {
        line.split()[0]: line
        for line in _capture_instance_details(orchestrator, instances, caplog)[3:]
    }

    assert "60s ago" in rows["healthy"]
    assert "OK" in rows["healthy"] and "OVERDUE" not in rows["healthy"]

    assert "400s ago" in rows["silent"]
    assert "OVERDUE by 100s (limit 300s)" in rows["silent"]

    assert "never" in rows["young"]
    assert "awaiting first keep-alive (120s of 600s)" in rows["young"]

    assert "never" in rows["never-started"]
    assert "OVERDUE by 300s for its first keep-alive (limit 600s)" in rows["never-started"]

    # Terminated instances aren't monitored, so they get no keep-alive verdict
    assert "not active" in rows["gone"]


def test_log_instance_details_not_monitored_when_not_running(orchestrator, caplog) -> None:
    """The status command builds an orchestrator that never receives keep-alives.

    It must not report every healthy worker as silent, so the keep-alive columns say
    the instances aren't being monitored rather than that they never checked in.
    """
    orchestrator._running = False
    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0

    lines = _capture_instance_details(orchestrator, [_make_instance("instance-1")], caplog)

    assert "not monitored" in lines[3]
    assert "OVERDUE" not in lines[3]


def test_log_instance_details_not_monitored_when_timeouts_disabled(orchestrator, caplog) -> None:
    """With both timeouts disabled there is nothing to be overdue against."""
    orchestrator._running = True
    orchestrator._keepalive_startup_timeout = 0.0
    orchestrator._keepalive_timeout = 0.0

    lines = _capture_instance_details(orchestrator, [_make_instance("instance-1")], caplog)

    assert "not monitored" in lines[3]


def test_log_instance_details_azure_missing_fields(orchestrator, caplog) -> None:
    """Azure instances report a location and no creation time; the row still renders."""
    orchestrator._running = True
    instance = {
        "id": "azure-vm-1",
        "type": "Standard_D4s_v3",
        "state": "running",
        "location": "eastus",
    }

    lines = _capture_instance_details(orchestrator, [instance], caplog)

    assert "azure-vm-1" in lines[3]
    assert "eastus" in lines[3]


@pytest.mark.asyncio
async def test_get_job_instances_logs_instance_details(orchestrator) -> None:
    """The instance summary is preceded by the per-instance debug table."""
    instances = [_make_instance("instance-1")]
    orchestrator.list_job_instances = AsyncMock(return_value=instances)
    orchestrator._initialize_pricing_info = AsyncMock()
    orchestrator._all_instance_info = {"n1-standard-2": {"vcpu": 2}}
    orchestrator._pricing_info = {
        "n1-standard-2": {"us-central1-a": {"pd-balanced": {"total_price": 1.0}}}
    }

    with patch.object(orchestrator, "_log_instance_details") as mock_log:
        await orchestrator.get_job_instances()

    mock_log.assert_called_once_with(instances)
