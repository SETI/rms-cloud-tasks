"""Unit tests for the InstanceOrchestrator."""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from cloud_tasks.common.config import Config, RunConfig
from cloud_tasks.instance_manager.instance_manager import InstanceManager
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
    # local_credential_warning and effective_cpus_per_task are synchronous on the real
    # interface. The credentials are fine by default; the vCPUs each task gets is worked
    # out by the real implementation, since what the orchestrator derives from it is what
    # these tests are about
    orchestrator._instance_manager.local_credential_warning = Mock(return_value=None)

    def effective_cpus_per_task(instance_info, constraints=None):
        """Work out the vCPUs per task the way a real instance manager would.

        Parameters:
            instance_info: Instance type attributes
            constraints: Constraint dict, or None

        Returns:
            float: vCPUs per task, from the real InstanceManager implementation.
        """
        return InstanceManager.effective_cpus_per_task(
            orchestrator._instance_manager, instance_info, constraints
        )

    orchestrator._instance_manager.effective_cpus_per_task = Mock(
        side_effect=effective_cpus_per_task
    )
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
    orchestrator._image_uri = (
        "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/"
        "ubuntu-2404-lts"
    )

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


def _make_instance(
    instance_id: str,
    state: str = "running",
    zone: str = "us-central1-a",
    instance_type: str = "n1-standard-2",
) -> dict:
    """Build a minimal instance dict as returned by list_job_instances."""
    return {
        "id": instance_id,
        "type": instance_type,
        "state": state,
        "zone": zone,
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


def _instance_table(orchestrator, instances) -> list[str]:
    """Render the instance table and return its lines.

    Parameters:
        orchestrator: The orchestrator under test
        instances: Instance dictionaries to render

    Returns:
        list[str]: The lines of the rendered table.
    """
    table, _num_running, _cpus, _price = orchestrator._build_instance_table(instances)
    return table.split("\n")


def _instance_rows(lines: list[str]) -> dict[str, str]:
    """Map each instance ID to its rendered row of the table.

    Parameters:
        lines: Lines of the rendered table

    Returns:
        dict[str, str]: Instance ID to the whole line describing it, with the border, the
        header and the totals row left out.
    """
    rows = {}
    for line in lines:
        if "\u2502" not in line:
            continue
        cells = [cell.strip() for cell in line.strip().strip("\u2502").split("\u2502")]
        if not cells[0] or cells[0] == "Instance ID" or "running/starting" in cells[0]:
            continue
        rows[cells[0]] = line
    return rows


@pytest.mark.asyncio
async def test_instance_table_is_the_summary_logged_by_a_run(orchestrator) -> None:
    """There is one table, and it is what the run reports.

    An instance table and a separate summary of the same instances by type meant reading
    two tables against each other to answer one question.
    """
    instances = [_make_instance("instance-1")]
    orchestrator.list_job_instances = AsyncMock(return_value=instances)
    orchestrator._initialize_pricing_info = AsyncMock()
    orchestrator._all_instance_info = {"n1-standard-2": {"vcpu": 2}}
    orchestrator._pricing_info = {
        "n1-standard-2": {"us-central1-a": {"pd-balanced": {"total_price": 1.0}}}
    }

    num_running, running_cpus, running_price, summary = await orchestrator.get_job_instances()

    assert (num_running, running_cpus, running_price) == (1, 2, 1.0)
    assert "instance-1" in summary
    assert "$1.00" in summary
    assert "1 running/starting" in summary
    # Every column of the old summary is in this one table
    for column in ("Boot Disk", "vCPUs", "Price/Hour", "Mode"):
        assert column in summary


@pytest.mark.asyncio
async def test_instance_table_totals_only_what_is_running(orchestrator) -> None:
    """Terminated instances are listed but cost nothing and count for nothing."""
    orchestrator._all_instance_info = {"n1-standard-2": {"vcpu": 2}}
    orchestrator._pricing_info = {
        "n1-standard-2": {"us-central1-a": {"pd-balanced": {"total_price": 1.0}}}
    }
    instances = [
        _make_instance("alive", state="running"),
        _make_instance("starting-up", state="starting"),
        _make_instance("gone", state="terminated"),
    ]

    table, num_running, running_cpus, running_price = orchestrator._build_instance_table(instances)

    assert (num_running, running_cpus, running_price) == (2, 4, 2.0)
    rows = _instance_rows(table.split("\n"))
    assert rows["gone"].endswith("- \u2502")
    assert "$1.00" in rows["alive"]
    assert "2 running/starting" in table


@pytest.mark.asyncio
async def test_instance_table_prices_from_a_wildcard_zone(orchestrator) -> None:
    """GCP prices per region, so a price may be recorded against a wildcard zone."""
    orchestrator._all_instance_info = {"n1-standard-2": {"vcpu": 2}}
    orchestrator._pricing_info = {
        "n1-standard-2": {"us-central1-*": {"pd-balanced": {"total_price": 2.5}}}
    }

    table, _num, _cpus, price = orchestrator._build_instance_table([_make_instance("instance-1")])

    assert price == 2.5
    assert "$2.50" in table


def test_instance_table_rows_sorted_with_details(orchestrator) -> None:
    """One row per instance, sorted by state then ID, carrying the instance's details."""
    orchestrator._running = True
    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0
    instances = [_make_instance("instance-2", state="starting"), _make_instance("instance-1")]

    lines = _instance_table(orchestrator, instances)

    header = next(line for line in lines if "Instance ID" in line)
    for column in ("Type", "State", "Zone", "Created", "Keep-Alive", "Mode"):
        assert column in header

    rows = _instance_rows(lines)
    assert list(rows) == ["instance-1", "instance-2"]
    cells = [cell.strip() for cell in rows["instance-2"].strip().strip("\u2502").split("\u2502")]
    assert cells[0] == "instance-2"
    assert cells[1] == "n1-standard-2"
    assert cells[5] == "starting"
    assert cells[6] == "us-central1-a"
    assert cells[7] == "2026-01-01T00:00:00"
    assert cells[9].startswith("waiting for first keep-alive")


def test_instance_table_orders_by_what_instances_are_doing(orchestrator) -> None:
    """Instances doing the work come first; the ones on their way out come last."""
    orchestrator._running = True
    instances = [
        _make_instance("d", state="terminated"),
        _make_instance("e", state="tortoise"),  # A state this code doesn't know about
        _make_instance("b", state="starting"),
        _make_instance("c", state="stopping"),
        _make_instance("a"),
    ]

    rows = _instance_rows(_instance_table(orchestrator, instances))

    assert list(rows) == ["a", "b", "c", "d", "e"]


def test_instance_table_columns_size_to_content(orchestrator) -> None:
    """Columns widen to fit their contents so long GCP instance names stay aligned."""
    orchestrator._running = True
    long_id = "rmscr-parallel-addition-job-1riovtucuu1o1dx9lotafw5pb"
    instances = [_make_instance(long_id), _make_instance("short")]

    lines = _instance_table(orchestrator, instances)

    assert long_id in "".join(lines)
    # Every line of the table proper is the same width, so the columns line up whatever is
    # in them; the caption underneath is prose and is not part of the table
    table_lines = [line for line in lines if line[:1] in "\u250c\u2502\u251c\u2514"]
    assert len(set(len(line) for line in table_lines)) == 1


def test_instance_table_keepalive_states(orchestrator) -> None:
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

    rows = _instance_rows(_instance_table(orchestrator, instances))

    assert "60s ago" in rows["healthy"]
    assert "keep-alive wait (60s of 300s)" in rows["healthy"]

    assert "400s ago" in rows["silent"]
    assert "keep-alive timed out (overdue by 100s of 300s)" in rows["silent"]

    assert "never" in rows["young"]
    assert "waiting for first keep-alive (120s of 600s)" in rows["young"]

    assert "never" in rows["never-started"]
    assert (
        "keep-alive timed out (first keep-alive overdue by 300s of 600s)" in rows["never-started"]
    )

    # Terminated instances aren't monitored, so they get no keep-alive verdict
    assert "not active" in rows["gone"]


def test_instance_table_not_monitored_when_not_running(orchestrator) -> None:
    """The status command builds an orchestrator that never receives keep-alives.

    It must not report every healthy worker as silent, so the keep-alive columns say
    the instances aren't being monitored rather than that they never checked in.
    """
    orchestrator._running = False
    orchestrator._keepalive_startup_timeout = 600.0
    orchestrator._keepalive_timeout = 300.0

    row = _instance_rows(_instance_table(orchestrator, [_make_instance("instance-1")]))[
        "instance-1"
    ]

    assert "not monitored" in row
    assert "timed out" not in row


def test_instance_table_not_monitored_when_timeouts_disabled(orchestrator) -> None:
    """With both timeouts disabled there is nothing to be overdue against."""
    orchestrator._running = True
    orchestrator._keepalive_startup_timeout = 0.0
    orchestrator._keepalive_timeout = 0.0

    row = _instance_rows(_instance_table(orchestrator, [_make_instance("instance-1")]))[
        "instance-1"
    ]

    assert "not monitored" in row


def test_instance_table_azure_missing_fields(orchestrator) -> None:
    """Azure instances report a location and no creation time; the row still renders."""
    orchestrator._running = True
    instance = {
        "id": "azure-vm-1",
        "type": "Standard_D4s_v3",
        "state": "running",
        "location": "eastus",
    }

    row = _instance_rows(_instance_table(orchestrator, [instance]))["azure-vm-1"]

    assert "eastus" in row


def test_keepalive_from_terminated_instance_is_ignored(orchestrator) -> None:
    """A keep-alive that arrives after we terminated an instance doesn't revive it.

    Workers send keep-alives on a timer, so one can be in flight when its instance is
    terminated. Believing it would put a dead instance back into the health monitor's
    books, and would make the "no instance has ever checked in" abort think the startup
    script works.
    """
    orchestrator._terminated_instances.add("instance-1")

    orchestrator.record_keepalive("instance-1")

    assert "instance-1" not in orchestrator._keepalive_last_heard
    assert orchestrator._keepalive_ever_heard is False


def test_keepalive_from_live_instance_is_recorded(orchestrator) -> None:
    """Instances we haven't terminated are still tracked."""
    orchestrator._terminated_instances.add("instance-1")

    orchestrator.record_keepalive("instance-2")

    assert "instance-2" in orchestrator._keepalive_last_heard
    assert orchestrator._keepalive_ever_heard is True


@pytest.mark.asyncio
async def test_terminating_an_instance_stops_its_keepalives_counting(orchestrator) -> None:
    """An instance terminated for being unresponsive is remembered as gone."""
    orchestrator.record_keepalive("instance-1")

    await orchestrator._terminate_keepalive_instance(_make_instance("instance-1"))
    orchestrator.record_keepalive("instance-1")

    assert "instance-1" in orchestrator._terminated_instances
    assert "instance-1" not in orchestrator._keepalive_last_heard


def test_record_spot_termination_forgets_the_instance(orchestrator, caplog) -> None:
    """A reclaimed instance stops being watched for keep-alives and is counted."""
    orchestrator.record_keepalive("instance-1")

    with caplog.at_level(logging.INFO, logger="cloud_tasks.instance_manager.orchestrator"):
        orchestrator.record_spot_termination("instance-1")
        # A second report of the same instance says nothing more
        orchestrator.record_spot_termination("instance-1")

    assert orchestrator._spot_terminated_instances == {"instance-1"}
    assert "instance-1" not in orchestrator._keepalive_last_heard
    assert "instance-1" not in orchestrator._keepalive_first_seen
    reclaimed = [r for r in caplog.records if "reclaimed as spot capacity" in r.getMessage()]
    assert len(reclaimed) == 1

    # A keep-alive still in flight from the reclaimed instance is ignored
    orchestrator.record_keepalive("instance-1")
    assert "instance-1" not in orchestrator._keepalive_last_heard


@pytest.mark.asyncio
async def test_scaling_refills_a_pool_that_lost_instances(orchestrator) -> None:
    """Losing an instance to a spot reclamation is made good on the next cycle.

    The minimum constraints describe the pool, so they are compared against the pool the
    job would have. Comparing them against the number of instances being added means a
    pool that has lost one is never refilled, because one replacement is fewer than the
    minimum size of the whole pool.
    """
    orchestrator._running = True
    orchestrator._min_instances = 4
    orchestrator._max_instances = 4
    orchestrator._run_config.min_total_cpus = None
    orchestrator._run_config.min_total_price_per_hour = None
    orchestrator._run_config.min_simultaneous_tasks = None
    orchestrator._run_config.max_simultaneous_tasks = None
    orchestrator._run_config.max_total_cpus = None
    orchestrator._run_config.max_total_price_per_hour = None
    orchestrator._get_remaining_task_count = lambda: 100
    # Three of the four instances are up; one was reclaimed
    orchestrator.get_job_instances = AsyncMock(return_value=(3, 6, 17.25, "summary"))
    orchestrator.record_spot_termination("instance-gone")

    with patch.object(orchestrator, "_provision_instances", new=AsyncMock()) as provision:
        await orchestrator._check_scaling()

    provision.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_scaling_still_refuses_a_pool_below_the_minimum(orchestrator) -> None:
    """The minimum is still enforced against the pool the job would end up with."""
    orchestrator._running = True
    orchestrator._min_instances = 4
    orchestrator._max_instances = 2
    orchestrator._run_config.min_total_cpus = None
    orchestrator._run_config.min_total_price_per_hour = None
    orchestrator._run_config.min_simultaneous_tasks = None
    orchestrator._run_config.max_simultaneous_tasks = None
    orchestrator._run_config.max_total_cpus = None
    orchestrator._run_config.max_total_price_per_hour = None
    orchestrator._get_remaining_task_count = lambda: 100
    orchestrator.get_job_instances = AsyncMock(return_value=(0, 0, 0.0, "summary"))

    with patch.object(orchestrator, "_provision_instances", new=AsyncMock()) as provision:
        await orchestrator._check_scaling()

    provision.assert_not_awaited()


def test_local_credentials_warning_is_shown_and_can_be_declined(orchestrator, caplog) -> None:
    """A credential that won't last the job is called out before any instance is started."""
    orchestrator._instance_manager.local_credential_warning = Mock(
        return_value="These credentials expire.\nUse a service account."
    )

    with patch("sys.stdin.isatty", return_value=True), patch("builtins.input", return_value="no"):
        with caplog.at_level(logging.WARNING, logger="cloud_tasks.instance_manager.orchestrator"):
            with pytest.raises(RuntimeError, match="will not last the job"):
                orchestrator._check_local_credentials()

    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "These credentials expire." in logged
    assert "Use a service account." in logged


def test_local_credentials_warning_can_be_accepted(orchestrator) -> None:
    """Answering yes gets on with the job."""
    orchestrator._instance_manager.local_credential_warning = Mock(return_value="expiring")

    with patch("sys.stdin.isatty", return_value=True), patch("builtins.input", return_value="yes"):
        orchestrator._check_local_credentials()


def test_local_credentials_warning_does_not_block_a_non_interactive_run(orchestrator) -> None:
    """Nothing is there to answer the question when the job is started by a script."""
    orchestrator._instance_manager.local_credential_warning = Mock(return_value="expiring")

    with patch("sys.stdin.isatty", return_value=False), patch("builtins.input") as mock_input:
        orchestrator._check_local_credentials()

    mock_input.assert_not_called()


def test_no_credentials_warning_asks_nothing(orchestrator) -> None:
    """Credentials that will last the job are not worth interrupting anyone about."""
    orchestrator._instance_manager.local_credential_warning = Mock(return_value=None)

    with patch("sys.stdin.isatty", return_value=True), patch("builtins.input") as mock_input:
        orchestrator._check_local_credentials()

    mock_input.assert_not_called()


def _configure_run(orchestrator, **fields) -> None:
    """Give the orchestrator a real RunConfig, since vars() of a mock has no fields.

    Parameters:
        orchestrator: The orchestrator under test
        **fields: RunConfig fields to set
    """
    orchestrator._run_config = RunConfig(**fields)
    orchestrator._instance_manager.effective_cpus_per_task = Mock(
        side_effect=lambda instance_info, constraints=None: InstanceManager.effective_cpus_per_task(
            orchestrator._instance_manager, instance_info, constraints
        )
    )


def test_instance_table_counts_the_tasks_the_instances_can_run(orchestrator) -> None:
    """Each row says how many tasks that instance can run, and the total is the capacity."""
    _configure_run(orchestrator, cpus_per_task=4)
    orchestrator._all_instance_info = {"n1-standard-2": {"vcpu": 32, "mem_gb": 128}}

    table, _num, cpus, _price = orchestrator._build_instance_table(
        [_make_instance("one"), _make_instance("two")]
    )

    rows = _instance_rows(table.split("\n"))
    # 32 vCPUs at 4 per task is 8 tasks on each of the two instances
    assert [cell.strip() for cell in rows["one"].strip().strip("│").split("│")][4] == "8"
    totals = next(line for line in table.split("\n") if "running/starting" in line)
    assert "16" in totals
    assert "16 task(s) can run at once" in table
    assert "at 4 vCPU(s) per task" in table


def test_instance_table_task_count_respects_tasks_per_instance_limits(orchestrator) -> None:
    """max_tasks_per_instance bounds the capacity the table reports."""
    _configure_run(orchestrator, cpus_per_task=1, max_tasks_per_instance=3)
    orchestrator._all_instance_info = {"n1-standard-2": {"vcpu": 32, "mem_gb": 128}}

    table, _num, _cpus, _price = orchestrator._build_instance_table([_make_instance("one")])

    assert "3 task(s) can run at once" in table


def test_instance_table_says_when_cpus_per_task_was_raised_for_memory(orchestrator) -> None:
    """With allow_cpu_wasting the vCPUs per task is not what the configuration asked for."""
    _configure_run(orchestrator, cpus_per_task=1, min_memory_per_task=32, allow_cpu_wasting=True)
    # 8 GB per vCPU, so a 32 GB task needs 4 vCPUs and only 8 tasks fit
    orchestrator._all_instance_info = {"n1-standard-2": {"vcpu": 32, "mem_gb": 256}}
    orchestrator._optimal_instance_info = {"vcpu": 32, "mem_gb": 256}

    table, _num, _cpus, _price = orchestrator._build_instance_table([_make_instance("one")])

    assert "8 task(s) can run at once" in table
    assert "at 4 vCPU(s) per task" in table
    assert "cpus_per_task is 1" in table


@pytest.mark.asyncio
async def test_provision_restarts_stopped_instances_before_creating_new_ones(orchestrator):
    """A stopped instance is cheaper and faster to bring back than a replacement for it."""
    orchestrator.list_job_instances = AsyncMock(
        return_value=[
            _make_instance("running-1"),
            _make_instance("stopped-1", state="terminated"),
            _make_instance("stopped-2", state="stopped"),
        ]
    )
    orchestrator._instance_manager.restart_instance = AsyncMock()
    orchestrator._instance_manager.start_instance = AsyncMock(
        return_value=("new-1", "us-central1-a")
    )
    orchestrator._generate_worker_startup_script = MagicMock(return_value="#!/bin/bash\n")

    instance_ids = await orchestrator._provision_instances(3)

    assert instance_ids == ["stopped-1", "stopped-2", "new-1"]
    assert [
        call.args[0] for call in orchestrator._instance_manager.restart_instance.await_args_list
    ] == ["stopped-1", "stopped-2"]
    # Only the one instance the restarts couldn't supply is created
    orchestrator._instance_manager.start_instance.assert_awaited_once()


@pytest.mark.asyncio
async def test_provision_creates_nothing_when_restarts_cover_the_shortfall(orchestrator):
    """Restarting is provisioning; it isn't done on top of creating the same instances again."""
    orchestrator.list_job_instances = AsyncMock(
        return_value=[_make_instance("stopped-1", state="terminated")]
    )
    orchestrator._instance_manager.restart_instance = AsyncMock()
    orchestrator._instance_manager.start_instance = AsyncMock()
    orchestrator._generate_worker_startup_script = MagicMock(return_value="#!/bin/bash\n")

    instance_ids = await orchestrator._provision_instances(1)

    assert instance_ids == ["stopped-1"]
    orchestrator._instance_manager.start_instance.assert_not_awaited()


@pytest.mark.asyncio
async def test_provision_restarts_no_more_than_the_pool_is_short_by(orchestrator):
    """Restarting everything that is stopped would grow the pool past what was asked for."""
    orchestrator.list_job_instances = AsyncMock(
        return_value=[
            _make_instance("stopped-1", state="terminated"),
            _make_instance("stopped-2", state="terminated"),
            _make_instance("stopped-3", state="terminated"),
        ]
    )
    orchestrator._instance_manager.restart_instance = AsyncMock()
    orchestrator._instance_manager.start_instance = AsyncMock()
    orchestrator._generate_worker_startup_script = MagicMock(return_value="#!/bin/bash\n")

    instance_ids = await orchestrator._provision_instances(2)

    assert instance_ids == ["stopped-1", "stopped-2"]
    assert orchestrator._instance_manager.restart_instance.await_count == 2


@pytest.mark.asyncio
async def test_provision_does_not_create_where_a_stopped_instance_would_not_restart(orchestrator):
    """A zone that won't give an instance back has no capacity for a new one either."""
    orchestrator.list_job_instances = AsyncMock(
        return_value=[_make_instance("stopped-1", state="terminated", zone="us-central1-a")]
    )
    orchestrator._instance_manager.restart_instance = AsyncMock(
        side_effect=RuntimeError("ZONE_RESOURCE_POOL_EXHAUSTED")
    )
    orchestrator._instance_manager.start_instance = AsyncMock(
        return_value=("new-1", "us-central1-b")
    )
    orchestrator._generate_worker_startup_script = MagicMock(return_value="#!/bin/bash\n")

    instance_ids = await orchestrator._provision_instances(1)

    assert instance_ids == ["new-1"]
    assert orchestrator._instance_manager.start_instance.await_args[1]["exclude_zones"] == {
        "us-central1-a"
    }


@pytest.mark.asyncio
async def test_provision_only_excludes_zones_holding_the_type_it_would_create(orchestrator):
    """A stopped instance of some other type says nothing about the type this job creates."""
    orchestrator.list_job_instances = AsyncMock(
        return_value=[
            _make_instance(
                "other-type", state="terminated", zone="us-central1-a", instance_type="n2-highmem-4"
            )
        ]
    )
    orchestrator._instance_manager.restart_instance = AsyncMock(side_effect=RuntimeError("no"))
    orchestrator._instance_manager.start_instance = AsyncMock(
        return_value=("new-1", "us-central1-a")
    )
    orchestrator._generate_worker_startup_script = MagicMock(return_value="#!/bin/bash\n")

    await orchestrator._provision_instances(1)

    assert orchestrator._instance_manager.start_instance.await_args[1]["exclude_zones"] == set()


@pytest.mark.asyncio
async def test_provision_excludes_zones_of_stopped_instances_it_had_no_room_to_restart(
    orchestrator,
):
    """A zone already holding a stopped instance gets that one back before it gets a new one."""
    orchestrator.list_job_instances = AsyncMock(
        return_value=[
            _make_instance("stopped-a", state="terminated", zone="us-central1-a"),
            _make_instance("stopped-b", state="terminated", zone="us-central1-b"),
        ]
    )

    async def restart(instance_id, zone=None):
        if instance_id == "stopped-a":
            raise RuntimeError("ZONE_RESOURCE_POOL_EXHAUSTED")

    orchestrator._instance_manager.restart_instance = AsyncMock(side_effect=restart)
    orchestrator._instance_manager.start_instance = AsyncMock(
        return_value=("new-1", "us-central1-c")
    )
    orchestrator._generate_worker_startup_script = MagicMock(return_value="#!/bin/bash\n")

    instance_ids = await orchestrator._provision_instances(2)

    assert instance_ids == ["stopped-b", "new-1"]
    assert orchestrator._instance_manager.start_instance.await_args[1]["exclude_zones"] == {
        "us-central1-a"
    }


@pytest.mark.asyncio
async def test_restarted_instance_is_alive_again_for_keep_alive_purposes(orchestrator):
    """An instance that comes back must have its keep-alives counted again, not ignored."""
    orchestrator.record_spot_termination("stopped-1")
    assert "stopped-1" in orchestrator._terminated_instances

    orchestrator.list_job_instances = AsyncMock(
        return_value=[_make_instance("stopped-1", state="terminated")]
    )
    orchestrator._instance_manager.restart_instance = AsyncMock()
    orchestrator._instance_manager.start_instance = AsyncMock()
    orchestrator._generate_worker_startup_script = MagicMock(return_value="#!/bin/bash\n")

    await orchestrator._provision_instances(1)

    assert "stopped-1" not in orchestrator._terminated_instances
    assert "stopped-1" not in orchestrator._spot_terminated_instances
    orchestrator.record_keepalive("stopped-1")
    assert "stopped-1" in orchestrator._keepalive_last_heard
    # The reclamation still counts towards what happened over the job as a whole
    assert orchestrator._spot_termination_count == 1


@pytest.mark.asyncio
async def test_provision_creates_normally_when_the_instance_listing_fails(orchestrator):
    """Not knowing what is stopped is no reason to stop growing the pool."""
    orchestrator.list_job_instances = AsyncMock(side_effect=RuntimeError("API is down"))
    orchestrator._instance_manager.restart_instance = AsyncMock()
    orchestrator._instance_manager.start_instance = AsyncMock(
        return_value=("new-1", "us-central1-a")
    )
    orchestrator._generate_worker_startup_script = MagicMock(return_value="#!/bin/bash\n")

    instance_ids = await orchestrator._provision_instances(1)

    assert instance_ids == ["new-1"]
    orchestrator._instance_manager.restart_instance.assert_not_awaited()
