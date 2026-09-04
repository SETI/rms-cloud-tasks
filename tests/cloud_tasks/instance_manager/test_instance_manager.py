"""Tests for cloud_tasks.instance_manager: InstanceManager and provider factory."""

import pytest

from cloud_tasks.common.config import ProviderConfig
from cloud_tasks.instance_manager.instance_manager import InstanceManager


def _concrete_instance_manager() -> InstanceManager:
    """Build an InstanceManager with the abstract methods stubbed out.

    Returns:
        InstanceManager: A concrete subclass whose provider methods do nothing, for
        exercising the constraint logic the base class implements.
    """

    class ConcreteInstanceManager(InstanceManager):
        async def get_available_instance_types(self, constraints=None):
            pass

        async def get_instance_pricing(self, instance_types, use_spot=False):
            pass

        async def get_optimal_instance_type(self, constraints=None):
            pass

        async def start_instance(self, **kwargs):
            pass

        async def terminate_instance(self, instance_id, zone=None):
            pass

        async def list_running_instances(self, job_id=None, include_non_job=False):
            pass

        async def get_image_from_family(self, family_name):
            pass

        async def get_default_image(self):
            pass

        async def list_available_images(self):
            pass

        async def get_available_regions(self):
            pass

    return ConcreteInstanceManager(ProviderConfig())


class TestInstanceManager:
    """Validates InstanceManager constraint matching and factory behavior."""

    @pytest.fixture
    def base_instance_info(self) -> dict:
        """Base instance info fixture with typical values."""
        return {
            "vcpu": 4,
            "mem_gb": 16,  # 4GB per CPU
            "local_ssd_gb": 40,  # 10GB per CPU
            "architecture": "X86_64",
            "supports_spot": True,
        }

    @pytest.fixture
    def instance_manager(self) -> InstanceManager:
        """Create a concrete instance manager for testing."""
        return _concrete_instance_manager()

    def test_instance_matches_constraints_no_constraints(
        self, instance_manager: InstanceManager, base_instance_info: dict
    ) -> None:
        """Test with no constraints."""
        # Empty dict constraints
        assert instance_manager._instance_matches_constraints(base_instance_info, {})
        # None constraints should be treated the same as empty dict
        assert instance_manager._instance_matches_constraints(base_instance_info, None)
        # Explicit None constraints
        assert instance_manager._instance_matches_constraints(base_instance_info, constraints=None)

    def test_instance_matches_constraints_architecture(
        self, instance_manager: InstanceManager, base_instance_info: dict
    ) -> None:
        """Test architecture matching."""
        # Matching architecture
        assert instance_manager._instance_matches_constraints(
            base_instance_info, {"architecture": "X86_64"}
        )
        # Non-matching architecture
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"architecture": "ARM64"}
        )
        # No architecture constraint
        assert instance_manager._instance_matches_constraints(
            base_instance_info, {"architecture": None}
        )

    def test_instance_matches_constraints_cpu_limits(self, instance_manager, base_instance_info):
        """Test CPU limit constraints"""
        # Within limits
        assert instance_manager._instance_matches_constraints(
            base_instance_info, {"min_cpu": 2, "max_cpu": 8}
        )
        # Below minimum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"min_cpu": 8}
        )
        # Above maximum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"max_cpu": 2}
        )
        # Only min_cpu
        assert instance_manager._instance_matches_constraints(base_instance_info, {"min_cpu": 2})
        # Only max_cpu
        assert instance_manager._instance_matches_constraints(base_instance_info, {"max_cpu": 8})

    def test_instance_matches_constraints_tasks_per_instance(
        self, instance_manager, base_instance_info
    ):
        """Test tasks per instance constraints affecting CPU limits"""
        # Test min_tasks_per_instance affecting min_cpu
        assert not instance_manager._instance_matches_constraints(
            base_instance_info,
            {"cpus_per_task": 2, "min_tasks_per_instance": 3},  # Requires 6 CPUs
        )
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {"cpus_per_task": 1, "min_tasks_per_instance": 2},  # Requires 2 CPUs
        )

        # Test max_tasks_per_instance affecting max_cpu
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {"cpus_per_task": 1, "max_tasks_per_instance": 6},  # Allows up to 6 CPUs
        )
        assert not instance_manager._instance_matches_constraints(
            base_instance_info,
            {"cpus_per_task": 2, "max_tasks_per_instance": 1},  # Allows only 2 CPUs
        )

        # Test both min and max tasks
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {"cpus_per_task": 1, "min_tasks_per_instance": 2, "max_tasks_per_instance": 6},
        )

    def test_instance_matches_constraints_memory_total(self, instance_manager, base_instance_info):
        """Test total memory constraints"""
        # Within limits
        assert instance_manager._instance_matches_constraints(
            base_instance_info, {"min_total_memory": 8, "max_total_memory": 32}
        )
        # Below minimum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"min_total_memory": 32}
        )
        # Above maximum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"max_total_memory": 8}
        )
        # Only min
        assert instance_manager._instance_matches_constraints(
            base_instance_info, {"min_total_memory": 8}
        )
        # Only max
        assert instance_manager._instance_matches_constraints(
            base_instance_info, {"max_total_memory": 32}
        )

    def test_instance_matches_constraints_memory_per_cpu(
        self, instance_manager, base_instance_info
    ):
        """Test memory per CPU constraints"""
        # Within limits (4GB per CPU)
        assert instance_manager._instance_matches_constraints(
            base_instance_info, {"min_memory_per_cpu": 2, "max_memory_per_cpu": 8}
        )
        # Below minimum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"min_memory_per_cpu": 8}
        )
        # Above maximum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"max_memory_per_cpu": 2}
        )

    def test_instance_matches_constraints_memory_per_task(
        self, instance_manager, base_instance_info
    ):
        """Test memory per task constraints"""
        # Within limits (4GB per CPU * cpus_per_task)
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {"cpus_per_task": 2, "min_memory_per_task": 4, "max_memory_per_task": 16},
        )
        # Below minimum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"cpus_per_task": 1, "min_memory_per_task": 8}
        )
        # Above maximum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"cpus_per_task": 2, "max_memory_per_task": 4}
        )

    def test_instance_matches_constraints_local_ssd(self, instance_manager, base_instance_info):
        """Test local SSD constraints"""
        # Within limits
        assert instance_manager._instance_matches_constraints(
            base_instance_info, {"min_local_ssd": 20, "max_local_ssd": 80}
        )
        # Below minimum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"min_local_ssd": 80}
        )
        # Above maximum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"max_local_ssd": 20}
        )

    def test_instance_matches_constraints_local_ssd_per_cpu(
        self, instance_manager, base_instance_info
    ):
        """Test local SSD per CPU constraints"""
        # Within limits (10GB per CPU)
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {"local_ssd_base_size": 0, "min_local_ssd_per_cpu": 5, "max_local_ssd_per_cpu": 15},
        )
        # Below minimum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"local_ssd_base_size": 0, "min_local_ssd_per_cpu": 15}
        )
        # Above maximum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info, {"local_ssd_base_size": 0, "max_local_ssd_per_cpu": 5}
        )
        # With base size
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {"local_ssd_base_size": 10, "min_local_ssd_per_cpu": 5, "max_local_ssd_per_cpu": 15},
        )
        assert not instance_manager._instance_matches_constraints(
            base_instance_info,
            {"local_ssd_base_size": 30, "min_local_ssd_per_cpu": 5, "max_local_ssd_per_cpu": 15},
        )

    def test_instance_matches_constraints_local_ssd_per_task(
        self, instance_manager, base_instance_info
    ):
        """Test local SSD per task constraints"""
        # Within limits (10GB per CPU * cpus_per_task)
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {
                "cpus_per_task": 2,
                "local_ssd_base_size": 0,
                "min_local_ssd_per_task": 10,
                "max_local_ssd_per_task": 30,
            },
        )
        # Below minimum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info,
            {"cpus_per_task": 1, "local_ssd_base_size": 0, "min_local_ssd_per_task": 15},
        )
        # Above maximum
        assert not instance_manager._instance_matches_constraints(
            base_instance_info,
            {"cpus_per_task": 2, "local_ssd_base_size": 0, "max_local_ssd_per_task": 10},
        )
        # With base size
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {
                "cpus_per_task": 2,
                "local_ssd_base_size": 10,
                "min_local_ssd_per_task": 10,
                "max_local_ssd_per_task": 30,
            },
        )
        assert not instance_manager._instance_matches_constraints(
            base_instance_info,
            {
                "cpus_per_task": 2,
                "local_ssd_base_size": 30,
                "min_local_ssd_per_task": 10,
                "max_local_ssd_per_task": 30,
            },
        )

    def test_instance_matches_constraints_spot(self, instance_manager, base_instance_info):
        """Test spot instance constraints"""
        # Create a copy of base_instance_info to avoid modifying the fixture
        spot_instance = dict(base_instance_info)
        spot_instance["supports_spot"] = True
        non_spot_instance = dict(base_instance_info)
        non_spot_instance["supports_spot"] = False

        # Instance supports spot and spot requested
        assert instance_manager._instance_matches_constraints(spot_instance, {"use_spot": True})
        # Instance supports spot and use_spot is None
        assert instance_manager._instance_matches_constraints(spot_instance, {"use_spot": None})
        # Instance doesn't support spot but spot requested
        assert not instance_manager._instance_matches_constraints(
            non_spot_instance, {"use_spot": True}
        )
        # Instance doesn't support spot but use_spot is None
        assert instance_manager._instance_matches_constraints(non_spot_instance, {"use_spot": None})
        # No spot constraint for spot-supporting instance
        assert instance_manager._instance_matches_constraints(spot_instance, {})
        # No spot constraint for non-spot-supporting instance
        assert instance_manager._instance_matches_constraints(non_spot_instance, {})
        # Asking for on-demand instances says nothing about whether spot is supported
        assert instance_manager._instance_matches_constraints(spot_instance, {"use_spot": False})
        assert instance_manager._instance_matches_constraints(
            non_spot_instance, {"use_spot": False}
        )

    def test_instance_matches_constraints_tasks_per_instance_with_cpu_limits(
        self, instance_manager, base_instance_info
    ):
        """Test tasks per instance constraints when min/max_cpu are specified"""
        # Test min_cpu being set to max of min_cpu and min_tasks * cpus_per_task
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {
                "cpus_per_task": 1,
                "min_tasks_per_instance": 2,  # Requires 2 CPUs
                "min_cpu": 3,  # Should take precedence over min_tasks
            },
        )
        assert not instance_manager._instance_matches_constraints(
            base_instance_info,
            {
                "cpus_per_task": 2,
                "min_tasks_per_instance": 3,  # Requires 6 CPUs
                "min_cpu": 2,  # Should be overridden by min_tasks requirement
            },
        )

        # Test max_cpu being set to min of max_cpu and max_tasks * cpus_per_task
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {
                "cpus_per_task": 1,
                "max_tasks_per_instance": 6,  # Allows 6 CPUs
                "max_cpu": 5,  # Should take precedence over max_tasks
            },
        )
        assert not instance_manager._instance_matches_constraints(
            base_instance_info,
            {
                "cpus_per_task": 1,
                "max_tasks_per_instance": 2,  # Allows only 2 CPUs
                "max_cpu": 8,  # Should be overridden by max_tasks limit
            },
        )

        # Test both min and max with tasks per instance
        assert instance_manager._instance_matches_constraints(
            base_instance_info,
            {
                "cpus_per_task": 1,
                "min_tasks_per_instance": 2,  # Requires 2 CPUs
                "max_tasks_per_instance": 6,  # Allows 6 CPUs
                "min_cpu": 3,  # Takes precedence over min_tasks
                "max_cpu": 5,  # Takes precedence over max_tasks
            },
        )
        assert not instance_manager._instance_matches_constraints(
            base_instance_info,
            {
                "cpus_per_task": 1,
                "min_tasks_per_instance": 5,  # Requires 5 CPUs
                "max_tasks_per_instance": 8,  # Allows 8 CPUs
                "min_cpu": 2,  # Overridden by min_tasks
                "max_cpu": 6,  # Irrelevant as min_tasks already exceeds instance CPUs
            },
        )


class TestEffectiveCpusPerTask:
    """Validates how allow_cpu_wasting changes the vCPUs a task is given."""

    @pytest.fixture
    def instance_manager(self) -> InstanceManager:
        """A concrete instance manager; only the base class methods are exercised."""
        return _concrete_instance_manager()

    @pytest.fixture
    def low_memory_instance(self) -> dict:
        """An instance type with 1 GB of memory per vCPU."""
        return {
            "name": "c2-highcpu-8",
            "vcpu": 8,
            "mem_gb": 8,
            "local_ssd_gb": 0,
            "architecture": "X86_64",
            "supports_spot": True,
        }

    def test_unset_leaves_cpus_per_task_alone(self, instance_manager, low_memory_instance):
        """Without allow_cpu_wasting a task gets exactly what it asked for."""
        constraints = {"cpus_per_task": 1, "min_memory_per_task": 4}
        assert instance_manager.effective_cpus_per_task(low_memory_instance, constraints) == 1

    def test_raises_cpus_to_supply_the_memory_a_task_needs(
        self, instance_manager, low_memory_instance
    ):
        """A task needing 4 GB on a 1 GB-per-vCPU machine is given 4 vCPUs."""
        constraints = {
            "cpus_per_task": 1,
            "min_memory_per_task": 4,
            "allow_cpu_wasting": True,
        }
        assert instance_manager.effective_cpus_per_task(low_memory_instance, constraints) == 4

    def test_never_lowers_cpus_per_task(self, instance_manager, low_memory_instance):
        """Memory that is already satisfied doesn't take vCPUs away from a task."""
        constraints = {
            "cpus_per_task": 4,
            "min_memory_per_task": 2,
            "allow_cpu_wasting": True,
        }
        assert instance_manager.effective_cpus_per_task(low_memory_instance, constraints) == 4

    def test_capped_at_the_size_of_the_instance(self, instance_manager, low_memory_instance):
        """A task can't be given more vCPUs than the instance has.

        The whole instance still has less memory than the task needs, so the instance type
        has to fail the memory constraint rather than be made to look adequate.
        """
        constraints = {
            "cpus_per_task": 1,
            "min_memory_per_task": 32,
            "allow_cpu_wasting": True,
        }
        assert instance_manager.effective_cpus_per_task(low_memory_instance, constraints) == 8
        assert not instance_manager._instance_matches_constraints(low_memory_instance, constraints)

    def test_memory_constraint_is_satisfied_by_wasting_cpus(
        self, instance_manager, low_memory_instance
    ):
        """An instance type rejected for memory per task is accepted with cpu wasting."""
        constraints = {"cpus_per_task": 1, "min_memory_per_task": 4}
        assert not instance_manager._instance_matches_constraints(low_memory_instance, constraints)

        constraints["allow_cpu_wasting"] = True
        assert instance_manager._instance_matches_constraints(low_memory_instance, constraints)

    def test_tasks_per_instance_constraints_use_the_raised_cpus_per_task(
        self, instance_manager, low_memory_instance
    ):
        """Wasting vCPUs means fewer tasks fit, and min_tasks_per_instance knows it."""
        constraints = {
            "cpus_per_task": 1,
            "min_memory_per_task": 4,
            "allow_cpu_wasting": True,
            # 8 vCPUs at 4 per task is 2 tasks, so 3 is out of reach
            "min_tasks_per_instance": 3,
        }
        assert not instance_manager._instance_matches_constraints(low_memory_instance, constraints)

        constraints["min_tasks_per_instance"] = 2
        assert instance_manager._instance_matches_constraints(low_memory_instance, constraints)


class TestDescribeUnmetConstraints:
    """Validates the report produced when the constraints select no instance type."""

    @pytest.fixture
    def instance_manager(self) -> InstanceManager:
        """A concrete instance manager; only the base class methods are exercised."""
        return _concrete_instance_manager()

    @pytest.fixture
    def instance_types(self) -> list[dict]:
        """Two instance types with different amounts of memory."""
        return [
            {
                "name": "small",
                "vcpu": 2,
                "mem_gb": 4,
                "local_ssd_gb": 0,
                "architecture": "X86_64",
                "supports_spot": True,
            },
            {
                "name": "large",
                "vcpu": 8,
                "mem_gb": 32,
                "local_ssd_gb": 0,
                "architecture": "X86_64",
                "supports_spot": False,
            },
        ]

    def test_reports_only_constraints_no_instance_type_meets(
        self, instance_manager, instance_types
    ):
        """A constraint some instance type satisfies is not the one to change."""
        lines = instance_manager.describe_unmet_constraints(
            instance_types, {"min_cpu": 4, "min_total_memory": 64}
        )
        # min_cpu is met by "large", so only the memory constraint is reported
        assert lines == ["min_total_memory: needs >= 64, closest available 32"]

    def test_reports_the_closest_value_available(self, instance_manager, instance_types):
        """The report says how close the best instance type came, in its own units."""
        lines = instance_manager.describe_unmet_constraints(instance_types, {"max_total_memory": 2})
        assert lines == ["max_total_memory: needs <= 2, closest available 4"]

    def test_reports_the_values_available_for_an_equality_constraint(
        self, instance_manager, instance_types
    ):
        """For architecture there is no 'closest', so the available values are listed."""
        lines = instance_manager.describe_unmet_constraints(
            instance_types, {"architecture": "ARM64"}
        )
        assert lines == ["architecture: needs ARM64, available: X86_64"]

    def test_reports_an_instance_type_name_filter_that_matches_nothing(
        self, instance_manager, instance_types
    ):
        """A name filter that excludes everything is itself a constraint to relax."""
        lines = instance_manager.describe_unmet_constraints(
            instance_types, {"instance_types": ["^n2-"]}
        )
        assert lines[0].startswith("instance_types: needs ^n2-, available: ")

    def test_no_report_when_every_constraint_is_met_by_something(
        self, instance_manager, instance_types
    ):
        """Constraints that conflict only in combination leave nothing to single out."""
        lines = instance_manager.describe_unmet_constraints(
            instance_types, {"min_cpu": 8, "max_total_memory": 4}
        )
        assert lines == []


class TestSpotConstraint:
    """Validates that spot support is only required by a run that asks for spot."""

    @pytest.fixture
    def instance_manager(self) -> InstanceManager:
        """A concrete instance manager; only the base class methods are exercised."""
        return _concrete_instance_manager()

    @pytest.fixture
    def on_demand_only(self) -> dict:
        """An instance type the provider does not offer as spot capacity."""
        return {
            "name": "m5.large",
            "vcpu": 2,
            "mem_gb": 8,
            "local_ssd_gb": 0,
            "architecture": "X86_64",
            "supports_spot": False,
        }

    def test_on_demand_run_accepts_a_type_without_spot(self, instance_manager, on_demand_only):
        """use_spot=False must not throw away instance types that never offer spot.

        AWS reports which usage classes an instance type supports, so requiring spot
        support for a run that will never buy spot capacity excludes instance types that
        would do the job perfectly well.
        """
        assert instance_manager._instance_matches_constraints(on_demand_only, {"use_spot": False})

    def test_spot_run_still_requires_spot(self, instance_manager, on_demand_only):
        """A run that asks for spot instances can only use types that offer them."""
        assert not instance_manager._instance_matches_constraints(
            on_demand_only, {"use_spot": True}
        )

    def test_unmet_spot_constraint_is_reported(self, instance_manager, on_demand_only):
        """A spot run with nothing spot-capable to run on is told that's the problem."""
        lines = instance_manager.describe_unmet_constraints([on_demand_only], {"use_spot": True})
        assert lines == [
            "use_spot: needs an instance type that supports spot, available: not supported"
        ]
