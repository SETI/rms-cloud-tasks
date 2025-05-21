import pytest
from src.cloud_tasks.instance_manager.instance_manager import InstanceManager
from src.cloud_tasks.common.config import ProviderConfig


class TestInstanceManager:
    @pytest.fixture
    def base_instance_info(self):
        """Base instance info fixture with typical values"""
        return {
            "vcpu": 4,
            "mem_gb": 16,  # 4GB per CPU
            "local_ssd_gb": 40,  # 10GB per CPU
            "architecture": "X86_64",
            "supports_spot": True,
        }

    @pytest.fixture
    def instance_manager(self):
        """Create a concrete instance manager for testing"""

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

    def test_instance_matches_constraints_no_constraints(
        self, instance_manager, base_instance_info
    ):
        """Test with no constraints"""
        # Empty dict constraints
        assert instance_manager._instance_matches_constraints(base_instance_info, {})
        # None constraints should be treated the same as empty dict
        assert instance_manager._instance_matches_constraints(base_instance_info, None)
        # Explicit None constraints
        assert instance_manager._instance_matches_constraints(base_instance_info, constraints=None)

    def test_instance_matches_constraints_architecture(self, instance_manager, base_instance_info):
        """Test architecture matching"""
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
            base_instance_info, {"cpus_per_task": 2, "min_tasks_per_instance": 3}  # Requires 6 CPUs
        )
        assert instance_manager._instance_matches_constraints(
            base_instance_info, {"cpus_per_task": 1, "min_tasks_per_instance": 2}  # Requires 2 CPUs
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
        # False use_spot still requires spot support
        assert instance_manager._instance_matches_constraints(spot_instance, {"use_spot": False})
        assert not instance_manager._instance_matches_constraints(
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
