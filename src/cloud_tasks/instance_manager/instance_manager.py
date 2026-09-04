import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, NamedTuple

from ..common.config import ProviderConfig
from ..common.logging_config import wrap_log_text


class ConstraintCheck(NamedTuple):
    """The result of testing one configured constraint against one instance type.

    Attributes:
        name: The configuration option the constraint comes from, so a report can name
            the thing the user has to change.
        limit: The value the configuration asks for.
        actual: What this instance type provides.
        satisfied: Whether this instance type meets the constraint.
        prefer_larger: True when a larger `actual` is closer to meeting `limit`, False when
            a smaller one is, None when the constraint is an equality and neither is.
    """

    name: str
    limit: Any
    actual: Any
    satisfied: bool
    prefer_larger: bool | None


# Type aliases for get_instance_pricing return structure (instance_type -> region -> zone -> pricing_info)
# PricingInfo may contain mixed metadata (prices, strings, etc.) so values are Any.
PricingInfo = dict[str, Any]
ZonePricing = dict[str, PricingInfo | None]
RegionPricing = dict[str, ZonePricing]
InstancePricingResult = dict[str, RegionPricing]


class InstanceManager(ABC):
    """Base interface for instance management operations."""

    #: Replaced by each provider with its own module logger
    _logger: logging.Logger = logging.getLogger(__name__)

    # These rankings are valid across all providers
    _PROCESSOR_FAMILY_TO_PERFORMANCE_RANKING = {
        # Unknown/Other
        "Unknown": 0,
        "Intel": 1,  # Generic/legacy Intel, very low performance
        # Legacy/Oldest
        "Intel Nehalem": 2,  # Xeon 5500, ~2009
        "Intel Westmere": 3,  # Xeon 5600, ~2010
        "Intel Sandy Bridge": 4,  # Xeon E5-2600, ~2012
        "Intel Ivy Bridge": 5,  # Xeon E5 v2, ~2013
        "Intel Haswell": 6,  # Xeon E5 v3, ~2014
        "Intel Broadwell": 7,  # Xeon E5 v4, ~2016
        "Intel Core i7": 8,  # Mac1, ~2017
        # Early cloud ARM
        "AWS Graviton": 9,  # A1, ~2018
        # Early AMD EPYC
        "AMD Naples": 10,  # EPYC 7001, Zen 1, ~2017
        # 1st Gen Xeon Scalable
        "Intel Skylake": 11,  # Xeon Scalable 1st Gen, ~2017
        # 2nd Gen Xeon Scalable
        "Intel Cascade Lake": 12,  # Xeon Scalable 2nd Gen, ~2019
        # 2nd Gen AMD EPYC
        "AMD Rome": 13,  # EPYC 7002, Zen 2, ~2019
        # Early ARM/Apple
        "Apple M1": 14,  # Mac2, ~2020
        "Ampere Altra": 15,  # Arm Neoverse N1, ~2020
        # 3rd Gen Xeon Scalable
        "Intel Ice Lake": 16,  # Xeon Scalable 3rd Gen, ~2021
        # 3rd Gen AMD EPYC
        "AMD Milan": 17,  # EPYC 7003, Zen 3, ~2021
        # AWS Graviton2
        "AWS Graviton2": 18,  # M6g, ~2020
        # AWS Graviton3
        "AWS Graviton3": 19,  # M7g, ~2022
        # AWS Graviton3E
        "AWS Graviton3E": 20,  # HPC, ~2023
        # 4th Gen Xeon Scalable
        "Intel Sapphire Rapids": 21,  # Xeon Scalable 4th Gen, ~2023
        # 4th Gen AMD EPYC
        "AMD Genoa": 22,  # EPYC 9004, Zen 4, ~2022
        # AWS Graviton4
        "AWS Graviton4": 23,  # M8g, ~2024
        # AWS Inferentia2
        "AWS Inferentia2": 24,  # Modern AWS accelerator
        # 5th Gen Xeon Scalable
        "Intel Emerald Rapids": 25,  # Xeon Scalable 5th Gen, ~2024
        # 5th Gen AMD EPYC
        "AMD Turin": 26,  # EPYC 9005, Zen 5, ~2024 (expected)
        # Google Custom ARM
        "Google Axion": 27,  # Custom ARM, 2024 (early results)
        # NVIDIA's Arm CPU, paired with its GPUs in the GB200 superchip
        "NVIDIA Grace": 27,  # Arm Neoverse V2, the same generation as Axion
    }

    def __init__(self, config: ProviderConfig) -> None:
        """Initialize the instance manager with configuration."""
        self.config = config

    def local_credential_warning(self) -> str | None:
        """Describe a problem with the credentials this process is running on, or None.

        A job outlives the command that starts it: instances are created, monitored and
        terminated for as long as there are tasks left, which can be days. Credentials that
        belong to a person rather than to a service account stop working partway through
        that, and the run then cannot even terminate the instances it is paying for.

        Returns:
            A warning to show the user before the job starts, or None if the credentials
            are suitable for a long run. The base implementation returns None; providers
            that can tell the difference override it.
        """
        return None

    def effective_cpus_per_task(
        self, instance_info: dict[str, Any], constraints: dict[str, Any] | None = None
    ) -> float:
        """Return the vCPUs each task gets on this instance type.

        This is normally just cpus_per_task. With allow_cpu_wasting set, a task is given
        more vCPUs than it asked for when that is the only way to give it the memory it
        asked for: an instance type is sized in vCPUs, so the only way to give one task
        more memory is to leave some of the vCPUs it comes with unused. The result is
        capped at the size of the instance, so an instance type too small to run even one
        task still fails the memory constraint rather than appearing to satisfy it.

        Parameters:
            instance_info: Instance type attributes; "vcpu" and "mem_gb" are used
            constraints: Constraint dict; cpus_per_task, min_memory_per_task and
                allow_cpu_wasting are used

        Returns:
            float: vCPUs per task, never more than the instance's vCPU count.
        """
        if constraints is None:
            constraints = {}
        cpus_per_task = constraints.get("cpus_per_task")
        if not cpus_per_task:
            cpus_per_task = 1
        if not constraints.get("allow_cpu_wasting"):
            return float(cpus_per_task)

        min_memory_per_task = constraints.get("min_memory_per_task")
        if not min_memory_per_task:
            return float(cpus_per_task)

        num_cpus = instance_info["vcpu"]
        memory_per_cpu = instance_info["mem_gb"] / num_cpus
        if memory_per_cpu <= 0:
            return float(cpus_per_task)

        cpus_for_memory = min_memory_per_task / memory_per_cpu
        return float(min(max(cpus_per_task, cpus_for_memory), num_cpus))

    def _check_instance_constraints(
        self, instance_info: dict[str, Any], constraints: dict[str, Any] | None = None
    ) -> list[ConstraintCheck]:
        """Test every configured constraint against one instance type.

        Every constraint is reported, whether or not it is met, so that a caller left with
        no instance types at all can say which constraints did the excluding instead of
        only that nothing was left.

        Parameters:
            instance_info: Dict of instance attributes. Keys used: "name", "vcpu",
                "mem_gb", "local_ssd_gb", "architecture", "cpu_rank", "supports_spot".
            constraints: Optional dict of constraints. Missing keys mean "no constraint".

        Returns:
            list[ConstraintCheck]: One entry per constraint that the configuration
            actually sets. Empty if constraints is None or sets nothing.
        """
        if constraints is None:
            return []

        checks: list[ConstraintCheck] = []

        def check(name: str, limit: Any, actual: Any, satisfied: bool, prefer_larger: bool | None):
            checks.append(ConstraintCheck(name, limit, actual, satisfied, prefer_larger))

        def check_min(name: str, actual: Any, limit: Any = None, as_name: str | None = None):
            if limit is None:
                limit = constraints.get(name)
            if limit is None:
                return
            check(as_name or name, limit, actual, actual >= limit, True)

        def check_max(name: str, actual: Any, limit: Any = None, as_name: str | None = None):
            if limit is None:
                limit = constraints.get(name)
            if limit is None:
                return
            check(as_name or name, limit, actual, actual <= limit, False)

        instance_types = constraints.get("instance_types")
        if instance_types:
            if isinstance(instance_types, str):
                instance_types = [instance_types]
            name = str(instance_info.get("name", ""))
            matched = any(re.match(pattern, name) for pattern in instance_types)
            check("instance_types", ", ".join(instance_types), name, matched, None)

        cpus_per_task = self.effective_cpus_per_task(instance_info, constraints)
        min_tasks_per_instance = constraints.get("min_tasks_per_instance")
        max_tasks_per_instance = constraints.get("max_tasks_per_instance")

        num_cpus = instance_info["vcpu"]
        memory_per_cpu = instance_info["mem_gb"] / num_cpus
        memory_per_task = memory_per_cpu * cpus_per_task

        local_ssd_base_size = constraints.get("local_ssd_base_size")
        if local_ssd_base_size is None:
            local_ssd_base_size = 0
        local_ssd_per_cpu = (instance_info["local_ssd_gb"] - local_ssd_base_size) / num_cpus
        local_ssd_per_task = local_ssd_per_cpu * cpus_per_task

        if constraints.get("architecture") is not None:
            check(
                "architecture",
                constraints["architecture"],
                instance_info["architecture"],
                instance_info["architecture"] == constraints["architecture"],
                None,
            )

        # cpu_rank is only read when something asks about it, so an instance type that
        # doesn't report one can still be matched against the other constraints
        if constraints.get("min_cpu_rank") is not None:
            check_min("min_cpu_rank", instance_info["cpu_rank"])
        if constraints.get("max_cpu_rank") is not None:
            check_max("max_cpu_rank", instance_info["cpu_rank"])

        # min/max_tasks_per_instance are constraints on the number of vCPUs once the vCPUs
        # each task needs is known, so they are reported under their own names
        check_min("min_cpu", num_cpus)
        check_max("max_cpu", num_cpus)
        if min_tasks_per_instance is not None:
            check_min(
                "min_tasks_per_instance",
                num_cpus,
                limit=cpus_per_task * min_tasks_per_instance,
                as_name="min_tasks_per_instance (vCPUs needed)",
            )
        if max_tasks_per_instance is not None:
            check_max(
                "max_tasks_per_instance",
                num_cpus,
                limit=cpus_per_task * max_tasks_per_instance,
                as_name="max_tasks_per_instance (vCPUs allowed)",
            )

        check_min("min_total_memory", instance_info["mem_gb"])
        check_max("max_total_memory", instance_info["mem_gb"])
        check_min("min_memory_per_cpu", memory_per_cpu)
        check_max("max_memory_per_cpu", memory_per_cpu)
        check_min("min_memory_per_task", memory_per_task)
        check_max("max_memory_per_task", memory_per_task)
        check_min("min_local_ssd", instance_info["local_ssd_gb"])
        check_max("max_local_ssd", instance_info["local_ssd_gb"])
        check_min("min_local_ssd_per_cpu", local_ssd_per_cpu)
        check_max("max_local_ssd_per_cpu", local_ssd_per_cpu)
        check_min("min_local_ssd_per_task", local_ssd_per_task)
        check_max("max_local_ssd_per_task", local_ssd_per_task)

        # Only asking for spot instances requires an instance type that offers them. Asking
        # for on-demand instances says nothing about spot, and requiring spot support for a
        # run that will never use it throws away instance types that would do the job
        if constraints.get("use_spot"):
            supports_spot = bool(instance_info["supports_spot"])
            check(
                "use_spot",
                "an instance type that supports spot",
                "supported" if supports_spot else "not supported",
                supports_spot,
                None,
            )

        return checks

    def _instance_matches_constraints(
        self, instance_info: dict[str, Any], constraints: dict[str, Any] | None = None
    ) -> bool:
        """Check whether instance_info satisfies all provided constraints.

        Matching uses exact equality for scalar constraints (e.g. architecture)
        and numeric comparisons for min/max constraints. instance_info must
        contain keys such as "vcpu", "mem_gb", "local_ssd_gb", "architecture",
        "cpu_rank", "supports_spot". constraints may define optional keys
        (cpus_per_task, min_cpu, max_cpu, architecture, use_spot, etc.); missing
        keys in constraints are treated as "no constraint" (any value matches).

        Parameters:
            instance_info: Dict mapping instance attribute names to values.
                Required keys used by the implementation: "vcpu", "mem_gb",
                "local_ssd_gb", "architecture", "cpu_rank", "supports_spot".
                Constraint-derived values use cpus_per_task, min_cpu, max_cpu
                (from constraints) for comparisons.
            constraints: Optional dict defining expected key->value pairs or
                min/max predicates. None means match all instances.

        Returns:
            True if instance_info satisfies all constraints; False otherwise.
            If constraints is None, returns True.

        Raises:
            TypeError: If instance_info or constraints have wrong types (e.g.
                non-dict). KeyError may be raised if instance_info is missing
                required keys used in the implementation.
        """
        return all(
            check.satisfied
            for check in self._check_instance_constraints(instance_info, constraints)
        )

    def describe_unmet_constraints(
        self,
        instance_infos: Iterable[dict[str, Any]],
        constraints: dict[str, Any] | None = None,
    ) -> list[str]:
        """Describe the constraints that not one of these instance types satisfies.

        Meant for the case where the configuration selects nothing at all: knowing that
        no instance type was left says nothing about which of a dozen constraints to
        relax, and a constraint that some type met is not the one to change.

        Parameters:
            instance_infos: Every instance type that was considered
            constraints: The constraints they were tested against

        Returns:
            list[str]: One line per constraint that every instance type failed, naming the
            configuration option, what it asks for, and the closest any instance type
            came. Empty if no constraint was failed by all of them.
        """
        satisfied_by_any: set[str] = set()
        failures: dict[str, list[ConstraintCheck]] = {}
        for instance_info in instance_infos:
            for check in self._check_instance_constraints(instance_info, constraints):
                if check.satisfied:
                    satisfied_by_any.add(check.name)
                else:
                    failures.setdefault(check.name, []).append(check)

        lines = []
        for name, checks in failures.items():
            if name in satisfied_by_any:
                continue
            first = checks[0]
            if first.prefer_larger is None:
                available = sorted({str(check.actual) for check in checks})
                if len(available) > 4:
                    available = available[:4] + ["..."]
                lines.append(f"{name}: needs {first.limit}, available: {', '.join(available)}")
                continue
            actuals = [check.actual for check in checks]
            closest = max(actuals) if first.prefer_larger else min(actuals)
            comparison = ">=" if first.prefer_larger else "<="
            lines.append(
                f"{name}: needs {comparison} {first.limit:g}, closest available {closest:g}"
            )
        return lines

    def describe_constraint_relaxations(
        self,
        instance_infos: Iterable[dict[str, Any]],
        constraints: dict[str, Any] | None = None,
        limit: int = 3,
    ) -> list[str]:
        """Say which constraints have to give, and by how far, to allow any instance type.

        Every constraint being satisfiable on its own is no help when they are not
        satisfiable together: what the user needs to know is the smallest change that makes
        the configuration workable. Each instance type is scored by the set of constraints
        it fails, and the smallest of those sets are the smallest groups of constraints that
        have to be relaxed together, with the value each would have to reach.

        Parameters:
            instance_infos: Every instance type that was considered
            constraints: The constraints they were tested against
            limit: How many alternatives to describe

        Returns:
            list[str]: One line per alternative, each naming the constraints to relax
            together, the value each would have to reach, and how many instance types would
            then match. Empty if some instance type already matches.
        """
        # The instance types that come closest, grouped by the set of constraints they
        # fail: the smallest of those sets are the smallest groups of constraints that have
        # to give way together
        failures: dict[frozenset[str], list[list[ConstraintCheck]]] = {}
        for instance_info in instance_infos:
            checks = self._check_instance_constraints(instance_info, constraints)
            failed = [check for check in checks if not check.satisfied]
            if not failed:
                # Something matches after all, so there is nothing to relax
                return []
            failures.setdefault(frozenset(check.name for check in failed), []).append(failed)

        if not failures:
            return []

        def shortfall(check: ConstraintCheck) -> float:
            """How far this instance type is from the constraint, as a ratio of 1 or more.

            Parameters:
                check: A constraint this instance type failed

            Returns:
                float: limit/actual for a minimum, actual/limit for a maximum, and 1 for a
                constraint with no ordering, so that the alternatives can be compared by
                how much of a change they ask for rather than by absolute size.
            """
            if check.prefer_larger is None:
                return 1.0
            try:
                if check.prefer_larger:
                    return (
                        float(check.limit) / float(check.actual) if check.actual else float("inf")
                    )
                return float(check.actual) / float(check.limit) if check.limit else float("inf")
            except (TypeError, ValueError, ZeroDivisionError):  # pragma: no cover - defensive
                return float("inf")

        def closest(group: list[list[ConstraintCheck]]) -> list[ConstraintCheck]:
            """Return the failed checks of the instance type asking for the least change."""
            return min(group, key=lambda failed: max(shortfall(check) for check in failed))

        def admitted(group: list[list[ConstraintCheck]], targets: dict[str, Any]) -> int:
            """Count the instance types that these relaxed limits would let in."""
            total = 0
            for failed in group:
                if all(
                    check.prefer_larger is None
                    or (check.actual >= targets[check.name]) == bool(check.prefer_larger)
                    or check.actual == targets[check.name]
                    for check in failed
                ):
                    total += 1
            return total

        fewest = min(len(key) for key in failures)
        # Prefer the alternatives that let in the most instance types
        candidates = sorted(
            (key for key in failures if len(key) == fewest),
            key=lambda key: (-len(failures[key]), sorted(key)),
        )[:limit]

        lines = []
        for key in candidates:
            group = failures[key]
            # Quote one instance type's numbers rather than the best of each constraint
            # separately, which can describe a machine that doesn't exist
            reference = {check.name: check for check in closest(group)}
            targets = {name: check.actual for name, check in reference.items()}
            parts = []
            for name in sorted(key):
                check = reference[name]
                if check.prefer_larger is None:
                    parts.append(f"{name} would have to accept {check.actual}")
                elif check.prefer_larger:
                    parts.append(
                        f"{name} would have to come down from {check.limit:g} to {check.actual:g}"
                    )
                else:
                    parts.append(
                        f"{name} would have to go up from {check.limit:g} to {check.actual:g}"
                    )
            count = admitted(group, targets)
            lines.append(f"{' and '.join(parts)}, and {count} instance type(s) would match")
        return lines

    async def _report_no_instance_types(self, constraints: dict[str, Any] | None) -> None:
        """Log why the constraints left no instance type to start.

        Every instance type the provider offers is tested again here, without the
        constraints, so the report can say which constraints not one of them satisfies.
        That is the only part of the configuration worth changing: a constraint that some
        instance type meets is not what emptied the list.

        Parameters:
            constraints: The constraints that selected nothing
        """
        try:
            all_instance_types = await self.get_available_instance_types()
        except Exception as e:  # pragma: no cover - diagnosis must not mask the real error
            self._logger.debug(f"Could not list unconstrained instance types: {e}")
            return

        def report(text: str, indent: str = "") -> None:
            """Log one paragraph, wrapped, with continuations indented under it."""
            for line in wrap_log_text(text, indent="  "):
                self._logger.error(f"{indent}{line}")

        report(
            f"No instance type meets the requirements. Of the {len(all_instance_types)} "
            "instance types offered here:"
        )
        unmet = self.describe_unmet_constraints(all_instance_types.values(), constraints)
        if unmet:
            report("these constraints are met by none of them:", indent="  ")
            for line in unmet:
                report(line, indent="    ")

        relaxations = self.describe_constraint_relaxations(all_instance_types.values(), constraints)
        if relaxations:
            if unmet:
                report("the fewest changes that would allow an instance type:", indent="  ")
            else:
                report(
                    "every constraint is met by some instance type, but no single instance "
                    "type meets all of them at once. The fewest changes that would allow "
                    "one:",
                    indent="  ",
                )
            for line in relaxations:
                report(line, indent="    ")
        elif not unmet:
            # Every instance type passes every constraint, so the constraints are not what
            # emptied the list: the provider passed these types over for a reason of its
            # own, such as a machine family this version has no information for
            report(
                "no constraint rules them out, so they were passed over for another "
                "reason; see the warnings above about skipped machine families",
                indent="  ",
            )

    def _get_boot_disk_size(
        self, instance_info: dict[str, Any], boot_disk_constraints: dict[str, Any]
    ) -> float:
        """Compute boot disk size in GB from instance and constraint settings.

        Missing constraint keys boot_disk_base_size, boot_disk_per_cpu, and
        boot_disk_per_task are treated as 0. total_boot_disk_size (minimum
        floor) defaults to 10 GB if missing. cpus_per_task defaults to 1.
        Formula: boot_disk_from_cpus = boot_disk_base_size + boot_disk_per_cpu
        * num_cpus; boot_disk_from_tasks = boot_disk_base_size +
        boot_disk_per_task * tasks_per_instance. The returned size is
        max(total_boot_disk_size, boot_disk_from_cpus, boot_disk_from_tasks),
        so the effective minimum is 10 GB. No exception is raised for missing
        keys.

        Parameters:
            instance_info: dict[str, Any] – instance attributes; "vcpu" is
                used for per-cpu/per-task sizing.
            boot_disk_constraints: dict[str, Any] – keys read: boot_disk_base_size
                (numeric, default 0), boot_disk_per_cpu (numeric, default 0),
                boot_disk_per_task (numeric, default 0), total_boot_disk_size
                (numeric, default 10), cpus_per_task (int, default 1).

        Returns:
            float: Computed boot disk size in gigabytes (minimum 10 GB). No
            exception is raised for missing keys.
        """
        boot_disk_base_size = boot_disk_constraints.get("boot_disk_base_size")
        if boot_disk_base_size is None:
            boot_disk_base_size = 0
        boot_disk_per_cpu = boot_disk_constraints.get("boot_disk_per_cpu")
        if boot_disk_per_cpu is None:
            boot_disk_per_cpu = 0
        boot_disk_per_task = boot_disk_constraints.get("boot_disk_per_task")
        if boot_disk_per_task is None:
            boot_disk_per_task = 0
        num_cpus = instance_info["vcpu"]
        cpus_per_task = boot_disk_constraints.get("cpus_per_task")
        if cpus_per_task is None:
            cpus_per_task = 1
        tasks_per_instance = num_cpus // cpus_per_task

        boot_disk = boot_disk_constraints.get("total_boot_disk_size")
        if boot_disk is None:
            boot_disk = 10  # TODO Default is for GCP
        boot_disk_from_cpus = boot_disk_base_size + boot_disk_per_cpu * num_cpus
        boot_disk_from_tasks = boot_disk_base_size + boot_disk_per_task * tasks_per_instance

        return max(boot_disk, boot_disk_from_cpus, boot_disk_from_tasks)

    @abstractmethod
    async def get_available_instance_types(
        self, constraints: dict[str, Any] | None = None
    ) -> dict[str, dict[str, Any]]:
        """Get available instance types with their specifications.


        Args:
            constraints: Dictionary of constraints to filter instance types by. Constraints
                include::
                    "instance_types": List of regex patterns to filter instance types by name
                    "architecture": Architecture (X86_64 or ARM64)
                    "min_cpu": Minimum number of vCPUs
                    "max_cpu": Maximum number of vCPUs
                    "min_total_memory": Minimum total memory in GB
                    "max_total_memory": Maximum total memory in GB
                    "min_memory_per_cpu": Minimum memory per vCPU in GB
                    "max_memory_per_cpu": Maximum memory per vCPU in GB
                    "min_local_ssd": Minimum amount of local SSD storage in GB
                    "max_local_ssd": Maximum amount of local SSD storage in GB
                    "min_local_ssd_per_cpu": Minimum amount of local SSD storage per vCPU
                    "max_local_ssd_per_cpu": Maximum amount of local SSD storage per vCPU
                    "min_boot_disk": Minimum amount of boot disk storage in GB
                    "max_boot_disk": Maximum amount of boot disk storage in GB
                    "min_boot_disk_per_cpu": Minimum amount of boot disk storage per vCPU
                    "max_boot_disk_per_cpu": Maximum amount of boot disk storage per vCPU
                    "use_spot": Whether to filter for spot-capable instance types

        Returns:
            Dictionary mapping instance type to a dictionary of instance type specifications::
                "name": instance type name
                "vcpu": number of vCPUs
                "mem_gb": amount of RAM in GB
                "local_ssd_gb": amount of local SSD storage in GB
                "boot_disk_gb": amount of boot disk storage in GB
                "architecture": architecture of the instance type
                "supports_spot": whether the instance type supports spot pricing
                "description": description of the instance type
                "url": URL to the instance type details
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_instance_pricing(
        self,
        instance_types: dict[str, dict[str, Any]],
        *,
        use_spot: bool = False,
        boot_disk_constraints: dict[str, Any] | None = None,
    ) -> InstancePricingResult:
        """
        Get the hourly price for one or more specific instance types.

        Parameters:
            instance_types: A dictionary mapping instance type to a dictionary of instance type
                specifications as returned by get_available_instance_types().
            use_spot: Whether to use spot pricing.
            boot_disk_constraints: Dictionary of constraints used to determine the boot disk type
                and size. These are from the same config as the instance type constraints but are
                not used to filter instances.

        Returns:
            InstancePricingResult: A mapping of instance_type -> region -> zone -> PricingInfo.
            Each instance_type maps to regions; each region maps to zones; each zone maps to a
            PricingInfo dict with keys such as "cpu_price", "per_cpu_price", "mem_price",
            "mem_per_gb_price", "boot_disk_price", "boot_disk_per_gb_price", "local_ssd_price",
            "local_ssd_per_gb_price", "total_price", "total_price_per_cpu", "zone" (and possibly
            instance type info). All numeric values are in USD per hour (or per GB/hour where
            applicable). If any price is not available, it is set to None.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_optimal_instance_type(
        self, constraints: dict[str, Any] | None = None
    ) -> dict[str, float | str | None]:
        """
        Get the most cost-effective instance type that meets the constraints.

        Args:
            constraints: Dictionary of constraints to filter instance types by. Constraints
                include::
                    "instance_types": List of regex patterns to filter instance types by name
                    "architecture": Architecture (X86_64 or ARM64)
                    "min_cpu": Minimum number of vCPUs
                    "max_cpu": Maximum number of vCPUs
                    "min_total_memory": Minimum total memory in GB
                    "max_total_memory": Maximum total memory in GB
                    "min_memory_per_cpu": Minimum memory per vCPU in GB
                    "max_memory_per_cpu": Maximum memory per vCPU in GB
                    "min_local_ssd": Minimum amount of local SSD storage in GB
                    "max_local_ssd": Maximum amount of local SSD storage in GB
                    "min_local_ssd_per_cpu": Minimum amount of local SSD storage per vCPU
                    "max_local_ssd_per_cpu": Maximum amount of local SSD storage per vCPU
                    "min_storage": Minimum amount of other storage in GB
                    "max_storage": Maximum amount of other storage in GB
                    "min_storage_per_cpu": Minimum amount of other storage per vCPU
                    "max_storage_per_cpu": Maximum amount of other storage per vCPU
                    "use_spot": Whether to use spot instances

        Returns:
            Tuple of:
                - GCP instance type name (e.g., 'n1-standard-2')
                - Zone in which the instance type is cheapest
                - Price of the instance type in USD/hour
        """
        pass  # pragma: no cover

    @abstractmethod
    async def start_instance(
        self,
        *,
        instance_type: str,
        startup_script: str,
        job_id: str,
        use_spot: bool,
        image_uri: str,
        boot_disk_type: str,
        boot_disk_size: int,  # GB
        boot_disk_iops: int | None = None,
        boot_disk_throughput: int | None = None,  # MB/s
        zone: str | None = None,
    ) -> tuple[str, str]:
        """
        Start a new instance and return its ID.

        Args:
            instance_type: Type of instance to start
            startup_script: The startup script
            job_id: Job ID to use for the instance
            use_spot: Whether to use a spot instance
            image_uri: Image URI to use
            zone: Zone to use for the instance; if not specified use the default zone,
                or if none choose a random zone

        Returns:
            A tuple containing the ID of the started instance and the zone it was started
            in
        """
        pass  # pragma: no cover

    @abstractmethod
    async def terminate_instance(self, instance_id: str, zone: str | None = None) -> None:
        """Terminate an instance by ID.

        Args:
            instance_id: Instance name
            zone: The zone the instance is in; if not specified use the default zone
        """
        pass  # pragma: no cover

    @abstractmethod
    async def list_running_instances(
        self, job_id: str | None = None, include_non_job: bool = False
    ) -> list[dict[str, Any]]:
        """List currently running instances, optionally filtered by job.

        "Running" means instances that are in a runnable state (e.g. RUNNING
        or equivalent provider status). Filtering: if job_id is set, only
        instances associated with that job are included unless include_non_job
        is True, in which case instances not tied to any job may also be
        included. Ordering of the list is implementation-defined.

        Parameters:
            job_id: Optional str used to filter instances by associated job.
            include_non_job: If True, include instances not tied to any job.

        Returns:
            list[dict[str, Any]]: Each dict has at least: id (str), state (str),
            tags (list[str]), creation_time (str | datetime), zone (str), type
            (str). Optional key job_id (str | None). Additional provider-specific
            metadata keys (e.g. boot_disk_type, private_ip) may be present.

        Raises:
            Provider-specific exceptions on API or credential errors.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def list_available_images(self) -> list[dict[str, Any]]:
        """
        List available VM images.
        Returns common public OS images and the user's own custom images.

        Returns:
            List of dictionaries with image information
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_image_from_family(self, family_name: str) -> str | None:
        """
        Get the latest image from a specific family.

        Args:
            family_name: Image family name

        Returns:
            Image URI
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_default_image(self) -> str | None:
        """
        Get the latest Ubuntu 24.04 LTS image for Compute Engine.

        Returns:
            Image URI
        """
        pass  # pragma: no cover

    @abstractmethod
    async def get_available_regions(self, prefix: str | None = None) -> dict[str, Any]:
        """Get all available regions and their attributes.

        Parameters:
            prefix: Optional filter; if given, return only regions whose name or
                identifier starts with this string. None means no filtering.

        Returns:
            dict[str, Any]: Mapping of region identifiers (or names) to their
            attribute dicts (e.g. availability, zones, metadata). Value types
            depend on the provider.
        """
        pass  # pragma: no cover
