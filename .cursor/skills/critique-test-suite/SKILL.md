---
name: critique-test-suite
description: Analyze the rms-cloud-tasks test suite for consistency, completeness, redundancy, parallel safety, assertion quality, and mocking. Produces a report only (no test modifications). Use when the user asks to critique tests, review the test suite, or generate a report for fixing tests.
---

# Critique Test Suite

Analyze all tests in the rms-cloud-tasks project and produce a **report only**—do not modify any test files. The report is intended to be used as a prompt for an AI agent (or developer) to fix the tests later.

## Scope

- **All tests:** Files under `tests/` matching `test_*.py` (pytest).
- **Conftest:** `tests/conftest.py` and `tests/cloud_tasks/instance_manager/gcp/conftest.py`.
- **Modules under test:** `cloud_tasks.common`, `cloud_tasks.queue_manager`, `cloud_tasks.instance_manager`, `cloud_tasks.worker`, and CLI (`cloud_tasks.cli`).
- **Manual scripts:** `tests/manual/` contains shell scripts and configs for manual runs; exclude from automated-test critique or note as manual-only.

## Checklist for Analysis

Apply these criteria when reviewing each test file and each test case.

### 1. Return values and assertions

- **Explicit values:** Assert exact expected values where known (e.g. `assert counts["pending"] == 2`, not just `assert "pending" in counts` or `assert counts.get("pending")`).
- **Dynamic values:** When the value is dynamic (UUIDs, timestamps, env-dependent), assert **type** and **format** (e.g. UUID regex, ISO datetime, enum membership) rather than only existence or non-None.
- **Lists/collections:** Prefer asserting **exact length** (e.g. `assert len(tasks) == 1`) when the expected count is known; avoid only `assert len(items) >= 1` unless the count truly varies.
- **Dict/object shape:** Assert that results have the expected keys or structure where it matters (e.g. task dict has `task_id`, `data`, `ack_id`).

### 2. Success and failure conditions

- **Success paths:** Every function/behavior under test should have at least one test that asserts the happy-path result (return value or side effect).
- **Failure paths:** For each operation, consider and test: invalid input, validation errors (Pydantic), missing required config, not-found or provider errors. Note missing failure cases in the report.
- **Edge cases:** Empty lists, None/optional fields, boundary values (min/max, zero, negative where invalid), empty strings.

### 3. Consistency

- **Naming:** Test names should follow a consistent style (e.g. `test_<behavior>_<condition>_<expected>` or `test_<function>_<when>_<result>`).
- **Structure:** Similar modules (e.g. queue_manager AWS vs GCP) should have similar test structure (init, send, receive, ack, error handling).
- **Fixtures:** Same concepts (e.g. "mock queue", "config", "tmp_path") should be reused via fixtures; avoid duplicating setup logic across files.
- **Assertion style:** Prefer one logical assertion per concept; group related assertions consistently. Per project rules, use a single check per assert (no `and` in asserts) (this improves test failure clarity and aligns with project testing standards).

### 4. Completeness

- **Coverage map:** For each package (common, queue_manager, instance_manager, worker, cli), list which behaviors are tested and which are missing.
- **Parameters:** Config fields, CLI args, and function parameters that affect behavior should have at least one test (valid and, where relevant, invalid).
- **Documentation:** Tests that match DESIGN.md, docstrings, or module contracts should be noted; gaps between documented behavior and tests should be listed.

### 5. Redundancy

- **Duplicate coverage:** Identify tests that assert the same behavior in the same way; suggest merging or removing duplicates.
- **Overlap:** Note tests that are subsets of others (e.g. one test checks return type only, another checks return type + exact value for the same call).
- **Fixtures:** Flag repeated inline setup that could be a shared fixture (e.g. repeated MagicMock configs).

### 6. Parallel execution and isolation

- **Isolation:** Tests must not depend on global state, shared mutable objects, or execution order. Note any use of module/class-level mutable state or singletons.
- **GCP instance_manager:** First check whether symbols such as `_thread_local` and `_pricing_cache_lock` actually exist in the repo before referencing them. Treat those symbols only as potential signposts for areas that might leak state (e.g., thread-local storage or shared locks), not as facts about the codebase. When reviewing files such as `conftest` and `instance_manager`, (1) search for those symbols and document findings if present; (2) if not found, report that they were not observed rather than implying they must exist. Also check tests that mutate fixture-derived instances for possible state leaks.
- **Resources:** Note any shared files, temp paths, or external service assumptions that could cause flakiness under `pytest -n auto` (if used).

### 7. Mocking and dependency isolation

- **Cloud providers:** GCP Pub/Sub, AWS SQS, and Azure Service Bus should be mocked in unit tests; note tests that make real network calls.
- **Credentials:** Default credentials and provider clients should be patched; note tests that depend on real env or real credentials.
- **Time:** Tests involving `time.sleep`, `datetime.now()`, or expiration logic should mock or freeze time for determinism; note time-sensitive tests without freezing.
- **Environment and argv:** Tests that depend on `os.environ` or `sys.argv` should patch them; note tests that would fail with different env or argv.
- **AsyncMock vs Mock:** Async methods (e.g. queue `send_message`, `receive_tasks`) should be mocked with AsyncMock where the test is async; note misuse.

### 8. Config and validation testing

- **Pydantic validation:** Config and RunConfig validation tests should cover both valid and invalid combinations (min/max ordering, forbidden values). Note tests that only assert "raises ValueError" without checking exception message content.
- **Load config:** File-based config loading should test missing file, invalid YAML, and invalid structure; note gaps.
- **Secrets:** Tests should not rely on real secrets; config fixtures should use clearly fake values. Note any exposure of real credentials or project IDs in test code.

### 9. Parameterization and data-driven tests

- **`@pytest.mark.parametrize`:** Similar test cases (e.g. multiple invalid configs, multiple providers) should be parameterized instead of copy-pasted; note repeated test bodies that differ only in input.
- **Boundary values:** For numeric config (min/max instances, CPUs, memory), test min, max, and off-by-one; note missing boundary tests.
- **Fixtures with params:** The root conftest uses `@pytest.fixture(params=["aws", "gcp", "azure"])` for provider; note other places where parameterized fixtures would reduce duplication.

### 10. Async and concurrency testing

- **Async fixtures:** Fixtures that return async resources or are used in async tests should use `@pytest_asyncio.fixture`; note sync fixtures in async test files or misuse of event_loop scope.
- **Timeouts:** Long-running async operations in tests should have explicit timeouts; note tests that could hang.
- **Worker tests:** Worker uses multiprocessing and asyncio; note tests that patch `_wait_for_shutdown`, `create_task`, or process spawning for isolation and whether they are sufficient.

### 11. Error handling and exception messages

- **Exception type and message:** When testing exceptions that have defined messages (e.g. validation errors, custom errors), tests must assert on the **contents** of the exception message, not only that the exception was raised. Use `pytest.raises(SomeError) as exc_info` and assert on `str(exc_info.value)` or the exception's message attribute. Note tests that only check exception type.
- **Error response shape:** For CLI or functions that return error info, verify consistent structure; note tests that don't verify error message or code.
- **Provider errors:** Queue and instance manager tests should verify that provider exceptions (e.g. NotFound, PermissionDenied) are handled or propagated as expected; note missing exception tests.

### 12. State and workflow testing

- **Task database:** Status transitions (pending → enqueued → completed / exception / timed_out) should be tested; note missing transition or status-count tests.
- **Worker lifecycle:** Start, task processing, shutdown, and event logging should have clear success-path tests; note missing lifecycle or integration-style tests.
- **CLI subcommands:** Each subcommand (e.g. yield_tasks_from_file, run_argv, list instances) should have at least one success and, where relevant, one failure test; note missing subcommands or failure paths.
- **Idempotency:** Operations that should be idempotent (e.g. config load, queue create-if-not-exists) should be tested for repeated calls; note missing idempotency tests.

### 13. Test data and fixtures

- **Realistic data:** Test data should be realistic enough to catch edge cases (e.g. Unicode, long strings, empty dicts); note tests using only trivial data.
- **Cleanup:** Tests that create temp files or DBs should clean up (tmp_path, yield fixtures, or explicit unlink/close); note tests that leak state.
- **Fixture scope:** Fixtures should use the narrowest appropriate scope (`function` > `class` > `module` > `session`). The GCP conftest uses module/package scope for speed; note whether any fixture scope causes isolation issues or cross-test pollution.
- **Deepcopy in GCP tests:** The `copy_gcp_instance_manager_for_test` helper exists to avoid thread-local serialization; note tests that mutate shared manager instances without copying.

### 14. Flakiness indicators

- **Time-based assertions:** Tests asserting on wall-clock time (e.g. "created within last N seconds") are flaky; note and suggest freezing or mocking time.
- **Order dependence:** Tests that pass only when run in a specific order indicate shared state; note any such patterns.
- **External dependencies:** Tests depending on network, real cloud APIs, or unpatched file system are flaky in CI; note and suggest mocking.
- **Random data:** Tests using `uuid4()` or `random` for assertions without seeding are non-deterministic; note and suggest seeding or fixed values where assertion stability matters.

### 15. Regression and documentation

- **Bug reference:** Tests written to reproduce bugs should reference the issue/ticket in docstring or comment; note tests that appear to be regression tests but lack context.
- **Spec alignment:** Tests should map to documented behavior (DESIGN.md, docstrings, README); note tests for undocumented behavior or missing tests for documented behavior.
- **Manual verification comments:** Some test files have "Manually verified" dates; note whether these are sufficient or if automated regression coverage is missing.

### 16. Other good practices

- **Independence:** Each test should be runnable in isolation; document any hidden dependencies (e.g. "must run after X").
- **Clarity:** Test names and docstrings should describe intent; report tests whose purpose is unclear.
- **Speed:** Note slow tests (e.g. many patches, sleeps, real I/O) that could be sped up with mocks or smaller scope.
- **Assertion messages:** Assertions should use clear messages where it helps (e.g. `assert x == y, f"Expected {x} to equal {y}"`); note assertions that would be hard to debug on failure.
- **Single responsibility:** Each test should verify one behavior; note tests that assert unrelated things or have multiple "acts."
- **Arrange-Act-Assert:** Tests should follow AAA pattern clearly; note tests with interleaved setup and assertions.
- **No logic in tests:** Tests should not contain conditionals (`if`/`else`/`elif`), Python ternary expressions (`x if condition else y`), other branching (e.g. `match`/`case`, which provides pattern matching and was added in Python 3.10; Python historically lacked a traditional switch/case—use `match`/`case` where appropriate), loops, or complex logic; note tests that do and suggest splitting or parameterizing.

### 17. Code coverage

- **Target:** At least 80% code coverage for code under test (per .coveragerc and project rules).
- **Scope:** Coverage should cover almost all non-exception lines; exception branches may be excluded from the percentage target but should still be tested where they represent distinct behavior.
- **Measurement:** Coverage must be checked by running the **entire test suite** (e.g. `pytest tests --cov=src/cloud_tasks --cov-report=term-missing`), not a subset. .coveragerc omits `tests/*`, `azure.py`, and `aws.py`; note whether omit list is appropriate and whether any critical code is excluded from coverage expectations.
- **Report:** Note whether coverage is or should be measured with the full suite; list modules or packages below 80% or with significant uncovered non-exception lines.

## Output: Report Format

Produce a single markdown report with the following structure. Do **not** edit any test files; only write the report.

```markdown
# Test Suite Critique Report

**Generated:** [date]
**Scope:** tests/ (pytest); modules: common, queue_manager, instance_manager, worker, cli

## Executive summary
- Overall assessment (strengths, main gaps).
- **Coverage:** At least 80%; measured by running the entire test suite. Note if 80% is met and whether measurement is full-suite.
- **Exception messages:** When testing exceptions with defined messages, tests must assert on message contents (e.g. `pytest.raises(...) as exc_info`, then `str(exc_info.value)`). Note violations.
- High-priority fixes vs. nice-to-have.

## 1. Return values and assertions
- Tests that only check existence or non-None; suggest exact value or type/format checks.
- Lists asserted with `>= N` where exact length is knowable; suggest exact length.
- Missing shape or key checks for dicts/objects.

## 2. Success and failure conditions
- Per area (common, queue_manager, instance_manager, worker, cli): table of "behavior | tested? | notes".
- Missing: validation errors, missing config, provider/not-found errors, edge cases.

## 3. Consistency
- Naming/structure inconsistencies with examples.
- Fixture usage and duplication across files.

## 4. Completeness
- Coverage map (what's tested, what's missing per module).
- Doc/spec gaps.

## 5. Redundancy
- Duplicate or overlapping tests with file:test references.

## 6. Parallel execution and isolation
- Global state, order dependence, GCP fixture scope and deepcopy usage, shared resources.

## 7. Mocking and dependency isolation
- Real external calls, time-sensitive tests without freezing, env/argv dependencies, AsyncMock vs Mock.

## 8. Config and validation testing
- Pydantic/config validation coverage, exception message assertions, load_config edge cases, secrets in tests.

## 9. Parameterization
- Tests that could be parameterized, missing boundary value tests.

## 10. Async and concurrency
- Async fixture usage, timeouts, worker multiprocessing/async isolation.

## 11. Error handling
- Missing error body or message verification.
- Exception tests that only assert type; require asserting message contents where defined.

## 12. State and workflow
- Task DB transitions, worker lifecycle, CLI subcommands, idempotency.

## 13. Test data and fixtures
- Unrealistic data, cleanup issues, fixture scope, GCP deepcopy usage.

## 14. Flakiness indicators
- Time-based assertions, order dependence, external dependencies, unseeded randomness.

## 15. Regression and documentation
- Missing bug references, spec/test alignment, manual-only coverage.

## 16. Other
- Unclear tests, slow tests, missing assertion messages, multi-responsibility tests, AAA violations, logic in tests.

## 17. Code coverage
- Target 80%; full-suite measurement. List modules below 80% or with significant uncovered lines. Note .coveragerc omit list.

## Prompt for an AI agent to fix tests

This section is a **reusable prompt template** to be filled with the report output. Use the placeholders below; include either the full report or a summarized version as specified.

**Template (fill placeholders):**

```text

Apply the following test-suite fixes. Use the critique report as context.

<REPORT_SUMMARY>
Paste the full report or a concise summary (sections 1–17) here.
</REPORT_SUMMARY>

<FAILURES>
List specific failures, file names, test names, and line references from the report.
</FAILURES>

<FILES_TO_EDIT>
List the test/conftest files to modify (paths under tests/).
</FILES_TO_EDIT>

Constraints:
- **Coverage:** Run the full test suite for coverage; require ≥80% for code under test; cover almost all non-exception lines.
- **Exception messages:** For tests that expect exceptions with defined messages, use `pytest.raises(...) as exc_info` and assert message content: `assert "expected substring" in str(exc_info.value)` (or equivalent). Do not only assert that an exception was raised.
- **Production code:** Do not modify production code. Fix only tests and conftest files.
- **Behavior:** Preserve existing passing behavior; only add or change assertions and test structure as indicated by the report.

```

**Example filled prompt:**

```text

Apply the following test-suite fixes.

<REPORT_SUMMARY>
[Section 2] test_config.py: missing failure-path tests for invalid provider.
[Section 4] test_worker_init.py: no test for num_simultaneous_tasks boundary.
[Section 8] test_config.py: exception tests do not assert message content.
</REPORT_SUMMARY>

<FAILURES>
- tests/cloud_tasks/common/test_config.py: add tests for invalid provider; in test_runconfig_raises_*, use exc_info and assert str(exc_info.value).
- tests/cloud_tasks/worker/test_worker_init.py: add parameterized test for num_simultaneous_tasks at min/max.
</FAILURES>

<FILES_TO_EDIT>
tests/cloud_tasks/common/test_config.py
tests/cloud_tasks/worker/test_worker_init.py
</FILES_TO_EDIT>

Constraints: Run full test suite for coverage (≥80%). Use pytest.raises(...) as exc_info and assert str(exc_info.value) for exception message checks. Do not modify production code. Preserve existing passing behavior.

```

## Execution steps

1. **Gather:** List all test files under `tests/` (test_*.py) and conftest files.
2. **Read:** For each file, read test names, docstrings, and assertion patterns (focus on `assert`, `pytest.raises`, response/return checks, and fixtures).
3. **Classify:** For each criterion (1–17), note specific file names, test names, and line references or short quotes.
4. **Write:** Produce the full report in the format above, including the "Prompt for an AI agent to fix tests" section at the end.
5. **Do not:** Change, add, or remove any line in any test or conftest file.

## When to use this skill

- User asks to "critique the test suite", "review the tests", "analyze tests", or "generate a report to fix tests".
- User wants a "prompt for an AI to fix the tests" based on the current test suite.
