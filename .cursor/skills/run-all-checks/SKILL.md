---
name: run-all-checks
description: Run all linting, type checking, tests, and documentation builds for the rms-cloud-tasks project. Check for errors and warnings, then fix any problems found. Use when the user asks to run checks, verify the build, run CI locally, or fix lint/type/test errors.
---

# Run All Checks

Execute all project checks (ruff, mypy, pytest, Sphinx docs) and fix any errors found.

## Quick Start

Run all checks and fix errors:

1. Execute the run-all-checks script (or run checks manually).
2. Review output for errors and warnings.
3. Fix any issues found.
4. Re-run checks to verify fixes.

## Preferred Method: Script

From project root with venv activated:

```bash
./scripts/run-all-checks.sh
```

Options:

- `-p, --parallel` — Run code checks and docs in parallel (default).
- `-s, --sequential` — Run all checks one after another (easier to debug).
- `-c, --code` — Only ruff, mypy, pytest.
- `-d, --docs` — Only Sphinx documentation build.
- `-h, --help` — Show usage.

## Check Commands (Manual)

All commands assume project root and `source venv/bin/activate` (or `venv\Scripts\activate` on Windows).

### Code (from project root)

```bash
# Lint (ruff)
python -m ruff check src tests examples
python -m ruff format --check src tests examples

# Type check (mypy)
python -m mypy src tests examples

# Tests (pytest)
python -m pytest tests -q
```

### Documentation (from project root)

```bash
cd docs && make clean && make html SPHINXOPTS="-W"
```

The script and docs build use `SPHINXOPTS="-W"` so Sphinx treats warnings as errors; the docs check fails if any warnings are produced.

## Execution Workflow

Copy this checklist and track progress:

```text
Check Progress:
- [ ] Ruff check (src, tests, examples)
- [ ] Ruff format --check
- [ ] Mypy (src, tests, examples)
- [ ] Pytest (tests)
- [ ] Docs build without warnings
- [ ] All errors fixed
- [ ] Re-verify all checks pass
```

### Step 1: Run All Checks

**Option A – Script (recommended):**

```bash
./scripts/run-all-checks.sh
```

**Option B – Sequential manual:**

```bash
source venv/bin/activate
python -m ruff check src tests examples && \
python -m ruff format --check src tests examples && \
python -m mypy src tests examples && \
python -m pytest tests -q && \
(cd docs && make clean && make html SPHINXOPTS="-W")
```

### Step 2: Analyze Results

Check output for:

- **Errors**: Must be fixed (non-zero exit code).
- **Warnings**: Must be fixed. The docs build is run with `SPHINXOPTS="-W"`, so Sphinx warnings cause the documentation check to fail.

Common error types:

| Check   | Error Pattern              | Typical Fix                          |
|---------|----------------------------|--------------------------------------|
| ruff    | `F401` unused import       | Remove import                        |
| ruff    | `UP035` typing import      | Use `collections.abc` for ABCs       |
| ruff    | `ARG001` unused argument   | Prefix with `_` or `# noqa: ARG001`  |
| mypy    | `error: ... is not defined` | Add import or fix typo               |
| mypy    | `assignment`, `attr-defined` | Add type annotations or `# type: ignore[...]` |
| pytest  | `FAILED` or `ERROR`        | Fix test or code under test          |
| sphinx  | `WARNING: duplicate object`| Add `:no-index:` or fix duplicate    |

### Step 3: Fix Issues

For each error:

1. Read the error message and file/line.
2. Open the file and apply the appropriate fix.
3. Re-run the failing check to confirm.

### Step 4: Re-verify

After fixing, run all checks again:

```bash
./scripts/run-all-checks.sh
```

All checks should pass with exit code 0.

## Common Fixes Reference

### Ruff Unused Argument (ARG001)

For pytest fixtures that are dependencies but not directly used:

```python
def my_fixture(other_fixture: None) -> None:  # noqa: ARG001
    ...
```

### Ruff UP035 (typing → collections.abc)

Use `collections.abc` for `AsyncGenerator`, `Iterable`, etc.:

```python
from collections.abc import AsyncGenerator, Iterable
```

### Sphinx Duplicate Object Warning

Add `:no-index:` to automodule directive:

```rst
.. automodule:: cloud_tasks.config
   :members:
   :no-index:
```

### Mypy in Tests

Tests use overrides in `pyproject.toml` (`module = "tests.*"`) for `method-assign` and `attr-defined` (mocks). For other mypy errors in tests, add targeted `# type: ignore[code]` or fix the type.

### Type Annotation Errors

For union or forward-reference issues:

```python
from __future__ import annotations  # Add at top of file
```

## Success Criteria

All checks pass when:

- `ruff check src tests examples` → "All checks passed!"
- `ruff format --check src tests examples` → "All files formatted"
- `mypy src tests examples` → "Success: no issues found"
- `pytest tests -q` → All tests pass
- `make html SPHINXOPTS="-W"` (in docs/) → Build completes with exit 0 (no errors or warnings)
