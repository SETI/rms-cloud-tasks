"""Tests for cloud_tasks.cli.yield_tasks_from_file."""

from pathlib import Path

import pytest

from cloud_tasks.cli import yield_tasks_from_file


def test_yield_tasks_from_file_json_basic(tmp_path: Path) -> None:
    """Yield tasks from a JSON array file.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts two tasks yielded with expected task_id and data.
    """
    task_file = tmp_path / "tasks.json"
    task_file.write_text(
        '[{"task_id": "t1", "data": {"x": 1}}, {"task_id": "t2", "data": {"x": 2}}]'
    )
    out = list(yield_tasks_from_file(str(task_file)))
    assert len(out) == 2
    assert out[0]["task_id"] == "t1"
    assert out[0]["data"]["x"] == 1
    assert out[1]["task_id"] == "t2"
    assert out[1]["data"]["x"] == 2


def test_yield_tasks_from_file_json_with_start_task(tmp_path: Path) -> None:
    """Skip first N tasks when start_task is given.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts one task yielded (t3) after skipping with start_task=2.
    """
    task_file = tmp_path / "tasks.json"
    task_file.write_text(
        '[{"task_id": "t1", "data": {}}, {"task_id": "t2", "data": {}}, {"task_id": "t3", "data": {}}]'
    )
    out = list(yield_tasks_from_file(str(task_file), start_task=2))
    assert len(out) == 1
    assert out[0]["task_id"] == "t3"


def test_yield_tasks_from_file_json_with_limit(tmp_path: Path) -> None:
    """Limit number of tasks yielded when limit is given.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts two tasks yielded when limit=2.
    """
    task_file = tmp_path / "tasks.json"
    task_file.write_text(
        '[{"task_id": "t1", "data": {}}, {"task_id": "t2", "data": {}}, {"task_id": "t3", "data": {}}]'
    )
    out = list(yield_tasks_from_file(str(task_file), limit=2))
    assert len(out) == 2
    assert out[0]["task_id"] == "t1"
    assert out[1]["task_id"] == "t2"


def test_yield_tasks_from_file_json_start_task_and_limit(tmp_path: Path) -> None:
    """Combine start_task and limit to skip and cap yielded tasks.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts one task (t2) yielded with start_task=1, limit=1.
    """
    task_file = tmp_path / "tasks.json"
    task_file.write_text(
        '[{"task_id": "t1", "data": {}}, {"task_id": "t2", "data": {}}, {"task_id": "t3", "data": {}}]'
    )
    out = list(yield_tasks_from_file(str(task_file), start_task=1, limit=1))
    assert len(out) == 1
    assert out[0]["task_id"] == "t2"


def test_yield_tasks_from_file_yaml_basic(tmp_path: Path) -> None:
    """Yield tasks from a YAML file with list of items.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts two tasks yielded with expected task_id and data.
    """
    task_file = tmp_path / "tasks.yaml"
    task_file.write_text("- task_id: t1\n  data: {x: 1}\n- task_id: t2\n  data: {x: 2}\n")
    out = list(yield_tasks_from_file(str(task_file)))
    assert len(out) == 2
    assert out[0]["task_id"] == "t1"
    assert out[1]["task_id"] == "t2"


def test_yield_tasks_from_file_yml_extension(tmp_path: Path) -> None:
    """Yield tasks from a file with .yml extension.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts one task yielded.
    """
    task_file = tmp_path / "tasks.yml"
    task_file.write_text("- task_id: t1\n  data: {}\n")
    out = list(yield_tasks_from_file(str(task_file)))
    assert len(out) == 1
    assert out[0]["task_id"] == "t1"


def test_yield_tasks_from_file_unsupported_format_raises(tmp_path: Path) -> None:
    """Unsupported file extension raises ValueError.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts ValueError with 'Unsupported file format' is raised.
    """
    task_file = tmp_path / "tasks.txt"
    task_file.write_text("not json or yaml")
    with pytest.raises(ValueError, match="Unsupported file format"):
        list(yield_tasks_from_file(str(task_file)))


def test_yield_tasks_from_file_limit_zero_returns_nothing(tmp_path: Path) -> None:
    """limit=0 yields no tasks.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts yielded list is empty.
    """
    task_file = tmp_path / "tasks.json"
    task_file.write_text('[{"task_id": "t1", "data": {}}]')
    out = list(yield_tasks_from_file(str(task_file), limit=0))
    assert out == []


def test_yield_tasks_from_file_limit_negative_returns_nothing(tmp_path: Path) -> None:
    """Negative limit yields no tasks.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts yielded list is empty.
    """
    task_file = tmp_path / "tasks.json"
    task_file.write_text('[{"task_id": "t1", "data": {}}]')
    out = list(yield_tasks_from_file(str(task_file), limit=-1))
    assert out == []


def test_yield_tasks_from_file_yaml_with_start_task(tmp_path: Path) -> None:
    """YAML with start_task skips first item and yields remaining.

    Parameters:
        tmp_path: Pytest fixture providing a temporary directory.

    Returns:
        None. Asserts two tasks (t2, t3) yielded after start_task=1.
    """
    task_file = tmp_path / "tasks.yaml"
    task_file.write_text(
        "- task_id: t1\n  data: {x: 1}\n- task_id: t2\n  data: {x: 2}\n- task_id: t3\n  data: {x: 3}\n"
    )
    out = list(yield_tasks_from_file(str(task_file), start_task=1))
    assert len(out) == 2
    assert out[0]["task_id"] == "t2"
    assert out[1]["task_id"] == "t3"
