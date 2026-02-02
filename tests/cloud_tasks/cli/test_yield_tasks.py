"""Tests for cloud_tasks.cli.yield_tasks_from_file."""

from pathlib import Path

import pytest

from cloud_tasks.cli import yield_tasks_from_file


def test_yield_tasks_from_file_json_basic(tmp_path: Path) -> None:
    """Yield tasks from a JSON array file."""
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
    """Skip first N tasks with start_task."""
    task_file = tmp_path / "tasks.json"
    task_file.write_text(
        '[{"task_id": "t1", "data": {}}, {"task_id": "t2", "data": {}}, {"task_id": "t3", "data": {}}]'
    )
    out = list(yield_tasks_from_file(str(task_file), start_task=2))
    assert len(out) == 1
    assert out[0]["task_id"] == "t3"


def test_yield_tasks_from_file_json_with_limit(tmp_path: Path) -> None:
    """Limit number of tasks yielded."""
    task_file = tmp_path / "tasks.json"
    task_file.write_text(
        '[{"task_id": "t1", "data": {}}, {"task_id": "t2", "data": {}}, {"task_id": "t3", "data": {}}]'
    )
    out = list(yield_tasks_from_file(str(task_file), limit=2))
    assert len(out) == 2
    assert out[0]["task_id"] == "t1"
    assert out[1]["task_id"] == "t2"


def test_yield_tasks_from_file_json_start_task_and_limit(tmp_path: Path) -> None:
    """Combine start_task and limit."""
    task_file = tmp_path / "tasks.json"
    task_file.write_text(
        '[{"task_id": "t1", "data": {}}, {"task_id": "t2", "data": {}}, {"task_id": "t3", "data": {}}]'
    )
    out = list(yield_tasks_from_file(str(task_file), start_task=1, limit=1))
    assert len(out) == 1
    assert out[0]["task_id"] == "t2"


def test_yield_tasks_from_file_yaml_basic(tmp_path: Path) -> None:
    """Yield tasks from a YAML file with list of items."""
    task_file = tmp_path / "tasks.yaml"
    task_file.write_text("- task_id: t1\n  data: {x: 1}\n- task_id: t2\n  data: {x: 2}\n")
    out = list(yield_tasks_from_file(str(task_file)))
    assert len(out) == 2
    assert out[0]["task_id"] == "t1"
    assert out[1]["task_id"] == "t2"


def test_yield_tasks_from_file_yml_extension(tmp_path: Path) -> None:
    """Yield tasks from .yml file."""
    task_file = tmp_path / "tasks.yml"
    task_file.write_text("- task_id: t1\n  data: {}\n")
    out = list(yield_tasks_from_file(str(task_file)))
    assert len(out) == 1
    assert out[0]["task_id"] == "t1"


def test_yield_tasks_from_file_unsupported_format_raises(tmp_path: Path) -> None:
    """Unsupported file extension raises ValueError."""
    task_file = tmp_path / "tasks.txt"
    task_file.write_text("not json or yaml")
    with pytest.raises(ValueError, match="Unsupported file format"):
        list(yield_tasks_from_file(str(task_file)))


def test_yield_tasks_from_file_limit_zero_returns_nothing(tmp_path: Path) -> None:
    """limit=0 yields nothing."""
    task_file = tmp_path / "tasks.json"
    task_file.write_text('[{"task_id": "t1", "data": {}}]')
    out = list(yield_tasks_from_file(str(task_file), limit=0))
    assert out == []


def test_yield_tasks_from_file_limit_negative_returns_nothing(tmp_path: Path) -> None:
    """limit<=0 yields nothing."""
    task_file = tmp_path / "tasks.json"
    task_file.write_text('[{"task_id": "t1", "data": {}}]')
    out = list(yield_tasks_from_file(str(task_file), limit=-1))
    assert out == []


def test_yield_tasks_from_file_yaml_with_start_task(tmp_path: Path) -> None:
    """YAML with start_task skips first item and yields remaining (hits YAML accumulation branch)."""
    task_file = tmp_path / "tasks.yaml"
    task_file.write_text(
        "- task_id: t1\n  data: {x: 1}\n- task_id: t2\n  data: {x: 2}\n- task_id: t3\n  data: {x: 3}\n"
    )
    out = list(yield_tasks_from_file(str(task_file), start_task=1))
    assert len(out) == 2
    assert out[0]["task_id"] == "t2"
    assert out[1]["task_id"] == "t3"
