from __future__ import annotations

import json
from pathlib import Path

import pytest

from etl_quality_gate import ControlConfig, FailureAction, InvalidConfigError


def test_valid_config_from_dict() -> None:
    cfg = ControlConfig.from_dict(
        {
            "process_name": "ETL_CLIENTES",
            "min_rows": 80_000,
            "max_rows": 120_000,
            "on_violation": "WARN",
            "csv_path": "logs/audit.csv",
        }
    )
    assert cfg.process_name == "ETL_CLIENTES"
    assert cfg.min_rows == 80_000
    assert cfg.max_rows == 120_000
    assert cfg.on_violation is FailureAction.WARN
    assert cfg.csv_path == "logs/audit.csv"


def test_minimal_config() -> None:
    cfg = ControlConfig.from_dict({"process_name": "p"})
    assert cfg.min_rows is None
    assert cfg.max_rows is None
    assert cfg.on_violation is FailureAction.FAIL
    assert cfg.csv_path is None
    assert cfg.execution_id is None


def test_missing_process_name() -> None:
    with pytest.raises(InvalidConfigError, match="process_name"):
        ControlConfig.from_dict({"min_rows": 1})


def test_empty_process_name() -> None:
    with pytest.raises(InvalidConfigError, match="process_name"):
        ControlConfig(process_name="  ")


def test_wrong_type_threshold() -> None:
    with pytest.raises(InvalidConfigError, match="min_rows"):
        ControlConfig.from_dict({"process_name": "p", "min_rows": "80000"})


def test_boolean_is_not_int() -> None:
    with pytest.raises(InvalidConfigError, match="max_rows"):
        ControlConfig.from_dict({"process_name": "p", "max_rows": True})


def test_negative_threshold() -> None:
    with pytest.raises(InvalidConfigError, match="min_rows"):
        ControlConfig.from_dict({"process_name": "p", "min_rows": -1})


def test_incoherent_thresholds() -> None:
    with pytest.raises(InvalidConfigError, match="incoherente"):
        ControlConfig.from_dict({"process_name": "p", "min_rows": 100, "max_rows": 10})


def test_unknown_key() -> None:
    with pytest.raises(InvalidConfigError, match="no reconocidas"):
        ControlConfig.from_dict({"process_name": "p", "unknown": 1})


def test_invalid_on_violation() -> None:
    with pytest.raises(InvalidConfigError, match="FAIL"):
        ControlConfig.from_dict({"process_name": "p", "on_violation": "IGNORE"})


def test_on_violation_case_insensitive() -> None:
    cfg = ControlConfig.from_dict({"process_name": "p", "on_violation": "warn"})
    assert cfg.on_violation is FailureAction.WARN


def test_empty_csv_path() -> None:
    with pytest.raises(InvalidConfigError, match="csv_path"):
        ControlConfig.from_dict({"process_name": "p", "csv_path": ""})


def test_from_json_file(tmp_path: Path) -> None:
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps({"process_name": "ETL_X", "min_rows": 5}), encoding="utf-8")
    cfg = ControlConfig.from_json(path)
    assert cfg.process_name == "ETL_X"
    assert cfg.min_rows == 5


def test_from_json_string() -> None:
    cfg = ControlConfig.from_json('{"process_name": "ETL_X"}')
    assert cfg.process_name == "ETL_X"


def test_from_json_invalid() -> None:
    with pytest.raises(InvalidConfigError, match="JSON"):
        ControlConfig.from_json("{not json")


def test_from_json_not_object() -> None:
    with pytest.raises(InvalidConfigError, match="objeto"):
        ControlConfig.from_json("[1, 2]")
