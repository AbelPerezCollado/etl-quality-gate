from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from etl_quality_gate import (
    ControlConfig,
    CSVAuditSink,
    ETLControl,
    InMemoryAuditSink,
    VolumeViolationError,
)


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_emit_appends_rows_and_creates_parent_dirs(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "audit.csv"
    control = ETLControl(ControlConfig(process_name="ETL_CSV"), audit_sink=CSVAuditSink(path))
    with control:
        control.validate(row_count=10)
    rows = _read_rows(path)
    assert [row["status"] for row in rows] == ["RUNNING", "SUCCESS"]
    assert rows[0]["process_name"] == "ETL_CSV"
    assert rows[1]["rows_count"] == "10"
    assert rows[1]["error_code"] == ""


def test_header_written_once_across_runs(tmp_path: Path) -> None:
    path = tmp_path / "audit.csv"
    for name in ("ETL_A", "ETL_B"):
        with ETLControl(ControlConfig(process_name=name), audit_sink=CSVAuditSink(path)):
            pass
    assert path.read_text(encoding="utf-8").count("execution_id") == 1
    rows = _read_rows(path)
    assert len(rows) == 4
    assert [row["process_name"] for row in rows] == ["ETL_A", "ETL_A", "ETL_B", "ETL_B"]


def test_failed_run_persists_error_fields(tmp_path: Path) -> None:
    path = tmp_path / "audit.csv"
    control = ETLControl(ControlConfig(process_name="ETL_FAIL"), audit_sink=CSVAuditSink(path))
    with pytest.raises(RuntimeError, match="boom"):
        with control:
            raise RuntimeError("boom")
    rows = _read_rows(path)
    assert rows[-1]["status"] == "FAILED"
    assert rows[-1]["error_code"] == "RuntimeError"
    assert rows[-1]["error_message"] == "boom"


def test_volume_violation_fail_writes_failed_row(tmp_path: Path) -> None:
    path = tmp_path / "audit.csv"
    control = ETLControl(
        ControlConfig(process_name="ETL_VOL", min_rows=100, csv_path=str(path)),
        audit_sink=CSVAuditSink(path),
    )
    with pytest.raises(VolumeViolationError):
        with control:
            control.validate(row_count=5)
    rows = _read_rows(path)
    assert rows[-1]["status"] == "FAILED"
    assert rows[-1]["error_code"] == "VolumeViolationError"
    assert rows[-1]["rows_count"] == "5"


def test_config_csv_path_wires_sink(tmp_path: Path) -> None:
    path = tmp_path / "audit.csv"
    config = ControlConfig(process_name="ETL_CFG", csv_path=str(path))
    control = ETLControl(config)
    with control:
        control.validate(row_count=5)
    rows = _read_rows(path)
    assert rows[-1]["status"] == "SUCCESS"
    assert rows[-1]["rows_count"] == "5"


def test_from_json_wires_csv_sink(tmp_path: Path) -> None:
    path = tmp_path / "audit.csv"
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"process_name": "ETL_JSON", "csv_path": str(path), "min_rows": 1}),
        encoding="utf-8",
    )
    control = ETLControl.from_json(config_path)
    with control:
        control.validate(row_count=1)
    rows = _read_rows(path)
    assert rows[-1]["status"] == "SUCCESS"
    assert rows[-1]["process_name"] == "ETL_JSON"


def test_explicit_sink_overrides_csv_path(tmp_path: Path) -> None:
    path = tmp_path / "audit.csv"
    override = InMemoryAuditSink()
    control = ETLControl(
        ControlConfig(process_name="ETL_CFG", csv_path=str(path)),
        audit_sink=override,
    )
    with control:
        pass
    assert not path.exists()
    assert override.records


def test_warn_violation_writes_warning_status_row(tmp_path: Path) -> None:
    path = tmp_path / "audit.csv"
    control = ETLControl(
        ControlConfig(process_name="ETL_WARN", min_rows=100, on_violation="WARN", csv_path=str(path)),
        audit_sink=CSVAuditSink(path),
    )
    with control:
        control.validate(row_count=1)
    rows = _read_rows(path)
    assert rows[-1]["status"] == "SUCCESS_WITH_WARNINGS"
    assert rows[-1]["rows_count"] == "1"
