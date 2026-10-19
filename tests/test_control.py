from __future__ import annotations

import uuid

import pytest

from etl_quality_gate import (
    ControlConfig,
    ControlUsageError,
    ETLControl,
    ExecutionRecord,
    ExecutionStatus,
    InMemoryAuditSink,
    VolumeViolationError,
)


class FakeFrame:
    def __init__(self, n: int) -> None:
        self.n = n
        self.calls = 0

    def count(self) -> int:
        self.calls += 1
        return self.n


def _control(**config_kwargs: object) -> tuple[ETLControl, InMemoryAuditSink]:
    data: dict[str, object] = {"process_name": "ETL_TEST"}
    data.update(config_kwargs)
    sink = InMemoryAuditSink()
    control = ETLControl(ControlConfig.from_dict(data), audit_sink=sink)  # type: ignore[arg-type]
    return control, sink


def test_successful_run_without_validation() -> None:
    control, sink = _control()
    with control:
        pass
    assert control.record.status is ExecutionStatus.SUCCESS
    assert control.record.rows_count is None
    assert control.record.start_time is not None
    assert control.record.end_time is not None
    assert control.record.duration_seconds is not None
    assert control.record.duration_seconds >= 0
    assert control.record.error_code is None
    assert [item.status for item in sink.records] == [ExecutionStatus.RUNNING, ExecutionStatus.SUCCESS]


def test_exception_is_recorded_and_propagated() -> None:
    control, sink = _control()
    with pytest.raises(RuntimeError, match="boom"):
        with control:
            raise RuntimeError("boom")
    assert control.record.status is ExecutionStatus.FAILED
    assert control.record.error_code == "RuntimeError"
    assert control.record.error_message == "boom"
    assert sink.records[-1].status is ExecutionStatus.FAILED


def test_volume_within_range() -> None:
    control, _ = _control(min_rows=80, max_rows=120)
    with control:
        count = control.validate(FakeFrame(100))
    assert count == 100
    assert control.record.rows_count == 100
    assert control.record.status is ExecutionStatus.SUCCESS


def test_min_threshold_fail() -> None:
    control, _ = _control(min_rows=80, max_rows=120)
    with pytest.raises(VolumeViolationError) as exc_info:
        with control:
            control.validate(FakeFrame(10))
    err = exc_info.value
    assert err.row_count == 10
    assert err.min_rows == 80
    assert err.max_rows == 120
    assert err.process_name == "ETL_TEST"
    assert control.record.status is ExecutionStatus.FAILED
    assert control.record.error_code == "VolumeViolationError"


def test_max_threshold_fail() -> None:
    control, _ = _control(min_rows=10, max_rows=100)
    with pytest.raises(VolumeViolationError) as exc_info:
        with control:
            control.validate(row_count=500)
    assert exc_info.value.row_count == 500


def test_warn_policy_does_not_raise() -> None:
    control, sink = _control(min_rows=100, max_rows=200, on_violation="WARN")
    with control:
        control.validate(row_count=1)
    assert control.record.status is ExecutionStatus.SUCCESS_WITH_WARNINGS
    assert sink.records[-1].status is ExecutionStatus.SUCCESS_WITH_WARNINGS


def test_explicit_row_count_skips_dataframe_count() -> None:
    frame = FakeFrame(999)
    control, _ = _control(min_rows=1)
    with control:
        control.validate(frame, row_count=50)
    assert frame.calls == 0
    assert control.record.rows_count == 50


def test_count_is_cached_for_same_dataframe() -> None:
    frame = FakeFrame(40)
    control, _ = _control(min_rows=1)
    with control:
        control.validate(frame)
        control.validate(frame)
    assert frame.calls == 1


def test_execution_id_generated_and_overridable() -> None:
    auto, _ = _control()
    auto.start()
    auto.finish()
    uuid.UUID(auto.execution_id)

    sink = InMemoryAuditSink()
    forced = ETLControl(ControlConfig(process_name="p", execution_id="engine-123"), audit_sink=sink)
    assert forced.execution_id == "engine-123"


def test_failed_process_is_never_success() -> None:
    control, sink = _control()
    with pytest.raises(ValueError):
        with control:
            raise ValueError("negocio")
    assert all(item.status is not ExecutionStatus.SUCCESS for item in sink.records)
    assert sink.records[-1].status is ExecutionStatus.FAILED


def test_manual_start_finish() -> None:
    control, sink = _control(min_rows=1)
    control.start()
    control.validate(row_count=3)
    control.finish()
    assert control.record.status is ExecutionStatus.SUCCESS
    assert control.record.rows_count == 3
    assert [item.status for item in sink.records] == [
        ExecutionStatus.RUNNING,
        ExecutionStatus.SUCCESS,
    ]


def test_aborted_path_via_atexit_hook() -> None:
    control, sink = _control()
    control.start()
    control._on_interpreter_exit()
    assert control.record.status is ExecutionStatus.ABORTED
    assert control.record.error_code == "ABORTED"
    assert sink.records[-1].status is ExecutionStatus.ABORTED


def test_audit_payload() -> None:
    control, sink = _control(min_rows=1)
    with control:
        control.validate(row_count=8)
    payload = sink.records[-1].to_dict()
    assert payload["process_name"] == "ETL_TEST"
    assert payload["execution_id"] == control.execution_id
    assert payload["status"] == "SUCCESS"
    assert payload["rows_count"] == 8
    assert payload["start_time"]
    assert payload["end_time"]
    assert payload["duration_seconds"] is not None
    assert payload["error_code"] is None


def test_cannot_validate_before_start() -> None:
    control, _ = _control()
    with pytest.raises(ControlUsageError):
        control.validate(row_count=1)


def test_cannot_validate_after_close() -> None:
    control, _ = _control(min_rows=1)
    control.start()
    control.finish()
    with pytest.raises(ControlUsageError):
        control.validate(row_count=1)


def test_config_must_be_control_config() -> None:
    with pytest.raises(ControlUsageError):
        ETLControl({"process_name": "p"})  # type: ignore[arg-type]


def test_from_json_construction() -> None:
    control = ETLControl.from_json('{"process_name": "ETL_JSON", "min_rows": 1}')
    with control:
        control.validate(row_count=1)
    assert control.record.status is ExecutionStatus.SUCCESS


def test_broken_sink_does_not_break_etl() -> None:
    class BrokenSink:
        def emit(self, record: ExecutionRecord) -> None:
            raise OSError("disco lleno")

    control = ETLControl(ControlConfig(process_name="p"), audit_sink=BrokenSink())
    with control:
        pass
    assert control.record.status is ExecutionStatus.SUCCESS
