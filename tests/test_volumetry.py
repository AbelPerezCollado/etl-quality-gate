from __future__ import annotations

import pytest

from etl_quality_gate.exceptions import ControlUsageError
from etl_quality_gate.volumetry import evaluate_thresholds, resolve_row_count


class FakeFrame:
    def __init__(self, n: int) -> None:
        self.n = n
        self.calls = 0

    def count(self) -> int:
        self.calls += 1
        return self.n


def test_within_range() -> None:
    result = evaluate_thresholds(row_count=100, min_rows=80, max_rows=120)
    assert result.ok
    assert result.violation is None


def test_below_min() -> None:
    result = evaluate_thresholds(row_count=10, min_rows=80, max_rows=120)
    assert not result.ok
    assert result.violation == "below_min"


def test_above_max() -> None:
    result = evaluate_thresholds(row_count=200, min_rows=80, max_rows=120)
    assert not result.ok
    assert result.violation == "above_max"


def test_only_min_or_max() -> None:
    assert evaluate_thresholds(row_count=1, min_rows=None, max_rows=10).ok
    assert evaluate_thresholds(row_count=1, min_rows=1, max_rows=None).ok


def test_no_thresholds_always_ok() -> None:
    assert evaluate_thresholds(row_count=12345, min_rows=None, max_rows=None).ok


def test_messages() -> None:
    assert "mínimo" in evaluate_thresholds(row_count=1, min_rows=10, max_rows=None).message
    assert "máximo" in evaluate_thresholds(row_count=99, min_rows=None, max_rows=10).message
    assert "rango" in evaluate_thresholds(row_count=5, min_rows=1, max_rows=10).message


def test_resolve_prefers_explicit_count() -> None:
    frame = FakeFrame(99)
    assert resolve_row_count(frame, 5) == 5
    assert frame.calls == 0


def test_resolve_calls_count_once_per_invocation() -> None:
    frame = FakeFrame(7)
    assert resolve_row_count(frame, None) == 7
    assert frame.calls == 1


def test_resolve_requires_df_or_count() -> None:
    with pytest.raises(ControlUsageError):
        resolve_row_count(None, None)


def test_resolve_rejects_negative() -> None:
    with pytest.raises(ControlUsageError):
        resolve_row_count(None, -1)


def test_resolve_rejects_non_countable() -> None:
    with pytest.raises(ControlUsageError):
        resolve_row_count(object(), None)  # type: ignore[arg-type]
