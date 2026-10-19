"""Evaluación de umbrales de volumetría sin acoplarse a Spark."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from etl_quality_gate.exceptions import ControlUsageError


class Countable(Protocol):
    """Cualquier objeto con ``count()`` (p.ej. un DataFrame de PySpark)."""

    def count(self) -> int: ...


@dataclass(frozen=True)
class VolumeCheckResult:
    """Resultado de comparar un recuento con umbrales opcionales."""

    row_count: int
    min_rows: int | None
    max_rows: int | None
    ok: bool
    violation: str | None

    @property
    def message(self) -> str:
        if self.violation == "below_min":
            return f"Volumetría por debajo del mínimo: {self.row_count} < {self.min_rows}."
        if self.violation == "above_max":
            return f"Volumetría por encima del máximo: {self.row_count} > {self.max_rows}."
        return f"Volumetría dentro de rango: {self.row_count} (min={self.min_rows}, max={self.max_rows})."


def resolve_row_count(df: Countable | None, row_count: int | None) -> int:
    """Obtiene el recuento sin acciones Spark innecesarias.

    Si ``row_count`` está informado, **no** se llama a ``df.count()``.
    ``count()`` es una acción distribuida potencialmente costosa; la ETL
    debe reutilizar recuentos ya calculados siempre que existan.
    """
    if row_count is not None:
        return _require_count(row_count, "row_count")
    if df is None:
        raise ControlUsageError(
            "Debe proporcionar un DataFrame con count() o un row_count explícito."
        )
    count_fn = getattr(df, "count", None)
    if not callable(count_fn):
        raise ControlUsageError(
            "El objeto a validar no expone count(); pase row_count o un DataFrame Spark."
        )
    value = count_fn()
    return _require_count(value, "DataFrame.count()")


def evaluate_thresholds(
    *,
    row_count: int,
    min_rows: int | None,
    max_rows: int | None,
) -> VolumeCheckResult:
    """Compara un recuento con umbrales. Un lado ``None`` no se evalúa."""
    if min_rows is not None and row_count < min_rows:
        return VolumeCheckResult(
            row_count=row_count,
            min_rows=min_rows,
            max_rows=max_rows,
            ok=False,
            violation="below_min",
        )
    if max_rows is not None and row_count > max_rows:
        return VolumeCheckResult(
            row_count=row_count,
            min_rows=min_rows,
            max_rows=max_rows,
            ok=False,
            violation="above_max",
        )
    return VolumeCheckResult(
        row_count=row_count,
        min_rows=min_rows,
        max_rows=max_rows,
        ok=True,
        violation=None,
    )


def _require_count(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ControlUsageError(f"{field_name} debe ser un entero no negativo.")
    if value < 0:
        raise ControlUsageError(f"{field_name} no puede ser negativo.")
    return value
