"""Excepciones públicas de ETL Quality Gate."""

from __future__ import annotations

from typing import Any


class QualityGateError(Exception):
    """Error base de la librería. No se usa para envolver fallos de negocio de la ETL."""


class InvalidConfigError(QualityGateError):
    """La configuración es incompleta, tiene tipos incorrectos o umbrales incoherentes."""


class ControlUsageError(QualityGateError):
    """Uso incorrecto de la API (por ejemplo, validar sin DataFrame ni recuento)."""


class VolumeViolationError(QualityGateError):
    """Incumplimiento bloqueante de un umbral de volumetría."""

    def __init__(
        self,
        message: str,
        *,
        row_count: int,
        min_rows: int | None,
        max_rows: int | None,
        execution_id: str | None = None,
        process_name: str | None = None,
    ) -> None:
        super().__init__(message)
        self.row_count = row_count
        self.min_rows = min_rows
        self.max_rows = max_rows
        self.execution_id = execution_id
        self.process_name = process_name

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_count": self.row_count,
            "min_rows": self.min_rows,
            "max_rows": self.max_rows,
            "execution_id": self.execution_id,
            "process_name": self.process_name,
            "message": str(self),
        }
