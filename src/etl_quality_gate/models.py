"""Modelos de estado y auditoría de una ejecución."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class ExecutionStatus(StrEnum):
    """Estados normalizados de una ejecución controlada."""

    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_WARNINGS = "SUCCESS_WITH_WARNINGS"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


@dataclass
class ExecutionRecord:
    """Instantánea estructurada de una ejecución.

    Los campos desconocidos permanecen en ``None``. Este registro es el contrato
    de auditoría: los destinos futuros (S3, repositorio corporativo, etc.)
    deben consumirlo sin acoplarse a ``ETLControl``.
    """

    execution_id: str
    process_name: str
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_seconds: float | None = None
    status: ExecutionStatus = ExecutionStatus.RUNNING
    rows_count: int | None = None
    error_code: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serializa el registro a un diccionario JSON-compatible."""
        payload = asdict(self)
        payload["status"] = str(self.status)
        if self.start_time is not None:
            payload["start_time"] = self.start_time.isoformat()
        if self.end_time is not None:
            payload["end_time"] = self.end_time.isoformat()
        return payload
