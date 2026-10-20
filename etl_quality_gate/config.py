"""Configuración de controles por ETL, validada sin dependencias externas."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping

from etl_quality_gate.exceptions import InvalidConfigError


class FailureAction(StrEnum):
    """Política ante un incumplimiento de control."""

    FAIL = "FAIL"
    WARN = "WARN"


_ALLOWED_KEYS = {"process_name", "min_rows", "max_rows", "on_violation", "csv_path", "execution_id"}


def _require_non_empty_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidConfigError(f"'{field_name}' es obligatorio y debe ser un string no vacío.")
    return value.strip()


def _optional_non_negative_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidConfigError(f"'{field_name}' debe ser un entero no negativo o null.")
    if value < 0:
        raise InvalidConfigError(f"'{field_name}' no puede ser negativo.")
    return value


def _parse_failure_action(value: Any, field_name: str) -> FailureAction:
    if isinstance(value, FailureAction):
        return value
    if not isinstance(value, str):
        raise InvalidConfigError(f"'{field_name}' debe ser 'FAIL' o 'WARN'.")
    try:
        return FailureAction(value.upper())
    except ValueError as exc:
        raise InvalidConfigError(f"'{field_name}' debe ser 'FAIL' o 'WARN'.") from exc


@dataclass(frozen=True)
class ControlConfig:
    """Configuración inmutable de una ejecución controlada.

    ``min_rows`` / ``max_rows``: umbrales de volumetría; un lado ``None``
    no se valida. ``on_violation``: ``FAIL`` lanza ``VolumeViolationError``;
    ``WARN`` deja el estado final en ``SUCCESS_WITH_WARNINGS``.
    ``csv_path``: si se indica, el registro de auditoría también se persiste
    a un CSV (además de ir a los logs).
    """

    process_name: str
    min_rows: int | None = None
    max_rows: int | None = None
    on_violation: FailureAction = FailureAction.FAIL
    csv_path: str | None = None
    execution_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "process_name", _require_non_empty_str(self.process_name, "process_name"))
        object.__setattr__(self, "min_rows", _optional_non_negative_int(self.min_rows, "min_rows"))
        object.__setattr__(self, "max_rows", _optional_non_negative_int(self.max_rows, "max_rows"))
        object.__setattr__(
            self,
            "on_violation",
            _parse_failure_action(self.on_violation, "on_violation"),
        )
        if self.csv_path is not None:
            object.__setattr__(self, "csv_path", _require_non_empty_str(self.csv_path, "csv_path"))
        if self.execution_id is not None:
            object.__setattr__(
                self,
                "execution_id",
                _require_non_empty_str(self.execution_id, "execution_id"),
            )
        if self.min_rows is not None and self.max_rows is not None and self.min_rows > self.max_rows:
            raise InvalidConfigError(
                f"Umbral incoherente: 'min_rows' ({self.min_rows}) es mayor que 'max_rows' ({self.max_rows})."
            )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ControlConfig:
        if not isinstance(data, Mapping):
            raise InvalidConfigError("La configuración debe ser un objeto.")
        unknown = set(data) - _ALLOWED_KEYS
        if unknown:
            raise InvalidConfigError(f"Claves de configuración no reconocidas: {sorted(unknown)}.")
        if "process_name" not in data:
            raise InvalidConfigError("'process_name' es obligatorio.")
        return cls(
            process_name=data["process_name"],
            min_rows=data.get("min_rows"),
            max_rows=data.get("max_rows"),
            on_violation=data.get("on_violation", FailureAction.FAIL),
            csv_path=data.get("csv_path"),
            execution_id=data.get("execution_id"),
        )

    @classmethod
    def from_json(cls, source: str | Path) -> ControlConfig:
        """Carga configuración desde una ruta a fichero JSON o una cadena JSON."""
        path = Path(source)
        raw: str
        if path.is_file():
            raw = path.read_text(encoding="utf-8")
        else:
            raw = str(source)
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise InvalidConfigError("El JSON de configuración no es válido.") from exc
        if not isinstance(parsed, dict):
            raise InvalidConfigError("El JSON de configuración debe ser un objeto.")
        return cls.from_dict(parsed)
