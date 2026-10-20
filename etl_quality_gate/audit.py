"""Destinos de auditoría: logs estructurados y persistencia en CSV."""

from __future__ import annotations

import csv
import json
import logging
import os
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Iterator, Protocol, Sequence, TextIO

from etl_quality_gate.models import ExecutionRecord, ExecutionStatus

try:
    import fcntl
except ImportError:  # pragma: no cover - solo plataformas POSIX
    fcntl = None  # type: ignore[assignment]


class AuditSink(Protocol):
    """Contrato mínimo para emitir un ``ExecutionRecord``.

    Implementaciones futuras (JSON en S3, repositorio corporativo, monitorización)
    deben cumplir este protocolo. El núcleo no asume permisos AWS.
    """

    def emit(self, record: ExecutionRecord) -> None: ...


class LoggingAuditSink:
    """Serializa el registro como JSON en el logger estándar."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("etl_quality_gate.audit")

    def emit(self, record: ExecutionRecord) -> None:
        payload = json.dumps(record.to_dict(), ensure_ascii=False)
        extra = {
            "execution_id": record.execution_id,
            "process_name": record.process_name,
            "status": str(record.status),
        }
        if record.status in {ExecutionStatus.FAILED, ExecutionStatus.ABORTED}:
            self._logger.error(payload, extra=extra)
        elif record.status is ExecutionStatus.SUCCESS_WITH_WARNINGS:
            self._logger.warning(payload, extra=extra)
        else:
            self._logger.info(payload, extra=extra)


class InMemoryAuditSink:
    """Acumula copias del registro. Pensado para pruebas y desarrollo local."""

    def __init__(self) -> None:
        self.records: list[ExecutionRecord] = []

    def emit(self, record: ExecutionRecord) -> None:
        self.records.append(deepcopy(record))


class CompositeAuditSink:
    """Reenvía el mismo evento a varios destinos."""

    def __init__(self, sinks: Sequence[AuditSink]) -> None:
        self._sinks = list(sinks)

    def emit(self, record: ExecutionRecord) -> None:
        for sink in self._sinks:
            sink.emit(record)


class CSVAuditSink:
    """Persiste cada ``ExecutionRecord`` como fila de un CSV (append).

    Cabecera única: se escribe solo si el fichero es nuevo o está vacío.
    Las escrituras concurrentes de varios jobs sobre el mismo fichero se
    serializan con un bloqueo ``flock`` (best-effort; en plataformas sin
    ``fcntl`` se escribe sin bloqueo). Un fallo de E/S no debe afectar a
    la ETL: ``ETLControl`` ya captura las excepciones del sink.
    """

    FIELDNAMES: tuple[str, ...] = (
        "execution_id",
        "process_name",
        "start_time",
        "end_time",
        "duration_seconds",
        "status",
        "rows_count",
        "error_code",
        "error_message",
    )

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    @property
    def path(self) -> Path:
        return self._path

    def emit(self, record: ExecutionRecord) -> None:
        payload = record.to_dict()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._open_locked() as handle:
            if os.fstat(handle.fileno()).st_size == 0:
                self._write_row(handle, payload, write_header=True)
            else:
                self._write_row(handle, payload, write_header=False)

    def _write_row(
        self, handle: TextIO, payload: dict[str, object], *, write_header: bool
    ) -> None:
        writer = csv.DictWriter(handle, fieldnames=self.FIELDNAMES, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({key: payload.get(key) for key in self.FIELDNAMES})
        handle.flush()

    @contextmanager
    def _open_locked(self) -> Iterator[TextIO]:
        handle = self._path.open("a", newline="", encoding="utf-8")
        try:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield handle
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
