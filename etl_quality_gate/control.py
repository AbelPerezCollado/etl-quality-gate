"""Contexto de ejecución: ciclo de vida, volumetría y propagación de errores."""

from __future__ import annotations

import atexit
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import Self

from etl_quality_gate.audit import (
    AuditSink,
    CompositeAuditSink,
    CSVAuditSink,
    LoggingAuditSink,
)
from etl_quality_gate.config import ControlConfig, FailureAction
from etl_quality_gate.exceptions import ControlUsageError, VolumeViolationError
from etl_quality_gate.models import ExecutionRecord, ExecutionStatus
from etl_quality_gate.volumetry import Countable, VolumeCheckResult, evaluate_thresholds, resolve_row_count

_LOGGER = logging.getLogger("etl_quality_gate")


class ETLControl:
    """Capa de control reutilizable para una ejecución de ETL.

    No crea ``SparkSession``. No escribe en S3 ni notifica a servicios AWS.
    Las excepciones de la ETL se registran y se vuelven a lanzar.

    Uso::

        with ETLControl.from_json("config_etl.json") as control:
            df = spark.read.parquet("s3://entrada")
            control.validate(df)  # compara .count() con min_rows/max_rows
            df.write.parquet("s3://salida")
    """

    def __init__(
        self,
        config: ControlConfig,
        *,
        audit_sink: AuditSink | None = None,
        execution_id: str | None = None,
    ) -> None:
        if not isinstance(config, ControlConfig):
            raise ControlUsageError(
                "config debe ser un ControlConfig; use ETLControl.from_json(path) para cargarlo de un fichero."
            )
        self._config = config
        resolved_id = execution_id or config.execution_id or str(uuid.uuid4())
        self._record = ExecutionRecord(
            execution_id=resolved_id,
            process_name=config.process_name,
            status=ExecutionStatus.RUNNING,
        )
        self._sink: AuditSink = audit_sink or self._build_default_sink(config)
        self._started = False
        self._closed = False
        self._had_warnings = False
        self._count_cache: dict[int, int] = {}
        self._atexit_registered = False
        self._log = logging.LoggerAdapter(
            _LOGGER,
            extra={"execution_id": self._record.execution_id, "process_name": self._record.process_name},
        )

    @classmethod
    def from_json(
        cls,
        source: str | Path,
        *,
        audit_sink: AuditSink | None = None,
        execution_id: str | None = None,
    ) -> ETLControl:
        """Construye el control desde un fichero (o cadena) JSON de configuración."""
        return cls(
            ControlConfig.from_json(source),
            audit_sink=audit_sink,
            execution_id=execution_id,
        )

    @property
    def execution_id(self) -> str:
        return self._record.execution_id

    @property
    def process_name(self) -> str:
        return self._record.process_name

    @property
    def record(self) -> ExecutionRecord:
        """Registro vivo de la ejecución. Trátelo como de solo lectura."""
        return self._record

    @property
    def config(self) -> ControlConfig:
        return self._config

    def start(self) -> Self:
        """Marca el inicio, emite auditoría RUNNING y registra el cierre abrupto."""
        if self._closed:
            raise ControlUsageError("No se puede iniciar un control ya finalizado.")
        if self._started:
            raise ControlUsageError("El control ya está iniciado.")
        self._started = True
        self._record.start_time = datetime.now(timezone.utc)
        self._record.status = ExecutionStatus.RUNNING
        self._emit()
        self._register_atexit()
        self._log.info("Ejecución iniciada.")
        return self

    def finish(self, error: BaseException | None = None) -> ExecutionRecord:
        """Cierra el control. Si ``error`` está presente, el estado es FAILED."""
        if not self._started:
            raise ControlUsageError("finish() requiere start() previo.")
        if error is not None:
            self._finalize_failure(error)
        else:
            status = (
                ExecutionStatus.SUCCESS_WITH_WARNINGS
                if self._had_warnings
                else ExecutionStatus.SUCCESS
            )
            self._finalize(status=status)
        return self._record

    def __enter__(self) -> Self:
        return self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if exc is not None:
            self._finalize_failure(exc)
            return False
        status = (
            ExecutionStatus.SUCCESS_WITH_WARNINGS
            if self._had_warnings
            else ExecutionStatus.SUCCESS
        )
        self._finalize(status=status)
        return False

    def validate(self, df: Countable | None = None, *, row_count: int | None = None) -> int:
        """Valida el volumen contra los umbrales ``min_rows`` / ``max_rows`` de la configuración.

        Si ``row_count`` está informado, no se llama a ``df.count()``.
        Política ``WARN``: registra la anomalía y continúa (estado final
        ``SUCCESS_WITH_WARNINGS``). Política ``FAIL``: lanza
        ``VolumeViolationError`` y la ejecución termina ``FAILED``.
        """
        self._ensure_open()
        count = self._resolve_count(df, row_count)
        self._record.rows_count = count
        result = evaluate_thresholds(
            row_count=count,
            min_rows=self._config.min_rows,
            max_rows=self._config.max_rows,
        )
        if result.ok:
            self._log.info(result.message)
            return count
        return self._handle_volume_violation(result)

    def _resolve_count(self, df: Countable | None, row_count: int | None) -> int:
        if row_count is not None:
            return resolve_row_count(None, row_count)
        if df is not None:
            key = id(df)
            cached = self._count_cache.get(key)
            if cached is not None:
                self._log.debug("Reutilizando recuento cacheado (id=%s): %s", key, cached)
                return cached
            count = resolve_row_count(df, None)
            self._count_cache[key] = count
            return count
        return resolve_row_count(df, row_count)

    def _handle_volume_violation(self, result: VolumeCheckResult) -> int:
        extra = {
            "row_count": result.row_count,
            "min_rows": result.min_rows,
            "max_rows": result.max_rows,
        }
        if self._config.on_violation is FailureAction.WARN:
            self._had_warnings = True
            self._log.warning("%s (política WARN)", result.message, extra=extra)
            return result.row_count
        error = VolumeViolationError(
            result.message,
            row_count=result.row_count,
            min_rows=result.min_rows,
            max_rows=result.max_rows,
            execution_id=self.execution_id,
            process_name=self.process_name,
        )
        self._log.error("%s (política FAIL)", result.message, extra=extra)
        raise error

    def _ensure_open(self) -> None:
        if self._closed:
            raise ControlUsageError("El control ya está finalizado.")
        if not self._started:
            raise ControlUsageError("El control no está iniciado. Use el context manager o start().")

    @staticmethod
    def _build_default_sink(config: ControlConfig) -> AuditSink:
        """Sink por defecto: logging, o logging + CSV si ``csv_path`` está configurado."""
        if config.csv_path:
            return CompositeAuditSink([LoggingAuditSink(), CSVAuditSink(config.csv_path)])
        return LoggingAuditSink()

    def _finalize_failure(self, error: BaseException) -> None:
        message = str(error) or type(error).__name__
        # Nunca se oculta el fallo: FAILED + re-raise en __exit__.
        self._finalize(
            status=ExecutionStatus.FAILED,
            error_code=type(error).__name__,
            error_message=message,
        )

    def _finalize(
        self,
        *,
        status: ExecutionStatus,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if self._closed:
            return
        self._closed = True
        self._record.end_time = datetime.now(timezone.utc)
        if self._record.start_time is not None:
            self._record.duration_seconds = (
                self._record.end_time - self._record.start_time
            ).total_seconds()
        self._record.status = status
        self._record.error_code = error_code
        self._record.error_message = error_message
        self._emit()
        self._log.info("Ejecución finalizada con estado %s.", status)

    def _emit(self) -> None:
        try:
            self._sink.emit(self._record)
        except Exception:
            # Un fallo del destino de auditoría no debe marcar la ETL como OK,
            # pero tampoco debe silenciar el error original de la ETL.
            _LOGGER.exception(
                "Fallo al emitir auditoría (execution_id=%s).",
                self._record.execution_id,
            )

    def _register_atexit(self) -> None:
        if self._atexit_registered:
            return
        atexit.register(self._on_interpreter_exit)
        self._atexit_registered = True

    def _on_interpreter_exit(self) -> None:
        """Mejor esfuerzo si el proceso termina sin salir del contexto.

        No cubre SIGKILL, kill -9 ni un fallo del driver Spark que aborte
        el intérprete sin ejecutar ``atexit``.
        """
        if self._started and not self._closed:
            self._finalize(
                status=ExecutionStatus.ABORTED,
                error_code="ABORTED",
                error_message="El proceso terminó sin completar el contexto de control.",
            )
