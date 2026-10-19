"""ETL Quality Gate: controles reutilizables para procesos ETL PySpark."""

from etl_quality_gate.audit import (
    AuditSink,
    CompositeAuditSink,
    CSVAuditSink,
    InMemoryAuditSink,
    LoggingAuditSink,
)
from etl_quality_gate.config import ControlConfig, FailureAction
from etl_quality_gate.control import ETLControl
from etl_quality_gate.exceptions import (
    ControlUsageError,
    InvalidConfigError,
    QualityGateError,
    VolumeViolationError,
)
from etl_quality_gate.models import ExecutionRecord, ExecutionStatus

__version__ = "0.3.0"

__all__ = [
    "AuditSink",
    "CompositeAuditSink",
    "ControlConfig",
    "ControlUsageError",
    "CSVAuditSink",
    "ETLControl",
    "ExecutionRecord",
    "ExecutionStatus",
    "FailureAction",
    "InMemoryAuditSink",
    "InvalidConfigError",
    "LoggingAuditSink",
    "QualityGateError",
    "VolumeViolationError",
    "__version__",
]
