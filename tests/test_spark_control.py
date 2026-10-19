from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from etl_quality_gate import (  # noqa: E402
    ControlConfig,
    ETLControl,
    ExecutionStatus,
    InMemoryAuditSink,
    VolumeViolationError,
)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("etl-quality-gate-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_validate_counts_spark_dataframe(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    sink = InMemoryAuditSink()
    config = ControlConfig(process_name="ETL_SPARK", min_rows=1, max_rows=10)
    with ETLControl(config, audit_sink=sink) as control:
        count = control.validate(df)
    assert count == 3
    assert control.record.rows_count == 3
    assert control.record.status is ExecutionStatus.SUCCESS


def test_spark_volume_violation(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,)], ["id"])
    config = ControlConfig(process_name="ETL_SPARK_FAIL", min_rows=5)
    with pytest.raises(VolumeViolationError):
        with ETLControl(config) as control:
            control.validate(df)
