"""Runnable example of ETL Quality Gate that does not require Spark.

Any object exposing ``count()`` satisfies the library's ``Countable``
protocol, so this example runs anywhere -- no PySpark install needed.

No installation required; it works from any working directory:

    python examples/example.py

It validates a row count against the thresholds in ``examples/config.json``
and writes the audit record to ``examples/audit.csv``.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etl_quality_gate import ETLControl  # noqa: E402

HERE = Path(__file__).resolve().parent
OUTPUT = HERE / "audit2.csv"


class RowSet:
    """Minimal stand-in for a Spark DataFrame: it only needs ``count()``."""

    def __init__(self, rows: int) -> None:
        self._rows = rows

    def count(self) -> int:
        return self._rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Keep the committed sample stable while this example runs locally
    OUTPUT.unlink(missing_ok=True)

    source = RowSet(1)  # stands in for rows read from the source

    with ETLControl.from_json(HERE / "config.json") as control:
        rows = control.validate(source)  # calls source.count(), applies thresholds
        print(f"Validated {rows:,} rows against the configured min/max thresholds.")

    record = control.record
    print(f"Final status: {record.status} (duration {record.duration_seconds:.3f}s)")
    print(f"Audit CSV written to: {OUTPUT.relative_to(HERE.parent)}")


if __name__ == "__main__":
    main()
