"""Lightweight, dependency-free data-quality "expectations" + cuadraturas.

Works on PySpark DataFrames. Exposes a GE-like API so the notebooks can keep
the same shape if Great Expectations or DLT expectations are plugged in later.

Evidence (report) is written to a Volume as JSON + Markdown, so the result sets
are visible in the portfolio without external tooling.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

_EXPECTATION_TYPES = {
    "expect_column_values_to_not_be_null",
    "expect_column_distinct_values_to_contain_set",
    "expect_column_values_to_be_between",
    "expect_column_value_lengths_to_be_between",
    "expect_row_count_to_be_between",
    "expect_column_sum_to_be_between",
    "expect_column_unique_value_count_to_be_between",
}


@dataclass
class ExpectationResult:
    expectation_type: str
    column: str
    passed: bool
    observed: str
    detail: dict = field(default_factory=dict)

    def as_row(self) -> dict:
        return {
            "expectation_type": self.expectation_type,
            "column": self.column,
            "passed": self.passed,
            "observed": self.observed,
            "detail": json.dumps(self.detail, default=str),
        }


class DataFrameExpectations:
    """Run named expectations against a Spark DataFrame."""

    def __init__(self, df, table: str = "df", dq_threshold: float = 0.05):
        self.df = df
        self.table = table
        self.threshold = dq_threshold
        self.results: list[ExpectationResult] = []

    def expect_column_values_to_not_be_null(self, column: str) -> DataFrameExpectations:
        nulls = self.df.filter(self.df[column].isNull()).count()
        total = self.df.count()
        passed = nulls == 0
        self.results.append(
            ExpectationResult(
                "expect_column_values_to_not_be_null",
                column,
                passed,
                f"{nulls}/{total} nulls",
                {"null_count": nulls, "row_count": total},
            )
        )
        return self

    def expect_row_count_to_be_between(
        self, min_value: int, max_value: int | None = None
    ) -> DataFrameExpectations:
        total = self.df.count()
        ok_floor = total >= min_value
        ok_ceil = max_value is None or total <= max_value
        passed = ok_floor and ok_ceil
        self.results.append(
            ExpectationResult(
                "expect_row_count_to_be_between",
                "",
                passed,
                f"{total} rows",
                {"min_value": min_value, "max_value": max_value, "observed": total},
            )
        )
        return self

    def expect_column_distinct_values_to_contain_set(
        self, column: str, values: set
    ) -> DataFrameExpectations:

        observed = {row[0] for row in self.df.select(column).distinct().collect()}
        missing = sorted(values - observed)
        passed = not missing
        self.results.append(
            ExpectationResult(
                "expect_column_distinct_values_to_contain_set",
                column,
                passed,
                f"missing={missing}",
                {"expected": sorted(values), "observed": sorted(observed)},
            )
        )
        return self

    def expect_column_values_to_be_between(
        self, column: str, min_value: float | None = None, max_value: float | None = None
    ) -> DataFrameExpectations:
        from pyspark.sql import functions as F

        agg = []
        if max_value is not None:
            agg.append(F.max(F.col(column)).alias("max"))
        if min_value is not None:
            agg.append(F.min(F.col(column)).alias("min"))
        row = self.df.agg(*agg).collect()[0]
        passed = True
        detail = {}
        if max_value is not None:
            observed = row["max"]
            detail["max_value"] = max_value
            detail["observed_max"] = None if observed is None else float(observed)
            passed = passed and (observed is None or float(observed) <= max_value)
        if min_value is not None:
            observed = row["min"]
            detail["min_value"] = min_value
            detail["observed_min"] = None if observed is None else float(observed)
            passed = passed and (observed is None or float(observed) >= min_value)
        self.results.append(
            ExpectationResult(
                "expect_column_values_to_be_between",
                column,
                passed,
                json.dumps(detail),
                detail,
            )
        )
        return self

    def expect_column_sum_to_be_between(
        self, column: str, min_value: float | None = None, max_value: float | None = None
    ) -> DataFrameExpectations:
        from pyspark.sql import functions as F

        s = float(self.df.select(F.coalesce(F.sum(column), F.lit(0)).alias("s")).collect()[0]["s"])
        passed = (min_value is None or s >= min_value) and (max_value is None or s <= max_value)
        detail = {"sum": s, "min_value": min_value, "max_value": max_value}
        self.results.append(
            ExpectationResult(
                "expect_column_sum_to_be_between", column, passed, f"sum={s:,.2f}", detail
            )
        )
        return self

    def run_all(self) -> list[dict]:
        """Return results as rows plus a single 'overall' pass/fail row."""
        failures = [r for r in self.results if not r.passed]
        overall = len(failures) <= self.threshold * max(len(self.results), 1)
        rows = [r.as_row() for r in self.results]
        rows.append(
            {
                "expectation_type": "overall",
                "column": "",
                "passed": overall,
                "observed": f"{len(failures)}/{len(rows)} failed",
                "detail": json.dumps({"threshold": self.threshold}),
            }
        )
        if not overall:
            details = "; ".join(f"{r.expectation_type}:{r.column}={r.observed}" for r in failures)
            raise ValueError(f"Data quality failed for {self.table} ({details})")
        return rows

    def to_frame(self):
        from pyspark.sql import types as T

        schema = T.StructType(
            [
                T.StructField("expectation_type", T.StringType()),
                T.StructField("column", T.StringType()),
                T.StructField("passed", T.BooleanType()),
                T.StructField("observed", T.StringType()),
                T.StructField("detail", T.StringType()),
            ]
        )
        return self.df.sparkSession.createDataFrame(self.run_all(), schema=schema)


def assert_balances(checks: list[dict]) -> list[dict]:
    """Simple cuadratura checks: expected vs actual totals within a tolerance.

    Each check: {"name", "expected", "actual", "tolerance_pct": 0.01}
    """
    rows = []
    for c in checks:
        diff_pct = abs(c["expected"] - c["actual"]) / max(abs(c["expected"]), 1e-9)
        passed = diff_pct <= c.get("tolerance_pct", 0.01)
        rows.append(
            {
                "name": c["name"],
                "expected": float(c["expected"]),
                "actual": float(c["actual"]),
                "diff_pct": round(diff_pct * 100, 4),
                "passed": passed,
            }
        )
    return rows


def write_report(spark, results: list[dict], path: str, title: str) -> str:
    """Persist DQ results as JSON + friendly Markdown in a Volume."""
    import datetime as dt

    payload = {"title": title, "generated_at": dt.datetime.utcnow().isoformat(), "results": results}
    json_path = f"{path}/dq_results.json"
    md_path = f"{path}/dq_report.md"

    md_lines = [
        f"# {title}",
        "",
        f"Generated: {payload['generated_at']}  ",
        "",
        "| check | passed | observed |",
        "|---|---|---|",
    ]
    for r in results:
        passed = "PASS" if r.get("passed") else "FAIL"
        md_lines.append(
            f"| {r.get('expectation_type') or r.get('name', '')}:{r.get('column', '')} | {passed} | {r.get('observed', '')} |"
        )
    spark.createDataFrame([{"json": json.dumps(payload, default=str)}]).select("json").write.mode(
        "overwrite"
    ).text(json_path)
    spark.createDataFrame([{"md": "\n".join(md_lines)}]).select("md").write.mode("overwrite").text(
        md_path
    )
    return f"{json_path} | {md_path}"
