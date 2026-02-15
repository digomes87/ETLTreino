from typing import Dict, Any
from core.interfaces import Reporter
from adapters.reporters import ParquetReporter, PostgresReporter, DeltaReporter
from pyspark.sql import SparkSession


def make_reporter(spark: SparkSession, cfg: Dict[str, Any]) -> Reporter:
    out = cfg.get("output") or {}
    out_type = (out.get("type") or "parquet").lower()
    if out_type == "postgres":
        table = out.get("table") or "public.transformed_transactions"
        mode = out.get("mode") or "append"
        return PostgresReporter(spark, table=table, mode=mode)
    if out_type == "delta":
        path = out.get("path") or cfg.get("output_dir") or "./dist/output_delta"
        return DeltaReporter(path)
    path = out.get("path") or cfg.get("output_dir") or "./dist/output_parquet"
    return ParquetReporter(path)
