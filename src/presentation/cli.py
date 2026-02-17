import os
import logging
from dotenv import load_dotenv
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

from app import Pipeline
from adapters.spark.session import build_spark
from adapters.readers import SparkCSVReader
from adapters.transformers import TransactionTransformer
from adapters.validators import TransactionValidator
from bootstrap.config import load_config
from core.interfaces import HealthMonitor
from bootstrap.factories import make_reporter

try:
    from adapters.monitoring import MflowMonitor  # type: ignore
except Exception(ImportError, ModuleNotFoundError, AttributeError):  # pragma: no cover
    MflowMonitor = None  # type: ignore


def main():
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    cfg = load_config(os.getenv("CONFIG_PATH"))
    input_path = cfg.get("input_path") or os.getenv(
        "DATA_SOURCE_PATH", "./tests/fixtures/transactions.csv"
    )
    output_dir = cfg.get("output_dir") or os.getenv(
        "OUTPUT_DIR", "./dist/output_parquet"
    )
    enable_mlflow = bool(
        cfg.get("mlflow", {}).get("enabled", False)
        or os.getenv("MLFLOW_ENABLED", "false").lower() == "true"
    )

    spark = build_spark()
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("product", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("status", StringType(), False),
        ]
    )

    monitor: HealthMonitor | None = None
    if enable_mlflow:
        monitor = MflowMonitor(
            experiment=cfg.get("mlflow", {}).get("experiment", "idea"),
            run_name="pipeline-run",
        )

    reporter = make_reporter(spark, cfg | {"output_dir": output_dir})
    pipeline = Pipeline(
        reader=SparkCSVReader(spark, input_path, schema),
        transformer=TransactionTransformer(),
        validator=TransactionValidator(),
        reporter=reporter,
        monitor=monitor,
    )
    try:
        metrics = pipeline.run()
        for k, v in metrics.items():
            print(f"{k}={v}")
    except Exception as e:  # pragma: no cover
        logging.exception("Pipeline failed")
        raise e

    if __name__ == "__main__":
        main()
