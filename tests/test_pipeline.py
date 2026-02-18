import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from app import Pipeline
from adapters.readers import SparkCSVReader
from adapters.transformers import TransactionTransformer
from adapters.validators import TransactionValidator
from adapters.reporters import ParquetReporter


def _schema():
    return StructType(
        [
            StructField("id", StringType(), False),
            StructField("product", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("status", StringType(), False),
        ]
    )


def test_pipeline_run_and_writes_parquet(spark, tmp_output):
    csv_path = os.path.join(os.path.dirname(__file__), "fixtures", "transactions.csv")
    pipeline = Pipeline(
        reader=SparkCSVReader(spark, csv_path, _schema()),
        transformer=TransactionTransformer(),
        validator=TransactionValidator(),
        reporter=ParquetReporter(tmp_output),
    )

    metrics = pipeline.run()

    assert metrics["rows_raw"] == 4
    assert metrics["rows_transformed"] == 3

    assert os.path.isdir(tmp_output)
