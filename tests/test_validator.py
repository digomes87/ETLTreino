from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from datetime import datetime
from adapters.validators import TransactionValidator


def test_validator_detects_multiple_issues(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("product", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("status", StringType(), True),
        ]
    )

    rows = [
        Row(
            id=None,
            product="credit_card",
            amount=-1.0,
            currency="XYZ",
            timestamp=datetime(2026, 1, 1, 0, 0, 0),
            status="posted",
        ),
        Row(
            id="2",
            product="weird",
            amount=0.0,
            currency="usd",
            timestamp=datetime(2026, 1, 1, 0, 0, 0),
            status="unknown",
        ),
    ]
    df = spark.createDataFrame(rows, schema=schema)
    v = TransactionValidator()
    errors = list[str](v.validate(df))
    assert any("Null fields" in e for e in errors)
    assert any("Non-positive amounts" in e for e in errors)
    assert any("Invalid products" in e for e in errors)
    assert any("Invalid currencies" in e for e in errors)
    assert any("Invalid status" in e for e in errors)
