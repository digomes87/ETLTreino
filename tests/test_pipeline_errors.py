from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from adapters.validators import TransactionValidator
from adapters.reporters import ParquetReporter
from app import Pipeline
from core.interfaces import DataReader, Transformer


class _Reader(DataReader):
    def __init__(self, df):
        self._df = df

    def read(self):
        return self._df


class _IdentityTransformer(Transformer):
    def transform(self, df):
        return df


def test_pipeline_raises_on_validation_errors(spark, tmp_output):
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
            id=1,
            product="credit_card",
            amount=1.0,
            currency="BRL",
            timestamp=None,
            status="posted",
        )
    ]

    df = spark.createDataFrame(rows, schema=schema)
    pipeline = Pipeline(
        reader=_Reader(df),
        transformer=_IdentityTransformer(),
        validator=TransactionValidator(),
        reporter=ParquetReporter(tmp_output),
    )

    try:
        pipeline.run()
        assert False, "Pipeline should have raised an error"
    except ValueError as e:
        assert f"Data quality errors in {str(e)}"
