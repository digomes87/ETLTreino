from typing import Any
from pyspark.sql import Row
from datetime import datetime
from adapters.transformers import TransactionTransformer


def test_transformer_filter_reversed_and_derives_columns(spark):
    rows = [
        Row(
            id="1",
            product="credit_card",
            amount=1.3,
            currency="usd",
            timestamp=datetime(2026, 1, 1, 0, 0, 0),
            status="posted",
        ),
        Row(
            id="2",
            product="credit_card",
            amount=1.3,
            currency="brl",
            timestamp=datetime(2026, 1, 1, 0, 0, 0),
            status="reversed",
        ),
    ]

    df = spark.createDataFrame(rows)
    t = TransactionTransformer()
    out = t.transform(df)
    assert out.count() == 1
    cols = set[Any](out.columns)
    assert "date" in cols and "month" in cols
