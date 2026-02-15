from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, to_date, month
from core.interfaces import Transformer


class TransactionsTransformer(Transformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return(
            df.withColumn("currency", upper(col("currency")))
            .withColumn("date", to_date(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .filter(col("status").isin("posted", "pending"))
        )
