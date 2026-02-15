import os
from pyspark.sql import DataFrame, SparkSession
from core.interfaces import Reporter


class PostgresReporter(Reporter):
    def __init__(self, spark: SparkSession, table: str, mode: str = "append") -> None:
        self._spark = spark
        self._table = table
        self._mode = mode
        self._url = os.getenv("PG_URL", "jdbc:postgresql://localhost:5432/postgres")
        self._user = os.getenv("PG_USER", "postgres")
        self._password = os.getenv("PG_PASSWORD", "postgres")
        self._driver = os.getenv("PG_DRIVER", "org.postgresql.Driver")

    def write(self, df: DataFrame) -> None:
        (
            df.write.mode(self._mode)
            .format("jdbc")
            .option("url", self._url)
            .option("dbtable", self._table)
            .option("user", self._user)
            .option("password", self._password)
            .option("driver", self._driver)
            .save()
        )
