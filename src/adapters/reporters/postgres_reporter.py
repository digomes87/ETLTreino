import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

from core.interfaces import Reporter


_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"
load_dotenv(_CONFIG_DIR / ".env")


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Environment variable {name} is required for PostgresReporter")
    return value


class PostgresReporter(Reporter):
    def __init__(self, spark: SparkSession, table: str, mode: str = "append") -> None:
        self._spark = spark
        self._table = table
        self._mode = mode
        self._url = _required_env("PG_URL")
        self._user = _required_env("PG_USER")
        self._password = _required_env("PG_PASSWORD")
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
