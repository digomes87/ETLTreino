from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from core.interfaces import DataReader

class SparkCSVReader(DataReader):
    def __init__(self, spark: SparkSession, path: str, schema: StructType) -> None:
        self._spark = spark
        self._path = path
        self._schema = schema

    def read(self) -> DataFrame:
        return (
            self._spark.read.option("header", "true")
            .option("mode","FAILFAST")
            .schema(self._schema)
            .csv(self._path)
        )