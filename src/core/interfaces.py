from typing import Protocol, Iterable, Mapping, Any
from pyspark.sql import DataFrame


class DataReader(Protocol):
    def read(self) -> DataFrame: ...


class Transformer(Protocol):
    def transform(self, df: DataFrame) -> DataFrame: ...


class Validator(Protocol):
    def validate(self, df: DataFrame) -> Iterable[str]: ...


class Reporter(Protocol):
    def write(self, df: DataFrame) -> None: ...



class HealthMonitor(Protocol):
    def report_metrics(self, metrics: Mapping[str, Any]) -> None: ...
