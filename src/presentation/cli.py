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
    from adapters.monitoring import MflowMonitor # type: ignore
except Exception(ImportError, ModuleNotFoundError, AttributeError): # pragma: no cover
    MflowMonitor = None # type: ignore

def main(): ...

