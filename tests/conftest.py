import os
import shutil
from typing import Any, Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, Any, None]:
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("IdeiaPipelineTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def tmp_output(tmp_path):
    # Provide a clean directory path for writer
    p = tmp_path / "out"
    yield str(p)
    if p.exists():
        shutil.rmtree(p, ignore_errors=True)
