import os
from pyspark.sql import SparkSession

def build_spark(app_name: str = "IdeaPipeline") -> SparkSession:
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    if os.getenv("PG_JDBC", "false").lower() == "true":
        builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
    if os.getenv("DELTA_ENABLED", "false").lower() == "true":
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0")
        )
    return builder.getOrCreate()