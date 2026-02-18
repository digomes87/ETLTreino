def test_build_spark_creates_session(spark):
    assert spark.sparkContext is not None
    assert "local" in spark.sparkContext.master.lower()
