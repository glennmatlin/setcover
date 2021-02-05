from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark_fixture():
    spark = (
        SparkSession.builder.master("local").appName("pyspark-setcover").getOrCreate()
    )
    yield spark
