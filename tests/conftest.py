from pyspark import SparkContext
from pyspark.sql import SQLContext
import pytest


@pytest.fixture(scope="session")
def sql_context():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    yield sql_context
    spark_context.stop()
