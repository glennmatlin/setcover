import pytest
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession

conf = SparkConf()
conf.set("spark.logConf", "true")


@pytest.fixture(scope="session")
def spark_fixture():
    spark = (
        SparkSession.builder.config(conf=conf)
        .master("local")
        .appName("setcover")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")
    yield spark
