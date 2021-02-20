#!/usr/bin/env python
# coding: utf-8

""" Imports """
import logging
import traceback
import sys
import confuse
import pandas as pd
import pyspark.sql.functions as F
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession

from setcover.utils import get_p_values

"""Logging"""
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d %H:%M",
    filemode="w",
)
# TODO [Low] Make sure to log to file all silenced modules but silent in console, or only show warn/errors
# Silence logging for backend services not needed
silenced_modules = ["botocore", "aiobotocore", "s3fs", "fsspec", "asyncio", "numexpr", "py4j", "urllib3"]
for module in silenced_modules:
    logging.getLogger(module).setLevel(logging.CRITICAL)

logging.getLogger("setcover.etl").setLevel(logging.INFO)

log = logging.getLogger(__name__)
# Create stream handler which writes ERROR messages or higher to the sys.stderr
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# Set a format which is simpler for console use
ch.setFormatter(logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s"))
# Create file handlers for info and debug logs
fh = logging.FileHandler("run.log")
# Add handlers to logger
log.addHandler(ch), log.addHandler(fh)


def icd_map(config_dx_map: confuse.core.Subview) -> pd.DataFrame:
    # TODO [Medium] Put PandasDF into SparkDF
    icd_to_desc_map = pd.read_csv(config_dx_map["bucket"].get(str)).rename(
        index=str,
        columns={
            config_dx_map["code_field"].get(str): "code",
            config_dx_map["desc_field"].get(str): "code_description",
            config_dx_map["category_field"].get(str): "code_category",
        },
    )[["code", "code_description", "code_category"]]
    return icd_to_desc_map


def registry_etl(
    spark: SparkSession, config: confuse.core.Configuration, icd_to_desc_map: pd.DataFrame
) -> pd.DataFrame:

    registry_claims_bucket = config["buckets"]["registry_claims"].get(str)
    log.info(f"Registry claim bucket: {registry_claims_bucket}")

    log.info(f"Reading in registry claims data")
    registry_rdd = spark.read.parquet(
        registry_claims_bucket.replace("s3:", "s3a:")
    ).withColumnRenamed("patient_id", "registry_id")

    # Select claims around patient reference date and filter out claims before 2017 to remove ICD9 codes
    registry_rdd = (
        registry_rdd.where(  # Filters to claims falling before reference date
            F.col("claim_date") < F.date_sub(F.col("reference_date"), 0)
        )
        .where(
            F.col("claim_date") > F.date_sub(F.col("reference_date"), 1 * 365)
        )  # TODO [Low] Move time length into YAML
        .where(
            F.col("reference_date") > F.lit("2017-01-01")
        )  # TODO [Low] Move cut-off date into YAML
    )
    registry_id_count = registry_rdd.select("registry_id").distinct().count()
    log.info(f"Registry ID Count: {registry_id_count}")

    registry_count_min = config["etl"]["registry_count_min"].get(int)
    log.info(f"ETL of registry data and bringing to pandas")
    registry_df = (
        registry_rdd.select("registry_id", F.explode(F.col("dx_list")).alias("code"))
        .where(F.col("code").isNotNull())
        .groupBy("code")
        .agg(
            F.collect_set(F.col("registry_id")).alias("registry_ids"),
            F.countDistinct(F.col("registry_id")).alias("registry_count"),
        )
        .where(
            F.col("registry_count") > registry_count_min
        )
        .withColumn("registry_rate", F.col("registry_count") / F.lit(registry_id_count))
    ).toPandas()

    # TODO [Medium] Move below this into Spark
    log.info(f"Registry ETL pandas operations")
    registry_df.drop(
        registry_df.index[~registry_df.code.isin(icd_to_desc_map.code)], inplace=True
    )
    registry_df.sort_values("code").reset_index(drop=True, inplace=True)
    n_total_test = int(
        round(
            registry_df["registry_count"].iloc[0]
            / registry_df["registry_rate"].iloc[0],
            0,
        )
    )
    log.info(f"N Total Test: {n_total_test}")
    registry_df["n_total_test"] = n_total_test
    return registry_df


def control_etl(
    spark: SparkSession, control: confuse.core.Configuration, icd_to_desc_map: pd.DataFrame
) -> pd.DataFrame:
    control_claims_bucket = config["buckets"]["control_claims"].get(str)
    log.info(f"Control claim bucket: {control_claims_bucket}")
    log.info(f"Loading control data")
    control_rdd = spark.read.parquet(
        control_claims_bucket.replace("s3:", "s3a:")
    ).withColumnRenamed("patient_id", "control_id")
    control_id_count = control_rdd.select("control_id").distinct().count()
    log.info(f"Control Sample ID Count: {control_id_count}")
    log.info(f"Bringing control sample into pandas DF")
    control_df = (
        control_rdd.select("control_id", F.explode(F.col("dx_list")).alias("code"))
        .where(F.col("code").isNotNull())
        .groupBy("code")
        .agg(
            F.collect_set(F.col("control_id")).alias("control_ids"),
            F.countDistinct(F.col("control_id")).alias("control_count"),
        )
        .withColumn("control_rate", F.col("control_count") / F.lit(control_id_count))
    ).toPandas()

    # TODO [Medium] Move below this into Spark
    log.info(f"Control sample pandas ETL operations")
    control_df.drop(
        control_df.index[~control_df.code.isin(icd_to_desc_map.code)], inplace=True
    )
    control_df.sort_values("code").reset_index(drop=True, inplace=True)
    n_total_control = int(
        round(
            control_df["control_count"].iloc[0] / control_df["control_rate"].iloc[0], 0
        )
    )
    log.info(f"N Total Control: {n_total_control}")
    control_df["n_total_control"] = n_total_control
    return control_df


def merge_etl(config: confuse.core.Configuration,
    registry_df: pd.DataFrame, control_df: pd.DataFrame, icd_to_desc_map: pd.DataFrame
) -> pd.DataFrame:
    # TODO [Medium] Move functionality into Spark
    log.info(f"Merging data frames")
    df = pd.merge(registry_df, control_df, how="inner", on="code")
    log.info(f"Merge complete, performing ETL tasks")
    df.query("registry_count != 0 & control_count != 0", inplace=True)
    df.drop(
        labels=["registry_rate", "control_rate"], axis="columns", inplace=True
    )
    df.rename(
        mapper={"registry_count": "n_test", "control_count": "n_control"},
        axis="columns",
        inplace=True,
    )
    log.info(f"P-Value statistical testing")
    df["pval"] = get_p_values(df)
    p_value_max = config["etl"]["p_value_max"].get(float)
    df.query(
        f"pval < {p_value_max}", inplace=True
    )
    df["rate_test"] = df.n_test.divide(df.n_total_test)
    df["rate_control"] = df.n_control.divide(df.n_total_control)
    df["rate_ratio"] = df.rate_test.divide(df.rate_control)
    log.info(f"Merging ICD10 Code Map")
    df = df.merge(
        icd_to_desc_map, on="code", how="inner"
    )  # Remove Non-ICD10 codes with inner join
    df.sort_values(["rate_ratio"], ascending=False, inplace=True)

    # Convert dtypes and make lists into strings for saving to file
    # TODO [Medium] Figure out a better way to save and deal with lists
    # Drop ICD Codes with low rate in registry patients
    test_rate_min = config["etl"]["test_rate_min"].get(float)
    log.info(f"Data set has length of {len(df)}")
    log.info(f"Filtered out codes with rate_test<={test_rate_min}")
    df.query(
        f"rate_test>{test_rate_min}"
    ).sort_values("rate_ratio", ascending=False, inplace=True)
    df = df[["code", "registry_ids", "control_ids"]]
    log.info(f"DF length is now {len(df)}")

    log.info(f"Making dataframe ready for export")
    df = df.convert_dtypes()
    df["registry_ids"] = df["registry_ids"].apply(", ".join)
    df["control_ids"] = df["control_ids"].apply(", ".join)
    return df


def main(
    spark_: SparkSession, config_: confuse.core.Configuration,
) -> pd.DataFrame:

    """ Get ICD10 Code Descriptions """
    icd_to_desc_map = icd_map(config_["clinical_mapping"]["dx"])

    """ ETL: Rare Disease Registry Patients """
    registry_df = registry_etl(spark_, config_, icd_to_desc_map)

    """ ETL: Controlled Representative Sample"""
    control_df = control_etl(spark_, config_, icd_to_desc_map)

    """ ETL: Merging Registry and Control"""
    merge_df = merge_etl(config_, registry_df, control_df, icd_to_desc_map)

    return merge_df


if __name__ == "__main__":
    """Spark Configuration"""
    try:
        conf = SparkConf()
        conf.set("spark.logConf", "true")
        spark = (
            SparkSession.builder.config(conf=conf)
            .master("local")
            .appName("setcover")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("OFF")
    except ValueError:
        log.warn("Existing SparkSession detected: Using existing SparkSession")

    """ Load YAML Configuration """
    config = confuse.Configuration("setcover", __name__)
    config.set_file("config.yaml")

    """ ETL Operations """
    log.info(f"Running main()")
    try:
        df = main(
            spark, config
        )
    except ValueError:
        log.error("main() failed, possible issue with SparkSession")

    output_bucket = config["buckets"]["output"].get(str)
    log.info(f"Attempting to export dataframe to {output_bucket}")
    try:
        df.to_parquet(
            output_bucket,
            index=False,
        )
    except (FileNotFoundError, TypeError, NameError) as e:
        log.error(f"{e}")
        log.error(traceback.format_exc())
        log.error(sys.exc_info()[2])
    log.info(f"Export successful!")
    spark.stop()
