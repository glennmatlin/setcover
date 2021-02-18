#!/usr/bin/env python
# coding: utf-8

""" Imports """
import confuse
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.conf import SparkConf
import pandas as pd
from setcover.utils import get_p_values


def registry_etl(
    spark: SparkSession, registry_claims_bucket: str, icd_to_desc_map: pd.DataFrame
) -> pd.DataFrame:
    registry_rdd = spark.read.parquet(
        registry_claims_bucket  # TODO [TBD] May need .replace("s3:", "s3a:")
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
    registry_df = (
        registry_rdd.select("registry_id", F.explode(F.col("dx_list")).alias("code"))
        .where(F.col("code").isNotNull())
        .groupBy("code")
        .agg(
            F.collect_set(F.col("registry_id")).alias("registry_ids"),
            F.countDistinct(F.col("registry_id")).alias("registry_count"),
        )
        .where(
            F.col("registry_count") > 3
        )  # TODO [Low] Move registry minimum into YAML
        .withColumn("registry_rate", F.col("registry_count") / F.lit(registry_id_count))
    ).toPandas()

    # TODO [Low] Move below this into Spark
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
    registry_df["n_total_test"] = n_total_test
    return registry_df


def control_etl(
    spark: SparkSession, control_claims_bucket: str, icd_to_desc_map: pd.DataFrame
) -> pd.DataFrame:
    control_rdd = spark.read.parquet(
        control_claims_bucket  # TODO [TBD] May need.replace("s3:", "s3a:")
    ).withColumnRenamed("patient_id", "control_id")
    control_id_count = control_rdd.select("control_id").distinct().count()
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

    # TODO [Low] Move below this into Spark
    control_df.drop(
        control_df.index[~control_df.code.isin(icd_to_desc_map.code)], inplace=True
    )
    control_df.sort_values("code").reset_index(drop=True, inplace=True)
    n_total_control = int(
        round(
            control_df["control_count"].iloc[0] / control_df["control_rate"].iloc[0], 0
        )
    )
    control_df["n_total_control"] = n_total_control
    return control_df


def merge_etl(
    registry_df: pd.DataFrame, control_df: pd.DataFrame, icd_to_desc_map: pd.DataFrame
) -> pd.DataFrame:
    # TODO [Low] Move functionality into Spark
    merged_df = pd.merge(registry_df, control_df, how="inner", on="code")
    merged_df.query("registry_count != 0 & control_count != 0", inplace=True)
    merged_df.drop(
        labels=["registry_rate", "control_rate"], axis="columns", inplace=True
    )
    merged_df.rename(
        mapper={"registry_count": "n_test", "control_count": "n_control"},
        axis="columns",
        inplace=True,
    )
    merged_df["pval"] = get_p_values(merged_df)
    merged_df.query("pval < 0.05", inplace=True)  # TODO [Low] Move PVAL into YAML
    merged_df["rate_test"] = merged_df.n_test.divide(merged_df.n_total_test)
    merged_df["rate_control"] = merged_df.n_control.divide(merged_df.n_total_control)
    merged_df["rate_ratio"] = merged_df.rate_test.divide(merged_df.rate_control)
    merged_df = merged_df.merge(
        icd_to_desc_map, on="code", how="inner"
    )  # Remove Non-ICD10 codes with inner join
    merged_df.sort_values(["rate_ratio"], ascending=False, inplace=True)

    # Convert dtypes and make lists into strings for saving to file
    # TODO [High] Figure out a better way to save and deal with lists
    merged_df = merged_df.convert_dtypes()
    merged_df["registry_ids"] = merged_df["registry_ids"].apply(", ".join)
    merged_df["control_ids"] = merged_df["control_ids"].apply(", ".join)

    # Drop ICD Codes with low rate in registry patients
    merged_df.query(
        "rate_test>0.01"
    ).sort_values(  # TODO [Low] Move rate_test minimum into YAML
        "rate_ratio", ascending=False, inplace=True
    )
    return merged_df


def icd_map(dx_clinical_mapping: object) -> pd.DataFrame:
    # TODO [Low] Put PandasDF into SparkDF
    icd_to_desc_map = pd.read_csv(dx_clinical_mapping["path"].get("str")).rename(
        index=str,
        columns={
            dx_clinical_mapping["code_field"].get("str"): "code",
            dx_clinical_mapping["desc_field"].get("str"): "code_description",
            dx_clinical_mapping["category_field"].get("str"): "code_category",
        },
    )[["code", "code_description", "code_category"]]
    return icd_to_desc_map


def main(
    spark: SparkSession,
    registry_claims_bucket: str,
    control_claims_bucket: str,
    dx_clinical_mapping: str,
) -> pd.DataFrame:
    """ Get ICD10 Code Descriptions """
    icd_to_desc_map = icd_map(dx_clinical_mapping)

    """ ETL: Rare Disease Registry Patients """
    registry_df = registry_etl(spark, registry_claims_bucket, icd_to_desc_map)

    """ ETL: Controlled Representative Sample"""
    control_df = control_etl(spark, control_claims_bucket, icd_to_desc_map)

    """ ETL: Merging Registry and Control"""
    merged_df = merge_etl(registry_df, control_df, icd_to_desc_map)

    return merged_df


if __name__ == "__main__":
    """Spark Configuration"""
    conf = SparkConf()
    conf.set("spark.logConf", "true")
    spark = (
        SparkSession.builder.config(conf=conf)
        .master("local")
        .appName("setcover")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")

    """Load configuration from .yaml file."""
    config = confuse.Configuration("setcover", __name__)
    config.set_file("config.yaml")
    _registry_claims_bucket = config["buckets"]["registry_claims"].get("str")
    _control_claims_bucket = config["buckets"]["control_claims"].get("str")
    _dx_clinical_mapping = config["clinical_mapping"]["dx"]

    """ ETL Operations"""
    merged_df = main(
        spark, _registry_claims_bucket, _control_claims_bucket, _dx_clinical_mapping
    )

    output_bucket = config["buckets"]["output"].get("str")
    if merged_df is not None:
        merged_df.to_parquet(
            output_bucket,
            index=False,
        )
