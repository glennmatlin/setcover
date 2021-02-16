#!/usr/bin/env python
# coding: utf-8

""" Imports """
import pandas as pd
import pyspark.sql.functions as F
from tqdm.auto import tqdm

""" Configs """
RUN_ID = "20210206"
S3_HOME = "s3://kh-data/glenn_sandbox/adhoc_requests/ALXN/aHUS-Predictive/"

claims_bucket = f"{S3_HOME}20201203/target_claims_df.parquet"
control_bucket = (
    f"{S3_HOME}20201209/stratified_claims_control_sample_ahus_20210210.parquet/"
)
reference_bucket = f"{S3_HOME}20201209/reference_df.parquet/"
cohort_bucket = f"{S3_HOME}20201210/target_cohort_df.parquet/"

output_bucket = f"{S3_HOME}{RUN_ID}/merged_df.parquet"

clinical_mapping_configs = {
    "dx": {
        "path": "s3://pulse-prod/data/code_mappings/ccs_dx_icd10cm_2019.csv",
        "code_field": "ICD-10-CM Code",
        "desc_field": "ICD-10-CM Code Definition",
    },
    "px": {
        "path": "s3://pulse-prod/data/code_mappings/procedure_code_mapping_snowflake_20200527.csv",
        "code_field": "CODE",
        "desc_field": "CODE_DESCRIPTION",
    },
}

""" Functions """


def get_p_values(
    df,
    mode="chi2_contingency",
):
    from scipy.stats import chi2_contingency, fisher_exact

    pval_list = []
    for i in tqdm(range(len(df))):
        Row = df.iloc[i]
        contingency_table = [
            [Row["n_test"], Row["n_total_test"] - Row["n_test"]],
            [Row["n_control"], Row["n_total_control"] - Row["n_control"]],
        ]

        if mode == "chi2_contingency":
            _, pval, _, _ = chi2_contingency(contingency_table)
        elif mode == "fisher_exact":
            _, pval = fisher_exact(contingency_table)
        else:
            raise ValueError(
                "Stat test mode must be 'chi2_contingency' or 'fisher_exact'"
            )

        pval_list.append(pval)
    return pval_list


def main():
    icd_to_desc_map = pd.read_csv(clinical_mapping_configs["dx"]["path"]).rename(
        index=str,
        columns={
            "ICD-10-CM Code": "code",
            "ICD-10-CM Code Definition": "code_description",
            "Beta Version CCS Category Description": "code_category",
        },
    )[["code", "code_description", "code_category"]]

    # ### `registry_df()`
    # ##### Spark
    # Load Registry RDD
    registry_rdd = spark.read.parquet(
        claims_bucket.replace("s3:", "s3a:")
    ).withColumnRenamed("patient_id", "registry_id")
    # Select claims around patient reference date
    # Filter out ICD9 set_ids_used used before 2017
    registry_rdd = (
        registry_rdd.where(  # Filters to claims falling before reference date
            F.col("claim_date") < F.date_sub(F.col("reference_date"), 0)
        )
        .where(F.col("claim_date") > F.date_sub(F.col("reference_date"), 1 * 365))
        .where(F.col("reference_date") > F.lit("2017-01-01"))
    )
    registry_id_count = registry_rdd.select("registry_id").distinct().count()
    # ##### Pandas
    registry_df = (
        registry_rdd.select("registry_id", F.explode(F.col("dx_list")).alias("code"))
        .where(F.col("code").isNotNull())
        .groupBy("code")
        .agg(
            F.collect_set(F.col("registry_id")).alias("registry_ids"),
            F.countDistinct(F.col("registry_id")).alias("registry_count"),
        )
        .where(F.col("registry_count") > 3)
        .withColumn("registry_rate", F.col("registry_count") / F.lit(registry_id_count))
    ).toPandas()
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

    # ### `control_df()`
    # ##### Spark
    control_rdd = spark.read.parquet(
        control_bucket.replace("s3:", "s3a:")
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
    # ##### Pandas
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

    # ### `merged_df()`
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
    merged_df.query("pval < 0.05", inplace=True)
    merged_df["rate_test"] = merged_df.n_test.divide(merged_df.n_total_test)
    merged_df["rate_control"] = merged_df.n_control.divide(merged_df.n_total_control)
    merged_df["rate_ratio"] = merged_df.rate_test.divide(merged_df.rate_control)
    merged_df = merged_df.merge(
        icd_to_desc_map, on="code", how="inner"
    )  # Remove Non-ICD10 set_ids_used with inner join
    merged_df.sort_values(["rate_ratio"], ascending=False, inplace=True)

    # ### `output`
    # Convert dtypes and make lists into strings for saving to file
    merged_df = merged_df.convert_dtypes()
    merged_df["registry_ids"] = merged_df["registry_ids"].apply(", ".join)
    merged_df["control_ids"] = merged_df["control_ids"].apply(", ".join)

    if merged_df is not None:
        merged_df.to_parquet(
            output_bucket,
            index=False,
        )


if __name__ == "__main__":
    main()
