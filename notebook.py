#!/usr/bin/env python
# # Set Cover Optimization of Trigger Codes

# ## Setup
# ### Imports
RUN_ID = "20210206"
S3_HOME = "s3://kh-data/glenn_sandbox/adhoc_requests/ALXN/aHUS-Predictive/"

# #### Inputs
claims_bucket = f"{S3_HOME}20201203/target_claims_df.parquet"
control_bucket = (
    f"{S3_HOME}20201209/stratified_claims_control_sample_ahus_20210210.parquet/"
)
reference_bucket = f"{S3_HOME}20201209/reference_df.parquet/"
cohort_bucket = f"{S3_HOME}20201210/target_cohort_df.parquet/"

# #### Outputs
merged_df_bucket = f"{S3_HOME}{RUN_ID}/merged_df.parquet"
cover_problem_bucket = f"{S3_HOME}{RUN_ID}/cover_problem.parquet"
registry_coverage_bucket = f"{S3_HOME}{RUN_ID}/registry_coverage_metrics.parquet"
control_coverage_bucket = f"{S3_HOME}{RUN_ID}/control_coverage_metrics.parquet"


import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from tqdm.auto import tqdm
import pandas as pd
from pyspark import StorageLevel
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    col,
    lit,
    desc,
    broadcast,
    count,
    countDistinct,
    trim,
    regexp_replace,
    when,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType,
    ArrayType,
    LongType,
    IntegerType,
    BooleanType,
    DateType,
    FloatType,
)


# ### Configs

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

icd_to_desc_map = pd.read_csv(clinical_mapping_configs["dx"]["path"]).rename(
    index=str,
    columns={
        "ICD-10-CM Code": "code",
        "ICD-10-CM Code Definition": "code_description",
        "Beta Version CCS Category Description": "code_category",
    },
)[["code", "code_description", "code_category"]]


# ### Functions
#
# **TODOs**
# - Refactor into `nostradamus`
# - Unit testing

# ##### `get_p_values`



from typing import List, Set, Dict




def get_p_values(
    df,
    mode="chi2_contingency",
):
    from scipy.stats import fisher_exact, chi2_contingency

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


# ##### `get_coverage_metrics`



def get_coverage_metrics(
    rdd: object,
    codes: List[str],
    id_field: str,
    dxs_field="dx_list",
    limit_n_codes=None,
) -> object:
    """
    claims_dataframe: dataframe of claims from regsitry patients for the desired target
    triggers_dataframe: dataframe of cohort claims used to get icd codes
    """

    codes = codes[:limit_n_codes]

    rdd = rdd.select(id_field, dxs_field)
    distinct_count = rdd.select(id_field).distinct().count()

    rdd_filter = (
        rdd.select(id_field, F.explode(F.col(dxs_field)).alias("dx"))
        .where(F.col("dx").isNotNull())
        .groupBy("dx")
        .agg(F.collect_set(F.col(id_field)).alias(id_field))
        .where(F.col("dx").isin(codes))
        .agg(F.array_distinct(F.flatten(F.collect_set(id_field))).alias(id_field))
        .select(F.explode(F.col(id_field)).alias(id_field))
    )

    filtered_rdd = rdd.join(rdd_filter, [id_field], "leftsemi")
    filtered_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    metrics = dict()
    covered_tokens = set()  # we track patients already covered by a code to skip them
    covered_count = 0

    for n in tqdm(range(len(codes))):

        trigger = codes[n]

        newly_covered_tokens = set(
            filtered_rdd.where(
                ~col(id_field).isin(covered_tokens)
            )  # TODO Is there a faster way to remove these?
            .where(F.array_contains(dxs_field, trigger))
            .select(id_field)
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        newly_covered_count = len(newly_covered_tokens)

        covered_tokens |= newly_covered_tokens  # union the sets to track covered tokens
        covered_count += newly_covered_count  # track the new total

        metrics[n] = (
            trigger,
            newly_covered_count,
            covered_count,
            (covered_count / distinct_count) * 100,
        )

        # TODO Write out metrics here every time :P Can I write out one at a time instead of a whole dict?

    coverage_metrics = (
        pd.DataFrame(metrics)
        .T.rename_axis("n_codes")
        .rename(
            columns={
                0: "icd_code",
                1: "newly_covered_count",
                2: "coverage_count",
                3: "coverage_percent",
            }
        )
    )

    coverage_metrics.loc[-1] = ("None", 0, 0, 0)
    coverage_metrics.sort_index(inplace=True)
    coverage_metrics["percent_diff"] = coverage_metrics.coverage_percent.diff()

    filtered_rdd.unpersist()
    return coverage_metrics


# ###### Test case for `get_coverage_metrics`
foo = spark.createDataFrame(
    [
        ("Larry", ["DX1"]),
        ("Curly", ["DX2", "DX1"]),
        ("Moe", ["DX3"]),
        ("Shemp", ["DX0"]),
    ],
    ["id", "dx_list"],
)
dxs = ["DX2", "DX1", "DX4"]get_coverage_metrics(rdd=foo, codes=dxs, id_field="id", dxs_field="dx_list")
# ## Data

# ### Registry DataFrame

# ##### Spark



# Load Registry RDD
registry_rdd = spark.read.parquet(
    claims_bucket.replace("s3:", "s3a:")
).withColumnRenamed("patient_id", "registry_id")




# Select claims around patient reference date
# Filter out ICD9 codes used before 2017
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
        registry_df["registry_count"].iloc[0] / registry_df["registry_rate"].iloc[0], 0
    )
)
registry_df["n_total_test"] = n_total_test


# ### Control DataFrame

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
    round(control_df["control_count"].iloc[0] / control_df["control_rate"].iloc[0], 0)
)
control_df["n_total_control"] = n_total_control


# ### Merge DataFrames for Registry and Control Sample

# ##### Pandas: Merge DataFrame



merged_df = pd.merge(registry_df, control_df, how="inner", on="code")




merged_df.query("registry_count != 0 & control_count != 0", inplace=True)




merged_df.drop(labels=["registry_rate", "control_rate"], axis="columns", inplace=True)




merged_df.rename(
    mapper={"registry_count": "n_test", "control_count": "n_control"},
    axis="columns",
    inplace=True,
)




merged_df["pval"] = get_p_values(merged_df)




merged_df.query("pval < 0.05", inplace=True)




merged_df["rate_test"] = merged_df.n_test.divide(merged_df.n_total_test)

merged_df["rate_control"] = merged_df.n_control.divide(merged_df.control_id_count)

merged_df["rate_ratio"] = merged_df.rate_test.divide(merged_df.rate_control)




merged_df = merged_df.merge(
    icd_to_desc_map, on="code", how="inner"
)  # Remove Non-ICD10 codes with inner join




merged_df.sort_values(["rate_ratio"], ascending=False, inplace=True)


# ### Save Merged DataFrame to S3
merged_df = merged_df.convert_dtypes()
merged_df['registry_ids'] = merged_df['registry_ids'].apply(', '.join)
merged_df['control_ids'] = merged_df['control_ids'].apply(', '.join)

if merged_df is not None:
    merged_df.to_parquet(
    merged_df_bucket,
    index=False,
)

# ### Load Merged DataFrame from S3
merged_df = pd.read_parquet(merged_df_bucket)
merged_df["control_ids"] = merged_df["control_ids"].str.split(",").apply(lambda row: [id.strip() for id in row])
merged_df["registry_ids"] = merged_df["registry_ids"].str.split(",").apply(lambda row: [id.strip() for id in row])


# ## Top k-Codes by `rate_ratio`

# ### Top k-Codes: Solution



top_k_codes_by_rate_ratio = (
    merged_df.query("rate_test>0.01")
    .sort_values("rate_ratio", ascending=False)
    .code.to_list()
)


# ### Top k-Codes: Results

# #### Top k-Codes: Registry



top_k__registry_coverage_bucket = registry_coverage_bucket.replace(
    "registry_coverage", "top_k_registry_coverage"
)




top_k__registry_coverage_metrics = pd.read_parquet(top_k__registry_coverage_bucket)
top_k__registry_coverage_metrics.index = (
    top_k__registry_coverage_metrics.index.set_names(["n_codes"])
)

if 'top_k__registry_coverage_metrics' not in vars():
    top_k__registry_coverage_metrics = get_coverage_metrics(
        rdd=registry_rdd, codes=top_k_codes_by_rate_ratio, id_field="registry_id", limit_n_codes=None
    )


if top_k__registry_coverage_metrics is not None:
    top_k__registry_coverage_metrics.to_parquet(
        top_k__registry_coverage_bucket,
        index=False,
    )




fig = px.line(
    top_k__registry_coverage_metrics.reset_index(),
    x="n_codes",
    y="coverage_percent",
    labels={
        "n_codes": "Number (N) ICD-10 DX Trigger Codes",
        "coverage_percent": "Percentage of Tokens Containing Trigger in DX",
    },
    title="<b>Top K-Codes by Rate Ratio</b><br>Percent of aHUS Positive Patients Covered by Trigger Codes",
)
fig.show()




fig = px.line(
    top_k__registry_coverage_metrics.reset_index(),
    x="n_codes",
    y="percent_diff",
    labels={
        "n_codes": "Number (N) ICD-10 DX Trigger Codes",
        "percent_diff": "Percentage Difference in Coverage from Additional Trigger Code",
    },
    title="<b>Top K-Codes by Rate Ratio</b><br>Percentage Difference of aHUS Positive Patients Covered by Trigger Codes",
)


fig.show()


# #### Top k-Codes: Control Sample



top_k__control_coverage_bucket = control_coverage_bucket.replace(
    "control_coverage", "top_k_control_coverage"
)




top_k__control_coverage_metrics = pd.read_parquet(top_k__control_coverage_bucket)
top_k__control_coverage_metrics.index = top_k__control_coverage_metrics.index.set_names(
    ["n_codes"]
)

del(top_k__control_coverage_metrics)
# ---



id_field = "control_id"
dx_field = "dx_list"




total = control_rdd.select(id_field).distinct().count()




codes = top_k_codes_by_rate_ratio[:251]
answer_250 = (
    control_rdd.select(id_field, F.explode(F.col(dx_field)).alias(dx_field))
    .where(F.col(dx_field).isNotNull())
    .filter(F.col(dx_field).isin(codes))
    .select(id_field)
    .distinct()
    .count()
)




codes = top_k_codes_by_rate_ratio[:451]
answer_450 = (
    control_rdd.select(id_field, F.explode(F.col(dx_field)).alias(dx_field))
    .where(F.col(dx_field).isNotNull())
    .filter(F.col(dx_field).isin(codes))
    .select(id_field)
    .distinct()
    .count()
)




answer_250 / total * 100




answer_450 / total * 100


# ---



if "top_k__control_coverage_metrics" not in vars():
    top_k__control_coverage_metrics = get_coverage_metrics(
        rdd=control_rdd,
        codes=top_k_codes_by_rate_ratio,
        id_field="control_id",
        limit_n_codes=250,
    )




if top_k__control_coverage_metrics is not None:
    top_k__control_coverage_metrics.to_parquet(
        top_k__control_coverage_bucket,
        index=False,
    )




fig = px.line(
    top_k__control_coverage_metrics.reset_index(),
    x="n_codes",
    y="coverage_percent",
    labels={
        "n_codes": "Number (N) ICD-10 DX Trigger Codes",
        "coverage_percent": "Percentage of Tokens Containing Trigger in DX",
    },
    title="<b>pyspark-setcover: ExcludeSetProblem</b><br>Percent of Control Sample Patients Covered by Trigger Codes",
)
fig.show()


# #### Top k-Codes: Registry vs Control



# Weighted Set Cover Results
fig = make_subplots(specs=[[{"secondary_y": True}]])

# Add traces
fig.add_trace(
    go.Scatter(
        x=top_k__registry_coverage_metrics.index.to_list(),
        y=top_k__registry_coverage_metrics.coverage_percent.to_list(),
        name="Registry Patients",
    ),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(
        x=top_k__control_coverage_metrics.index.to_list(),
        y=[100 - p for p in top_k__control_coverage_metrics.coverage_percent.to_list()],
        name="Control Sample",
    ),
    secondary_y=True,
)

# Add figure title
fig.update_layout(
    title_text="<b>Top k-Codes by Rate Ratio</b><br>Registry VS Control – Percentage Covered by Trigger Codes"
)

# Set x-axis title
fig.update_xaxes(title_text="Number (N) ICD-10 DX Trigger Codes")

# Set y-axes titles
fig.update_yaxes(
    title_text="Percentage of Registry Containing Trigger in DX", secondary_y=False
)
fig.update_yaxes(
    title_text="Percentage of Control Sample NOT Containing Trigger in DX",
    secondary_y=True,
)

fig.show()


# ## Set Cover Optimization



if 'ExclusionSetCoverProblem' in vars():
    del(ExclusionSetCoverProblem)

from setcover import ExclusionSetCoverProblem


# ### Set Cover Solution



exclusion_df = merged_df.query("rate_test>0.01")[
    ["code", "registry_ids", "control_ids"]
]




exclusion_problem = ExclusionSetCoverProblem()




rows = list(exclusion_df.itertuples(name="Row", index=False))
sets = exclusion_problem._rows_to_sets(rows)




exclusion_problem = ExclusionSetCoverProblem(sets)




exclusion_problem.solve()




exclusion_cover_solution = [code for code, cost in exclusion_problem.cover_solution]




exclusion_cover_solution = pd.Series(exclusion_cover_solution)




if exclusion_cover_solution is not None:
    exclusion_cover_solution.to_csv(cover_problem_bucket.replace('parquet','csv'))


# ### Set Cover Results

# #### Coverage: Registry



del(registry_coverage_metrics)




registry_coverage_metrics = pd.read_parquet(registry_coverage_bucket)
registry_coverage_metrics.index = registry_coverage_metrics.index.set_names(["n_codes"])




if 'registry_coverage_metrics' not in vars():
    registry_coverage_metrics = get_coverage_metrics(
        rdd=registry_rdd,
        codes=exclusion_cover_solution,
        id_field="registry_id",
        dxs_field="dx_list",
        limit_n_codes=10
    )




if registry_coverage_metrics is not None:
    registry_coverage_metrics.to_parquet(
        registry_coverage_bucket,
        index=False,
    )




fig = px.line(
    registry_coverage_metrics.reset_index(),
    x="n_codes",
    y="coverage_percent",
    labels={
        "n_codes": "Number (N) ICD-10 DX Trigger Codes",
        "coverage_percent": "Percentage of Tokens Containing Trigger in DX",
    },
    title="<b>pyspark-setcover: ExcludeSetProblem</b><br>Percent of aHUS Positive Patients Covered by Trigger Codes",
)
fig.show()




fig = px.line(
    registry_coverage_metrics.reset_index(),
    x="n_codes",
    y="percent_diff",
    labels={
        "n_codes": "Number (N) ICD-10 DX Trigger Codes",
        "percent_diff": "Percentage Difference in Coverage from Additional Trigger Code",
    },
    title="<b>pyspark-setcover: ExcludeSetProblem</b><br>Percentage Difference of aHUS Positive Patients Covered by Trigger Codes",
)


fig.show()


# #### Coverage: Control Sample



control_rdd.select("control_id").distinct().count()




control_coverage_metrics = pd.read_parquet(control_coverage_bucket)
control_coverage_metrics.index = control_coverage_metrics.index.set_names(["n_codes"])




if control_coverage_metrics is None:
    control_coverage_metrics = get_coverage_metrics(
        control_rdd,
        exclusion_problem.cover_solution,
        field="control_id",
        limit_n_codes=None,
    )




if control_coverage_metrics is not None:
    control_coverage_metrics.to_parquet(
        control_coverage_bucket,
        index=False,
    )




fig = px.line(
    control_coverage_metrics.reset_index(),
    x="n_codes",
    y="coverage_percent",
    labels={
        "n_codes": "Number (N) ICD-10 DX Trigger Codes",
        "coverage_percent": "Percentage of Tokens Containing Trigger in DX",
    },
    title="<b>pyspark-setcover: ExcludeSetProblem</b><br>Percent of Control Sample Patients Covered by Trigger Codes",
)
fig.show()


# #### Coverage: Registry vs Control



# Weighted Set Cover Results
fig = make_subplots(specs=[[{"secondary_y": True}]])

# Add traces
fig.add_trace(
    go.Scatter(
        x=registry_coverage_metrics.index.to_list(),
        y=registry_coverage_metrics.coverage_percent.to_list(),
        name="Registry Patients",
    ),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(
        x=control_coverage_metrics.index.to_list(),
        y=[100 - p for p in control_coverage_metrics.coverage_percent.to_list()],
        name="Control Sample",
    ),
    secondary_y=True,
)

# Add figure title
fig.update_layout(
    title_text="<b>pyspark-setcover: ExcludeSetProblem</b><br>Registry VS Control – Percentage Covered by Trigger Codes"
)

# Set x-axis title
fig.update_xaxes(title_text="Number (N) ICD-10 DX Trigger Codes")

# Set y-axes titles
fig.update_yaxes(
    title_text="Percentage of Registry Containing Trigger in DX", secondary_y=False
)
fig.update_yaxes(
    title_text="Percentage of Control Sample NOT Containing Trigger in DX",
    secondary_y=True,
)

fig.show()

