#!/usr/bin/env python
# # Set Cover Optimization of Trigger Codes

import logging
from typing import List
import confuse
import pandas as pd
from setcover.problem import SetCoverProblem, Subset

"""Logging"""
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d %H:%M",
    filemode="w",
)
# TODO Make sure to log to file all silenced modules but silent in console
# Silence logging for backend services not needed
silenced_modules = ["botocore", "aiobotocore", "s3fs", "fsspec", "asyncio", "numexpr"]
for module in silenced_modules:
    logging.getLogger(module).setLevel(logging.CRITICAL)

logging.getLogger("setcover.problem").setLevel(logging.INFO)

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

def make_data(input_path: str, filetype="parquet") -> List[Subset]:
    """
    prepares data to be used in set cover problem using pandas
    :param input_path: path to parquet data
    :param filetype: file extension
    :return: data struct to be used set coverage problem
    """
    log.info(f"Reading in data from {input_path}")
    if filetype == "parquet":
        df = pd.read_parquet(
            input_path, columns=["code", "registry_ids", "control_ids"]
        )
    else:
        raise TypeError
    log.info(f"Fixing issue with data")
    df["control_ids"] = (
        df["control_ids"].str.split(",").apply(lambda row: [s.strip() for s in row])
    )
    df["registry_ids"] = (
        df["registry_ids"].str.split(",").apply(lambda row: [s.strip() for s in row])
    )
    # TODO: Replace with .from_df
    log.info(f"Final prep")
    rows = list(df.itertuples(name="Row", index=False))
    sets = SetCoverProblem._rows_to_sets(rows)
    return sets


# TODO Mock s3 data objects for testing
def main(config: confuse.core.Configuration) -> SetCoverProblem:
    # TODO [Medium] Run ETL as part of the main run.py

    """ Load """
    input_bucket = config["buckets"]["input"].get(str)
    log.info(f"Building problem data from bucket {input_bucket}")
    data = make_data(input_bucket)
    log.info(f"Loading the data into problem")
    problem = SetCoverProblem(data)

    """ Solve """
    log.info(f"Solving problem")
    problem.solve(limit=config["problem"]["limit"].get(int))

    return problem

if __name__ == "__main__":
    # import cProfile
    #
    # cProfile.run("main()", "run_output.dat")
    #
    # import pstats
    #
    # with open("run_output_time.txt", "w") as f:
    #     p = pstats.Stats("run_output.dat", stream=f)
    #     p.sort_stats("time").print_stats()
    # with open("run_output_calls.txt", "w") as f:
    #     p = pstats.Stats("run_output.dat", stream=f)
    #     p.sort_stats("calls").print_stats()

    # """Spark Configuration"""
    # try:
    #     conf = SparkConf()
    #     conf.set("spark.logConf", "true")
    #     spark = (
    #         SparkSession.builder.config(conf=conf)
    #         .master("local")
    #         .appName("setcover.run")
    #         .getOrCreate()
    #     )
    #     spark.sparkContext.setLogLevel("OFF")
    # except ValueError:
    #     log.warn("Existing SparkSession detected: Using existing SparkSession")

    """ Load YAML Configuration """
    config = confuse.Configuration("setcover", __name__)
    config.set_file("config.yaml")

    """ Set Cover Problem """
    log.info(f"Running main()")
    try:
        problem = main(config)
    except ValueError:
        log.error("main() failed, possible issue with SparkSession")

    """ Export Solution """
    output_bucket = config["buckets"]["output"].get(str)
    log.info(f"Exporting solution to bucket{output_bucket}")
    problem_solution = pd.DataFrame(
        problem.cover_solution,
        columns=[
            "set_id",
            "set_cost",
            "new_covered_inclusive",
            "new_covered_exclusive",
        ],
    )
    problem_solution.to_csv(output_bucket)
