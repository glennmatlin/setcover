#!/usr/bin/env python
# # Set Cover Optimization of Trigger Codes

# ## Imports
import pandas as pd
from setcover.exclusion import ExclusionSetCoverProblem
from setcover.set import ExclusionSet
from typing import List
import logging
import confuse

"""Logging"""
log = logging.getLogger(__name__)
log.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d %H:%M",
    filemode="w",
)
log = logging.getLogger(__name__)
# Create stream handler which writes ERROR messages or higher to the sys.stderr
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# Set a format which is simpler for console use
ch.setFormatter(logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s"))
# Create file handlers for info and debug logs
fh = logging.FileHandler("run_debug.log")
# Add handlers to logger
log.addHandler(ch), log.addHandler(fh)


"""Load configuration from .yaml file."""
config = confuse.Configuration('setcover', __name__)
config.set_file('config.yaml')
input_bucket = config['buckets']['input'].get(str)
output_bucket = config['buckets']['input'].get(str)
problem_limit = config['problem']['limit'].get(int)
print(input_bucket, output_bucket)

def make_data(input_bucket: str) -> List[ExclusionSet]:
    """
    prepares data to be used in set cover problem using pandas
    :param input_bucket: s3 path of data
    :return: data struct to be used set coverage problem
    """
    df = pd.read_parquet(input_bucket).query("rate_test>0.01")[
        ["code", "registry_ids", "control_ids"]
    ]
    df["control_ids"] = (
        df["control_ids"].str.split(",").apply(lambda row: [id.strip() for id in row])
    )
    df["registry_ids"] = (
        df["registry_ids"].str.split(",").apply(lambda row: [id.strip() for id in row])
    )
    # TODO: Replace with .from_df
    rows = list(df.itertuples(name="Row", index=False))
    sets = ExclusionSetCoverProblem._rows_to_sets(rows)
    return sets

def main():
    data = make_data(input_bucket)
    problem = ExclusionSetCoverProblem(data)
    problem.solve(limit=problem_limit)
    exclusion_cover_solution = pd.Series([code for code, _cost in problem.cover_solution])
    exclusion_cover_solution.to_csv(output_bucket.replace("parquet", "csv"))


if __name__ == "__main__":
    import cProfile

    cProfile.run("main()", "run_output.dat")

    import pstats

    with open("run_output_time.txt", "w") as f:
        p = pstats.Stats("run_output.dat", stream=f)
        p.sort_stats("time").print_stats()
    with open("run_output_calls.txt", "w") as f:
        p = pstats.Stats("run_output.dat", stream=f)
        p.sort_stats("calls").print_stats()
