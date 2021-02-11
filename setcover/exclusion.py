#!/usr/bin/python

from tqdm.auto import tqdm
import pandas as pd
from collections import OrderedDict
import os
import multiprocessing
from setcover.set import ExclusionSet
import logging
import concurrent.futures
from typing import List, Set, Iterable
from itertools import repeat
import numpy as np

# Logging To Dos
# TODO: Get root directory, make logs folder for logging/profiling output
# ROOT_DIR = os.path.dirname(os.path.abspath("file.py"))

# API To Dos
# TODO: Lazy data type checking on __init__
# TODO: Figure out why  __init__  `obj = obj = None` fails -- is it a pointer issue?

# Logging
logging.basicConfig(
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
fh = logging.FileHandler("exclusion.log")
# Add handlers to logger
log.addHandler(ch), log.addHandler(fh)


class ExclusionSetCoverProblem:
    """"""

    def __init__(self, input_sets=None):
        """

        :param input_sets:
        """
        self.subsets_include = OrderedDict()
        self.subsets_exclude = OrderedDict()
        self.include_covered = set()
        self.exclude_covered = set()
        self.elements_include = set()
        self.elements_exclude = set()
        self.cover_solution = []

        if input_sets:
            log.info("Building data set with included data")
            (
                self.elements_include,
                self.elements_exclude,
                self.subsets_include,
                self.subsets_exclude,
            ) = self._make_data(input_sets)

    @staticmethod
    def _make_data(
        sets: List[ExclusionSet],
    ) -> object:
        """

        :param sets:
        :return:
        """
        elements_include = set({})
        elements_exclude = set({})
        subsets_include = OrderedDict()
        subsets_exclude = OrderedDict()
        for set_ in sets:
            subset_id, subset_include, subset_exclude = set_
            subsets_include[subset_id] = set(subset_include)
            subsets_exclude[subset_id] = set(subset_exclude)
            elements_include |= set(subset_include)
            elements_exclude |= set(subset_exclude)
        return elements_include, elements_exclude, subsets_include, subsets_exclude

    def _define_data(self, sets: List[ExclusionSet]):
        """

        :param sets:
        :return:
        """
        (
            self.elements_include,
            self.elements_exclude,
            self.subsets_include,
            self.subsets_exclude,
        ) = self._make_data(sets)

    @staticmethod
    def _rows_to_sets(rows: Iterable) -> List[ExclusionSet]:
        """

        :param rows:
        :return:
        """
        return [ExclusionSet(r[0], r[1], r[2]) for r in rows]

    def from_lists(
        self, ids: List[str], sets_include: List[Set[str]], sets_exclude: List[Set[str]]
    ):
        """
        Used to import Python Lists
        :param ids:
        :param sets_include:
        :param sets_exclude:
        :return:
        """
        rows = list(zip(ids, sets_include, sets_exclude))
        sets = self._rows_to_sets(rows)
        self._define_data(sets)

    def from_dataframe(self, df: pd.DataFrame):
        """
        Used to import Pandas DataFrames
        :param df:
        :return:
        """
        rows = list(df.itertuples(name="Row", index=False))
        sets = self._rows_to_sets(rows)
        self._define_data(sets)

    @staticmethod
    def _calculate_set_cost(subsets_data, include_covered, exclude_covered):
        """
        Calculate the cost of adding the set to the problem solution

        :param subsets_data:
        :param include_covered:
        :param exclude_covered:
        :return:
        """
        process_id, process_name = (
            os.getpid(),
            multiprocessing.current_process().name,
        )
        log.debug(
            f"""
            Process ID: {process_id}
            Process Name: {process_name}
            """
        )
        (set_id, include_elements, exclude_elements) = subsets_data
        added_include_coverage = len(include_elements - include_covered)
        added_exclude_coverage = len(exclude_elements - exclude_covered)
        # set may have same elements as already covered -> Check to avoid division by 0 error
        if added_include_coverage != 0:
            set_cost = added_exclude_coverage / added_include_coverage
        else:
            set_cost = float("inf")
        log.debug(
            f"""
            Set ID: {set_id}
            Set Cost: {set_cost}
            New Include Elements: {added_include_coverage}
            New Exclude Elements: {added_exclude_coverage}
            """
        )
        return set_id, round(set_cost, 5)

    def solve(self, limit=float("inf")):
        """

        :param limit:
        :return:
        """
        log.info("Solving set coverage problem")
        # If elements don't cover problem -> invalid inputs for set cover problem
        set_ids = set(self.subsets_include.keys())  # TODO Move this out of solve
        log.debug(f"Sets IDs: {set_ids}")
        all_elements = set(
            e for s in self.subsets_include.keys() for e in self.subsets_include[s]
        )
        if all_elements != self.elements_include:
            log.error(f"All Elements: {all_elements}")
            log.error(f"Universe: {self.elements_include} self.elements_exclude,")
            raise Exception("Universe is incomplete")

        # track elements of problem covered
        log.info(f"Total number of Sets: {len(set_ids)}")
        with tqdm(total=len(set_ids), desc="Sets Used in Solution") as tqdm_sets, tqdm(
            total=len(self.elements_include),
            desc="Set Coverage of Include Set",
        ) as tqdm_include, tqdm(
            total=len(self.elements_exclude),
            desc="Set Coverage of Exclude Set",
        ) as tqdm_exclude:
            inf_sets = set()
            while (len(self.include_covered) < len(self.elements_include)) & (
                len(self.cover_solution) < limit
            ):
                used_set_ids = [set_id for set_id, _, _, _ in self.cover_solution]
                log.info(f"Sets used in solution: {len(used_set_ids)}")
                log.info(f"Skipped sets with no added coverage: {len(inf_sets)}")
                used_set_ids += list(inf_sets)
                set_zip = zip(
                    self.subsets_include.keys(),
                    self.subsets_include.values(),
                    self.subsets_exclude.values(),
                )
                set_data = [
                    (set_id, incl, excl)
                    for set_id, incl, excl in set_zip
                    if set_id not in used_set_ids
                ]
                n = len(set_data)
                if n == 0:
                    log.error("Available sets have been exhausted")
                    break
                log.info(f"Calculating cost for {n} sets")
                # Iterator repeats for multiprocessing
                ic, ec = repeat(self.include_covered), repeat(self.exclude_covered)
                # Find set with minimum cost:elements_added ratio
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    results = list(
                        tqdm(
                            executor.map(self._calculate_set_cost, set_data, ic, ec),
                            total=n,
                            desc="Calculating Set Costs",
                            leave=False,
                        )
                    )
                # Select the set with the lowest cost
                log.debug(results)
                min_set_id, min_set_cost = min(results, key=lambda t: t[1])

                # if any sets return as float("inf") we should stop checking them
                inf_sets |= set(
                    [set_id for set_id, set_cost in results if np.isinf(set_cost)]
                )
                min_set_include, min_set_exclude = (
                    self.subsets_include[min_set_id],
                    self.subsets_exclude[min_set_id],
                )
                # Find the new elements we covered
                new_covered_inclusive = min_set_include.difference(self.include_covered)
                if len(new_covered_inclusive) <= 1:
                    break
                new_covered_exclusive = min_set_exclude.difference(self.exclude_covered)
                tqdm_include.update(len(new_covered_inclusive))
                tqdm_exclude.update(len(new_covered_exclusive))
                # Add newly covered to tracking variables
                self.include_covered |= new_covered_inclusive
                self.exclude_covered |= new_covered_exclusive
                # Append to our solution
                self.cover_solution.append(
                    (
                        min_set_id,
                        min_set_cost,
                        len(new_covered_inclusive),
                        len(new_covered_exclusive),
                    )
                )
                tqdm_sets.update(1)
                log.info(
                    f"""
                    Set ID: {min_set_id}
                    Set Cost: {min_set_cost}
                    New Set Coverage: {len(new_covered_inclusive)}
                    Current Solution: {self.cover_solution}
                    """
                )
        log.info(f"Final Solution: {self.cover_solution}")


def main():
    log.info("Importing data to problem")
    from tests.test_sets import exclusion_sets

    problem = ExclusionSetCoverProblem(exclusion_sets)
    log.info("Finding solution to problem")
    problem.solve()


if __name__ == "__main__":
    import cProfile

    cProfile.run("main()", "output.dat")

    import pstats

    with open("output_time.txt", "w") as f:
        p = pstats.Stats("output.dat", stream=f)
        p.sort_stats("time").print_stats()
    with open("output_calls.txt", "w") as f:
        p = pstats.Stats("output.dat", stream=f)
        p.sort_stats("calls").print_stats()
