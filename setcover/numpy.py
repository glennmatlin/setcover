#!/usr/bin/python

# Logging To Dos
# TODO: Get root directory, make logs folder for logging/profiling output
# ROOT_DIR = os.path.dirname(os.path.abspath("file.py"))

# API To Dos
# TODO: Lazy data type checking on __init__
# TODO: Figure out why  __init__  `obj = obj = None` fails -- is it a pointer issue?

import concurrent.futures
import logging
import multiprocessing
import os
from collections import OrderedDict
from itertools import repeat
from typing import Dict, Iterable, List, Set

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from setcover.utils import flatten_nest, reverse_dictionary

# Logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d %H:%M",
    filemode="w",
)
log = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
ch.setFormatter(logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s"))
fh = logging.FileHandler("exclusion.log")
log.addHandler(ch), log.addHandler(fh)


class NumpySetCoverProblem:
    """"""

    def __init__(self):
        self.df: pd.DataFrame
        self.inclusive_total: int = self.df.iloc[0]["n_total_test"]  # TODO
        self.exclusive_total: int = self.df.iloc[0]["n_total_control"]  # TODO
        self.all_total: int = self.inclusive_total + self.exclusive_total

        self.include_elements: Set[object] = flatten_nest(self.df.set_include.to_list())
        self.exclude_elements: Set[object] = flatten_nest(self.df.set_exclude.to_list())

        self.n_include: int = len(self.include_elements)
        self.n_exclude: int = len(self.exclude_elements)
        self.n_all: int = self.n_include + self.n_exclude

        self.token_map: Dict[str, int] = {}  # TODO
        self.idx_map: Dict[int, str] = reverse_dictionary(self.token_map)

        self.label_array = np.concatenate(
            [np.ones(self.n_include), np.zeros(self.n_exclude)]
        ).astype("?")
        self.cover_array = np.zeros(self.n_all).astype("?")
        self.subset_array_dtypes = np.dtype(
            [("set_id", "S7"), ("set_array", "?", (1, self.n_all))]
        )
        self.subset_arrays = np.rec.array(
            self.df.apply(lambda row: self._make_subset_array(row), axis=1).to_list(),
            self.subset_array_dtypes,
        )  # TODO
        self.cover_solution: List[str]

    def _get_idxs(self, tokens) -> List[int]:
        return [self.token_map[token] for token in tokens]

    def _get_tokens(self, idxs) -> List[str]:
        return [self.idx_map[idx] for idx in idxs]

    def _make_subset_array(self, row, id_field="set_id", token_field="set_tokens"):
        output = (row[id_field], np.zeros(self.n_all))
        dx_positive_idxs = self._get_idxs(row[token_field])
        output[1][dx_positive_idxs] = 1.0
        return output

    def _calculate_subset_weight(self, subset_array):
        """ Calculate the weight of a set """
        set_id = subset_array[0].astype("str")
        process_id, process_name = (
            os.getpid(),
            multiprocessing.current_process().name,
        )
        log.debug(
            f"""
            Set ID: {set_id}
            Process ID: {process_id}
            Process Name: {process_name}
            """
        )
        exclude_added = np.sum(
            np.logical_and(
                subset_array[1][0] == True,
                self.label_array == False,
                self.cover_array == False,
            )
        ).astype("int")
        include_added = np.sum(
            np.logical_and(
                subset_array[1][0] == True,
                self.label_array == True,
                self.cover_array == False,
            )
        ).astype("int")
        set_weight = exclude_added / include_added
        log.debug(
            f"""
            Set ID: {set_id}
            Set Cost: {set_weight}
            New Include Elements: {include_added}
            New Exclude Elements: {exclude_added}
            """
        )
        return set_id, round(set_weight, 5), include_added, exclude_added

    # def solve(self, limit=float("inf")):
    #     """
    #
    #     :param limit:
    #     :return:
    #     """
    #     log.info("Solving set coverage problem")
    #     # If elements don't cover problem -> invalid inputs for set cover problem
    #     set_ids = set(self.subsets_include.keys())  # TODO Move this out of solve
    #     log.debug(f"Sets IDs: {set_ids}")
    #     all_elements = set(
    #         e for s in self.subsets_include.keys() for e in self.subsets_include[s]
    #     )
    #     if all_elements != self.elements_include:
    #         log.error(f"All Elements: {all_elements}")
    #         log.error(f"Universe: {self.elements_include} self.elements_exclude,")
    #         raise Exception("Universe is incomplete")
    #
    #     # track elements of problem covered
    #     log.info(f"Total number of Sets: {len(set_ids)}")
    #     with tqdm(total=len(set_ids), desc="Sets Used in Solution") as tqdm_sets, tqdm(
    #         total=len(self.elements_include),
    #         desc="Set Coverage of Include Set",
    #     ) as tqdm_include, tqdm(
    #         total=len(self.elements_exclude),
    #         desc="Set Coverage of Exclude Set",
    #     ) as tqdm_exclude:
    #         inf_sets = set()
    #         while (len(self.include_covered) < len(self.elements_include)) & (
    #             len(self.cover_solution) < limit
    #         ):
    #             used_set_ids = [set_id for set_id, _, _, _ in self.cover_solution]
    #             log.info(f"Sets used in solution: {len(used_set_ids)}")
    #             log.info(f"Skipped sets with no added coverage: {len(inf_sets)}")
    #             used_set_ids += list(inf_sets)
    #             set_zip = zip(
    #                 self.subsets_include.keys(),
    #                 self.subsets_include.values(),
    #                 self.subsets_exclude.values(),
    #             )
    #             set_data = [
    #                 (set_id, incl, excl)
    #                 for set_id, incl, excl in set_zip
    #                 if set_id not in used_set_ids
    #             ]
    #             n = len(set_data)
    #             if n == 0:
    #                 log.error("Available sets have been exhausted")
    #                 break
    #             log.info(f"Calculating cost for {n} sets")
    #             # Iterator repeats for multiprocessing
    #             ic, ec = repeat(self.include_covered), repeat(self.exclude_covered)
    #             # Find set with minimum cost:elements_added ratio
    #             with concurrent.futures.ProcessPoolExecutor() as executor:
    #                 results = list(
    #                     tqdm(
    #                         executor.map(self._calculate_set_cost, set_data, ic, ec),
    #                         total=n,
    #                         desc="Calculating Set Costs",
    #                         leave=False,
    #                     )
    #                 )
    #             # Select the set with the lowest cost
    #             log.debug(results)
    #             min_set_id, min_set_cost = min(results, key=lambda t: t[1])
    #
    #             # if any sets return as float("inf") we should stop checking them
    #             inf_sets |= set(
    #                 [set_id for set_id, set_cost in results if np.isinf(set_cost)]
    #             )
    #             min_set_include, min_set_exclude = (
    #                 self.subsets_include[min_set_id],
    #                 self.subsets_exclude[min_set_id],
    #             )
    #             # Find the new elements we covered
    #             new_covered_inclusive = min_set_include.difference(self.include_covered)
    #             new_covered_exclusive = min_set_exclude.difference(self.exclude_covered)
    #             tqdm_include.update(len(new_covered_inclusive))
    #             tqdm_exclude.update(len(new_covered_exclusive))
    #             # Add newly covered to tracking variables
    #             self.include_covered |= new_covered_inclusive
    #             self.exclude_covered |= new_covered_exclusive
    #             # Append to our solution
    #             self.cover_solution.append(
    #                 (
    #                     min_set_id,
    #                     min_set_cost,
    #                     len(new_covered_inclusive),
    #                     len(new_covered_exclusive),
    #                 )
    #             )
    #             tqdm_sets.update(1)
    #             log.info(
    #                 f"""
    #                 Set ID: {min_set_id}
    #                 Set Cost: {min_set_cost}
    #                 New Set Coverage: {len(new_covered_inclusive)}
    #                 Current Solution: {self.cover_solution}
    #                 """
    #             )
    #     log.info(f"Final Solution: {self.cover_solution}")
    #     log.info(f"Skipped Sets: {inf_sets}")


def main():
    pass
    # log.info("Importing data to problem")
    # from tests.test_sets import numpy_sets
    # problem = NumpySetCoverProblem(numpy_sets)
    # log.info("Finding solution to problem")
    # problem.solve()


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
