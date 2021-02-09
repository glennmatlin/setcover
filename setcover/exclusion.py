#!/usr/bin/python

from tqdm.auto import tqdm
import pandas as pd
from collections import OrderedDict
import os
from multiprocessing import current_process
from setcover.set import ExclusionSet
import logging
import concurrent.futures
from typing import List, Dict, Set, Iterable
from tests.test_sets import exclusion_sets
from itertools import cycle

log = logging.getLogger(__name__)


class ExclusionSetCoverProblem:
    # TODO Function argument for limiting sets selected
    # TODO Implement a maximization constraint for coverage
    # TODO: Better init process with lazy functionality to check datatype

    def __init__(self, input_sets=None):  # TODO Lazy detection of input data
        self.cover_solution = self.include_covered = self.exclude_covered = None
        self.universe = self.subsets_include = self.subsets_exclude = None
        if input_sets:
            log.info("Building data set with included data")
            self.universe, self.subsets_include, self.subsets_exclude = self._make_data(
                input_sets
            )

    @staticmethod
    def _make_data(
        sets: List[ExclusionSet],
    ) -> object:
        """

        :param sets: List of Named Tuples
        :return:
        """
        universe = set({})
        subsets_include = OrderedDict()
        subsets_exclude = OrderedDict()
        for set_ in sets:
            subset_id, subset_include, subset_exclude = set_
            subsets_include[subset_id] = set(subset_include)
            subsets_exclude[subset_id] = set(subset_exclude)
            universe |= set(subset_include)
        return universe, subsets_include, subsets_exclude

    def from_sets(self, sets: List[ExclusionSet]):
        self.universe, self.subsets_include, self.subsets_exclude = self._make_data(
            sets
        )

    @staticmethod
    def _rows_to_sets(rows: Iterable) -> List[ExclusionSet]:
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
        self.from_sets(sets)

    def from_dataframe(self, df: pd.DataFrame):
        """
        Used to import Pandas DataFrames
        :param df:
        :return:
        """
        rows = list(df.itertuples(name="Row", index=False))
        sets = self._rows_to_sets(rows)
        self.from_sets(sets)

    @staticmethod
    def _calculate_set_cost(subsets_data, include_covered, exclude_covered):
        """
        Calculate the cost of adding the set to the problem solution
        :param subsets_data:
        :param include_covered:
        :param exclude_covered:
        :return:
        """
        (set_id, include_elements, exclude_elements) = subsets_data
        process_id, process_name = (
            os.getpid(),
            current_process().name,
        )
        log.info(f"Process ID: {process_id}")
        log.info(f"Process Name: {process_name}")
        new_include_elements = len(include_elements - include_covered)
        new_exclude_elements = len(exclude_elements - exclude_covered)
        # set may have same elements as already covered -> Check to avoid division by 0 error
        if new_include_elements != 0:
            cost_elem_ratio = new_exclude_elements / new_include_elements
        else:
            cost_elem_ratio = float("inf")
        return set_id, round(cost_elem_ratio, 4)

    def solve(self, limit=float("inf")):
        log.info("Solving set coverage problem")
        # If elements don't cover problem -> invalid inputs for set cover problem
        log.debug(f"Universe: {self.universe}")
        log.debug(f"Subsets Include: {self.subsets_include.items()}")
        log.debug(f"Subsets Exclude: {self.subsets_exclude.items()}")
        all_elements = set(
            e for s in self.subsets_include.keys() for e in self.subsets_include[s]
        )
        if all_elements != self.universe:
            log.error(f"All Elements: {all_elements}")
            log.error(f"Universe: {self.universe}")
            raise Exception("Universe is incomplete")

        # track elements of problem covered
        include_covered = set()
        exclude_covered = set()
        cover_solution = []
        iteration_counter = 1
        # TODO return the weight for each set when it was used
        # TODO add limiter argument for k sets max, w/ tqdm monitoring
        codes = self.subsets_include.keys()
        log.info(f"Total DX Codes: {len(codes)}")
        with tqdm(total=len(codes), desc="Codes in Solution") as tqdm_iters, tqdm(
            total=len(self.universe), desc="Coverage of Universe"
        ) as tqdm_coverage:
            # TODO: Remove/skip in subset_data set_ids in cover_solution, move to while loop
            while (include_covered != self.universe) & (iteration_counter <= limit):
                skip_codes = [code for code, cost in cover_solution]
                log.debug(f"Skipping over {len(skip_codes)} codes already in solution")
                subsets_zip = zip(
                    codes, self.subsets_include.values(), self.subsets_exclude.values()
                )
                subsets_data = [
                    (code, incl, excl)
                    for code, incl, excl in subsets_zip
                    if code not in skip_codes
                ]
                n = len(subsets_data)
                log.debug(f"Calculating cost for {n} sets")
                # Iterator cycles for multiprocessing
                ic, ec = cycle([include_covered]), cycle([exclude_covered])
                # Find set with minimum cost:elements_added ratio
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    results = list(
                        tqdm(
                            executor.map(
                                self._calculate_set_cost, subsets_data, ic, ec
                            ),
                            total=n,
                            desc="Calculating Set Costs",
                            leave=False,
                        )
                    )
                min_set = min(results, key=lambda t: t[1])
                code_selected = min_set[0]
                cover_solution.append(min_set)
                newly_covered_inclusive = self.subsets_include[
                    code_selected
                ].difference(include_covered)
                newly_covered_exclusive = self.subsets_exclude[
                    code_selected
                ].difference(exclude_covered)
                iteration_counter += 1
                tqdm_iters.update(iteration_counter), tqdm_coverage.update(
                    len(newly_covered_inclusive)
                )
                include_covered |= newly_covered_inclusive  # Bitwise union of sets
                exclude_covered |= newly_covered_exclusive
                log.info(
                    f"""Set found: {min_set[0]}
                Cost: {min_set[1]}
                Added Coverage: {len(newly_covered_inclusive)}
                """
                )
        log.debug(f"Cover Solution: {cover_solution}")
        self.cover_solution = cover_solution
        self.include_covered = include_covered
        self.exclude_covered = exclude_covered


if __name__ == "__main__":
    problem = ExclusionSetCoverProblem(exclusion_sets)
    problem.solve()
    log.info(problem.cover_solution)
