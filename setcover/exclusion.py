#!/usr/bin/python

from tqdm.auto import tqdm
import pandas as pd
from collections import OrderedDict
import os
from multiprocessing import current_process
from setcover.set import ExclusionSet
import logging
import concurrent.futures
from typing import List, Dict, Set
from tests.test_sets import exclusion_sets
log = logging.getLogger(__name__)


class ExclusionSetCoverProblem:
    # TODO Function argument for limiting sets selected
    # TODO Implement a maximization constraint for coverage
    # TODO: Better init process with lazy functionality to check datatype

    def __init__(self, exclusion_sets=None):
        if exclusion_sets:
            log.info("Making datasets")
            self.universe, self.subsets_include, self.subsets_exclude = self.make_data(
                exclusion_sets
            )
        else:
            pass

    @classmethod
    def from_lists(
        cls, ids: List[str], sets_include: List[Set[str]], sets_exclude: List[Set[str]]
    ) -> object:
        # TODO Unit test
        rows = list(zip(ids, sets_include, sets_exclude))
        exclusion_sets = [ExclusionSet(r[0], r[1], r[2]) for r in rows]
        return cls(exclusion_sets)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> object:
        # TODO Unit test
        rows = list(df.itertuples(name="Row", index=False))
        exclusion_sets = [ExclusionSet(r[0], r[1], r[2]) for r in rows]
        return cls(exclusion_sets)

    @staticmethod
    def make_data(
        exclusion_sets: ExclusionSet,
    ) -> (Set[str], Dict[str, Set[str]], Dict[str, Set[str]]):
        universe = set()
        subsets_include = OrderedDict()
        subsets_exclude = OrderedDict()
        for exclusion_set in exclusion_sets:
            subset_id, subset_include, subset_exclude = exclusion_set
            subsets_include[subset_id] = set(subset_include)
            subsets_exclude[subset_id] = set(subset_exclude)
            universe |= set(subset_include)
        return universe, subsets_include, subsets_exclude

    @staticmethod
    def calculate_set_cost(subsets_data, include_covered, exclude_covered):
        ((set_id, include_elements), (_, exclude_elements)) = subsets_data
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
        return set_id, cost_elem_ratio

    def greedy_solver(self):
        log.info("Solving problem")
        # if elements don't cover problem -> invalid inputs for set cover problem
        log.info(f"Universe: {self.universe}")
        log.info(f"Subsets Include: {self.subsets_include.items()}")
        log.info(f"Subsets Exclude: {self.subsets_exclude.items()}")
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
        iteration_counter = 0
        # TODO return the weight for each set when it was used
        # TODO add limiter argument for k sets max, w/ tqdm monitoring
        with tqdm(
            total=len(self.subsets_include.keys()), desc="# Codes Used"
        ) as tqdm_iters, tqdm(
            total=len(self.universe), desc="Coverage of Universe"
        ) as tqdm_coverage:
            # TODO: Remove/skip in subset_data set_ids in cover_solution, move to while loop
            subsets_data = list(
                zip(self.subsets_include.items(), self.subsets_exclude.items())
            )
            while include_covered != self.universe:
                n = len(subsets_data)
                ic = [include_covered] * n  # TODO Find a way to avoid creating lists just to feed in
                ec = [exclude_covered] * n  # TODO Find a way to avoid creating lists just to feed in
                # Find set with minimum cost:elements_added ratio
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    results = list(
                        tqdm(
                            executor.map(self.calculate_set_cost, subsets_data, ic, ec),
                            total=n,
                            desc="Calculating Set Costs",
                            leave=False,
                        )
                    )
                min_set = min(results, key=lambda t: t[1])
                cover_solution.append(min_set)
                newly_covered_inclusive = self.subsets_include[min_set[0]].difference(
                    include_covered
                )
                newly_covered_exclusive = self.subsets_exclude[min_set[0]].difference(
                    exclude_covered
                )
                iteration_counter += 1
                tqdm_iters.update(tqdm_iters)
                tqdm_coverage.update(len(newly_covered_inclusive))
                include_covered |= newly_covered_inclusive  # Bitwise union of sets
                exclude_covered |= newly_covered_exclusive
                log.info(f"Minimum cost set found: {min_set}")
                log.info(f"Minimum cost set found: {len(newly_covered_inclusive)}")

        self.cover_solution, self.include_covered, self.exclude_covered = (
            cover_solution,
            include_covered,
            exclude_covered,
        )


if __name__ == '__main__':
    problem = ExclusionSetCoverProblem(exclusion_sets)
    problem.greedy_solver()
    print(problem.cover_solution)
