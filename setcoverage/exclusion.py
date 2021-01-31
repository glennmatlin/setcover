#!/usr/bin/python

import pandas as pd
from collections import OrderedDict

from setcoverage.set import ExclusionSet
import logging
log = logging.getLogger(__name__)

class ExclusionSetCoverProblem:
    # TODO testing, docstring, typing
    # TODO rename 'subsets_include' to 'set_cover'
    # TODO Function argument for limiting sets selected
    # TODO Implement a maximization constraint for coverage
    # TODO Finalize typing of inputs/outputs

    def __init__(self, exclusion_sets):
        self.universe, self.subsets_include, self.subsets_exclude = self.make_data(exclusion_sets)
        (
            self.cover_sets,
            self.include_covered,
            self.exclude_covered,
        ) = self.greedy_solver(self)

    # TODO: Lazy import functionality that checks for datatype
    @classmethod
    def from_lists(cls, ids, sets_include, sets_exclude):
        """
        Convert pandas DataFrame to into a list of named tuples
        """
        # TODO Unit test
        rows = list(zip(ids, sets_include, sets_exclude))
        exclusion_sets = [ExclusionSet(r[0], r[1], r[2]) for r in rows]
        return cls(exclusion_sets)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame):
        """
        Convert pandas DataFrame to into a list of named tuples
        """
        # TODO Unit test
        rows = list(df.itertuples(name="Row", index=False))
        exclusion_sets = [ExclusionSet(r[0], r[1], r[2]) for r in rows]
        return cls(exclusion_sets)

    def make_data(self, exclusion_sets):
        """
        input: ExclusionSet(id=str, include_set=set[str], exclude_set=set[str])
        """
        # TODO Unit test, better docstring
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
    def greedy_solver(self):
        """
        Inputs:
        - universe: set[str]
        - subsets_include: OrderedDict{str:set[str]}
        - subsets_exclude: OrderedDict{str:set[str]}
        """
        # TODO Unit test, better docstring, typing
        # TODO Finding most cost-effective using priority queue.

        # if elements don't cover problem -> invalid inputs for set cover problem
        log.info(self.universe)
        log.info(self.subsets_include.items())
        include_elements = set(
            e for s in self.subsets_include.keys() for e in self.subsets_include[s]
        )
        if include_elements != self.universe:
            log.error(include_elements)
            log.error(self.universe)
            raise Exception("universe is incomplete")

        # track elements of problem covered
        include_covered = set()
        exclude_covered = set()
        cover_sets = []

        while include_covered != self.universe:
            min_cost_elem_ratio = float("inf")
            min_set = None
            # find set with minimum cost:elements_added ratio
            for ((set_id, include_elements), (_, exclude_elements)) in zip(
                self.subsets_include.items(), self.subsets_exclude.items()
            ):  # TODO Rename unpacked variables
                new_include_elements = len(include_elements - include_covered)
                new_exclude_elements = len(exclude_elements - exclude_covered)
                # set may have same elements as already covered -> new_elements = 0
                # check to avoid division by 0 error
                if new_include_elements != 0:
                    cost_elem_ratio = new_exclude_elements / new_include_elements
                    if cost_elem_ratio < min_cost_elem_ratio:
                        min_cost_elem_ratio = cost_elem_ratio
                        min_set = set_id
            cover_sets.append(min_set)  # Track sets used
            include_covered |= self.subsets_include[min_set]  # Bitwise union of sets
            exclude_covered |= self.subsets_exclude[min_set]
        return cover_sets, include_covered, exclude_covered