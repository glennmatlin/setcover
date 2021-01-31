#!/usr/bin/python

import pandas as pd
from collections import OrderedDict

import tests.test_data
from setcoverage.set import ExclusionSet


class WeightedSetCoverProblem:
    # TODO testing, docstring, typing
    # TODO rename 'subsets_include' to 'set_cover'
    # TODO Function argument for limiting sets selected
    # TODO Implement a maximization constraint for coverage
    # TODO Finalize typing of inputs/outputs

    def __init__(self, exclusion_sets):
        self.exclusion_sets = exclusion_sets
        self.universe, self.subsets_include, self.subsets_exclude = self.make_data(self)
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

    @staticmethod
    def make_data(self):
        """
        input: ExclusionSet(id="A10", set=["Glenn"], weight=100.0)
        output: (Dict[Dict[str:set[str]]], list[str], list[set], list[float])
        """
        # TODO Unit test, better docstring
        universe = set()
        subsets_exclude = OrderedDict()
        subsets_include = OrderedDict()
        for exclusion_set in tests.test_data.exclusion_sets:
            subset_id, subset_include, subset_exclude = exclusion_set
            subset_include[subset_id] = set(subset_include)
            subsets_exclude[subset_id] = set(subset_exclude)
            universe |= set(subset_id)
        return universe, subsets_include, subsets_exclude

    @staticmethod
    def greedy_solver(self):
        """
        Inputs:
        - set_problem: list[list[str]]
        - subsets_include: dict{str:list[str]}
        - subsets_exclude: dict{str:list[str]}
        """
        # TODO Unit test, better docstring, typing
        # TODO Finding most cost-effective using priority queue.

        # if elements don't cover problem -> invalid inputs for set cover problem
        include_elements = set(
            e for s in self.subsets_include.keys() for e in self.subsets_include[s]
        )
        exclude_elements = set(
            e for s in self.subsets_exclude.keys() for e in self.subsets_exclude[s]
        )
        if include_elements != self.universe | exclude_elements != self.universe:
            return None

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
