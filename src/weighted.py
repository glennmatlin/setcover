#!/usr/bin/python

import pandas as pd
from collections import OrderedDict

import tests.test_data
from .set import WeightedSet


class WeightedSetCoverProblem:
    # TODO testing, docstring, typing
    # TODO rename 'subsets' to 'set_cover'
    # TODO Function argument for limiting sets selected
    # TODO Implement a maximization constraint for coverage
    # TODO Finalize typing of inputs/outputs

    def __init__(self, weighted_sets):
        self.weighted_sets = weighted_sets
        self.set_problem, self.subsets, self.weights = self.make_data(self)
        self.universe = set(self.set_problem.keys())
        self.covered, self.cover_solution, self.weight_total = self.greedy_solver(self)

    @classmethod
    def from_lists(cls, ids, sets, weights):
        """
        Convert pandas DataFrame to into a list of named tuples
        """
        # TODO Unit test
        rows = list(zip(ids, sets, weights))
        weighted_sets = [WeightedSet(r[0], r[1], r[2]) for r in rows]
        return cls(weighted_sets)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame):
        """
        Convert pandas DataFrame to into a list of named tuples
        """
        # TODO Unit test
        rows = list(df.itertuples(name="Row", index=False))
        weighted_sets = [WeightedSet(r[0], r[1], r[2]) for r in rows]
        return cls(weighted_sets)

    @staticmethod
    def make_data(self):
        """
        input: WeightedSet(id="A10", set=["Glenn"], weight=100.0)
        output: (Dict[Dict[str:set[str]]], list[str], list[set], list[float])
        """
        # TODO Unit test, better docstring
        set_problem = OrderedDict()
        subsets = OrderedDict()
        weights = OrderedDict()
        for weighted_set in self.weighted_sets:
            subset_id, subset, weight = weighted_set
            subsets[subset_id] = set(subset)
            weights[subset_id] = weight
            for set_element in subset:
                if set_element not in set_problem:
                    set_problem[set_element] = set()
                set_problem[set_element].add(subset_id)

        return set_problem, subsets, weights

    @staticmethod
    def greedy_solver(self):
        """
        Greedy algorithm implementation for a proximal solution for Weighted set Coverage
        pick the set which is the most cost-effective: min(w[s]/|s-C|),
        where C is the current covered elements set.

        The complexity of the algorithm: O(|U| * log|S|) .
        The operation has time complexity of O(log|S|).

        Inputs:
        - set_problem: list[list[str]]
        - subsets: dict{str:list[str]}
        - weights: dict{str:float}
        """
        # TODO Unit test, better docstring, typing
        # TODO Finding most cost-effective using priority queue.

        # if elements don't cover problem -> invalid inputs for set cover problem
        elements = set(e for s in self.subsets.keys() for e in self.subsets[s])
        if elements != self.universe:
            return None

        # track elements of problem covered
        covered = set()
        cover_sets = []
        weight_total = 0

        while covered != self.universe:
            min_cost_elem_ratio = float("inf")
            min_set = None
            # find set with minimum cost:elements_added ratio
            for s, elements in self.subsets.items():  # TODO Rename unpacked variables
                new_elements = len(elements - covered)
                # set may have same elements as already covered -> new_elements = 0
                # check to avoid division by 0 error
                if new_elements != 0:
                    cost_elem_ratio = self.weights[s] / new_elements
                    if cost_elem_ratio < min_cost_elem_ratio:
                        min_cost_elem_ratio = cost_elem_ratio
                        min_set = s
            cover_sets.append(min_set)  # Track sets used
            covered |= self.subsets[min_set]  # Bitwise union of sets
            weight_total += self.weights[min_set]
        return covered, cover_sets, weight_total