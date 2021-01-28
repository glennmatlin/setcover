#!/usr/bin/python
from __future__ import annotations
from src.queue import SetQueue
import pandas as pd
from collections import OrderedDict
from src.set import WeightedSet

MAXPRIORITY = 999999


class WeightedSetCoverProblem:
    # TODO Function argument for limiting sets selected
    # TODO Implement a maximization constraint for coverage
    # TODO Finalize typing of inputs/outputs

    def __init__(self, weighted_sets: list[tuple]):
        self.weighted_sets = weighted_sets
        self.universe, self.set_ids, self.problem_sets, self.weights = self.make_data(
            self
        )
        self.pq = self.prioritize(self)
        # self.selection, self.total_weight = self.solver(self)
        # self.cover_solution = None
        # self.total_weight = None

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
    def make_data(self):# -> (OrderedDict[str:set], list[str], list[set], list[float]):
        """
        input: WeightedSet(id="A10", set=["Glenn"], weight=100.0)
        output: universe, ids, problem_sets, weights
            universe: OrderedDict[str:set]
            set_ids: list[str]
            problem_sets: list[set[str]]
            weights: list[float]
        """
        # TODO Unit test
        universe = OrderedDict()
        set_ids = []
        sets = []
        weights = []
        for weighted_set in self.weighted_sets:
            set_ids.append(weighted_set.id)
            sets.append(set(weighted_set.set))
            weights.append(weighted_set.weight)
            for ele in weighted_set.set:
                if ele not in universe:
                    universe[ele] = set()
                universe[ele].add(weighted_set.id)

        return universe, set_ids, sets, weights

    @staticmethod
    def prioritize(self):
        # TODO How do I make a unit test for something that
        pq = SetQueue()
        for index, problem_set in enumerate(
                self.problem_sets
        ):  # add all sets to the priorityqueue
            if len(problem_set) == 0:
                pq.add_task(index, MAXPRIORITY)
            else:
                pq.add_task(index, float(self.weights[index]) / len(problem_set))
        return pq

    @staticmethod
    def solver(self) -> (list[str], int):
        """
        Greedy algorithm implementation for a proximal solution for Weighted set Coverage
        pick the set which is the most cost-effective: min(w[s]/|s-C|),
        where C is the current covered elements set.

        The complexity of the algorithm: O(|U| * log|S|) .
        Finding the most cost-effective set is done by a priority queue.
        The operation has time complexity of O(log|S|).

        Input:
        sets: a collection of sets
        weights: corresponding weight to each set

        Output:
        selected: selected set ids in order (list)
        cost: the total cost of the selected sets.
        """
        # TODO Unit test
        # TODO Record the order of ICD codes used for best set

        set_indices = []
        selected_ids = []
        total_weight = 0
        covered = 0

        while covered < len(self.universe):
            set_idx = self.pq.pop_task()  # get the most cost-effective set
            set_indices.append(set_idx)  # record the set idx we picked
            selected_ids.append(self.set_ids[set_idx])  # record the set itself we picked
            total_weight += self.weights[set_idx]  # add set weight to solution total
            covered += len(self.problem_sets[set_idx])
            # TODO `covered`
            # understand `covered` var more -- this seems wrong to me bc
            # its adding length but not checking the length that was actually added
            # i thought this would not count dupe elements

            # Update the sets that contains the new covered elements
            for set_element in self.problem_sets[set_idx]:  # for ele in list[str]
                for set_id in self.universe[set_element]: # for id in set[str] <-from- dict[str:set][ele]
                    if set_id != set_idx: # TODO Error is occuring here ... set_id is the code and set_idx is a int
                        self.problem_sets[set_id].discard(set_element)
                        # TODO FIX ^^ TypeError: list indices must be integers or slices, not str
                        if len(self.problem_sets[set_id]) == 0:
                            self.pq.add_task(set_id, MAXPRIORITY)
                        else:
                            self.pq.add_task(
                                set_id,
                                float(self.weights[set_id])
                                / len(self.problem_sets[set_id]),
                            )
            self.problem_sets[set_idx].clear()
            self.pq.add_task(set_idx, MAXPRIORITY)

        return set_indices, total_weight
