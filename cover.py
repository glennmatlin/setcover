#!/usr/bin/python
from collections import namedtuple
from typing import List, Iterable
from queue import PriorityQueue
import pandas as pd

MAXPRIORITY = 999999

WeightedSet = namedtuple("WeightedSet", "id set weight")


class WeightedSetCoverProblem:
    # TODO Function argument for limiting sets selected
    # TODO Implement a maximization constraint for coverage
    # TODO Finalize typing of inputs/outputs

    def __init__(self, weighted_sets: List[WeightedSet]):
        self.weighted_sets = weighted_sets
        self.universe, self.set_ids, self.problem_sets, self.weights = self.make_data(
            self
        )
        self.selection, self.total_weight = self.solver(self)
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
    def make_data(self):
        """
        input: WeightedSet(id="A10", set=["Glenn"], weight=100.0)
        output: universe, problem_sets, weights
            universe: Dict[str:set]
            problem_sets: List[List[str]
            weights: List[float]
        """
        # TODO Unit test
        universe = {}
        ids = []
        sets = []
        weights = []
        for weighted_set in self.weighted_sets:
            ids.append(weighted_set.id)
            sets.append(set(weighted_set.set))
            weights.append(weighted_set.weight)
            for ele in weighted_set.set:
                if ele not in universe:
                    universe[ele] = set()
                universe[ele].add(weighted_set.id)

        return universe, ids, sets, weights

    @staticmethod
    def prioritize(self):
        # TODO Unit test
        pq = PriorityQueue()
        for index, problem_set in enumerate(
            self.problem_sets
        ):  # add all sets to the priorityqueue
            if len(problem_set) == 0:
                pq.add_task(index, MAXPRIORITY)
            else:
                pq.add_task(index, float(self.weights[index]) / len(problem_set))
        return pq

    @staticmethod
    def solver(self) -> (Iterable[str], int):
        """
        Greedy algorithm implementation for a proximal solution for Weighted Set Coverage
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

        pq = self.prioritize(self)
        set_indices = []
        selected_ids = []
        total_weight = 0
        covered = 0

        while covered < len(self.universe):
            set_idx = pq.pop_task()  # get the most cost-effective set
            set_indices.append(set_idx)  # build a list of index values
            selected_ids.append(self.set_ids[set_idx])

            total_weight += self.weights[set_idx]
            covered += len(self.problem_sets[set_idx])
            # TODO `covered`
            # understand `covered` var more -- this seems wrong to me bc
            # its adding length but not checking the length that was actually added
            # i thought this would not count dupe elements

            # Update the sets that contains the new covered elements
            for set_element in self.problem_sets[set_idx]:
                for set_id in self.universe[set_element]:
                    if set_id != set_idx: # TODO Error is occuring here ... set_id is the code and set_idx is a int
                        self.problem_sets[set_id].discard(set_element)
                        # TODO FIX ^^ TypeError: list indices must be integers or slices, not str
                        if len(self.problem_sets[set_id]) == 0:
                            pq.add_task(set_id, MAXPRIORITY)
                        else:
                            pq.add_task(
                                set_id,
                                float(self.weights[set_id])
                                / len(self.problem_sets[set_id]),
                            )
            self.problem_sets[set_idx].clear()
            pq.add_task(set_idx, MAXPRIORITY)

        return set_indices, total_weight
