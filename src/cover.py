#!/usr/bin/python
from __future__ import annotations
from src.queue import SetQueue
import pandas as pd
from collections import OrderedDict
from src.set import WeightedSet

MAXPRIORITY = 999999

# TODO rename 'subsets' to 'set_cover'
# TODO Decompose `problem` variable


class WeightedSetCoverProblem:
    # TODO Function argument for limiting sets selected
    # TODO Implement a maximization constraint for coverage
    # TODO Finalize typing of inputs/outputs

    def __init__(self, weighted_sets: list[tuple]):
        self.weighted_sets = weighted_sets
        self.set_problem, self.subsets, self.weights = self.make_data(self)
        self.universe = set(self.set_problem.keys())
        self.set_queue = self.prioritize(self)
        self.cover_solution, self.total_weight = None, None

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
    def prioritize(self):
        # TODO Unit test, better docstring
        set_queue = SetQueue()
        for weighted_set in self.weighted_sets:  # add all sets to the priority queue
            subset_id, subset, weight = weighted_set
            if len(subset) == 0:
                set_queue.add_task(subset_id, MAXPRIORITY)
            else:
                set_queue.add_task(subset_id, float(weight) / len(subset))
        return set_queue

    @staticmethod
    def greedy_solver(self) -> (list[str], int):
        """
        Greedy algorithm implementation for a proximal solution for Weighted set Coverage
        pick the set which is the most cost-effective: min(w[s]/|s-C|),
        where C is the current covered elements set.

        The complexity of the algorithm: O(|U| * log|S|) .
        Finding the most cost-effective set is done by a priority queue.
        The operation has time complexity of O(log|S|).


        NEEDED:
        - set_problem: List[PatientIDs]
        - subsets: Dict{ICDCode:[PatientIDs]}
        - weights: Dict{ICDCode:Weight}
        """
        # TODO Unit test, better docstring

        # elements don't cover problem -> invalid input for set cover

        elements = set(e for s in self.subsets.keys() for e in self.subsets[s])
        if elements != self.universe:  # elements don't cover universe -> invalid input for set cover
            return None

        # track elements of problem covered
        covered = set()
        cover_solution = []
        total_weight = 0

        while covered != self.universe:
            break
            # # (1)
            # set_id = (
            #     self.set_queue.pop_task()
            # )  # get label for the most cost-effective set
            #
            # # (2)
            # cover_solution.append(set_id)  # record the set itself we picked
            # picked_set = self.problem["id_key"][set_id]["set"]
            # picked_weight = self.problem["id_key"][set_id]["weight"]
            # total_weight += picked_weight  # add weight of set to solution total
            # covered.add(picked_set)  # add set elements to coverage track
            # # TODO `covered`
            # # understand `covered` var more -- this seems wrong to me bc
            # # its adding length but not checking the length that was actually added
            # # i thought this would not count dupe elements
            #
            # # Update the sets that contains the new covered elements
            # for set_element in picked_set:  # for ele in list[str]
            #     for set_id in self.problem[
            #         set_element
            #     ]:  # for id in set[str] <-from- dict[str:set][ele]
            #         if (
            #             set_id != set_id
            #         ):  # TODO Error is occuring here ... set_id is the code and set_idx is a int
            #             self.subsets[set_id].discard(set_element)
            #             # TODO FIX ^^ TypeError: list indices must be integers or slices, not str
            #             if len(self.subsets[set_id]) == 0:
            #                 self.set_queue.add_task(set_id, MAXPRIORITY)
            #             else:
            #                 self.set_queue.add_task(
            #                     set_id,
            #                     float(self.weights[set_id]) / len(self.subsets[set_id]),
            #                 )
            # self.subsets[set_id].clear()
            # self.set_queue.add_task(set_id, MAXPRIORITY)

        # self.cover_solution, self.total_weight = cover_solution, total_weight
