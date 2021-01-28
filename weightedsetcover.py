#!/usr/bin/python


from collections import namedtuple
from typing import List, Set, Iterable, Dict
from priorityqueue import PriorityQueue
import pandas as pd

MAXPRIORITY = 999999

# TODO Function argument for limiting sets selected
# TODO Implement a maximization constraint for coverage
# TODO Decide how inputs and outputs will be typed

# df.to_dict()
# {'code': {0: 'A0100', 1: 'A020', 2: 'A028', 3: 'A029', 4: 'A030'},
#  'patient_ids': {0: ['ImwKm7mel9wAhH9HV3HYny1nJD6vvzGLBOy/wctFNkA='],
#   1: ['HYEm8QH+Kf6EeAw0GKUyfsMd26EADF64+P3wM+Nw4AQ=',
#    'GFCZeovKt3sH95oZDsHqtoEbk+1mpVSJbRdSmJXNhuo='],
#   2: ['HYEm8QH+Kf6EeAw0GKUyfsMd26EADF64+P3wM+Nw4AQ='],
#   3: ['HYEm8QH+Kf6EeAw0GKUyfsMd26EADF64+P3wM+Nw4AQ=',
#    'GFCZeovKt3sH95oZDsHqtoEbk+1mpVSJbRdSmJXNhuo='],
#   4: ['iG9yboiqP8gjbSsq/XYW6hEvZnzuUqGjlI/XjEuiqkw=',
#    'wTZqXY2//Ja1Zj/lZTkZS+2ReS37zB/0z54t//w/2FY=']},
#  'patient_count': {0: 161.0, 1: 782.0, 2: 56.0, 3: 310.0, 4: 36.0}}


WeightedSet = namedtuple("WeightedSet", "id set set_weight")


class WeightedSetCoverProblem:
    def __init__(self, sets: List[WeightedSet]):
        self.sets = sets
        self.universe = None
        self.cover_solution = None
        self.total_weight = None

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> List[WeightedSet]:
        """
        Convert pandas DataFrame to into a list of named tuples
        """
        # TODO Fn unit test
        rows = list(df.itertuples(name="Row", index=False))
        weighted_sets = [WeightedSet(r[0], r[1], r[2]) for r in rows]
        return weighted_sets

    def make_universe(self):
        universe = {}
        selection = list()
        set_problem = []
        for index, item in enumerate(sets):
            set_problem.append(set(item))
            for j in item:
                if j not in universe:
                    universe[j] = set()
                universe[j].add(index)

    @staticmethod
    def solver(sets: List[List[int]], weights: List[float]) -> (Iterable[str], int):
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

        universe = {}
        selection = list()
        set_problem = []
        for index, item in enumerate(sets):
            set_problem.append(set(item))
            for j in item:
                if j not in universe:
                    universe[j] = set()
                universe[j].add(index)

        pq = PriorityQueue()
        weight = 0
        covered = 0
        for index, item in enumerate(set_problem):  # add all sets to the priorityqueue
            if len(item) == 0:
                pq.add_task(index, MAXPRIORITY)
            else:
                pq.add_task(index, float(weights[index]) / len(item))
        while covered < len(universe):
            a = pq.pop_task()  # get the most cost-effective set
            selection.append(a)  # a: set id
            weight += weights[a]
            covered += len(set_problem[a])
            # Update the sets that contains the new covered elements
            for m in set_problem[a]:  # m: element
                for n in universe[m]:  # n: set id
                    if n != a:
                        set_problem[n].discard(m)
                        if len(set_problem[n]) == 0:
                            pq.add_task(n, MAXPRIORITY)
                        else:
                            pq.add_task(n, float(weights[n]) / len(set_problem[n]))
            set_problem[a].clear()
            pq.add_task(a, MAXPRIORITY)

        return selection, weight
