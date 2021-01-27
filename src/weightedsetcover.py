#!/usr/bin/python

# Copyright (C) 2020 Glenn Matlin, Zhiyang Su
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

from typing import List, Set, Iterable
from .priorityqueue import PriorityQueue

MAXPRIORITY = 999999

# TODO Function argument for limiting sets selected
# TODO Implement a maximization constraint for coverage
# TODO Decide how inputs and outputs will be typed

def weightedsetcover(sets: Iterable[Iterable[str]], weights: Iterable[int]) -> (Iterable[str], int):
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