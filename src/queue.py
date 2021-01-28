#!/usr/bin/python

import itertools
from heapq import heappush, heappop

max_priority = 999999


class SetQueue:
    def __init__(self):
        self._pq = []
        self._entry_map = {}
        self._counter = itertools.count()

    def add_task(self, task, priority=0.0):
        """Add a new task or update the priority of an existing task"""
        if task in self._entry_map:
            self.remove_task(task)
        count = next(self._counter)
        entry = [priority, count, task]
        self._entry_map[task] = entry
        heappush(self._pq, entry)

    def remove_task(self, task):
        """Mark an existing task as REMOVED."""
        entry = self._entry_map.pop(task)
        entry[-1] = "removed"

    def pop_task(self):
        """Remove and return the lowest priority task."""
        while self._pq:
            priority, count, task = heappop(self._pq)
            if task != "removed":
                del self._entry_map[task]
                return task

    def __len__(self):
        return len(self._entry_map)

    def __str__(self):
        return self._entry_map
