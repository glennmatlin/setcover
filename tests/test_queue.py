from src.queue import SetQueue
from tests.test_cover import test_weighted_sets


def test_priority_queue():
    pq = SetQueue()
    for weighted_set in test_weighted_sets:
        subset_id, _, weight = weighted_set
        pq.add_task(subset_id, weight)
        assert pq

    for weighted_set in test_weighted_sets:
        subset_id, _, _ = weighted_set
        assert pq.pop_task() == subset_id
