from src.queue import SetQueue


def test_priority_queue():
    pq = SetQueue()
    pq.add_task("A10", 100.0)
    pq.add_task("B20", 200.0)
    pq.add_task("C30", 300.0)
    pq.add_task("D40", 400.0)
    pq.add_task("E50", 500.0)
    assert pq
    assert pq.pop_task() == "A10"
    assert pq.pop_task() == "B20"
    assert pq.pop_task() == "C30"
    assert pq.pop_task() == "D40"
    assert pq.pop_task() == "E50"
