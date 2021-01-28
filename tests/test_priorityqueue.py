from queue import PriorityQueue


def test_priorityqueue():
    pq = PriorityQueue()
    pq.add_task(1, 3)
    pq.add_task(2, 4)
    pq.add_task(3, 5)
    pq.add_task(2, 1)
    pq.add_task(3, 0.5)
    while pq:
        print(pq.pop_task())
