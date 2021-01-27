from src.weightedsetcover import weightedsetcover

# TODO Get test cases from existing dataset

sets = [
    [1, 2, 3],
    [3, 6, 7, 10],
    [8],
    [9, 5],
    [4, 5, 6, 7, 8],
    [4, 5, 9, 10],
]
weights = [1, 2, 3, 4, 3, 5]


def test_weightedsetcover():
    selected, cost = weightedsetcover(sets, weights)
    assert (selected, cost) == ([0, 4, 1, 3], 10)