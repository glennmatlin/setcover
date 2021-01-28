from weightedsetcover import WeightedSetCoverProblem, WeightedSet

# TODO Get test cases from existing dataset
# TODO sets should be ICD[Patient,Patient] since we want to find the overlapping ICD codes
# TODO create a named tuple object to contain the ICD/Patients/Weight


def test_init():
    sets = [WeightedSet(id='A0100', set=['Glenn'], set_weight=161.0),
     WeightedSet(id='A020', set=['Jeremy', 'Ben'], set_weight=782.0),
     WeightedSet(id='A028', set=['Jeremy'], set_weight=56.0),
     WeightedSet(id='A029', set=['Jeremy', 'Ben'], set_weight=310.0),
     WeightedSet(id='A030', set=['Justin', 'Vijay'], set_weight=36.0)]
    cover_problem = WeightedSetCoverProblem(sets)
    print(cover_problem.sets)
    assert cover_problem


def test_solver():
    sets = [
        [1, 2, 3],
        [3, 6, 7, 10],
        [8],
        [9, 5],
        [4, 5, 6, 7, 8],
        [4, 5, 9, 10],
    ]
    weights = [1.0, 2.0, 3.0, 4.0, 3.0, 5.0]
    selected, cost = WeightedSetCoverProblem.solver(sets, weights)
    assert (selected, cost) == ([0, 4, 1, 3], 10.0)
