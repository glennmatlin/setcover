from cover import WeightedSetCoverProblem
from set import WeightedSet
# import pytest

# TODO Get test cases from existing dataset
# TODO sets should be ICD[Patient,Patient] since we want to find the overlapping ICD codes
# TODO create a named tuple object to contain the ICD/Patients/Weight
test_weighted_sets = [
    WeightedSet(id="A10", set=["Glenn"], weight=100.0),
    WeightedSet(id="B20", set=["Jeremy", "Ben"], weight=800.0),
    WeightedSet(id="C30", set=["Jeremy"], weight=200.0),
    WeightedSet(id="D40", set=["Jeremy", "Ben"], weight=300.0),
    WeightedSet(id="E50", set=["Justin", "Vijay"], weight=600.0),
]


def test_init():
    cover_problem = WeightedSetCoverProblem(test_weighted_sets)
    assert cover_problem
    assert cover_problem.universe == {
        "Glenn": {"A10"},
        "Jeremy": {"B20", "D40", "C30"},
        "Ben": {"B20", "D40"},
        "Justin": {"E50"},
        "Vijay": {"E50"},
    }


def test_prioritize():
    cover_problem = WeightedSetCoverProblem(test_weighted_sets)
    assert cover_problem
    assert cover_problem.universe == {
        "Glenn": {"A10"},
        "Jeremy": {"B20", "D40", "C30"},
        "Ben": {"B20", "D40"},
        "Justin": {"E50"},
        "Vijay": {"E50"},
    }

# @pytest.mark.skip
# def test_solver():
#     ids = [1, 2, 3, 4, 5]
#     sets = [[1, 2, 3], [2, 3, 4], [4, 5], [1, 5], [2, 4]]
#     weights = [10.0, 20.0, 30.0, 40.0, 50.0]
#     cover_problem = WeightedSetCoverProblem.from_lists(ids, sets, weights)
#     selected, cost = cover_problem.solver
#     # assert (selected, cost) == ([0, 4, 1, 3], 10.0)
