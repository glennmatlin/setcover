from src.cover import WeightedSetCoverProblem
from src.set import WeightedSet
import pytest

# TODO Get test cases from existing dataset
# TODO sets should be ICD[Patient,Patient] since we want to find the overlapping ICD codes
# TODO create a named tuple object to contain the ICD/Patients/Weight
test_weighted_sets = [
    WeightedSet(id="A10", set=["Glenn"], weight=100.0),
    WeightedSet(id="B20", set=["Jeremy", "Ben"], weight=200.0),
    WeightedSet(id="C30", set=["Jeremy"], weight=300.0),
    WeightedSet(id="D40", set=["Jeremy", "Ben"], weight=400.0),
    WeightedSet(id="E50", set=["Justin", "Vijay"], weight=500.0),
]


def test_universe():
    cover_problem = WeightedSetCoverProblem(test_weighted_sets)
    assert cover_problem
    assert cover_problem.problem['ele_key'] == {
        "Glenn": {"A10"},
        "Jeremy": {"B20", "D40", "C30"},
        "Ben": {"B20", "D40"},
        "Justin": {"E50"},
        "Vijay": {"E50"},
    }
    assert set(cover_problem.problem['ele_key'].keys()) == {
        "Glenn",
        "Jeremy",
        "Ben",
        "Justin",
        "Vijay"
    }
    assert cover_problem.problem['id_key'] == {
        "A10": {'set':{"Glenn"}, 'weight': 100},
        "B20": {'set':{"Jeremy", "Ben"}, 'weight': 200},
        "C30": {'set':{"Jeremy"}, 'weight': 300},
        "D40": {'set':{"Jeremy", "Ben"}, 'weight': 400},
        "E50": {'set':{"Justin", "Vijay"}, 'weight': 500}
    }


def test_prioritize():
    cover_problem = WeightedSetCoverProblem(test_weighted_sets)
    assert cover_problem
    q = cover_problem.set_queue
    assert q.pop_task() == "A10"
    assert q.pop_task() == "B20"
    assert q.pop_task() == "D40"
    assert q.pop_task() == "E50"
    assert q.pop_task() == "C30"


# @pytest.mark.skip
def test_solver():
    cover_problem = WeightedSetCoverProblem(test_weighted_sets)
    assert cover_problem
    # assert (selected, cost) == ([0, 4, 1, 3], 10.0)
