from setcoverage.weighted import WeightedSetCoverProblem
from tests.test_data import weighted_sets


class TestWeighted:
    def test_weighted():
        cover_problem = WeightedSetCoverProblem(weighted_sets)
        assert cover_problem
        assert cover_problem.set_problem == {
            "Glenn": {"A10", "D40"},
            "Jeremy W": {"B20", "D40", "C30"},
            "Ben": {"B20", "D40"},
            "Victor": {"E50"},
            "Vijay": {"E50"},
        }
        assert set(cover_problem.set_problem.keys()) == {
            "Glenn",
            "Jeremy W",
            "Ben",
            "Victor",
            "Vijay",
        }
        assert cover_problem.subsets == {
            "A10": {"Glenn"},
            "B20": {"Jeremy W", "Ben"},
            "C30": {"Jeremy W"},
            "D40": {"Jeremy W", "Ben", "Glenn"},
            "E50": {"Victor", "Vijay"},
        }
        assert cover_problem.weights == {
            "A10": 100,
            "B20": 200,
            "C30": 300,
            "D40": 400,
            "E50": 500,
        }
        assert cover_problem.cover_solution

