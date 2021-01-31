from setcoverage.exclusion import ExclusionSetCoverProblem
from tests.test_data import exclusion_sets

class TestExclusion:
    @staticmethod
    def test_init():
        exclusion_problem = ExclusionSetCoverProblem(exclusion_sets)
        assert exclusion_problem
        assert exclusion_problem.universe == {
            "Glenn",
            "Jeremy W",
            "Ben",
            "Victor",
            "Vijay",
        }
        # assert exclusion_problem.include_covered
        # assert exclusion_problem.exclude_covered
        # print(exclusion_problem.include_covered)
        # print(exclusion_problem.exclude_covered)

    @staticmethod
    def test_make_data():
        assert False
