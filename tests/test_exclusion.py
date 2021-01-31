from setcoverage.exclusion import ExclusionSetCoverProblem

from tests.test_data import exclusion_sets
import logging

log = logging.getLogger(__name__)


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
        log.info(exclusion_problem.subsets_include)
        log.info(exclusion_problem.subsets_exclude)
