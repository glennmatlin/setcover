from setcover.exclusion import ExclusionSetCoverProblem
from tests.test_sets import exclusion_sets
import logging

log = logging.getLogger(__name__)


# def test_set_calc():


class TestExclusion:
    def test_exclusion(self):
        exclusion_problem = ExclusionSetCoverProblem(exclusion_sets)
        assert exclusion_problem
        log.info("universe: {}".format(exclusion_problem.universe))
        log.info("cover_solution: {}".format(exclusion_problem.cover_solution))
        log.info("subsets_include: {}".format(exclusion_problem.subsets_include))
        log.info("subsets_exclude: {}".format(exclusion_problem.subsets_exclude))
