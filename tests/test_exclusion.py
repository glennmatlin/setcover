from setcover.exclusion import ExclusionSetCoverProblem
from tests.test_sets import exclusion_sets
import logging

log = logging.getLogger(__name__)


class TestExclusion:
    def test_exclusion(self):
        exclusion_problem = ExclusionSetCoverProblem(exclusion_sets)
        log.debug(f"universe: {exclusion_problem.universe}")
        log.debug(f"subsets_include: {exclusion_problem.subsets_include}")
        log.debug(f"subsets_exclude: {exclusion_problem.subsets_exclude}")
        exclusion_problem.solve()
        log.debug(f"cover_solution: {exclusion_problem.cover_solution}")
