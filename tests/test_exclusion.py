from setcover.exclusion import ExclusionSetCoverProblem
from tests.test_sets import exclusion_sets
from tests.test_data import exclusion_df
import logging

logging.getLogger("py4j").setLevel(logging.ERROR)
log = logging.getLogger(__name__)


class TestExclusion:
    def test_exclusion(self):
        exclusion_problem = ExclusionSetCoverProblem(exclusion_sets)
        exclusion_problem.solve()
        assert exclusion_problem.cover_solution == [("E50", 4.3333), ("B20", 2.0)]

    def test_exclusion_from_df(self):
        exclusion_problem = ExclusionSetCoverProblem()
        exclusion_problem.from_dataframe(exclusion_df)
        exclusion_problem.solve()
        assert exclusion_problem.cover_solution == [("E50", 4.3333), ("B20", 2.0)]


# ({'Jeremy W', 'Ben', 'Glenn', 'Vijay', 'Victor'},
# OrderedDict([('A10', {'Glenn', 'Vijay'}), ('B20', {'Jeremy W', 'Ben'}), ('C30', {'Jeremy W', 'Victor'}), ('D40', {'Glenn', 'Ben'}), ('E50', {'Glenn', 'Vijay', 'Victor'})]),
# OrderedDict([('A10', {'Young', 'Jeremy K', 'Daniel', 'Kamalesh', 'Prestinario', 'Andrea', 'Ethan', 'Youzhi', 'Jean', 'Eric', 'Haroon'}), ('B20', {'Young', 'Jeremy K', 'Alex', 'Daniel', 'Justin', 'Prestinario', 'Kamalesh', 'Andrea', 'Youzhi', 'Doug', 'Jean', 'Earnest', 'Andrew', 'Andrey', 'Haroon'}), ('C30', {'Young', 'Jeremy K', 'Alex', 'Kamalesh', 'Prestinario', 'Andrea', 'Youzhi', 'Doug', 'Earnest', 'Andrew', 'Eric', 'Andrey', 'Haroon'}), ('D40', {'Young', 'Alex', 'Daniel', 'Kamalesh', 'Justin', 'Andrea', 'Prestinario', 'Youzhi', 'Doug', 'Ethan', 'Jean', 'Earnest', 'Andrew', 'Eric', 'Andrey', 'Haroon'}), ('E50', {'Young', 'Jeremy K', 'Alex', 'Daniel', 'Kamalesh', 'Prestinario', 'Justin', 'Ethan', 'Doug', 'Jean', 'Andrew', 'Eric', 'Haroon'})]))
