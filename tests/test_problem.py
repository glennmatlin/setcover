import logging

from setcover.problem import SetCoverProblem, Subset
from tests.test_data import data, df

logging.getLogger("py4j").setLevel(logging.ERROR)
log = logging.getLogger(__name__)

subsets = [
    Subset(set_id=w[0], include_elements=w[1], exclude_elements=w[2]) for w in data
]


class TestProblem:
    def test_subsets(self):
        assert subsets
        for subset in subsets[:1]:
            set_id, include_elements, exclude_elements = subset
            assert set_id == "A10"
            assert include_elements == ["Glenn", "Vijay"]
            assert exclude_elements == [
                "Jean",
                "Daniel",
                "Youzhi",
                "Prestinario",
                "Young",
                "Haroon",
                "Andrea",
                "Eric",
                "Kamalesh",
                "Jeremy K",
                "Ethan",
            ]

    def test_problem(self):
        problem = SetCoverProblem(subsets)
        problem.solve()
        assert problem.cover_solution == [
            ("E50", 4.33333, 3, 13),
            ("B20", 2.0, 2, 4),
        ]

    def test_problem_from_df(self):
        problem = SetCoverProblem()
        problem.from_dataframe(df)
        problem.solve()
        assert problem.cover_solution == [
            ("E50", 4.33333, 3, 13),
            ("B20", 2.0, 2, 4),
        ]


# ({'Jeremy W', 'Ben', 'Glenn', 'Vijay', 'Victor'},
# OrderedDict([('A10', {'Glenn', 'Vijay'}), ('B20', {'Jeremy W', 'Ben'}), ('C30', {'Jeremy W', 'Victor'}), ('D40', {'Glenn', 'Ben'}), ('E50', {'Glenn', 'Vijay', 'Victor'})]),
# OrderedDict([('A10', {'Young', 'Jeremy K', 'Daniel', 'Kamalesh', 'Prestinario', 'Andrea', 'Ethan', 'Youzhi', 'Jean', 'Eric', 'Haroon'}), ('B20', {'Young', 'Jeremy K', 'Alex', 'Daniel', 'Justin', 'Prestinario', 'Kamalesh', 'Andrea', 'Youzhi', 'Doug', 'Jean', 'Earnest', 'Andrew', 'Andrey', 'Haroon'}), ('C30', {'Young', 'Jeremy K', 'Alex', 'Kamalesh', 'Prestinario', 'Andrea', 'Youzhi', 'Doug', 'Earnest', 'Andrew', 'Eric', 'Andrey', 'Haroon'}), ('D40', {'Young', 'Alex', 'Daniel', 'Kamalesh', 'Justin', 'Andrea', 'Prestinario', 'Youzhi', 'Doug', 'Ethan', 'Jean', 'Earnest', 'Andrew', 'Eric', 'Andrey', 'Haroon'}), ('E50', {'Young', 'Jeremy K', 'Alex', 'Daniel', 'Kamalesh', 'Prestinario', 'Justin', 'Ethan', 'Doug', 'Jean', 'Andrew', 'Eric', 'Haroon'})]))
