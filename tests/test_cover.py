from setcoverage.cover import WeightedSetCoverProblem
from setcoverage.set import WeightedSet, ExclusionSet

# TODO Move mock test data to another file
# ["Glenn", "Jeremy W", "Ben", "Victor", "Vijay"]
# ["Prestinario", "Kamalesh", "Young", "Alex", "Andrea", "Andrew", "Andrey", "Youzhi",
# "Doug", "Daniel", "Eric","Earnest", "Ethan", "Haroon", 'Justin', "Jean", "Jeremy"]

weighted_sets = [
    WeightedSet(set_id="A10", subset=["Glenn"], weight=100.0),
    WeightedSet(set_id="B20", subset=["Jeremy W", "Ben"], weight=200.0),
    WeightedSet(set_id="C30", subset=["Jeremy W"], weight=300.0),
    WeightedSet(set_id="D40", subset=["Jeremy W", "Glenn", "Ben"], weight=400.0),
    WeightedSet(set_id="E50", subset=["Victor", "Vijay"], weight=500.0),
]

exclusion_sets = [
    ExclusionSet(set_id="A10", subset_include=["Glenn"], subset_exclude=['Jean', 'Daniel', 'Youzhi', 'Prestinario', 'Young', 'Haroon', 'Andrea', 'Eric', 'Kamalesh', 'Jeremy K', 'Ethan']),
    ExclusionSet(set_id="B20", subset_include=["Jeremy W", "Ben"], subset_exclude=['Earnest', 'Daniel', 'Andrew', 'Alex', 'Prestinario', 'Justin', 'Kamalesh', 'Jean', 'Haroon', 'Andrea', 'Doug', 'Andrey', 'Young', 'Jeremy K', 'Youzhi']),
    ExclusionSet(set_id="C30", subset_include=["Jeremy W"], subset_exclude=['Andrey', 'Haroon', 'Eric', 'Jeremy K', 'Andrea', 'Earnest', 'Alex', 'Doug', 'Young', 'Kamalesh', 'Andrew', 'Prestinario', 'Youzhi']),
    ExclusionSet(set_id="D40", subset_include=["Jeremy W", "Glenn", "Ben"], subset_exclude=['Andrea', 'Andrey', 'Kamalesh', 'Andrew', 'Alex', 'Youzhi', 'Justin', 'Earnest', 'Prestinario', 'Young', 'Haroon', 'Daniel', 'Jean', 'Eric', 'Doug', 'Ethan']),
    ExclusionSet(set_id="E50", subset_include=["Victor", "Vijay"], subset_exclude=['Jeremy K', 'Kamalesh', 'Andrew', 'Prestinario', 'Daniel', 'Ethan', 'Jean', 'Haroon', 'Eric', 'Justin', 'Doug', 'Young', 'Alex']),
]


def test_weighted():
    # TODO make this a class level test and break into functions
    cover_problem = WeightedSetCoverProblem(weighted_sets)
    assert cover_problem
    assert cover_problem.set_problem == {
        "Glenn": {"A10"},
        "Jeremy W": {"B20", "D40", "C30"},
        "Ben": {"B20", "D40"},
        "Victor": {"E50"},
        "Vijay": {"E50"},
    }
    assert set(cover_problem.set_problem.keys()) == {
        "Glenn",
        "Jeremy",
        "Ben",
        "Victor",
        "Vijay",
    }
    assert cover_problem.subsets == {
        "A10": {"Glenn"},
        "B20": {"Jeremy", "Ben"},
        "C30": {"Jeremy"},
        "D40": {"Jeremy", "Ben"},
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
    print(cover_problem.universe)
    print(cover_problem.cover_solution)
    print(cover_problem.covered)
    print(cover_problem.weight_total)


def test_exclusion():
    assert exclusion_sets