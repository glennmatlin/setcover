from setcover.set import ExclusionSet
from .test_data import exclusion_data

exclusion_sets = [
    ExclusionSet(set_id=w[0], subset_include=w[1], subset_exclude=w[2])
    for w in exclusion_data
]


def test_exclusion_sets():
    assert exclusion_sets
    for exclusion_set in exclusion_sets[:1]:
        set_id, subset_include, subset_exclude = exclusion_set
        assert set_id == "A10"
        assert subset_include == ["Glenn", "Vijay"]
        assert subset_exclude == [
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