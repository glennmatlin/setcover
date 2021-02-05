from src.set import WeightedSet, ExclusionSet
from .test_data import weighted_data, exclusion_data

weighted_sets = [
    WeightedSet(set_id=w[0], subset=w[1], weight=w[2]) for w in weighted_data
]
exclusion_sets = [
    ExclusionSet(set_id=w[0], subset_include=w[1], subset_exclude=w[2])
    for w in exclusion_data
]


def test_weighted_sets():
    assert weighted_sets
    for weighted_set in weighted_sets[:1]:
        set_id, subset, weight = weighted_set
        assert set_id == "A10"
        assert subset == ["Glenn", "Vijay"]
        assert weight == 100.0


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


def test_spark_data_struct(spark_fixture):
    df = spark_fixture.createDataFrame(
        [
            ("X70", ["Glenn"], ["Tomo"]),
            ("Y80", ["Jeremy W"], ["Justin"]),
            ("Z90", ["Bennie"], ["Olga"]),
        ],
        ["set_id", "subset_include", "subset_exclude"],
    )
    assert df
