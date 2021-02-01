from src.set import WeightedSet, ExclusionSet
from .test_data import weighted_data

def test_weighted_set():

    weighted_sets = [WeightedSet(set_id=s[0], subset=s[1], weight=s[2]) for s in weighted_data]
    assert weighted_sets
    for weighted_set in weighted_sets:
    set_id, subset, weight = weighted_set
    assert set_id == "A10"
    assert subset == ["Glenn"]
    assert weight == 100.0


def test_exclusion_set():
    exclusion_set = ExclusionSet(
        set_id="A10",
        subset_include=["Glenn", "Vijay"],
        subset_exclude=["Justin", "Jean", "Jeremy K"],
    )
    assert exclusion_set
    set_id, subset_include, subset_exclude = exclusion_set
    assert set_id == "A10"
    assert subset_include == ["Glenn", "Vijay"]
    assert subset_exclude == ["Justin", "Jean", "Jeremy K"]


def test_spark_data_struct(sql_context):
    input = sql_context.createDataFrame(
        [
            ("X70", ["Glenn"], ["Tomo"]),
            ("Y80", ["Jeremy W"], ["Justin"]),
            ("Z90", ["Bennie"], ["Olga"])
        ],
        ["set_id", "subset_include", "subset_exclude"],
    )
    assert input
