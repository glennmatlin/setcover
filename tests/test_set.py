from setcoverage.set import WeightedSet, ExclusionSet


def test_weighted_set():
    weighted_set = WeightedSet(set_id="A10", subset=["Glenn"], weight=100.0)
    assert weighted_set
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
