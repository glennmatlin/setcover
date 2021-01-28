from src.set import WeightedSet
import pytest


def test_weighted_set():
    weighted_set = WeightedSet(set_id="A10", subset=["Glenn"], weight=100.0)
    assert weighted_set
    set_id, subset, weight = weighted_set
    assert set_id == "A10"
    assert subset == ["Glenn"]
    assert weight == 100.0
