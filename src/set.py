from collections import namedtuple

__all__ = ['WeightedSet']

WeightedSet = namedtuple("WeightedSet", ["set_id", "subset", "weight"])
