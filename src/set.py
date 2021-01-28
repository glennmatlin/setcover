#!/usr/bin/python


__all__ = "WeightedSet"


from collections import namedtuple


WeightedSet = namedtuple("WeightedSet", ["set_id", "subset", "weight"])
