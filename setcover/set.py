#!/usr/bin/python

from collections import namedtuple

WeightedSet = namedtuple("WeightedSet", ["set_id", "subset", "weight"])

ExclusionSet = namedtuple(
    "ExclusionSet", ["set_id", "subset_include", "subset_exclude"]
)
