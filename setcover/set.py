#!/usr/bin/python

from collections import namedtuple

ExclusionSet = namedtuple(
    "ExclusionSet", ["set_id", "subset_include", "subset_exclude"]
)
