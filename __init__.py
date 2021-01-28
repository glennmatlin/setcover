__author__ = "Glenn Matlin"

# This relies on each of the submodules having an __all__ variable.
from .src.cover import *
from .src.set import *

__all__ = (cover.__all__ + set.__all__)

__version__ = '0.0.1'