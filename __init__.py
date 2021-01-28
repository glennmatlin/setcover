import sys
__version__ == "0.0.1"

# This relies on each of the submodules having an __all__ variable.
from .src.cover import *
from .src.set import *

__all__ = (cover.__all__ + set.__all__)

if sys.platform == 'win32':  # pragma: no cover
    from .windows_events import *
    __all__ += windows_events.__all__
else:
    from .unix_events import *  # pragma: no cover
    __all__ += unix_events.__all__