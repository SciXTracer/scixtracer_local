"""Package implementing the local plugin of scixtracer index"""
from .local import SxIndexLocal

export = [SxIndexLocal]

__all__ = [
    "SxIndexLocal"
]
