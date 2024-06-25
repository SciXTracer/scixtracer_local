"""Package implementing the local plugin of scixtracer runner"""
from .local import SxRunnerLocal

export = [SxRunnerLocal]

__all__ = [
    "SxRunnerLocal"
]
