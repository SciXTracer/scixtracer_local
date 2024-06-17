"""Package implementing the local plugin of scixtracer storage"""
from .local import SxStorageLocal

export = [SxStorageLocal]

__all__ = [
    "SxStorageLocal"
]
