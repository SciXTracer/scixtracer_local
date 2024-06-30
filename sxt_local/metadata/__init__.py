"""Package implementing the local plugin of scixtracer storage"""
from .local import SxMetadataLocal

export = [SxMetadataLocal]

__all__ = [
    "SxMetadataLocal"
]
