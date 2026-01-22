"""Pipeline for selecting, processing, and uploading recipes."""

from .upload import upload_from_local

__all__ = ["upload_from_local"]
