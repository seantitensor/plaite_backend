"""Firebase client and operations."""

from plaite.firebase.client import get_client, get_collection, get_storage_bucket
from plaite.firebase.stats import get_stats
from plaite.firebase.upload import upload_batch

__all__ = ["get_client", "get_collection", "get_storage_bucket", "get_stats", "upload_batch"]
