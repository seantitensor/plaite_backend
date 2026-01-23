"""Shared types for upload pipelines."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class UploadResult:
    """Results from an upload operation."""

    total_selected: int = 0
    total_valid: int = 0
    uploaded: int = 0
    failed: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)


