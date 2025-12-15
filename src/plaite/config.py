"""Configuration loading and validation."""

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, field_validator


class FirebaseConfig(BaseModel):
    """Firebase configuration for a specific environment."""

    credentials_path: Path
    storage_bucket: str
    collection: str = "recipes"

    @field_validator("credentials_path", mode="before")
    @classmethod
    def expand_path(cls, v: str) -> Path:
        """Expand environment variables and ~ in path."""
        expanded = os.path.expandvars(os.path.expanduser(str(v)))
        return Path(expanded)


class ImageConfig(BaseModel):
    """Image processing configuration."""

    output_dir: Path = Path("./processed_images")
    overlay_suffix: str = "_overlayed"
    max_width: int = 1000
    max_height: int = 1600
    quality: int = 80
    formats: list[str] = [".jpg", ".jpeg", ".png"]


class UploadConfig(BaseModel):
    """Upload pipeline configuration."""

    batch_size: int = 50
    images: ImageConfig = ImageConfig()
    skip_existing: bool = True
    image_storage_path: str = "recipe_images/"


Env = Literal["prod", "dev"]


def load_firebase_config(config_path: Path, env: Env = "dev") -> FirebaseConfig:
    """Load Firebase config for the specified environment."""
    with open(config_path) as f:
        data = yaml.safe_load(f)

    if env not in data:
        raise ValueError(f"Environment '{env}' not found in config. Available: {list(data.keys())}")

    return FirebaseConfig(**data[env])


def load_upload_config(config_path: Path) -> UploadConfig:
    """Load upload pipeline configuration."""
    with open(config_path) as f:
        data = yaml.safe_load(f)

    return UploadConfig(**data)
