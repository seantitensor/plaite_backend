"""Image downloading and processing."""

from pathlib import Path

import requests
from PIL import Image
from tqdm import tqdm

from plaite.config import ImageConfig


def download_image(url: str, output_path: Path, timeout: int = 10) -> bool:
    """
    Download an image from a URL.

    Args:
        url: Image URL
        output_path: Where to save the image
        timeout: Request timeout in seconds

    Returns:
        True if successful, False otherwise
    """
    try:
        response = requests.get(url, timeout=timeout, headers={"User-Agent": "Bot/1.0"})
        response.raise_for_status()

        with open(output_path, "wb") as f:
            f.write(response.content)
        return True

    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False


def sanitize_filename(title: str) -> str:
    """Convert a recipe title to a safe filename."""
    return "".join(c if c.isalnum() or c in " ._-" else "_" for c in title)


def resize_image(
    image: Image.Image,
    max_width: int,
    max_height: int,
) -> Image.Image:
    """Resize image while maintaining aspect ratio."""
    width, height = image.size

    # Calculate scaling factor
    width_ratio = max_width / width
    height_ratio = max_height / height
    ratio = min(width_ratio, height_ratio)

    if ratio < 1:
        new_size = (int(width * ratio), int(height * ratio))
        return image.resize(new_size, Image.Resampling.LANCZOS)

    return image


def add_overlay(
    image_path: Path,
    output_path: Path,
    config: ImageConfig,
) -> bool:
    """
    Process an image: resize and save with overlay suffix.

    For now this just resizes. Add actual overlay logic as needed.

    Args:
        image_path: Input image path
        output_path: Output image path
        config: Image processing configuration

    Returns:
        True if successful
    """
    try:
        with Image.open(image_path) as img:
            # Convert to RGB if necessary (for PNG with transparency)
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")

            # Resize
            img = resize_image(img, config.max_width, config.max_height)

            # Save with quality setting
            img.save(output_path, "JPEG", quality=config.quality)

        return True

    except Exception as e:
        print(f"Failed to process {image_path}: {e}")
        return False


def process_images(
    recipes: list[dict],
    output_dir: Path,
    config: ImageConfig,
    download: bool = True,
) -> dict[str, str]:
    """
    Process images for a batch of recipes.

    Args:
        recipes: List of recipe dicts with 'title' and 'image' (URL) fields
        output_dir: Directory to save processed images
        config: Image processing configuration
        download: Whether to download images from URLs

    Returns:
        Mapping of recipe title -> processed image filename
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Also create overlayed output dir
    overlayed_dir = output_dir.parent / f"{output_dir.name}{config.overlay_suffix}"
    overlayed_dir.mkdir(parents=True, exist_ok=True)

    image_mapping = {}

    for recipe in tqdm(recipes, desc="Processing images"):
        title = recipe.get("title")
        image_url = recipe.get("image")

        if not title or not image_url:
            continue

        # Create safe filename
        safe_name = sanitize_filename(title)
        raw_path = output_dir / f"{safe_name}.jpg"
        overlayed_path = overlayed_dir / f"{safe_name}{config.overlay_suffix}.jpg"

        # Download if needed
        if download and not raw_path.exists():
            if not download_image(image_url, raw_path):
                continue

        # Process (add overlay)
        if raw_path.exists():
            if add_overlay(raw_path, overlayed_path, config):
                image_mapping[title] = overlayed_path.name

    return image_mapping
