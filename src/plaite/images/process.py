"""Image downloading and processing."""

import io
from pathlib import Path

import requests
from PIL import Image


def download_image(url: str, timeout: int = 10) -> Image:
    """
    Download an image from a URL.

    Args:
        url: Image URL
        timeout: Request timeout in seconds

    Returns:
        PIL Image
    """
    try:
        response = requests.get(url, timeout=timeout, headers={"User-Agent": "Bot/1.0"})
        response.raise_for_status()
        return Image.open(io.BytesIO(response.content))

    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False


def add_overlay(img: Image) -> Image:
    """
    Add Plaite overlay to an image.

    Args:
        img: Input Image

    Returns:
        img: Output Image with overlay
    """
    try:
        # Convert to RGB if necessary (for PNG with transparency)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        overlay = Image.open(Path(__file__).parent / "plaite_overlay.png").convert("RGBA")
        if img.size != overlay.size:
            overlay_resized = overlay.resize(img.size)
        else:
            overlay_resized = overlay

        img.paste(overlay_resized, (0, 0), overlay_resized)
        return img

    except Exception as e:
        print(f"Failed to process image: {e}")
        return False
