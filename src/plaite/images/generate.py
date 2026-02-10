"""
Image generation using Google Imagen 4 API.

Provides simple interface for generating recipe images from text prompts.
"""

import io
import os
from typing import Literal

from google import genai
from google.genai import types
from PIL import Image


class ImageGenerator:
    """Generate images using Google Imagen 4."""

    def __init__(
        self,
        api_key: str | None = None,
        default_model: str | None = None,
    ):
        """
        Initialize the image generator.

        Args:
            api_key: Google API key. If None, reads from GOOGLE_API_KEY env var.
            default_model: Default model to use. If None, reads from IMAGE_GENERATION_MODEL env var,
                          falls back to "imagen-4.0-generate-001".
                          Can be any Google Generative AI image model (e.g. imagen-3.0-generate-001,
                          imagen-4.0-generate-001, etc).
        """
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("Google API key required. Set GOOGLE_API_KEY or pass api_key.")

        self.default_model = default_model or os.getenv(
            "IMAGE_GENERATION_MODEL", "imagen-4.0-generate-001"
        )
        self.client = genai.Client(http_options={"api_version": "v1alpha"})

    def generate(
        self,
        prompt: str,
        *,
        model: str | None = None,
        num_images: int = 1,
        aspect_ratio: Literal["1:1", "3:4", "4:3", "9:16", "16:9"] = "1:1",
        image_size: Literal["1K", "2K"] = "1K",
        person_generation: Literal["dont_allow", "allow_adult", "allow_all"] = "dont_allow",
    ) -> list[Image.Image]:
        """
        Generate images from a text prompt.

        Args:
            prompt: Text description of the image to generate (English only, max 480 tokens)
            model: Google Generative AI image model to use. If None, uses the default from env.
                   Can be any available Google model (e.g. imagen-4.0-generate-001,
                   imagen-3.0-generate-001, etc).
            num_images: Number of images to generate (1-4)
            aspect_ratio: Aspect ratio for the generated images
            image_size: Resolution ("1K" = 1024x1024, "2K" = 2048x2048 for 1:1)
            person_generation: Whether to allow person generation in images

        Returns:
            List of PIL Image objects

        Raises:
            ValueError: If parameters are invalid
            RuntimeError: If API call fails
        """
        # Use default model if not specified
        if model is None:
            model = self.default_model

        # Validate parameters
        if not prompt or not prompt.strip():
            raise ValueError("Prompt cannot be empty")

        if not 1 <= num_images <= 4:
            raise ValueError("num_images must be between 1 and 4")

        # Generate images
        try:
            response = self.client.models.generate_images(
                model=model,
                prompt=prompt,
                config=types.GenerateImagesConfig(
                    number_of_images=num_images,
                    aspect_ratio=aspect_ratio,
                    image_size=image_size,
                    person_generation=person_generation,
                ),
            )

            # Extract PIL images from response
            images = []
            for generated_image in response.generated_images:
                image_bytes = generated_image.image.image_bytes
                images.append(Image.open(io.BytesIO(image_bytes)))
            return images

        except Exception as e:
            raise RuntimeError(f"Image generation failed: {e}") from e
