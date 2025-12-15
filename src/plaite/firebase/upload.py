"""Recipe and image upload to Firebase."""

import json
import re
from pathlib import Path
from typing import Any

from rich.console import Console
from tqdm import tqdm

from plaite.config import FirebaseConfig, UploadConfig
from plaite.firebase.client import get_client, get_collection, get_storage_bucket


def upload_image(
    image_path: Path,
    recipe_id: str,
    config: FirebaseConfig,
    upload_config: UploadConfig,
) -> str | None:
    """
    Upload an image to Firebase Storage.

    Args:
        image_path: Local path to the image file
        recipe_id: Recipe ID to use as the blob name
        config: Firebase configuration
        upload_config: Upload configuration

    Returns:
        Public URL of the uploaded image, or None if upload fails
    """
    if not image_path.exists():
        return None

    bucket = get_storage_bucket(config)
    blob_path = f"{upload_config.image_storage_path}{recipe_id}"
    blob = bucket.blob(blob_path)

    # Determine content type
    suffix = image_path.suffix.lower()
    content_type = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
    }.get(suffix, "image/jpeg")

    try:
        blob.upload_from_filename(str(image_path), content_type=content_type)
        blob.make_public()
        return blob.public_url
    except Exception as e:
        print(f"Error uploading image {image_path}: {e}")
        return None


def load_batch(batch_path: Path) -> list[dict[str, Any]]:
    """Load a batch of recipes from a JSON file."""
    with open(batch_path, encoding="utf-8") as f:
        return json.load(f)


def save_batch(batch: list[dict[str, Any]], output_path: Path):
    """Save a batch of recipes to a JSON file."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(batch, f, ensure_ascii=False, indent=2)


def process_recipe_fields(recipe: dict[str, Any]) -> dict[str, Any]:
    """
    Process and normalize recipe fields before upload.

    Handles:
    - Renaming 'ingredients' to 'ingredientStrings' if needed
    - Renaming 'procesedIngredients' to 'ingredients'
    - Converting nutrients dict to array format
    - Normalizing numServings to float
    """
    # Field renames
    if "ingredients" in recipe and "ingredientStrings" not in recipe:
        if isinstance(recipe["ingredients"], list) and recipe["ingredients"]:
            first_item = recipe["ingredients"][0]
            if isinstance(first_item, str):
                recipe["ingredientStrings"] = recipe.pop("ingredients")

    if "procesedIngredients" in recipe:
        recipe["ingredients"] = recipe.pop("procesedIngredients")

    # Nutrients normalization
    if "nutrients" in recipe:
        nutrients = recipe["nutrients"]
        if isinstance(nutrients, dict):
            nutrients_array = [
                {"name": name, "quantity": str(quantity)} for name, quantity in nutrients.items()
            ]
            recipe["nutrients"] = nutrients_array
        elif not isinstance(nutrients, list):
            recipe["nutrients"] = []

    # numServings normalization
    if "numServings" in recipe:
        servings = recipe["numServings"]
        if isinstance(servings, str):
            numbers = re.findall(r"\d+\.?\d*", servings)
            recipe["numServings"] = float(numbers[0]) if numbers else None
        elif not isinstance(servings, (int, float)):
            recipe["numServings"] = None

    return recipe


def validate_recipe(recipe: dict[str, Any]) -> tuple[bool, str | None]:
    """
    Validate a recipe has required fields.

    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ["id", "title", "tags", "instructions", "ingredients"]

    for field in required_fields:
        if field not in recipe:
            return False, f"Missing required field: {field}"

        value = recipe[field]
        if field in ["tags", "instructions", "ingredients"]:
            if not isinstance(value, list):
                return False, f"{field} must be a list"

    return True, None


def upload_batch(
    batch_path: Path,
    config: FirebaseConfig,
    upload_config: UploadConfig,
    images_dir: Path | None = None,
    image_mapping: dict[str, str] | None = None,
    dry_run: bool = False,
    console: Console | None = None,
) -> dict[str, Any]:
    """
    Upload a batch of recipes to Firestore.

    Args:
        batch_path: Path to JSON file containing recipes
        config: Firebase configuration
        upload_config: Upload configuration
        images_dir: Optional directory containing images
        image_mapping: Optional mapping of recipe title -> image filename
        dry_run: If True, validate only without uploading
        console: Rich console for output

    Returns:
        Dict with upload results (success_count, failed, skipped)
    """
    if console is None:
        console = Console()

    batch = load_batch(batch_path)
    console.print(f"Loaded {len(batch)} recipes from {batch_path}")

    results = {
        "total": len(batch),
        "success": 0,
        "failed": [],
        "skipped": [],
    }

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]\n")

    # Get Firestore client and collection
    db = get_client(config)
    collection = get_collection(config)

    # Process and validate recipes
    valid_recipes = []
    for recipe in batch:
        recipe = process_recipe_fields(recipe)
        is_valid, error = validate_recipe(recipe)

        if not is_valid:
            results["failed"].append({"id": recipe.get("id"), "error": error})
            continue

        # Set channel
        recipe["channel"] = recipe.get("channel", "discover")

        valid_recipes.append(recipe)

    console.print(f"Valid recipes: {len(valid_recipes)}, Invalid: {len(results['failed'])}")

    if dry_run:
        console.print("\n[green]Dry run complete. No uploads performed.[/green]")
        return results

    # Upload images if provided
    if images_dir and image_mapping:
        console.print(f"\nUploading images from {images_dir}...")
        for recipe in tqdm(valid_recipes, desc="Uploading images"):
            title = recipe.get("title")
            image_filename = image_mapping.get(title)

            if image_filename:
                image_path = images_dir / image_filename
                if image_path.exists():
                    url = upload_image(image_path, recipe["id"], config, upload_config)
                    if url:
                        recipe["image"] = url
                    else:
                        results["skipped"].append(
                            {"id": recipe["id"], "reason": "Image upload failed"}
                        )

    # Batch upload to Firestore
    console.print("\nUploading recipes to Firestore...")
    firestore_batch = db.batch()
    batch_count = 0

    for recipe in tqdm(valid_recipes, desc="Uploading recipes"):
        try:
            doc_ref = collection.document(recipe["id"])
            firestore_batch.set(doc_ref, recipe)
            batch_count += 1

            # Commit every batch_size documents
            if batch_count >= upload_config.batch_size:
                firestore_batch.commit()
                results["success"] += batch_count
                console.print(f"Committed batch of {batch_count}. Total: {results['success']}")
                firestore_batch = db.batch()
                batch_count = 0

        except Exception as e:
            results["failed"].append({"id": recipe.get("id"), "error": str(e)})

    # Commit remaining
    if batch_count > 0:
        firestore_batch.commit()
        results["success"] += batch_count
        console.print(f"Committed final batch of {batch_count}")

    console.print("\n[green]Upload complete![/green]")
    console.print(f"  Success: {results['success']}")
    console.print(f"  Failed: {len(results['failed'])}")
    console.print(f"  Skipped: {len(results['skipped'])}")

    return results
