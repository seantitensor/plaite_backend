"""Recipe and image upload to Firebase."""
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

def upload_batch(
    recipes: list[dict[str, Any]],
    config: FirebaseConfig,
    upload_config: UploadConfig,
    dry_run: bool = False,
    console: Console | None = None,
) -> dict[str, Any]:
    """
    Upload a batch of recipes to Firestore.

    Args:
        batch_path: Path to JSON file containing recipes
        config: Firebase configuration
        upload_config: Upload configuration
        dry_run: If True, validate only without uploading
        console: Rich console for output

    Returns:
        Dict with upload results (success_count, failed, skipped)
    """
    if console is None:
        console = Console()

    results = {
        "total": len(recipes),
        "success": 0,
        "failed": [],
        "skipped": [],
    }

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]\n")

    # Get Firestore client and collection
    db = get_client(config)
    collection = get_collection(config)

    if dry_run:
        console.print("\n[green]Dry run complete. No uploads performed.[/green]")
        return results

    # Batch upload to Firestore
    console.print("\nUploading recipes to Firestore...")
    firestore_batch = db.batch()
    batch_count = 0

    for recipe in tqdm(recipes, desc="Uploading recipes"):
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
