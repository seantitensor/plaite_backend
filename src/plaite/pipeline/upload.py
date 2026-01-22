"""Pipeline for uploading recipes from local data to Firebase."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl
from rich.console import Console
from rich.table import Table
from tqdm import tqdm

from plaite.config import FirebaseConfig, UploadConfig
from plaite.data import get_recipes
from plaite.data.query import Filter
from plaite.firebase.client import get_client, get_collection, get_uploaded_recipe_ids
from plaite.pipeline.validation import validate_and_transform_recipe


@dataclass
class UploadResult:
    """Results from an upload operation."""

    total_selected: int = 0
    total_valid: int = 0
    uploaded: int = 0
    failed: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)


def df_to_recipes(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert a Polars DataFrame to a list of recipe dicts."""
    return df.to_dicts()


def select_recipes(
    count: int,
    filters: list[Filter] | None = None,
    random_sample: bool = True,
) -> pl.DataFrame:
    """
    Select recipes from local data with optional filtering.

    Args:
        count: Number of recipes to select
        filters: List of Filter objects from Col query builder
        random_sample: If True, randomly sample; otherwise take first N

    Returns:
        DataFrame with selected recipes
    """
    if filters:
        df = get_recipes(*filters)
    else:
        df = get_recipes()

    if random_sample and len(df) > count:
        df = df.sample(n=count, seed=None)
    else:
        df = df.head(count)

    return df


def preview_recipes(df: pl.DataFrame, console: Console, limit: int = 5):
    """Show a preview of selected recipes."""
    table = Table(title=f"Preview (first {min(limit, len(df))})", show_header=True)
    table.add_column("#", style="dim")
    table.add_column("Title")
    table.add_column("Grade", justify="center")
    table.add_column("Cook Time", justify="right")
    table.add_column("Rating", justify="right")

    for i, row in enumerate(df.head(limit).iter_rows(named=True)):
        cook_time = row.get("cookTime")
        cook_str = f"{cook_time} min" if cook_time else "-"
        rating = row.get("ratings")
        rating_str = f"{rating:.1f}" if rating else "-"
        grade = row.get("healthGrade", "-")

        table.add_row(
            str(i + 1),
            row.get("title", "Unknown")[:50],
            grade,
            cook_str,
            rating_str,
        )

    console.print(table)


def upload_recipes_to_firebase(
    recipes: list[dict[str, Any]],
    config: FirebaseConfig,
    upload_config: UploadConfig,
    console: Console,
    exclude_uploaded: bool = True,
) -> UploadResult:
    """
    Check Firebase, validate, process, and upload recipes.

    Flow:
    1. Check which recipes are already in Firebase
    2. Filter out already-uploaded recipes
    3. Validate and transform remaining recipes
    4. Upload to Firebase

    Args:
        recipes: List of recipe dicts from local data
        config: Firebase configuration
        upload_config: Upload configuration
        console: Rich console for output
        exclude_uploaded: If True, skip recipes already in Firebase

    Returns:
        UploadResult with counts and any failures
    """
    result = UploadResult(total_selected=len(recipes))

    # Check Firebase for existing recipes
    if exclude_uploaded:
        console.print("\n[bold]Checking Firebase for existing recipes...[/bold]")
        uploaded_ids = get_uploaded_recipe_ids(config)
        console.print(f"Found {len(uploaded_ids)} recipes in Firebase")

        # Filter out already-uploaded recipes
        recipes_to_process = [
            r for r in recipes if r.get("recipe_id") not in uploaded_ids
        ]
        skipped_count = len(recipes) - len(recipes_to_process)
        if skipped_count > 0:
            console.print(f"Skipping {skipped_count} already-uploaded recipes")
            for r in recipes:
                if r.get("recipe_id") in uploaded_ids:
                    result.skipped.append({"id": r.get("recipe_id")})
        recipes = recipes_to_process

    if not recipes:
        console.print("[yellow]All recipes are already uploaded.[/yellow]")
        return result

    # Validate and process recipes
    valid_recipes = []
    console.print("\n[bold]Validating and transforming recipes...[/bold]")

    for recipe in tqdm(recipes, desc="Validating"):
        # Map local data fields to Firebase schema
        mapped = _map_local_to_firebase(recipe)

        # Comprehensive validation with transformations
        validation_result = validate_and_transform_recipe(mapped)

        if validation_result.is_valid:
            valid_recipes.append(validation_result.recipe)
        else:
            # Collect all errors for this recipe
            error_msg = "; ".join([err.error for err in validation_result.errors])
            result.failed.append({"id": recipe.get("recipe_id"), "error": error_msg})

    result.total_valid = len(valid_recipes)
    console.print(f"Valid: {len(valid_recipes)}, Invalid: {len(result.failed)}")

    if not valid_recipes:
        console.print("[red]No valid recipes to upload.[/red]")
        return result

    # Upload to Firebase
    console.print("\n[bold]Uploading to Firebase...[/bold]")
    db = get_client(config)
    collection = get_collection(config)

    batch = db.batch()
    batch_count = 0

    for recipe in tqdm(valid_recipes, desc="Uploading"):
        try:
            doc_ref = collection.document(recipe["id"])
            batch.set(doc_ref, recipe)
            batch_count += 1

            if batch_count >= upload_config.batch_size:
                batch.commit()
                result.uploaded += batch_count
                batch = db.batch()
                batch_count = 0

        except Exception as e:
            result.failed.append({"id": recipe.get("id"), "error": str(e)})

    # Commit remaining
    if batch_count > 0:
        batch.commit()
        result.uploaded += batch_count

    return result


def _map_local_to_firebase(recipe: dict[str, Any]) -> dict[str, Any]:
    """Map local data schema to Firebase schema."""
    return {
        "id": recipe.get("recipe_id"),
        "title": recipe.get("title"),
        "description": recipe.get("description"),
        "url": recipe.get("url"),
        "host": recipe.get("host"),
        "image": recipe.get("image"),
        "author": recipe.get("author"),
        "instructions": recipe.get("instructions", []),
        "ingredientGroups": recipe.get("ingredientGroups", []),
        "ingredients": recipe.get("ingredientStrings", []),  # Map ingredientStrings from local
        "procesedIngredients": recipe.get("procesedIngredients", []),
        "tags": recipe.get("tags", []),
        "cookingMethod": recipe.get("cookingMethod"),
        "nutrients": recipe.get("nutrients", []),
        "healthScore": recipe.get("healthScore"),
        "healthGrade": recipe.get("healthGrade"),
        "numServings": recipe.get("numServings"),
        "cookTime": recipe.get("cookTime"),
        "prepTime": recipe.get("prepTime"),
        "totalTime": recipe.get("totalTime"),
        "ratings": recipe.get("ratings"),
        "ratingsCount": recipe.get("ratingsCount"),
        "channel": "discover",
    }


def upload_from_local(
    count: int,
    filters: list[Filter] | None,
    config: FirebaseConfig,
    upload_config: UploadConfig,
    console: Console,
    dry_run: bool = False,
    exclude_uploaded: bool = True,
) -> UploadResult:
    """
    Full pipeline: select -> check Firebase -> validate -> upload.

    Flow:
    1. Create batch using data pipelines and filters
    2. Check if recipes are in Firebase
    3. Validate and transform
    4. Upload to Firebase

    Args:
        count: Number of recipes to upload
        filters: Optional filters from Col query builder
        config: Firebase configuration
        upload_config: Upload configuration
        console: Rich console for output
        dry_run: If True, validate only without uploading
        exclude_uploaded: If True, skip recipes already in Firebase (default: True)

    Returns:
        UploadResult with counts and any failures
    """
    # 1. Select recipes from local data
    console.print("\n[bold]Selecting recipes from local data...[/bold]")
    df = select_recipes(count, filters)
    console.print(f"Selected {len(df)} recipes")

    if len(df) == 0:
        console.print("[yellow]No recipes match the filters.[/yellow]")
        return UploadResult()

    # Preview
    preview_recipes(df, console)

    # Convert to dicts
    recipes = df_to_recipes(df)

    if dry_run:
        console.print("\n[yellow]DRY RUN - validating only[/yellow]")
        result = UploadResult(total_selected=len(recipes))

        # 2. Check Firebase for existing recipes
        if exclude_uploaded:
            console.print("\n[bold]Checking Firebase for existing recipes...[/bold]")
            uploaded_ids = get_uploaded_recipe_ids(config)
            console.print(f"Found {len(uploaded_ids)} recipes in Firebase")

            recipes_to_process = [
                r for r in recipes if r.get("recipe_id") not in uploaded_ids
            ]
            skipped_count = len(recipes) - len(recipes_to_process)
            if skipped_count > 0:
                console.print(f"Skipping {skipped_count} already-uploaded recipes")
                for r in recipes:
                    if r.get("recipe_id") in uploaded_ids:
                        result.skipped.append({"id": r.get("recipe_id")})
            recipes = recipes_to_process

        if not recipes:
            console.print("[yellow]All recipes are already uploaded.[/yellow]")
            return result

        # 3. Validate and transform
        for recipe in recipes:
            mapped = _map_local_to_firebase(recipe)
            validation_result = validate_and_transform_recipe(mapped)
            if validation_result.is_valid:
                result.total_valid += 1
            else:
                error_msg = "; ".join([err.error for err in validation_result.errors])
                result.failed.append({"id": recipe.get("recipe_id"), "error": error_msg})

        console.print("\n[green]Validation complete.[/green]")
        console.print(f"  Valid: {result.total_valid}")
        console.print(f"  Invalid: {len(result.failed)}")
        if result.skipped:
            console.print(f"  Skipped: {len(result.skipped)} (already uploaded)")
        return result

    # Upload
    return upload_recipes_to_firebase(recipes, config, upload_config, console, exclude_uploaded)


def upload_from_file(
    file_path: Path,
    config: FirebaseConfig,
    upload_config: UploadConfig,
    console: Console,
    dry_run: bool = False,
    exclude_uploaded: bool = True,
) -> UploadResult:
    """
    Full pipeline: load from file -> check Firebase -> validate -> upload.

    Flow:
    1. Load recipes from JSON file
    2. Check if recipes are in Firebase
    3. Validate and transform
    4. Upload to Firebase

    Args:
        file_path: Path to JSON file containing recipes
        config: Firebase configuration
        upload_config: Upload configuration
        console: Rich console for output
        dry_run: If True, validate only without uploading
        exclude_uploaded: If True, skip recipes already in Firebase (default: True)

    Returns:
        UploadResult with counts and any failures
    """
    # 1. Load recipes from JSON file
    console.print(f"\n[bold]Loading recipes from {file_path}...[/bold]")

    with open(file_path, encoding="utf-8") as f:
        recipes = json.load(f)

    if not isinstance(recipes, list):
        recipes = [recipes]

    console.print(f"Loaded {len(recipes)} recipes")

    if len(recipes) == 0:
        console.print("[yellow]No recipes in file.[/yellow]")
        return UploadResult()

    # Preview
    console.print("\n[bold]Preview:[/bold]")
    for i, recipe in enumerate(recipes[:5]):
        title = recipe.get("title", "Unknown")[:50]
        console.print(f"  {i+1}. {title}")
    if len(recipes) > 5:
        console.print(f"  ... and {len(recipes) - 5} more")

    if dry_run:
        console.print("\n[yellow]DRY RUN - validating only[/yellow]")
        result = UploadResult(total_selected=len(recipes))

        # 2. Check Firebase for existing recipes
        if exclude_uploaded:
            console.print("\n[bold]Checking Firebase for existing recipes...[/bold]")
            uploaded_ids = get_uploaded_recipe_ids(config)
            console.print(f"Found {len(uploaded_ids)} recipes in Firebase")

            recipes_to_process = [
                r for r in recipes if r.get("id") not in uploaded_ids
            ]
            skipped_count = len(recipes) - len(recipes_to_process)
            if skipped_count > 0:
                console.print(f"Skipping {skipped_count} already-uploaded recipes")
                for r in recipes:
                    if r.get("id") in uploaded_ids:
                        result.skipped.append({"id": r.get("id")})
            recipes = recipes_to_process

        if not recipes:
            console.print("[yellow]All recipes are already uploaded.[/yellow]")
            return result

        # 3. Validate and transform
        for recipe in recipes:
            validation_result = validate_and_transform_recipe(recipe)
            if validation_result.is_valid:
                result.total_valid += 1
            else:
                error_msg = "; ".join([err.error for err in validation_result.errors])
                result.failed.append({"id": recipe.get("id"), "error": error_msg})

        console.print("\n[green]Validation complete.[/green]")
        console.print(f"  Valid: {result.total_valid}")
        console.print(f"  Invalid: {len(result.failed)}")
        if result.skipped:
            console.print(f"  Skipped: {len(result.skipped)} (already uploaded)")
        return result

    # Upload
    return upload_recipes_to_firebase(recipes, config, upload_config, console, exclude_uploaded)

def upload_from_url(
    url: str,
    config: FirebaseConfig,
    upload_config: UploadConfig,
    console: Console,
    dry_run: bool = False,
    exclude_uploaded: bool = True,
) -> UploadResult:
    """
    Full pipeline: scrape from URL -> check Firebase -> validate -> upload.

    Flow:
    1. Scrape recipe from URL
    2. Check if recipe is in Firebase
    3. Validate and transform
    4. Upload to Firebase

    Args:
        url: Recipe URL to scrape
        config: Firebase configuration
        upload_config: Upload configuration
        console: Rich console for output
        dry_run: If True, validate only without uploading
        exclude_uploaded: If True, skip recipes already in Firebase (default: True)

    Returns:
        UploadResult with counts and any failures
    """
    from plaite.scraper import scrape_recipe

    # 1. Scrape recipe from URL
    console.print("\n[bold]Scraping recipe from URL...[/bold]")
    console.print(f"URL: {url}")

    recipe = scrape_recipe(url)

    if not recipe:
        console.print("[red]Failed to scrape recipe from URL.[/red]")
        result = UploadResult(total_selected=1)
        result.failed.append({"id": url, "error": "Scraping failed"})
        return result

    console.print(f"[green]Successfully scraped: {recipe['title']}[/green]")

    # Preview
    console.print("\n[bold]Preview:[/bold]")
    console.print(f"  Title: {recipe.get('title', 'Unknown')}")
    console.print(f"  Host: {recipe.get('host', 'Unknown')}")
    console.print(f"  Author: {recipe.get('author', 'Unknown')}")
    if recipe.get('tags'):
        console.print(f"  Tags: {', '.join(recipe['tags'][:5])}")

    recipes = [recipe]

    if dry_run:
        console.print("\n[yellow]DRY RUN - validating only[/yellow]")
        result = UploadResult(total_selected=1)

        # 2. Check Firebase for existing recipes
        if exclude_uploaded:
            console.print("\n[bold]Checking Firebase for existing recipes...[/bold]")
            uploaded_ids = get_uploaded_recipe_ids(config)
            console.print(f"Found {len(uploaded_ids)} recipes in Firebase")

            if recipe.get("id") in uploaded_ids:
                console.print("Recipe already uploaded")
                result.skipped.append({"id": recipe.get("id")})
                return result

        # 3. Validate and transform
        validation_result = validate_and_transform_recipe(recipe)
        if validation_result.is_valid:
            result.total_valid = 1
            console.print("\n[green]Validation passed.[/green]")
        else:
            error_msg = "; ".join([err.error for err in validation_result.errors])
            result.failed.append({"id": recipe.get("id"), "error": error_msg})
            console.print(f"\n[red]Validation failed: {error_msg}[/red]")

        return result

    # Upload
    return upload_recipes_to_firebase(recipes, config, upload_config, console, exclude_uploaded)
