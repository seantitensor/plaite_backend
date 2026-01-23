"""Upload pipeline for local parquet data."""

from typing import Any

import polars as pl
from rich.console import Console
from rich.table import Table

from plaite.config import FirebaseConfig, UploadConfig
from plaite.data import get_recipes
from plaite.data.query import Filter
from plaite.firebase.client import get_uploaded_recipe_ids
from plaite.firebase.upload import upload_batch
from plaite.models.recipe import Recipe
from plaite.pipeline._shared import UploadResult


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
    console.print("\n[bold]Selecting recipes from local data...[/bold]")
    df = select_recipes(count, filters)
    console.print(f"Selected {len(df)} recipes")

    if len(df) == 0:
        console.print("[yellow]No recipes match the filters.[/yellow]")
        return UploadResult()

    preview_recipes(df, console)
    recipes = df_to_recipes(df)

    if dry_run:
        console.print("\n[yellow]DRY RUN - validating only[/yellow]")
        result = UploadResult(total_selected=len(recipes))

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

        console.print("\n[bold]Validating and transforming recipes...[/bold]")
        for recipe in recipes:
            try:
                model = Recipe.from_raw(recipe)
                model.validate()
                result.total_valid += 1
            except Exception as e:
                result.failed.append({"id": recipe.get("recipe_id"), "error": str(e)})

        console.print("\n[green]Validation complete.[/green]")
        console.print(f"  Valid: {result.total_valid}")
        console.print(f"  Invalid: {len(result.failed)}")
        if result.skipped:
            console.print(f"  Skipped: {len(result.skipped)} (already uploaded)")
        return result

    result = UploadResult(total_selected=len(recipes))

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

    console.print("\n[bold]Validating and transforming recipes...[/bold]")
    valid_recipes: list[dict[str, Any]] = []
    for recipe in recipes:
        try:
            model = Recipe.from_raw(recipe)
            model.validate()
            valid_recipes.append(model.model_dump())
        except Exception as e:
            result.failed.append({"id": recipe.get("recipe_id"), "error": str(e)})

    result.total_valid = len(valid_recipes)
    console.print(f"Valid: {len(valid_recipes)}, Invalid: {len(result.failed)}")

    if not valid_recipes:
        console.print("[red]No valid recipes to upload.[/red]")
        return result

    #generate images

    #upload images

    #save image url to recipe.image

    console.print("\n[bold]Uploading to Firebase...[/bold]")
    upload_results = upload_batch(
        recipes=valid_recipes,
        config=config,
        upload_config=upload_config,
        dry_run=False,
        console=console,
    )
    result.uploaded = upload_results.get("success", 0)
    result.failed.extend(upload_results.get("failed", []))
    return result
