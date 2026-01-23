"""Upload pipeline for JSON files."""

import json
from pathlib import Path
from typing import Any

from rich.console import Console

from plaite.config import FirebaseConfig, UploadConfig
from plaite.firebase.client import get_uploaded_recipe_ids
from plaite.firebase.upload import upload_batch
from plaite.models.recipe import Recipe
from plaite.pipeline._shared import UploadResult


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
    #load recipes from json
    console.print(f"\n[bold]Loading recipes from {file_path}...[/bold]")

    with open(file_path, encoding="utf-8") as f:
        recipes = json.load(f)

    if not isinstance(recipes, list):
        recipes = [recipes]

    console.print(f"Loaded {len(recipes)} recipes")

    if len(recipes) == 0:
        console.print("[yellow]No recipes in file.[/yellow]")
        return UploadResult()

    # preview recipes
    console.print("\n[bold]Preview:[/bold]")
    for i, recipe in enumerate(recipes[:5]):
        title = recipe.get("title", "Unknown")[:50]
        console.print(f"  {i+1}. {title}")
    if len(recipes) > 5:
        console.print(f"  ... and {len(recipes) - 5} more")

    #dry run
    if dry_run:
        console.print("\n[yellow]DRY RUN - validating only[/yellow]")
        result = UploadResult(total_selected=len(recipes))

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

        console.print("\n[bold]Validating and transforming recipes...[/bold]")
        for recipe in recipes:
            try:
                model = Recipe.from_raw(recipe)
                model.validate()
                result.total_valid += 1
            except Exception as e:
                result.failed.append({"id": recipe.get("id"), "error": str(e)})

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

    console.print("\n[bold]Validating and transforming recipes...[/bold]")
    valid_recipes: list[dict[str, Any]] = []
    for recipe in recipes:
        try:
            model = Recipe.from_raw(recipe)
            model.validate()
            valid_recipes.append(model.model_dump())
        except Exception as e:
            result.failed.append({"id": recipe.get("id"), "error": str(e)})

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
