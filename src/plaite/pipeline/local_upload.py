"""Upload pipeline for local parquet data."""

import tempfile
from pathlib import Path
from typing import Any

import polars as pl
from rich.console import Console
from rich.table import Table

from plaite.config import FirebaseConfig, UploadConfig
from plaite.data import get_recipes
from plaite.data.query import Filter
from plaite.firebase.client import get_uploaded_recipe_ids
from plaite.firebase.upload import upload_batch, upload_image
from plaite.images import ImageGenerator
from plaite.models.recipe import Recipe
from plaite.pipeline._shared import UploadResult


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
    recipes = df.to_dicts()

    if dry_run:
        console.print("\n[yellow]DRY RUN - validating only[/yellow]")

    result = UploadResult(total_selected=len(recipes))

    if exclude_uploaded:
        console.print("\n[bold]Checking Firebase for existing recipes...[/bold]")
        uploaded_ids = get_uploaded_recipe_ids(config)
        console.print(f"Found {len(uploaded_ids)} recipes in Firebase")
        recipes_to_process = [r for r in recipes if r.get("recipe_id") not in uploaded_ids]
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

    if dry_run:
        return result

    # Generate and upload images
    console.print("\n[bold]Generating and uploading images...[/bold]")

    # Initialize generator
    try:
        generator = ImageGenerator()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            for recipe in valid_recipes:
                try:
                    title = recipe.get("title", "Unknown Recipe")
                    image_url = upgen_images(
                        generator=generator,
                        temp_path=temp_path,
                        recipe=recipe,
                        config=config,
                        upload_config=upload_config,
                        console=console,
                    )
                    if image_url:
                        recipe["image"] = image_url
                        console.print(f"    [green]Image linked:[/green] {image_url}")
                    else:
                        console.print(f"    [red]Failed to upload image for {title}[/red]")

                except Exception as e:
                    console.print(f"    [red]Image processing failed for {title}: {e}[/red]")
                    # We continue even if image generation fails, uploading the recipe without new image

    except Exception as e:
        console.print(f"[red]Image generator initialization failed: {e}[/red]")
        console.print("[yellow]Proceeding with recipe upload without image generation...[/yellow]")

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


def upgen_images(
    generator: ImageGenerator,
    temp_path: Path,
    recipe: dict,
    config: FirebaseConfig,
    upload_config: UploadConfig,
    console: Console,
) -> str | None:
    title = recipe.get("title", "Unknown Recipe")
    ingredients = recipe.get("ingredientStrings", [])  # using ingredientStrings for prompt

    # Construct prompt
    ing_text = ", ".join(ingredients[:5])
    prompt = f"Professional food photography of {title} with {ing_text}"

    console.print(f"  Generating image for: [cyan]{title}[/cyan]")

    # Generate
    image = generator.generate(prompt=prompt, num_images=1, aspect_ratio="9:16", image_size="1K")

    if not image:
        console.print(f"    [yellow]No image generated for {title}[/yellow]")
        return None

    # Save to temp
    image_path = temp_path / f"{recipe['id']}.png"
    image[0].save(image_path, "PNG")
    console.print("    Uploading to Storage...")
    return upload_image(
        image_path=image_path, recipe_id=recipe["id"], config=config, upload_config=upload_config
    )
