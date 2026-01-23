"""Upload pipeline for scraping from a URL."""

from rich.console import Console

from plaite.config import FirebaseConfig, UploadConfig
from plaite.firebase.client import get_uploaded_recipe_ids
from plaite.firebase.upload import upload_batch
from plaite.models.recipe import Recipe
from plaite.pipeline._shared import UploadResult


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

    console.print("\n[bold]Scraping recipe from URL...[/bold]")
    console.print(f"URL: {url}")

    recipe = scrape_recipe(url)

    if not recipe:
        console.print("[red]Failed to scrape recipe from URL.[/red]")
        result = UploadResult(total_selected=1)
        result.failed.append({"id": url, "error": "Scraping failed"})
        return result

    console.print(f"[green]Successfully scraped: {recipe['title']}[/green]")

    console.print("\n[bold]Preview:[/bold]")
    console.print(f"  Title: {recipe.get('title', 'Unknown')}")
    console.print(f"  Host: {recipe.get('host', 'Unknown')}")
    console.print(f"  Author: {recipe.get('author', 'Unknown')}")
    if recipe.get("tags"):
        console.print(f"  Tags: {', '.join(recipe['tags'][:5])}")

    if dry_run:
        console.print("\n[yellow]DRY RUN - validating only[/yellow]")
        result = UploadResult(total_selected=1)

        if exclude_uploaded:
            console.print("\n[bold]Checking Firebase for existing recipes...[/bold]")
            uploaded_ids = get_uploaded_recipe_ids(config)
            console.print(f"Found {len(uploaded_ids)} recipes in Firebase")

            if recipe.get("id") in uploaded_ids:
                console.print("Recipe already uploaded")
                result.skipped.append({"id": recipe.get("id")})
                return result

        console.print("\n[bold]Validating and transforming recipes...[/bold]")
        try:
            model = Recipe.from_raw(recipe)
            model.validate()
            result.total_valid = 1
            console.print("\n[green]Validation passed.[/green]")
        except Exception as e:
            result.failed.append({"id": recipe.get("id"), "error": str(e)})
            console.print(f"\n[red]Validation failed: {e}[/red]")

        return result

    result = UploadResult(total_selected=1)

    if exclude_uploaded:
        console.print("\n[bold]Checking Firebase for existing recipes...[/bold]")
        uploaded_ids = get_uploaded_recipe_ids(config)
        console.print(f"Found {len(uploaded_ids)} recipes in Firebase")

        if recipe.get("id") in uploaded_ids:
            console.print("Recipe already uploaded")
            result.skipped.append({"id": recipe.get("id")})
            return result

    console.print("\n[bold]Validating and transforming recipes...[/bold]")
    try:
        model = Recipe.from_raw(recipe)
        model.validate()
        valid_recipe = model.model_dump()
    except Exception as e:
        result.failed.append({"id": recipe.get("id"), "error": str(e)})
        console.print(f"\n[red]Validation failed: {e}[/red]")
        return result

    result.total_valid = 1

    #generate images

    #upload images

    #save image url to recipe.image
    console.print("\n[bold]Uploading to Firebase...[/bold]")
    upload_results = upload_batch(
        recipes=[valid_recipe],
        config=config,
        upload_config=upload_config,
        dry_run=False,
        console=console,
    )
    result.uploaded = upload_results.get("success", 0)
    result.failed.extend(upload_results.get("failed", []))
    return result
