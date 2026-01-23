"""CLI entrypoint for plaite tools."""

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel

from plaite.config import Env, load_firebase_config, load_upload_config

app = typer.Typer(
    name="plaite",
    help="Backend tools for Plaite recipe management",
    no_args_is_help=True,
)
console = Console()

# Default config paths (relative to package root: src/plaite/cli.py -> plaite_backend/)
PACKAGE_ROOT = Path(__file__).parent.parent.parent  # src/plaite -> src -> plaite_backend
DEFAULT_FIREBASE_CONFIG = PACKAGE_ROOT / "configs" / "firebase.yaml"
DEFAULT_UPLOAD_CONFIG = PACKAGE_ROOT / "configs" / "upload.yaml"


def _prompt_filters() -> list:
    """Interactively prompt for recipe filters."""
    from plaite.data import Col

    filters = []
    console.print(Panel("[bold]Recipe Selection[/bold]", style="blue"))

    # Title filter
    title = typer.prompt("Filter by title (contains)", default="", show_default=False)
    if title.strip():
        filters.append(Col.title.contains(title.strip()))

    # Ingredient filter (searches through ingredientStrings list)
    ingredient = typer.prompt(
        "Filter by ingredient (contains)",
        default="",
        show_default=False,
    )
    if ingredient.strip():
        filters.append(Col.ingredient_strings.list_any_contains(ingredient.strip()))

    # Health grade filter
    grade = typer.prompt(
        "Filter by health grade (A/B/C/D/F)",
        default="",
        show_default=False,
    )
    if grade.strip().upper() in ["A", "B", "C", "D", "F"]:
        filters.append(Col.health_grade.eq(grade.strip().upper()))

    # Cook time filter
    cook_time = typer.prompt(
        "Max cook time (minutes)",
        default="",
        show_default=False,
    )
    if cook_time.strip().isdigit():
        filters.append(Col.cook_time.lte(int(cook_time.strip())))

    # Min rating filter
    rating = typer.prompt(
        "Min rating (0-5)",
        default="",
        show_default=False,
    )
    if rating.strip():
        try:
            filters.append(Col.ratings.gte(float(rating.strip())))
        except ValueError:
            pass

    return filters


@app.command()
def stats(
    source: Annotated[str, typer.Option(help="Data source: local, firebase, or both")] = "both",
    env: Annotated[Env, typer.Option(help="Environment: prod or dev")] = "dev",
    config: Annotated[Path, typer.Option(help="Firebase config path")] = DEFAULT_FIREBASE_CONFIG,
    output: Annotated[Path | None, typer.Option(help="Output JSON file")] = None,
    limit: Annotated[int | None, typer.Option(help="Limit recipes to analyze")] = None,
):
    """Get statistics from local data and/or Firebase."""
    from rich.table import Table

    combined_stats = {}

    # Local stats
    if source in ("local", "both"):
        from plaite.data import get_stats_of_all_recipes

        console.print("[bold]Local Data Statistics[/bold]")
        console.print("-" * 40)

        local_stats = get_stats_of_all_recipes()
        combined_stats["local"] = local_stats

        table = Table(show_header=False)
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")

        table.add_row("Total Recipes", f"{local_stats['total_recipes']:,}")
        table.add_row("Total Columns", str(local_stats['total_columns']))
        table.add_row("Unique Ingredients", f"{local_stats['unique_ingredients_count']:,}")

        console.print(table)

        # Health grade distribution
        console.print("\n[bold]Health Grade Distribution:[/bold]")
        grade_table = Table(show_header=True)
        grade_table.add_column("Grade")
        grade_table.add_column("Count", justify="right")
        grade_table.add_column("%", justify="right")

        total = local_stats['total_recipes']
        for grade in ["A", "B", "C", "D", "F"]:
            count = local_stats['recipes_per_health_grade'].get(grade, 0)
            pct = round(count / total * 100, 1) if total > 0 else 0
            grade_table.add_row(grade, f"{count:,}", f"{pct}%")

        console.print(grade_table)
        console.print()

    # Firebase stats
    if source in ("firebase", "both"):
        from plaite.firebase.stats import get_stats, print_stats

        console.print(f"[bold]Firebase Statistics ({env})[/bold]")
        console.print("-" * 40)

        firebase_config = load_firebase_config(config, env)
        console.print(f"Collection: {firebase_config.collection}\n")

        recipe_stats = get_stats(firebase_config, limit=limit)
        combined_stats["firebase"] = recipe_stats.to_dict()

        print_stats(recipe_stats, console)

    # Save to file if requested
    if output:
        with open(output, "w", encoding="utf-8") as f:
            json.dump(combined_stats, f, indent=2, ensure_ascii=False, default=str)
        console.print(f"\n[green]Stats saved to {output}[/green]")


@app.command()
def upload(
    batch: Annotated[Path, typer.Argument(help="Path to batch JSON file")],
    env: Annotated[Env, typer.Option(help="Environment: prod or dev")] = "dev",
    config: Annotated[
        Path, typer.Option("--config", "-c", help="Firebase config path")
    ] = DEFAULT_FIREBASE_CONFIG,
    upload_config: Annotated[Path, typer.Option(help="Upload config path")] = DEFAULT_UPLOAD_CONFIG,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Validate only, don't upload")] = False,
    include_uploaded: Annotated[
        bool,
        typer.Option("--include-uploaded", help="Include recipes already in Firebase"),
    ] = False,
):
    """Upload a batch of recipes from JSON file to Firebase."""
    from plaite.pipeline.file_upload import upload_from_file

    console.print(Panel(f"[bold]Plaite Upload - {env.upper()}[/bold]", style="blue"))
    console.print(f"Target: {env}")
    console.print(f"File: {batch}\n")

    if not batch.exists():
        console.print(f"[red]Error: File not found: {batch}[/red]")
        raise typer.Exit(1)

    # Load configs
    firebase_config = load_firebase_config(config, env)
    upload_cfg = load_upload_config(upload_config)

    # Run upload pipeline
    result = upload_from_file(
        file_path=batch,
        config=firebase_config,
        upload_config=upload_cfg,
        console=console,
        dry_run=dry_run,
        exclude_uploaded=not include_uploaded,
    )

    # Print summary
    console.print("\n[bold]Done![/bold]")
    console.print(f"  Selected: {result.total_selected}")
    console.print(f"  Valid: {result.total_valid}")
    console.print(f"  Uploaded: {result.uploaded}")

    if result.skipped:
        console.print(f"  Skipped: {len(result.skipped)} (already uploaded)")

    if result.failed:
        console.print(f"  Failed: {len(result.failed)}")
        console.print("\n[red]Failed recipes:[/red]")
        for fail in result.failed[:10]:
            console.print(f"    {fail['id']}: {fail.get('error', 'Unknown error')}")
        if len(result.failed) > 10:
            console.print(f"    ... and {len(result.failed) - 10} more")


@app.command()
def sync(
    count: Annotated[int, typer.Argument(help="Number of recipes to upload")],
    env: Annotated[Env, typer.Option(help="Environment: prod or dev")] = "dev",
    config: Annotated[
        Path, typer.Option("--config", "-c", help="Firebase config path")
    ] = DEFAULT_FIREBASE_CONFIG,
    upload_config: Annotated[Path, typer.Option(help="Upload config path")] = DEFAULT_UPLOAD_CONFIG,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Validate only, don't upload")] = False,
    no_confirm: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation")] = False,
    include_uploaded: Annotated[
        bool, typer.Option("--include-uploaded", help="Include recipes already in Firebase")
    ] = False,
):
    """Upload recipes from local data to Firebase (interactive)."""
    from plaite.data import get_recipes
    from plaite.pipeline.local_upload import upload_from_local

    console.print(f"[bold]Plaite Sync - Upload to Firebase ({env})[/bold]\n")

    # Load configs
    firebase_config = load_firebase_config(config, env)
    upload_cfg = load_upload_config(upload_config)

    console.print(f"Target: [cyan]{firebase_config.collection}[/cyan]")

    while True:
        # Interactive filter selection
        filters = _prompt_filters()

        # Show how many match
        df = get_recipes(*filters) if filters else get_recipes()
        total_matching = len(df)
        console.print(f"\n[bold]Found {total_matching} matching recipes.[/bold]")

        if total_matching == 0:
            console.print("[yellow]No recipes match the filters.[/yellow]")
            if typer.confirm("Try again?", default=True):
                continue
            raise typer.Exit(1)

        actual_count = min(count, total_matching)
        console.print(f"Will select {actual_count} random recipes.\n")
        break

    # Confirm
    if not no_confirm:
        if not typer.confirm("Continue with upload?", default=True):
            console.print("[yellow]Cancelled.[/yellow]")
            raise typer.Exit(0)

    # Run pipeline
    result = upload_from_local(
        count=actual_count,
        filters=filters if filters else None,
        config=firebase_config,
        upload_config=upload_cfg,
        console=console,
        dry_run=dry_run,
        exclude_uploaded=not include_uploaded,
    )

    # Summary
    console.print("\n[bold green]Done![/bold green]")
    console.print(f"  Selected: {result.total_selected}")
    console.print(f"  Valid: {result.total_valid}")
    console.print(f"  Uploaded: {result.uploaded}")

    if result.failed:
        console.print(f"  [red]Failed: {len(result.failed)}[/red]")
        for fail in result.failed[:5]:
            console.print(f"    {fail['id']}: {fail.get('error', 'Unknown')}")


@app.command()
def scrape(
    url: Annotated[str, typer.Argument(help="Recipe URL to scrape")],
    env: Annotated[Env, typer.Option(help="Environment: prod or dev")] = "dev",
    config: Annotated[
        Path, typer.Option("--config", "-c", help="Firebase config path")
    ] = DEFAULT_FIREBASE_CONFIG,
    upload_config: Annotated[Path, typer.Option(help="Upload config path")] = DEFAULT_UPLOAD_CONFIG,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Validate only, don't upload")] = False,
    include_uploaded: Annotated[
        bool,
        typer.Option("--include-uploaded", help="Include recipes already in Firebase"),
    ] = False,
):
    """Scrape a recipe from a URL and upload to Firebase."""
    from plaite.pipeline.url_upload import upload_from_url

    console.print(Panel(f"[bold]Plaite Scrape - {env.upper()}[/bold]", style="blue"))
    console.print(f"Target: {env}\n")

    # Load configs
    firebase_config = load_firebase_config(config, env)
    upload_cfg = load_upload_config(upload_config)

    # Run scrape and upload pipeline
    result = upload_from_url(
        url=url,
        config=firebase_config,
        upload_config=upload_cfg,
        console=console,
        dry_run=dry_run,
        exclude_uploaded=not include_uploaded,
    )

    # Print summary
    console.print("\n[bold]Done![/bold]")
    console.print(f"  Selected: {result.total_selected}")
    console.print(f"  Valid: {result.total_valid}")
    console.print(f"  Uploaded: {result.uploaded}")

    if result.skipped:
        console.print(f"  Skipped: {len(result.skipped)} (already uploaded)")

    if result.failed:
        console.print(f"  Failed: {len(result.failed)}")
        console.print("\n[red]Failed:[/red]")
        for fail in result.failed:
            console.print(f"    {fail['id']}: {fail.get('error', 'Unknown error')}")


@app.command("process-images")
def process_images_cmd(
    batch: Annotated[Path, typer.Argument(help="Path to batch JSON file with recipes")],
    output_dir: Annotated[Path, typer.Option("--output", "-o", help="Output directory")] = Path(
        "./processed_images"
    ),
    config: Annotated[Path, typer.Option(help="Upload config path")] = DEFAULT_UPLOAD_CONFIG,
    no_download: Annotated[bool, typer.Option(help="Skip downloading, process existing")] = False,
):
    """Download and process images for a batch of recipes."""
    import json

    from plaite.images.process import process_images

    upload_cfg = load_upload_config(config)

    console.print(f"[bold]Loading recipes from {batch}...[/bold]")
    with open(batch, encoding="utf-8") as f:
        recipes = json.load(f)

    console.print(f"Found {len(recipes)} recipes")
    console.print(f"Output directory: {output_dir}\n")

    mapping = process_images(
        recipes=recipes,
        output_dir=output_dir,
        config=upload_cfg.images,
        download=not no_download,
    )

    console.print(f"\n[green]Processed {len(mapping)} images[/green]")

    # Save mapping
    mapping_path = output_dir / "image_mapping.json"
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)
    console.print(f"Mapping saved to {mapping_path}")


@app.command()
def version():
    """Show version information."""
    from plaite import __version__

    console.print(f"plaite version {__version__}")


if __name__ == "__main__":
    app()
