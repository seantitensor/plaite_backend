"""CLI entrypoint for plaite tools."""

import json
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel

from plaite.config import Env, load_firebase_config, load_upload_config

# Load environment variables from .env file
load_dotenv()

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

    # Tag filter
    tag = typer.prompt(
        "Filter by tag (must contain, e.g. 'Vegan')",
        default="",
        show_default=False,
    )
    if tag.strip():
        filters.append(Col.tags.list_any_contains(tag.strip()))

    return filters


@app.command()
def tags(
    source: Annotated[str, typer.Option(help="Data source: local, firebase, or both")] = "both",
    env: Annotated[Env, typer.Option(help="Environment: prod or dev")] = "dev",
    config: Annotated[Path, typer.Option(help="Firebase config path")] = DEFAULT_FIREBASE_CONFIG,
    output: Annotated[Path | None, typer.Option(help="Output JSON file")] = None,
):
    """List all unique tags and their counts from local data and/or Firebase."""
    from collections import Counter

    from rich.table import Table

    combined: dict[str, dict[str, int]] = {}

    if source in ("local", "both"):
        from plaite.data import get_tags as get_local_tags
        console.print("[bold]Local tags…[/bold]")
        local_tags = get_local_tags()
        combined["local"] = local_tags
        console.print(f"  {len(local_tags)} unique tags across local recipes\n")

    if source in ("firebase", "both"):
        from plaite.firebase.stats import get_tags as get_firebase_tags
        firebase_config = load_firebase_config(config, env)
        console.print(f"[bold]Firebase tags ({env})…[/bold]")
        firebase_tags = get_firebase_tags(firebase_config)
        combined["firebase"] = firebase_tags
        console.print(f"  {len(firebase_tags)} unique tags across Firebase recipes\n")

    # Print table
    table = Table(show_header=True, header_style="bold")
    table.add_column("Tag")
    if source in ("local", "both"):
        table.add_column("Local", justify="right")
    if source in ("firebase", "both"):
        table.add_column("Firebase", justify="right")

    all_tags = sorted(
        set(combined.get("local", {}).keys()) | set(combined.get("firebase", {}).keys())
    )
    for tag in sorted(all_tags, key=lambda t: -combined.get("local", {}).get(t, 0) or -combined.get("firebase", {}).get(t, 0)):
        row = [tag]
        if source in ("local", "both"):
            row.append(str(combined["local"].get(tag, 0)))
        if source in ("firebase", "both"):
            row.append(str(combined["firebase"].get(tag, 0)))
        table.add_row(*row)

    console.print(table)

    if output:
        import json
        with open(output, "w", encoding="utf-8") as f:
            json.dump(combined, f, indent=2)
        console.print(f"\n[green]Saved to {output}[/green]")


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
        table.add_row("Total Columns", str(local_stats["total_columns"]))
        table.add_row("Unique Ingredients", f"{local_stats['unique_ingredients_count']:,}")

        console.print(table)

        # Health grade distribution
        console.print("\n[bold]Health Grade Distribution:[/bold]")
        grade_table = Table(show_header=True)
        grade_table.add_column("Grade")
        grade_table.add_column("Count", justify="right")
        grade_table.add_column("%", justify="right")

        total = local_stats["total_recipes"]
        for grade in ["A", "B", "C", "D", "F"]:
            count = local_stats["recipes_per_health_grade"].get(grade, 0)
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
    supervise: Annotated[
        bool, typer.Option("--supervise", help="Review each recipe and generated image before uploading")
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
        env=env,
        dry_run=dry_run,
        exclude_uploaded=not include_uploaded,
        supervise=supervise,
    )

    # Summary
    console.print("\n[bold green]Done![/bold green]")
    console.print(f"  Selected: {result.total_selected}")
    console.print(f"  Valid: {result.total_valid}")
    console.print(f"  Uploaded: {result.uploaded}")

    if result.images_generated:
        cost = result.estimated_cost()
        console.print(f"  Images generated: {result.images_generated} (~[yellow]${cost:.2f}[/yellow] @ $0.04/image)")

    if result.skipped:
        console.print(f"  Skipped: {len(result.skipped)}")

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


@app.command("generate-image")
def generate_image_cmd(
    prompt: Annotated[str, typer.Argument(help="Text description of the image to generate")],
    output: Annotated[Path, typer.Option("--output", "-o", help="Output file path")] = Path(
        "./generated_image.png"
    ),
    model: Annotated[
        str | None,
        typer.Option(
            help="Google image model to use (uses IMAGE_GENERATION_MODEL env if not specified)"
        ),
    ] = None,
    aspect_ratio: Annotated[
        str, typer.Option(help="Aspect ratio: 1:1, 3:4, 4:3, 9:16, or 16:9")
    ] = "9:16",
    num_images: Annotated[int, typer.Option(help="Number of images to generate (1-4)")] = 1,
):
    """Generate images using Google Generative AI.

    Requires GOOGLE_API_KEY environment variable to be set.
    Uses IMAGE_GENERATION_MODEL env var for default model.

    Examples:
        plaite generate-image "Delicious chocolate cake with strawberries"
        plaite generate-image "Homemade pizza" -o pizza.png --model nano-banana-pro-preview
        plaite generate-image "Pasta carbonara" --aspect-ratio 16:9 --num-images 2
    """
    from plaite.images import ImageGenerator

    console.print(Panel("[bold]Image Generation[/bold]", style="cyan"))
    console.print(f"Prompt: {prompt}")
    console.print(f"Model: {model or '(using env default)'}")
    console.print(f"Aspect ratio: {aspect_ratio}")
    console.print(f"Number of images: {num_images}\n")

    try:
        generator = ImageGenerator(default_model=model)

        # Generate images
        console.print("[yellow]Generating images...[/yellow]")
        images = generator.generate(
            prompt=prompt,
            num_images=num_images,
            aspect_ratio=aspect_ratio,
        )

        # Save images
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        saved_paths = []
        for i, image in enumerate(images, start=1):
            if num_images == 1:
                filepath = output_path
            else:
                # For multiple images, add number to filename
                stem = output_path.stem
                suffix = output_path.suffix or ".png"
                filepath = output_path.parent / f"{stem}_{i}{suffix}"

            image.save(filepath, "PNG")
            saved_paths.append(filepath)
            console.print(f"[green]✓[/green] Saved: {filepath}")

        console.print(f"\n[bold green]Success![/bold green] Generated {len(images)} image(s)")

    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from None
    except RuntimeError as e:
        console.print(f"[red]Generation failed:[/red] {e}")
        raise typer.Exit(1) from None


@app.command()
def version():
    """Show version information."""
    from plaite import __version__

    console.print(f"plaite version {__version__}")


if __name__ == "__main__":
    app()
