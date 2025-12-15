"""CLI entrypoint for plaite tools."""

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

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


@app.command()
def stats(
    env: Annotated[Env, typer.Option(help="Environment: prod or dev")] = "dev",
    config: Annotated[Path, typer.Option(help="Firebase config path")] = DEFAULT_FIREBASE_CONFIG,
    output: Annotated[Path | None, typer.Option(help="Output JSON file")] = None,
    limit: Annotated[int | None, typer.Option(help="Limit recipes to analyze")] = None,
):
    """Get statistics from the Firebase recipe database."""
    from plaite.firebase.stats import get_stats, print_stats

    console.print(f"[bold]Loading config for {env}...[/bold]")
    firebase_config = load_firebase_config(config, env)

    console.print(f"[bold]Connecting to Firebase ({env})...[/bold]")
    console.print(f"  Collection: {firebase_config.collection}")
    console.print(f"  Bucket: {firebase_config.storage_bucket}\n")

    recipe_stats = get_stats(firebase_config, limit=limit)

    # Print to console
    print_stats(recipe_stats, console)

    # Save to file if requested
    if output:
        with open(output, "w", encoding="utf-8") as f:
            json.dump(recipe_stats.to_dict(), f, indent=2, ensure_ascii=False)
        console.print(f"\n[green]Stats saved to {output}[/green]")


@app.command()
def upload(
    batch: Annotated[Path, typer.Argument(help="Path to batch JSON file")],
    env: Annotated[Env, typer.Option(help="Environment: prod or dev")] = "dev",
    config: Annotated[
        Path, typer.Option("--config", "-c", help="Firebase config path")
    ] = DEFAULT_FIREBASE_CONFIG,
    upload_config: Annotated[Path, typer.Option(help="Upload config path")] = DEFAULT_UPLOAD_CONFIG,
    images_dir: Annotated[Path | None, typer.Option(help="Directory with images")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Validate only, don't upload")] = False,
):
    """Upload a batch of recipes to Firebase."""
    from plaite.firebase.upload import upload_batch

    console.print(f"[bold]Loading config for {env}...[/bold]")
    firebase_config = load_firebase_config(config, env)
    upload_cfg = load_upload_config(upload_config)

    console.print(f"[bold]Target: Firebase ({env})[/bold]")
    console.print(f"  Collection: {firebase_config.collection}")
    console.print(f"  Batch file: {batch}\n")

    results = upload_batch(
        batch_path=batch,
        config=firebase_config,
        upload_config=upload_cfg,
        images_dir=images_dir,
        dry_run=dry_run,
        console=console,
    )

    if results["failed"]:
        console.print("\n[red]Failed uploads:[/red]")
        for fail in results["failed"][:10]:
            console.print(f"  {fail['id']}: {fail.get('error', 'Unknown')}")


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
