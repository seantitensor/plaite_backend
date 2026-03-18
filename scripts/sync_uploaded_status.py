#!/usr/bin/env python3
"""
Sync uploaded status from prod Firebase to local parquet.

Fetches all recipe IDs currently in the prod Firebase collection and marks
them as uploaded=True in the local parquet via status.py.

Usage:
    uv run python scripts/sync_uploaded_status.py
    uv run python scripts/sync_uploaded_status.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console

load_dotenv(override=True)

console = Console()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync uploaded status from prod Firebase.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be marked without writing")
    args = parser.parse_args()

    try:
        from plaite.config import load_firebase_config
        from plaite.firebase.client import get_collection
        from plaite.data.status import mark_uploaded
    except ImportError as e:
        console.print(f"[red]Import error: {e}[/red]")
        sys.exit(1)

    config_path = Path(__file__).parent.parent / "configs" / "firebase.yaml"
    config = load_firebase_config(config_path, "prod")

    console.print(f"[bold]Fetching recipe IDs from prod Firebase...[/bold]")
    collection = get_collection(config)
    recipe_ids = [doc.id for doc in collection.select([]).stream()]
    console.print(f"Found [bold]{len(recipe_ids)}[/bold] recipes in prod Firebase.")

    if args.dry_run:
        console.print(f"\n[yellow]DRY RUN — would mark {len(recipe_ids)} recipes as uploaded.[/yellow]")
        for rid in recipe_ids[:10]:
            console.print(f"  {rid}")
        if len(recipe_ids) > 10:
            console.print(f"  ... and {len(recipe_ids) - 10} more")
        return

    console.print(f"\nMarking {len(recipe_ids)} recipes as uploaded in local parquet...")
    mark_uploaded(recipe_ids)
    console.print(f"[bold green]Done.[/bold green]")


if __name__ == "__main__":
    main()
