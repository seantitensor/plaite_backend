"""Diagnostic: check embedding/cluster_id coverage in Firestore recipes.

Usage:
    uv run python scripts/check_embeddings.py --env dev
    uv run python scripts/check_embeddings.py --env prod --sample 500
"""

import argparse
import json
from collections import Counter
from pathlib import Path

from rich.console import Console
from rich.table import Table

from plaite.config import load_firebase_config
from plaite.firebase.client import get_collection

console = Console()


def check_embedding(value) -> tuple[bool, int]:
    """Return (is_valid, dim). Accepts list/tuple OR Firestore Vector type."""
    if value is None:
        return False, 0
    # Firestore Vector type — exposes .to_map_value() with {"value": [...]}
    if hasattr(value, "to_map_value"):
        try:
            inner = value.to_map_value().get("value", [])
            return (len(inner) > 0), len(inner)
        except Exception:
            pass
    # Firestore Vector may also be directly iterable
    if hasattr(value, "__iter__") and not isinstance(value, (str, bytes, dict)):
        try:
            as_list = list(value)
            if as_list and all(isinstance(x, (int, float)) for x in as_list[:3]):
                return True, len(as_list)
        except Exception:
            pass
    if isinstance(value, (list, tuple)) and len(value) > 0:
        if all(isinstance(x, (int, float)) for x in value[:3]):
            return True, len(value)
    return False, 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--sample", type=int, default=0, help="Limit to N docs (0 = all)")
    parser.add_argument("--show-sample", action="store_true", help="Print one full doc's top-level field shape")
    args = parser.parse_args()

    config_path = Path(__file__).parent.parent / "configs" / "firebase.yaml"
    config = load_firebase_config(config_path, env=args.env)

    console.print(f"[bold]Connecting to Firebase ({args.env})...[/bold]")
    collection = get_collection(config)

    query = collection
    if args.sample > 0:
        query = query.limit(args.sample)

    total = 0
    embedding_present = 0
    embedding_valid = 0
    cluster_present = 0
    channel_discover = 0
    tags_present = 0
    tags_non_empty = 0
    tag_counts_dist = Counter()
    embedding_dims = Counter()
    missing_embedding_sample_ids = []
    sample_doc_shape = None

    console.print("[bold]Scanning documents...[/bold]")
    for doc in query.stream():
        data = doc.to_dict() or {}
        total += 1

        if sample_doc_shape is None:
            sample_doc_shape = {
                k: (
                    f"list[{len(v)}]" if isinstance(v, list)
                    else f"dict({len(v)} keys)" if isinstance(v, dict)
                    else type(v).__name__
                )
                for k, v in data.items()
            }
            sample_doc_shape["__id__"] = doc.id

        if data.get("channel") == "discover":
            channel_discover += 1

        if "embedding" in data:
            embedding_present += 1
            valid, dim = check_embedding(data["embedding"])
            if valid:
                embedding_valid += 1
                embedding_dims[dim] += 1
            elif len(missing_embedding_sample_ids) < 5:
                missing_embedding_sample_ids.append((doc.id, f"invalid: {type(data['embedding']).__name__}"))
        elif len(missing_embedding_sample_ids) < 5:
            missing_embedding_sample_ids.append((doc.id, "missing field"))

        if data.get("cluster_id") is not None:
            cluster_present += 1

        tags = data.get("tags")
        if tags is not None:
            tags_present += 1
            if isinstance(tags, list) and len(tags) > 0:
                tags_non_empty += 1
                tag_counts_dist[len(tags)] += 1

        if total % 500 == 0:
            console.print(f"  ...{total} scanned")

    console.print()
    table = Table(title=f"Recipe Collection Coverage ({args.env})", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Count", justify="right")
    table.add_column("%", justify="right")

    def pct(n):
        return f"{(n / total * 100):.1f}%" if total else "—"

    table.add_row("Total docs scanned", str(total), "100%")
    table.add_row("channel == 'discover'", str(channel_discover), pct(channel_discover))
    table.add_row("Has 'embedding' field", str(embedding_present), pct(embedding_present))
    table.add_row("Embedding valid (non-empty list)", str(embedding_valid), pct(embedding_valid))
    table.add_row("Has 'cluster_id' field", str(cluster_present), pct(cluster_present))
    table.add_row("Has 'tags' field", str(tags_present), pct(tags_present))
    table.add_row("Tags non-empty", str(tags_non_empty), pct(tags_non_empty))
    console.print(table)

    if tag_counts_dist:
        console.print("\n[bold]Tag count distribution (non-empty docs):[/bold]")
        for n_tags, count in sorted(tag_counts_dist.items()):
            console.print(f"  {n_tags} tags: {count} docs")

    if embedding_dims:
        console.print("\n[bold]Embedding dimensions found:[/bold]")
        for dim, count in embedding_dims.most_common():
            console.print(f"  {dim}-dim: {count} docs ({count / total * 100:.1f}%)")

    if missing_embedding_sample_ids:
        console.print("\n[bold]Sample docs missing valid embedding:[/bold]")
        for rid, reason in missing_embedding_sample_ids:
            console.print(f"  {rid}: {reason}")

    if args.show_sample and sample_doc_shape:
        console.print("\n[bold]Sample doc field shape:[/bold]")
        console.print(json.dumps(sample_doc_shape, indent=2, default=str))

    console.print()
    if embedding_valid == 0:
        console.print("[bold red]❌ No valid embeddings found. Recommender v0 must run without embedding-based scoring.[/bold red]")
    elif embedding_valid / total < 0.8:
        console.print(f"[bold yellow]⚠️  Only {embedding_valid / total:.0%} coverage. Use embedding where present, fall back otherwise.[/bold yellow]")
    else:
        console.print(f"[bold green]✅ {embedding_valid / total:.0%} coverage. Embedding-based scoring is viable.[/bold green]")


if __name__ == "__main__":
    main()
