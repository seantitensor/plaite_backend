"""Firebase stats collection and analysis."""

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from rich.console import Console
from rich.table import Table
from tqdm import tqdm

from plaite.config import FirebaseConfig
from plaite.firebase.client import get_collection


@dataclass
class RecipeStats:
    """Statistics about recipes in the database."""

    total_recipes: int = 0

    # Tag distribution
    tag_counts: Counter = field(default_factory=Counter)

    # Ingredient distribution
    ingredient_counts: Counter = field(default_factory=Counter)

    # Nutrient coverage
    recipes_with_nutrients: int = 0
    nutrient_field_counts: Counter = field(default_factory=Counter)

    # Missing fields analysis
    missing_fields: dict[str, int] = field(default_factory=dict)

    # Required fields to check
    REQUIRED_FIELDS: list[str] = field(
        default_factory=lambda: [
            "title",
            "instructions",
            "ingredients",
            "tags",
            "image",
            "description",
            "numServings",
            "cookTime",
            "prepTime",
        ]
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert stats to dictionary for JSON export."""
        return {
            "total_recipes": self.total_recipes,
            "tag_distribution": dict(self.tag_counts.most_common()),
            "ingredient_distribution": dict(self.ingredient_counts.most_common(100)),
            "nutrients": {
                "recipes_with_nutrients": self.recipes_with_nutrients,
                "coverage_percent": round(self.recipes_with_nutrients / self.total_recipes * 100, 2)
                if self.total_recipes > 0
                else 0,
                "field_counts": dict(self.nutrient_field_counts.most_common()),
            },
            "missing_fields": self.missing_fields,
        }


def get_stats(config: FirebaseConfig, limit: int | None = None) -> RecipeStats:
    """
    Collect statistics from all recipes in the database.

    Args:
        config: Firebase configuration
        limit: Optional limit on number of recipes to analyze (for testing)

    Returns:
        RecipeStats object with collected statistics
    """
    collection = get_collection(config)
    stats = RecipeStats()

    # Get all documents
    query = collection.limit(limit) if limit else collection
    docs = list(query.stream())
    stats.total_recipes = len(docs)

    # Initialize missing fields counter
    for field_name in stats.REQUIRED_FIELDS:
        stats.missing_fields[field_name] = 0

    for doc in tqdm(docs, desc="Analyzing recipes"):
        data = doc.to_dict()

        # Tag distribution
        tags = data.get("tags", [])
        if isinstance(tags, list):
            stats.tag_counts.update(tags)

        # Ingredient distribution (extract ingredient names)
        ingredients = data.get("ingredients", [])
        if isinstance(ingredients, list):
            for ing in ingredients:
                if isinstance(ing, dict):
                    # Try to get ingredient name from various possible fields
                    name = ing.get("name") or ing.get("displayString", "")
                    if name:
                        # Normalize: lowercase, strip
                        stats.ingredient_counts[name.lower().strip()] += 1
                elif isinstance(ing, str):
                    stats.ingredient_counts[ing.lower().strip()] += 1

        # Also check ingredientStrings if present
        ingredient_strings = data.get("ingredientStrings", [])
        if isinstance(ingredient_strings, list):
            for ing in ingredient_strings:
                if isinstance(ing, str):
                    stats.ingredient_counts[ing.lower().strip()] += 1

        # Nutrient coverage
        nutrients = data.get("nutrients")
        if nutrients and (
            isinstance(nutrients, list)
            and len(nutrients) > 0
            or isinstance(nutrients, dict)
            and len(nutrients) > 0
        ):
            stats.recipes_with_nutrients += 1

            # Count which nutrient fields are present
            if isinstance(nutrients, list):
                for nutrient in nutrients:
                    if isinstance(nutrient, dict):
                        name = nutrient.get("name", "unknown")
                        stats.nutrient_field_counts[name] += 1
            elif isinstance(nutrients, dict):
                stats.nutrient_field_counts.update(nutrients.keys())

        # Missing fields analysis
        for field_name in stats.REQUIRED_FIELDS:
            value = data.get(field_name)
            if value is None or value == "" or value == []:
                stats.missing_fields[field_name] += 1

    return stats


def print_stats(stats: RecipeStats, console: Console | None = None):
    """Print stats in a formatted table."""
    if console is None:
        console = Console()

    console.print(f"\n[bold]Total Recipes:[/bold] {stats.total_recipes}\n")

    # Tag distribution table
    console.print("[bold]Tag Distribution (Top 30):[/bold]")
    tag_table = Table(show_header=True, header_style="bold")
    tag_table.add_column("Tag")
    tag_table.add_column("Count", justify="right")
    tag_table.add_column("%", justify="right")

    for tag, count in stats.tag_counts.most_common(30):
        pct = round(count / stats.total_recipes * 100, 1)
        tag_table.add_row(tag, str(count), f"{pct}%")

    console.print(tag_table)

    # Ingredient distribution table
    console.print("\n[bold]Ingredient Distribution (Top 30):[/bold]")
    ing_table = Table(show_header=True, header_style="bold")
    ing_table.add_column("Ingredient")
    ing_table.add_column("Count", justify="right")

    for ing, count in stats.ingredient_counts.most_common(30):
        ing_table.add_row(ing[:50], str(count))  # Truncate long names

    console.print(ing_table)

    # Nutrient coverage
    console.print("\n[bold]Nutrient Coverage:[/bold]")
    coverage_pct = (
        round(stats.recipes_with_nutrients / stats.total_recipes * 100, 1)
        if stats.total_recipes > 0
        else 0
    )
    console.print(f"  Recipes with nutrients: {stats.recipes_with_nutrients} ({coverage_pct}%)")

    if stats.nutrient_field_counts:
        console.print("  [dim]Top nutrient fields:[/dim]")
        for nutrient, count in stats.nutrient_field_counts.most_common(10):
            console.print(f"    {nutrient}: {count}")

    # Missing fields table
    console.print("\n[bold]Missing Fields:[/bold]")
    missing_table = Table(show_header=True, header_style="bold")
    missing_table.add_column("Field")
    missing_table.add_column("Missing", justify="right")
    missing_table.add_column("% Missing", justify="right")

    for field_name, count in sorted(stats.missing_fields.items(), key=lambda x: -x[1]):
        pct = round(count / stats.total_recipes * 100, 1) if stats.total_recipes > 0 else 0
        missing_table.add_row(field_name, str(count), f"{pct}%")

    console.print(missing_table)
