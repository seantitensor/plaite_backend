"""Query builder for simple, fluent recipe filtering.

Example usage:
    from plaite.data import Col, get_recipes

    recipes = get_recipes(
        Col.title.contains("chicken"),
        Col.health_grade.eq("A"),
        Col.health_score.gt(70),
    )
"""

from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass
class Filter:
    """A filter expression to apply to a query."""

    column: str
    operator: str
    value: Any

    def to_polars_expr(self) -> pl.Expr:
        """Convert this filter to a Polars expression."""
        col = pl.col(self.column)

        if self.operator == "eq":
            return col == self.value
        elif self.operator == "ne":
            return col != self.value
        elif self.operator == "lt":
            return col < self.value
        elif self.operator == "lte":
            return col <= self.value
        elif self.operator == "gt":
            return col > self.value
        elif self.operator == "gte":
            return col >= self.value
        elif self.operator == "is_in":
            return col.is_in(self.value)
        elif self.operator == "contains":
            return col.str.contains(f"(?i){self.value}")
        elif self.operator == "list_any_contains":
            # Check if any element in a list column contains the value (case-insensitive)
            return col.list.eval(pl.element().str.contains(f"(?i){self.value}")).list.any()
        else:
            raise ValueError(f"Unknown operator: {self.operator}")


class Column:
    """A column reference with filter methods."""

    def __init__(self, name: str):
        self._name = name

    def eq(self, value: Any) -> Filter:
        """Equal to."""
        return Filter(self._name, "eq", value)

    def ne(self, value: Any) -> Filter:
        """Not equal to."""
        return Filter(self._name, "ne", value)

    def gt(self, value: Any) -> Filter:
        """Greater than."""
        return Filter(self._name, "gt", value)

    def gte(self, value: Any) -> Filter:
        """Greater than or equal to."""
        return Filter(self._name, "gte", value)

    def lt(self, value: Any) -> Filter:
        """Less than."""
        return Filter(self._name, "lt", value)

    def lte(self, value: Any) -> Filter:
        """Less than or equal to."""
        return Filter(self._name, "lte", value)

    def is_in(self, values: list[Any]) -> Filter:
        """In a list of values."""
        return Filter(self._name, "is_in", values)

    def contains(self, value: str) -> Filter:
        """String contains (case-insensitive)."""
        return Filter(self._name, "contains", value)

    def list_any_contains(self, value: str) -> Filter:
        """Any element in a list contains the value (case-insensitive)."""
        return Filter(self._name, "list_any_contains", value)


class _Col:
    """Column accessor for building queries.

    Access columns as attributes:
        Col.title.contains("chicken")
        Col.health_score.gt(70)
    """

    # Basic Information
    recipe_id = Column("recipe_id")
    title = Column("title")
    description = Column("description")
    url = Column("url")
    host = Column("host")
    image = Column("image")
    author = Column("author")
    uuid = Column("uuid")

    # Content
    instructions = Column("instructions")
    ingredient_groups = Column("ingredientGroups")
    ingredients = Column("ingredients")
    ingredient_strings = Column("ingredientStrings")
    processed_ingredients = Column("procesedIngredients")

    # Metadata
    tags = Column("tags")
    cooking_method = Column("cookingMethod")

    # Nutrition
    nutrients = Column("nutrients")
    health_score = Column("healthScore")
    health_grade = Column("healthGrade")

    # Servings & Timing
    num_servings = Column("numServings")
    cook_time = Column("cookTime")
    prep_time = Column("prepTime")
    total_time = Column("totalTime")

    # Ratings & Reviews
    ratings = Column("ratings")
    ratings_count = Column("ratingsCount")

    # ML/Clustering
    embedding = Column("embedding")
    cluster_id = Column("cluster_id")


Col = _Col()
