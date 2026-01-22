"""
Data loading module for Plaite recipes using Polars.

This module provides efficient recipe data loading with filtering capabilities.
All functions return Polars DataFrames for high-performance data manipulation.

Quick Start
-----------
>>> from plaite.data import Col, get_recipes
>>>
>>> # Get all recipes
>>> recipes = get_recipes()
>>>
>>> # Filter with the Col query builder
>>> recipes = get_recipes(
...     Col.health_grade.eq("A"),
...     Col.health_score.gt(70),
...     Col.title.contains("chicken"),
... )
>>>
>>> # Get dataset statistics
>>> import plaite.data as data
>>> stats = data.get_stats_of_all_recipes()
>>> print(f"Total recipes: {stats['total_recipes']}")

Col Query Methods
-----------------
- eq(value): Equal to
- ne(value): Not equal to
- lt(value): Less than
- lte(value): Less than or equal to
- gt(value): Greater than
- gte(value): Greater than or equal to
- is_in([values]): In list
- contains(str): String contains (case-insensitive)
- list_any_contains(str): Any element in a list contains (case-insensitive)

Available Columns
-----------------
Col.recipe_id, Col.title, Col.description, Col.url, Col.host, Col.image,
Col.author, Col.uuid, Col.instructions, Col.ingredient_groups, Col.ingredients,
Col.ingredient_strings, Col.processed_ingredients, Col.tags, Col.cooking_method,
Col.nutrients, Col.health_score, Col.health_grade, Col.num_servings, Col.cook_time,
Col.prep_time, Col.total_time, Col.ratings, Col.ratings_count,
Col.embedding, Col.cluster_id

Performance Tips
----------------
- Filter early to reduce memory usage
- Use lazy evaluation via `recipes_table.scan()` for very large datasets

See Also
--------
plaite.data.loader : Core loading functions
plaite.data.query : Col query builder
"""

from ._tables import recipes_table
from .columns import RecipeColumn
from .loader import (
    filter_recipes,
    get_batch_of_recipes,
    get_filtered_recipes,
    get_recipes,
    get_recipes_columns,
    get_stats_of_all_recipes,
    load_recipes,
)
from .query import Col

__all__ = [
    # Primary API
    "Col",
    "get_recipes",
    # Other functions
    "get_filtered_recipes",
    "load_recipes",
    "filter_recipes",
    "get_recipes_columns",
    "get_batch_of_recipes",
    "get_stats_of_all_recipes",
    "recipes_table",
    "RecipeColumn",
]
