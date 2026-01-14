"""
Data loading module for Plaite recipes using Polars.

This module provides efficient recipe data loading with filtering capabilities.
All functions return Polars DataFrames for high-performance data manipulation.

Quick Start
-----------
>>> import plaite.data as data
>>> from plaite.data import RecipeColumn
>>>
>>> # Load all recipes
>>> recipes = data.load_recipes()
>>>
>>> # Load specific columns
>>> recipes = data.load_recipes(columns=[
...     RecipeColumn.RECIPE_ID,
...     RecipeColumn.TITLE,
...     RecipeColumn.HEALTH_SCORE
... ])
>>>
>>> # Filter recipes with type-safe columns
>>> healthy = data.filter_recipes({
...     RecipeColumn.HEALTH_SCORE: {"__gte": 70},
...     RecipeColumn.COOK_TIME: {"__lt": 30}
... })
>>>
>>> # Get a batch of recipes
>>> sample = data.get_batch_of_recipes(count=10, query={
...     RecipeColumn.HEALTH_GRADE: "A"
... })
>>>
>>> # Get dataset statistics
>>> stats = data.get_stats_of_all_recipes()
>>> print(f"Total recipes: {stats['total_recipes']}")
>>>
>>> # View available columns
>>> print(data.get_recipes_columns())

Available Filter Operators
---------------------------
- __eq or no suffix: Equal to
- __ne: Not equal to
- __lt: Less than
- __le: Less than or equal to
- __gt: Greater than
- __ge: Greater than or equal to
- __in: In list
- __contains: String contains (case-insensitive)

Performance Tips
----------------
- Use `columns` parameter to load only needed columns
- Use `RecipeColumn` enum for type-safe column references
- Use lazy evaluation via `recipes_table.scan()` for very large datasets
- Filter early to reduce memory usage

See Also
--------
plaite.data.loader : Core loading functions
plaite.data.columns : Column enum for type-safe queries
plaite.data._tables : Table abstraction layer
"""

from .loader import (
    load_recipes,
    filter_recipes,
    get_recipes_columns,
    get_batch_of_recipes,
    get_stats_of_all_recipes,
    get_recipes,
)
from ._tables import recipes_table
from .columns import RecipeColumn

__all__ = [
    "load_recipes",
    "filter_recipes",
    "get_recipes_columns",
    "recipes_table",
    "get_batch_of_recipes",
    "get_stats_of_all_recipes",
    "get_recipes",
    "RecipeColumn",
]
