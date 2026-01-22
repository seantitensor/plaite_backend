"""Functions for loading and filtering recipe data."""

from typing import Any

import polars as pl

from ._tables import recipes_table
from .query import Filter


def load_recipes(columns: list[str] | None = None) -> pl.DataFrame:
    """
    Load the complete recipes dataset.

    This function eagerly loads data into memory. For large datasets,
    consider using `recipes_table.scan()` directly for lazy evaluation.

    Parameters
    ----------
    columns : list of str, optional
        List of column names to include in the result.
        If None, returns all columns.
        Use `get_recipes_columns()` to see available columns.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing recipe data.

    Raises
    ------
    ColumnNotFoundError
        If any specified column does not exist in the dataset.

    Examples
    --------
    >>> import plaite.data as data
    >>> # Load all columns
    >>> df = data.load_recipes()
    >>> print(f"Loaded {len(df)} recipes")
    >>>
    >>> # Load specific columns
    >>> df = data.load_recipes(columns=["recipe_id", "title", "healthScore"])
    >>>
    >>> # Using RecipeColumn enum for type safety
    >>> from plaite.data import RecipeColumn
    >>> df = data.load_recipes(columns=[
    ...     RecipeColumn.RECIPE_ID,
    ...     RecipeColumn.TITLE,
    ...     RecipeColumn.HEALTH_SCORE
    ... ])

    See Also
    --------
    get_filtered_recipes : Load with optional filtering
    filter_recipes : Load with complex filters
    recipes_table.scan : Lazy loading for large datasets
    """
    query = recipes_table.scan()

    if columns is not None:
        query = query.select(columns)

    return query.collect()


def filter_recipes(filters: dict[str, Any]) -> pl.DataFrame:
    """
    Load recipes with custom filters.

    Parameters
    ----------
    filters : dict
        Dictionary of column names to filter values.
        Supports comparison operators in keys (e.g., "healthScore__lt", "healthGrade__eq").

        Operators:
        - __eq or no suffix: Equal to
        - __ne: Not equal to
        - __lt: Less than
        - __le: Less than or equal to
        - __gt: Greater than
        - __ge: Greater than or equal to
        - __in: In list
        - __contains: String contains (case-insensitive)

    Returns
    -------
    pl.DataFrame
        A DataFrame containing filtered recipe data.

    Raises
    ------
    ValueError
        If an unknown column name or operator is provided.

    Examples
    --------
    >>> import plaite.data as data
    >>> df = data.filter_recipes({
    ...     "healthGrade": "A",
    ...     "healthScore__gt": 70,
    ...     "ratings__gte": 4.0
    ... })
    >>> # Using RecipeColumn enum for type safety
    >>> from plaite.data import RecipeColumn
    >>> df = data.filter_recipes({
    ...     RecipeColumn.HEALTH_GRADE: "A",
    ...     f"{RecipeColumn.HEALTH_SCORE}__gt": 70
    ... })
    """
    query = recipes_table.scan()
    schema = query.collect_schema()
    filter_exprs = []

    for key, value in filters.items():
        if "__" in key:
            col_name, op = key.rsplit("__", 1)
        else:
            col_name, op = key, "eq"

        # Validate column exists
        if col_name not in schema:
            raise ValueError(
                f"Unknown column: '{col_name}'. "
                f"Available columns: {list(schema.keys())}\n"
                f"Use get_recipes_columns() to see all available columns."
            )

        col = pl.col(col_name)

        if op == "eq":
            filter_exprs.append(col == value)
        elif op == "ne":
            filter_exprs.append(col != value)
        elif op == "lt":
            filter_exprs.append(col < value)
        elif op == "le":
            filter_exprs.append(col <= value)
        elif op == "gt":
            filter_exprs.append(col > value)
        elif op == "ge":
            filter_exprs.append(col >= value)
        elif op == "in":
            filter_exprs.append(col.is_in(value))
        elif op == "contains":
            filter_exprs.append(col.str.contains(str(value)))
        else:
            raise ValueError(f"Unknown operator: {op}")

    if filter_exprs:
        query = query.filter(*filter_exprs)

    return query.collect()


def get_recipes_columns() -> str:
    """
    Return the available columns in the recipes dataset.

    This function provides a schema of all recipe-level fields that can be
    retrieved with the load functions. The output is a table listing each
    column name along with its corresponding data type.

    Returns
    -------
    str
        A string representation of a Polars DataFrame containing the
        column names and types for the recipes table.

    Examples
    --------
    >>> import plaite.data as data
    >>> print(data.get_recipes_columns())
    shape: (25, 2)
    ┌──────────────────┬────────┐
    │ column           ┆ dtype  │
    │ ---              ┆ ---    │
    │ str              ┆ str    │
    ╞══════════════════╪════════╡
    │ recipe_id        ┆ String │
    │ title            ┆ String │
    │ description      ┆ String │
    │ ...              ┆ ...    │
    └──────────────────┴────────┘
    """
    return recipes_table.columns()


def get_batch_of_recipes(count: int, query: dict[str, Any] | None = None) -> pl.DataFrame:
    """
    Get a batch of recipes with optional filtering.

    Useful for pagination or getting a sample of recipes for preview purposes.

    Parameters
    ----------
    count : int
        Number of recipes to return. Must be non-negative.
    query : dict, optional
        Query filters to apply before limiting results.
        See `filter_recipes()` for supported operators.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing up to 'count' recipes.
        May return fewer if not enough recipes match the filters.

    Raises
    ------
    ValueError
        If count is negative, or if query contains invalid columns/operators.

    Examples
    --------
    >>> import plaite.data as data
    >>> # Get first 10 recipes
    >>> df = data.get_batch_of_recipes(count=10)
    >>>
    >>> # Get 5 high-rated recipes
    >>> df = data.get_batch_of_recipes(
    ...     count=5,
    ...     query={"ratings__gte": 4.5}
    ... )
    >>>
    >>> # Using RecipeColumn enum
    >>> from plaite.data import RecipeColumn
    >>> df = data.get_batch_of_recipes(
    ...     count=10,
    ...     query={f"{RecipeColumn.HEALTH_GRADE}": "A"}
    ... )

    See Also
    --------
    get_filtered_recipes : Get all recipes with optional filtering
    filter_recipes : Advanced filtering with operators
    """
    if count < 0:
        raise ValueError(f"count must be non-negative, got: {count}")

    if count == 0:
        # Return empty DataFrame with correct schema
        return pl.DataFrame(schema=recipes_table.scan().collect_schema())

    if query:
        df = filter_recipes(query)
    else:
        df = load_recipes()
    return df.head(count)


def get_stats_of_all_recipes() -> dict[str, Any]:
    """
    Get comprehensive statistics about all recipes dataset.

    This function efficiently computes statistics using lazy evaluation
    to avoid loading the entire dataset into memory unnecessarily.

    Returns
    -------
    dict
        Dictionary containing:
        - total_recipes : int
            Total number of recipes in the dataset
        - total_columns : int
            Number of columns in the dataset
        - columns : list[str]
            List of all column names
        - schema : dict[str, str]
            Column names mapped to their data types
        - recipes_per_cluster : dict[int, int]
            Count of recipes in each cluster_id
        - recipes_per_health_grade : dict[str, int]
            Count of recipes for each health grade (A, B, C, etc.)
        - unique_ingredients_count : int
            Total number of unique ingredients across all recipes

    Examples
    --------
    >>> import plaite.data as data
    >>> stats = data.get_stats_of_all_recipes()
    >>> print(f"Total recipes: {stats['total_recipes']}")
    >>> print(f"Unique ingredients: {stats['unique_ingredients_count']}")
    >>> print(f"Health grades: {stats['recipes_per_health_grade']}")
    >>> print(f"Cluster distribution: {stats['recipes_per_cluster']}")

    See Also
    --------
    get_recipes_columns : Get detailed column information with formatting
    """
    # Use lazy evaluation for efficiency
    lazy_df = recipes_table.scan()
    schema = lazy_df.collect_schema()

    # Collect basic stats
    total_recipes = lazy_df.select(pl.len()).collect().item()

    # Get cluster distribution
    cluster_counts = (
        lazy_df.group_by("cluster_id")
        .agg(pl.len().alias("count"))
        .sort("cluster_id")
        .collect()
    )
    recipes_per_cluster = dict(
        zip(
            cluster_counts["cluster_id"].to_list(),
            cluster_counts["count"].to_list(),
            strict=True,
        )
    )

    # Get health grade distribution
    health_grade_counts = (
        lazy_df.group_by("healthGrade")
        .agg(pl.len().alias("count"))
        .sort("healthGrade")
        .collect()
    )
    recipes_per_health_grade = dict(
        zip(
            health_grade_counts["healthGrade"].to_list(),
            health_grade_counts["count"].to_list(),
            strict=True,
        )
    )

    # Count unique ingredients (explode list and count distinct)
    unique_ingredients = (
        lazy_df.select(pl.col("ingredients").explode().unique().len())
        .collect()
        .item()
    )

    return {
        "total_recipes": total_recipes,
        "total_columns": len(schema),
        "columns": list(schema.keys()),
        "schema": {k: str(v) for k, v in schema.items()},
        "recipes_per_cluster": recipes_per_cluster,
        "recipes_per_health_grade": recipes_per_health_grade,
        "unique_ingredients_count": unique_ingredients,
    }


def get_filtered_recipes(*filters: Filter, query: dict[str, Any] | None = None) -> pl.DataFrame:
    """
    Get recipes with optional filtering.

    Supports both the new Col-based query builder and legacy dict-based queries.

    Parameters
    ----------
    *filters : Filter
        Filter expressions created using Col (e.g., Col.title.contains("chicken")).
    query : dict, optional
        Legacy query filters. See `filter_recipes()` for supported operators.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing filtered or all recipe data.

    Examples
    --------
    >>> from plaite.data import Col, get_filtered_recipes
    >>>
    >>> # Using Col query builder (recommended)
    >>> df = get_filtered_recipes(
    ...     Col.health_grade.eq("A"),
    ...     Col.health_score.gt(70),
    ...     Col.title.contains("chicken"),
    ... )
    >>>
    >>> # Get all recipes
    >>> df = get_filtered_recipes()

    See Also
    --------
    Col : Column accessor for building queries
    load_recipes : Load all recipes without filtering
    """
    lf = recipes_table.scan()

    # Apply Col-based filters
    if filters:
        for f in filters:
            lf = lf.filter(f.to_polars_expr())

    # Apply legacy dict-based filters
    if query:
        df = filter_recipes(query)
        if filters:
            # Combine: apply Col filters to the dict-filtered result
            return lf.collect().join(df, on="recipe_id", how="inner")
        return df

    return lf.collect()


# Convenience alias for cleaner API
get_recipes = get_filtered_recipes
