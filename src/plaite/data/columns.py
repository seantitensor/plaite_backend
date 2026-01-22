"""Column definitions for type-safe recipe queries."""

from enum import Enum


class RecipeColumn(str, Enum):
    """
    Available columns in the recipes dataset.

    This enum provides type-safe access to recipe column names for use in
    queries, filtering, and data selection. Using the enum prevents typos
    and enables IDE autocomplete.

    Examples
    --------
    >>> from plaite.data.columns import RecipeColumn
    >>> import plaite.data as data
    >>>
    >>> # Type-safe column selection
    >>> df = data.load_recipes(columns=[
    ...     RecipeColumn.RECIPE_ID,
    ...     RecipeColumn.TITLE,
    ...     RecipeColumn.HEALTH_SCORE
    ... ])
    >>>
    >>> # Type-safe filtering
    >>> df = data.filter_recipes({
    ...     RecipeColumn.HEALTH_GRADE: "A",
    ...     f"{RecipeColumn.HEALTH_SCORE}__gt": 70
    ... })
    """

    # Basic Information
    RECIPE_ID = "recipe_id"
    TITLE = "title"
    DESCRIPTION = "description"
    URL = "url"
    HOST = "host"
    IMAGE = "image"
    AUTHOR = "author"
    UUID = "uuid"

    # Content
    INSTRUCTIONS = "instructions"
    INGREDIENT_GROUPS = "ingredientGroups"
    INGREDIENTS = "ingredients"
    INGREDIENT_STRINGS = "ingredientStrings"  # List of ingredient strings
    PROCESSED_INGREDIENTS = "procesedIngredients"  # Note: typo in source data

    # Metadata
    TAGS = "tags"
    COOKING_METHOD = "cookingMethod"

    # Nutrition
    NUTRIENTS = "nutrients"
    HEALTH_SCORE = "healthScore"
    HEALTH_GRADE = "healthGrade"

    # Servings & Timing
    NUM_SERVINGS = "numServings"
    COOK_TIME = "cookTime"
    PREP_TIME = "prepTime"
    TOTAL_TIME = "totalTime"

    # Ratings & Reviews
    RATINGS = "ratings"
    RATINGS_COUNT = "ratingsCount"

    # ML/Clustering
    EMBEDDING = "embedding"
    CLUSTER_ID = "cluster_id"

    @classmethod
    def values(cls) -> list[str]:
        """
        Get all column names as strings.

        Returns
        -------
        list[str]
            List of all available column names.

        Examples
        --------
        >>> RecipeColumn.values()
        ['recipe_id', 'title', 'description', ...]
        """
        return [col.value for col in cls]

    @classmethod
    def has_column(cls, column_name: str) -> bool:
        """
        Check if a column name exists in the enum.

        Parameters
        ----------
        column_name : str
            The column name to check.

        Returns
        -------
        bool
            True if the column exists, False otherwise.

        Examples
        --------
        >>> RecipeColumn.has_column("title")
        True
        >>> RecipeColumn.has_column("invalid_column")
        False
        """
        return column_name in cls.values()
