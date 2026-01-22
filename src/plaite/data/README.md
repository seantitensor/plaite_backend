# Plaite Data Module

Efficient data loading for Plaite recipes using Polars with parquet file backend.

## Setup

### Environment Variable

Add to your `.env` file:

```bash
RECIPES_PATH=/path/to/your/recipes.parquet
```

The `RECIPES_PATH` should point to a parquet file containing recipe data structured as a DataFrame with recipes as rows.

## Quick Start

```python
import plaite.data as data
from plaite.data import RecipeColumn

# Load all recipes
recipes = data.load_recipes()

# Get column information
print(data.get_recipes_columns())

# Filter recipes with type-safe columns
healthy_recipes = data.filter_recipes({
    RecipeColumn.HEALTH_SCORE: {"__gte": 70},
    RecipeColumn.COOK_TIME: {"__lt": 30}
})

# Get a batch of recipes with filters
quick_healthy = data.get_batch_of_recipes(
    count=10,
    query={
        RecipeColumn.HEALTH_GRADE: "A",
        RecipeColumn.PREP_TIME: {"__lt": 15}
    }
)

# Get dataset statistics
stats = data.get_stats_of_all_recipes()
print(f"Total recipes: {stats['total_recipes']}")
print(f"Recipes per cluster: {stats['recipes_per_cluster']}")
print(f"Health grade distribution: {stats['recipes_per_health_grade']}")
print(f"Unique ingredients: {stats['unique_ingredients_count']}")
```

## Available Functions

### `load_recipes() -> pl.DataFrame`

Load all recipes from the dataset.

```python
recipes = data.load_recipes()
print(f"Loaded {len(recipes)} recipes")
```

### `filter_recipes(filters: dict) -> pl.DataFrame`

Filter recipes using query operators.

**Filter Operators:**
- `__eq` or no suffix: Equal to
- `__ne`: Not equal to
- `__lt`: Less than
- `__le`: Less than or equal to
- `__gt`: Greater than
- `__ge`: Greater than or equal to
- `__in`: In list
- `__contains`: String contains (case-insensitive)

```python
# Single condition
desserts = data.filter_recipes({
    RecipeColumn.TITLE: {"__contains": "chocolate"}
})

# Multiple conditions
healthy_quick = data.filter_recipes({
    RecipeColumn.HEALTH_SCORE: {"__gte": 70},
    RecipeColumn.COOK_TIME: {"__lt": 30},
    RecipeColumn.HEALTH_GRADE: {"__in": ["A", "B"]}
})
```

### `get_batch_of_recipes(count: int, query: dict | None = None) -> pl.DataFrame`

Get a limited number of recipes, optionally filtered.

```python
# Get first 5 recipes
batch = data.get_batch_of_recipes(5)

# Get 10 high-protein recipes
protein_batch = data.get_batch_of_recipes(
    count=10,
    query={RecipeColumn.PROTEIN: {"__gte": 20}}
)

# Empty DataFrame with schema (count=0)
empty = data.get_batch_of_recipes(0)
```

### `get_stats_of_all_recipes() -> dict`

Get comprehensive dataset statistics using lazy evaluation.

```python
stats = data.get_stats_of_all_recipes()

# Returns:
{
    "total_recipes": 14706,
    "total_columns": 25,
    "columns": ["recipe_id", "title", ...],
    "schema": {"recipe_id": "Int64", "title": "String", ...},
    "recipes_per_cluster": {0: 1234, 1: 2345, ...},
    "recipes_per_health_grade": {"A": 3456, "B": 4567, ...},
    "unique_ingredients_count": 8234
}
```

### `get_recipes_columns() -> str`

Get a formatted string of all available columns with their types.

```python
print(data.get_recipes_columns())
```

## Available Columns (RecipeColumn Enum)

The `RecipeColumn` enum provides type-safe column access:

**Basic Information:**
- `RECIPE_ID`: Recipe identifier
- `TITLE`: Recipe title
- `DESCRIPTION`: Recipe description
- `INGREDIENTS`: List of ingredients
- `STEPS`: Cooking steps

**Nutritional Information:**
- `CALORIES`: Calorie count
- `FAT`: Fat content (g)
- `CARBS`: Carbohydrate content (g)
- `PROTEIN`: Protein content (g)
- `SUGAR`: Sugar content (g)
- `SODIUM`: Sodium content (mg)

**Health Metrics:**
- `HEALTH_SCORE`: Numerical health score (0-100)
- `HEALTH_GRADE`: Letter grade (A, B, C, D, F)

**Timing:**
- `PREP_TIME`: Preparation time (minutes)
- `COOK_TIME`: Cooking time (minutes)
- `TOTAL_TIME`: Total time (minutes)

**Categorization:**
- `CLUSTER_ID`: Recipe cluster identifier
- `TAGS`: Recipe tags/categories
- `CUISINE`: Cuisine type
- `MEAL_TYPE`: Meal category

**User Engagement:**
- `RATING`: User rating (0-5)
- `REVIEW_COUNT`: Number of reviews
- `SERVINGS`: Number of servings

**Media:**
- `IMAGE_URL`: Recipe image URL

```python
# Use enum for type safety
from plaite.data import RecipeColumn

# Check if column exists
if RecipeColumn.has_column("calories"):
    print("Column exists!")

# Get all column names
all_columns = RecipeColumn.values()
```

## Module Structure

```
data/
├── __init__.py          # Public API exports
├── _tables.py           # Table class for parquet loading
├── columns.py           # RecipeColumn enum definition
├── loader.py            # High-level query functions
└── README.md            # This file
```

## Advanced Usage

### Direct Table Access

For custom queries, use the table object directly:

```python
from plaite.data import recipes_table
import polars as pl

# Lazy query (efficient for large datasets)
result = (
    recipes_table.scan()
    .filter(pl.col(RecipeColumn.HEALTH_SCORE) > 80)
    .group_by(RecipeColumn.CLUSTER_ID)
    .agg([
        pl.len().alias("count"),
        pl.col(RecipeColumn.HEALTH_SCORE).mean().alias("avg_health_score")
    ])
    .sort("avg_health_score", descending=True)
    .collect()
)
```

### Error Handling

```python
from plaite.data import filter_recipes, RecipeColumn

try:
    recipes = filter_recipes({
        RecipeColumn.HEALTH_SCORE: {"__gte": 70},
        "invalid_column": 100  # Will raise ValueError
    })
except ValueError as e:
    print(f"Invalid column: {e}")
    # Error includes list of valid columns
```

## Performance Tips

1. **Use lazy evaluation**: The `recipes_table.scan()` API uses lazy evaluation for efficiency
2. **Filter early**: Apply filters before collecting data with `.collect()`
3. **Use get_batch_of_recipes()**: For limiting results, use this instead of `.head()` on full dataset
4. **Validate column names**: Use `RecipeColumn` enum to catch typos at development time

## Complete Example

```python
import plaite.data as data
from plaite.data import RecipeColumn

# 1. Check dataset stats
stats = data.get_stats_of_all_recipes()
print(f"Dataset has {stats['total_recipes']} recipes")
print(f"Cluster distribution: {stats['recipes_per_cluster']}")

# 2. View available columns
print(data.get_recipes_columns())

# 3. Find healthy, quick recipes
healthy_quick = data.filter_recipes({
    RecipeColumn.HEALTH_GRADE: {"__in": ["A", "B"]},
    RecipeColumn.TOTAL_TIME: {"__lt": 30},
    RecipeColumn.RATING: {"__gte": 4.0}
})

# 4. Get top 10 by health score
top_recipes = (
    healthy_quick
    .sort(RecipeColumn.HEALTH_SCORE, descending=True)
    .head(10)
)

# 5. Display results
for row in top_recipes.iter_rows(named=True):
    print(f"{row['title']}: {row['healthScore']}/100")
```

## Testing

Run tests with:

```bash
uv run python -m pytest tests/test_data_module.py -v
```

Coverage: 26 tests, 100% passing ✅
