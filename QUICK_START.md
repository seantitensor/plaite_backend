# Plaite Data Module - Quick Start

I've created a data module for loading your recipes using Polars, following the pattern you provided.

## What Was Created

```
src/plaite/data/
├── __init__.py          # Public API exports
├── _tables.py           # Table class supporting both .pkl and .parquet
├── _queries.py          # Pre-built query templates (placeholder)
├── loader.py            # Loading and filtering functions
└── README.md            # Full documentation

examples/
└── test_data_module.py  # Test script to verify everything works
```

## Key Features

1. **Supports both pickle and parquet files** - Auto-detected from file extension
2. **Lazy evaluation** - Efficient memory usage with Polars LazyFrames
3. **Flexible filtering** - Django-style filter operators (`__lt`, `__gte`, etc.)
4. **Direct table access** - For advanced custom queries

## Quick Test

Run the test script to verify everything works:

```bash
cd /Users/seantitensor/Documents/plaite-app/plaite_backend
python examples/test_data_module.py
```

This will:
1. Show you all available columns in your pickle file
2. Load the complete dataset
3. Test column selection
4. Demonstrate filtering capabilities

## Basic Usage

```python
import plaite.data as data

# 1. See what columns are available
print(data.get_recipes_columns())

# 2. Load all data
df = data.load_recipes()

# 3. Load specific columns only
df = data.load_recipes(columns=["id", "name", "ingredients"])

# 4. Filter data with Django-style operators
df = data.filter_recipes({
    "category": "dessert",
    "calories__lt": 500,
    "rating__gte": 4.0
})

# 5. Advanced queries with direct table access
import polars as pl
df = (
    data.recipes_table.scan()
    .filter(pl.col("calories") < 500)
    .group_by("category")
    .agg(pl.col("id").count())
    .collect()
)
```

## Filter Operators

The `filter_recipes()` function supports these operators:

- `column__eq` or `column`: Equal to
- `column__ne`: Not equal to
- `column__lt`: Less than
- `column__le`: Less than or equal to
- `column__gt`: Greater than
- `column__ge`: Greater than or equal to
- `column__in`: In list `[val1, val2, ...]`
- `column__contains`: String contains (case-insensitive)

## Next Steps

1. **Run the test script** to see your data structure
2. **Update `_queries.py`** with your own pre-built queries based on actual columns
3. **Add custom loader functions** to `loader.py` if needed
4. **Consider converting to parquet** for better performance on large datasets

## Converting Pickle to Parquet (Optional)

For better performance with large datasets:

```python
import polars as pl
import pickle

# Load pickle
with open("/path/to/recipes.pkl", "rb") as f:
    data = pickle.load(f)

# Convert to Polars and save as parquet
df = pl.DataFrame(data)  # Or pl.from_pandas(data) if pandas
df.write_parquet("/path/to/recipes.parquet")
```

Then update your `.env`:
```bash
RECIPES_PATH=/path/to/recipes.parquet
```

## Documentation

See [src/plaite/data/README.md](src/plaite/data/README.md) for complete documentation.
