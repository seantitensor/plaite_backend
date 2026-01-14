"""Pre-built query templates for common data operations.

Add your custom query templates here once you know the data schema.
Use `get_recipes_columns()` to see available columns.

Example templates (uncomment and adjust based on your actual columns):

# recipes_healthy = (
#     recipes_table.scan()
#     .filter(
#         pl.col("calories").lt(500),
#         pl.col("protein_g").gt(10),
#     )
#     .sort("calories")
# )
#
# recipes_by_category = (
#     recipes_table.scan()
#     .group_by("category")
#     .agg(
#         pl.col("id").count().alias("count"),
#         pl.col("rating").mean().alias("avg_rating"),
#     )
#     .sort("count", descending=True)
# )
"""

from ._tables import recipes_table
