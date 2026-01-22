#!/usr/bin/env python3
"""Restructure recipes pickle file from columns to rows format."""

import os
import pickle

import polars as pl
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

def restructure_recipes():
    """Transform recipes from columns format to rows format."""

    # Get the recipes path from environment
    recipes_path = os.getenv("RECIPES_PATH")
    if not recipes_path:
        print("❌ Error: RECIPES_PATH not found in .env file")
        return

    print(f"📂 Loading data from: {recipes_path}")

    # Load the original pickle file
    with open(recipes_path, 'rb') as f:
        data = pickle.load(f)

    # Convert to Polars DataFrame if it isn't already
    if isinstance(data, dict):
        df = pl.DataFrame(data)
    elif isinstance(data, pl.DataFrame):
        df = data
    else:
        print(f"❌ Unexpected data type: {type(data)}")
        return

    print(f"📊 Original shape: {df.shape} (rows × columns)")
    print(f"   This means: {df.shape[1]} recipes as columns")

    # Transform: recipes from columns to rows
    print("\n🔄 Transforming recipes from columns to rows...")

    recipe_ids = []
    recipe_data_list = []

    for col_name in df.columns:
        recipe_ids.append(col_name)
        # Get the first (and only) row's value for this column
        recipe_data_list.append(df[col_name][0])

    # Create normalized DataFrame
    normalized_df = pl.DataFrame({
        "recipe_id": recipe_ids,
        "recipe_data": recipe_data_list
    })

    print(f"✅ Normalized shape: {normalized_df.shape} (rows × columns)")
    print(f"   This means: {normalized_df.shape[0]} recipes as rows")

    # Unnest the struct to flatten all fields
    print("\n🔓 Unnesting recipe data struct...")

    try:
        # Unnest the struct column to create individual columns for each field
        unnested_df = normalized_df.unnest("recipe_data")
        print(f"✅ Final shape: {unnested_df.shape}")
        print(f"   Columns: {unnested_df.columns[:5]}... (showing first 5)")

        # Show sample
        print("\n📋 Sample of restructured data:")
        print(unnested_df.head(3))

    except Exception as e:
        print(f"⚠️  Could not unnest struct: {e}")
        print("   Keeping recipe_data as struct column")
        unnested_df = normalized_df

    # Create backup of original file
    backup_path = recipes_path + ".backup"
    print(f"\n💾 Creating backup: {backup_path}")
    with open(backup_path, 'wb') as f:
        pickle.dump(data, f)

    # Save the restructured data
    print(f"💾 Saving restructured data to: {recipes_path}")
    with open(recipes_path, 'wb') as f:
        pickle.dump(unnested_df, f)

    print("\n✅ Done! Your data has been restructured.")
    print(f"   Original backup saved as: {backup_path}")
    print(f"   New format: {unnested_df.shape[0]} recipes × {unnested_df.shape[1]} columns")

if __name__ == "__main__":
    print("🔧 Recipe Data Restructuring Script")
    print("=" * 50)
    restructure_recipes()
