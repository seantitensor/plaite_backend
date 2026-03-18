"""Read and write recipe status (uploaded, bad) columns in the local parquet."""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl


def _parquet_path() -> Path:
    path = os.getenv("RECIPES_PATH")
    if not path:
        raise RuntimeError("RECIPES_PATH env var not set.")
    return Path(path)


def _load(path: Path) -> pl.DataFrame:
    df = pl.read_parquet(path)
    if "uploaded" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("uploaded"))
    if "bad" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("bad"))
    return df


def _save(df: pl.DataFrame, path: Path) -> None:
    df.write_parquet(path)


def get_bad_ids() -> set[str]:
    """Return all recipe_ids marked as bad."""
    df = _load(_parquet_path())
    return set(df.filter(pl.col("bad"))["recipe_id"].to_list())


def get_uploaded_ids() -> set[str]:
    """Return all recipe_ids marked as uploaded."""
    df = _load(_parquet_path())
    return set(df.filter(pl.col("uploaded"))["recipe_id"].to_list())


def mark_uploaded(recipe_ids: list[str]) -> None:
    """Set uploaded=True for the given recipe_ids."""
    if not recipe_ids:
        return
    path = _parquet_path()
    df = _load(path)
    id_set = set(recipe_ids)
    df = df.with_columns(
        pl.when(pl.col("recipe_id").is_in(id_set))
        .then(True)
        .otherwise(pl.col("uploaded"))
        .alias("uploaded")
    )
    _save(df, path)


def mark_bad(recipe_ids: list[str]) -> None:
    """Set bad=True for the given recipe_ids."""
    if not recipe_ids:
        return
    path = _parquet_path()
    df = _load(path)
    id_set = set(recipe_ids)
    df = df.with_columns(
        pl.when(pl.col("recipe_id").is_in(id_set))
        .then(True)
        .otherwise(pl.col("bad"))
        .alias("bad")
    )
    _save(df, path)
