"""Table definitions for Plaite data sources."""

import os
from pathlib import Path

import dotenv
import polars as pl

dotenv.load_dotenv(override=True)


class Table:
    """
    A wrapper class for reading Polars DataFrames from parquet files.

    This class provides lazy and eager loading methods for parquet data files,
    optimized for efficient memory usage and query performance.

    Parameters
    ----------
    file_path : str
        Full path to the parquet data file (.parquet or .pq extension).

    Raises
    ------
    ValueError
        If file_path is None, empty, or points to unsupported format.
    FileNotFoundError
        If the specified file does not exist.

    Examples
    --------
    >>> table = Table("/path/to/data/recipes.parquet")
    >>> df = table.scan().filter(pl.col("category") == "dessert").collect()
    """

    def __init__(self, file_path: str) -> None:
        if not file_path:
            raise ValueError(
                "file_path cannot be None or empty. "
                "Ensure RECIPES_PATH environment variable is set."
            )

        self._path_obj = Path(file_path)

        if not self._path_obj.exists():
            raise FileNotFoundError(
                f"Data file not found: {file_path}\n"
                f"Please verify the path or set the RECIPES_PATH environment variable correctly."
            )

        if not self._path_obj.is_file():
            raise ValueError(
                f"Path is not a file: {file_path}\n"
                f"Expected a parquet file (.parquet or .pq extension)."
            )

        suffix = self._path_obj.suffix.lower()
        if suffix not in [".parquet", ".pq"]:
            raise ValueError(
                f"Unsupported file format: {suffix}\n"
                f"Only parquet files (.parquet, .pq) are supported.\n"
                f"Use scripts/convert_pickle_to_parquet.py to convert pickle files."
            )

        self._file_path = file_path

    def scan(self) -> pl.LazyFrame:
        """
        Lazily scan the parquet file without loading into memory.

        This enables query optimization and efficient memory usage for large datasets.

        Returns
        -------
        pl.LazyFrame
            A lazy Polars DataFrame that can be further queried.

        Examples
        --------
        >>> lf = table.scan()
        >>> filtered = lf.filter(pl.col("calories") < 500).collect()
        """
        return pl.scan_parquet(self._file_path)

    def read(self) -> pl.DataFrame:
        """
        Eagerly read the parquet file into memory.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame loaded into memory.

        Examples
        --------
        >>> df = table.read()
        >>> print(df.head())
        """
        return pl.read_parquet(self._file_path)

    def columns(self) -> str:
        """
        Get the schema of the table as a formatted string.

        Returns
        -------
        str
            A string representation of the table schema showing
            column names and their data types.

        Examples
        --------
        >>> print(table.columns())
        shape: (5, 2)
        ┌─────────────┬────────┐
        │ column      ┆ dtype  │
        │ ---         ┆ ---    │
        │ str         ┆ str    │
        ╞═════════════╪════════╡
        │ recipe_id   ┆ Int64  │
        │ recipe_name ┆ String │
        │ ...         ┆ ...    │
        └─────────────┴────────┘
        """
        pl.Config.set_tbl_rows(-1)
        schema = self.scan().collect_schema()
        df_str = str(
            pl.DataFrame(
                {
                    "column": list(schema.keys()),
                    "dtype": [str(t) for t in schema.values()],
                }
            )
        )
        pl.Config.set_tbl_rows(10)
        return df_str


# Initialize table instances from environment variables
recipes_table = Table(os.getenv("RECIPES_PATH"))
