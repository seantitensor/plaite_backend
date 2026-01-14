"""Table definitions for Plaite data sources."""

import os
import pickle
import dotenv
import polars as pl
from pathlib import Path

dotenv.load_dotenv(override=True)


class Table:
    """
    A wrapper class for reading Polars DataFrames from parquet or pickle files.

    This class provides lazy and eager loading methods, supporting both
    parquet snapshots and pickle files. File format is auto-detected from extension.

    .. warning::
        **Security Notice for Pickle Files:**
        Pickle files can execute arbitrary code during deserialization.
        Only load pickle files from trusted sources. For production use,
        prefer parquet format which is safer and more efficient.

    Parameters
    ----------
    file_path : str
        Full path to the data file (parquet or pickle).

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
                f"Expected a parquet (.parquet, .pq) or pickle (.pkl, .pickle) file."
            )

        self._file_path = file_path
        self._format = self._detect_format()
        self._cached_df: pl.DataFrame | None = None

    def _detect_format(self) -> str:
        """Detect file format from extension."""
        suffix = self._path_obj.suffix.lower()
        if suffix in [".parquet", ".pq"]:
            return "parquet"
        elif suffix in [".pkl", ".pickle"]:
            return "pickle"
        else:
            raise ValueError(
                f"Unsupported file format: {suffix}. Use .parquet or .pkl"
            )

    def _load_pickle(self) -> pl.DataFrame:
        """
        Load pickle file and convert to Polars DataFrame.

        .. warning::
            Pickle files can execute arbitrary code during loading.
            Only use pickle files from trusted sources.

        Returns
        -------
        pl.DataFrame
            The loaded DataFrame.

        Raises
        ------
        TypeError
            If pickle data cannot be converted to Polars DataFrame.
        """
        with open(self._file_path, "rb") as f:
            data = pickle.load(f)

        # Handle different pickle data structures
        if isinstance(data, pl.DataFrame):
            return data
        elif isinstance(data, dict):
            return pl.DataFrame(data)
        elif isinstance(data, list):
            return pl.DataFrame(data)
        else:
            # Try pandas conversion if available
            try:
                import pandas as pd

                if isinstance(data, pd.DataFrame):
                    return pl.from_pandas(data)
            except ImportError:
                pass
            raise TypeError(
                f"Unsupported pickle data type: {type(data)}. "
                "Expected Polars/Pandas DataFrame, dict, or list."
            )

    def scan(self) -> pl.LazyFrame:
        """
        Lazily scan the data file without loading into memory.

        For parquet files, this enables query optimization.
        For pickle files, the data is loaded once and cached.

        Returns
        -------
        pl.LazyFrame
            A lazy Polars DataFrame that can be further queried.

        Examples
        --------
        >>> lf = table.scan()
        >>> filtered = lf.filter(pl.col("calories") < 500).collect()
        """
        if self._format == "parquet":
            return pl.scan_parquet(self._file_path)
        else:
            # For pickle, load once and cache, then return lazy
            if self._cached_df is None:
                self._cached_df = self._load_pickle()
            return self._cached_df.lazy()

    def read(self) -> pl.DataFrame:
        """
        Eagerly read the data file into memory.

        Returns
        -------
        pl.DataFrame
            A Polars DataFrame loaded into memory.

        Examples
        --------
        >>> df = table.read()
        >>> print(df.head())
        """
        if self._format == "parquet":
            return pl.read_parquet(self._file_path)
        else:
            if self._cached_df is None:
                self._cached_df = self._load_pickle()
            return self._cached_df

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
