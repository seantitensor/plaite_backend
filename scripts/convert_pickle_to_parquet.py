#!/usr/bin/env python3
"""Convert pickle data file to parquet format.

Usage:
    python scripts/convert_pickle_to_parquet.py /path/to/recipes.pkl
    python scripts/convert_pickle_to_parquet.py /path/to/recipes.pkl /path/to/output.parquet
    python scripts/convert_pickle_to_parquet.py /path/to/recipes.pkl --compression snappy
"""

import argparse
import pickle
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Convert pickle to parquet")
    parser.add_argument("input_file", type=Path, help="Path to input pickle file")
    parser.add_argument(
        "output_file",
        type=Path,
        nargs="?",
        default=None,
        help="Path to output parquet file (default: same name with .parquet)",
    )
    parser.add_argument(
        "--compression",
        choices=["zstd", "snappy", "gzip", "lz4", "none"],
        default="zstd",
        help="Compression algorithm (default: zstd)",
    )
    args = parser.parse_args()

    input_file: Path = args.input_file
    output_file: Path = args.output_file
    compression: str = args.compression

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    if input_file.suffix.lower() not in [".pkl", ".pickle"]:
        print("Error: Input file must be a pickle file (.pkl or .pickle)")
        sys.exit(1)

    # Default output path
    if output_file is None:
        output_file = input_file.with_suffix(".parquet")

    print(f"Converting {input_file} to parquet...")

    # Load pickle
    print("Loading pickle file...")
    with open(input_file, "rb") as f:
        data = pickle.load(f)

    # Import polars here to avoid slow startup if args are wrong
    import polars as pl

    # Convert to Polars DataFrame
    if isinstance(data, pl.DataFrame):
        df = data
    elif isinstance(data, dict):
        df = pl.DataFrame(data)
    elif isinstance(data, list):
        df = pl.DataFrame(data)
    else:
        try:
            import pandas as pd

            if isinstance(data, pd.DataFrame):
                df = pl.from_pandas(data)
            else:
                print(f"Error: Unsupported data type: {type(data)}")
                sys.exit(1)
        except ImportError:
            print(f"Error: Unsupported data type: {type(data)}")
            sys.exit(1)

    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")

    # Write to parquet
    compression_arg = None if compression == "none" else compression
    print(f"Writing to {output_file} with {compression} compression...")
    df.write_parquet(output_file, compression=compression_arg)

    # Show file sizes
    input_size = input_file.stat().st_size / (1024 * 1024)
    output_size = output_file.stat().st_size / (1024 * 1024)
    reduction = (1 - output_size / input_size) * 100 if input_size > 0 else 0

    print("\nConversion complete!")
    print(f"  Input:  {input_size:.1f} MB")
    print(f"  Output: {output_size:.1f} MB")
    print(f"  Size reduction: {reduction:.1f}%")
    print("\nUpdate your .env file:")
    print(f"  RECIPES_PATH={output_file.absolute()}")


if __name__ == "__main__":
    main()
