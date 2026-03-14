"""Utility script to list columns in the taxi dataset and optionally export to CSV."""

import os
import sys

import pandas as pd


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'data', 'yellow_tripdata_2024-01.parquet')

    if not os.path.isfile(file_path):
        print(f"Error: data file not found at {file_path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_parquet(file_path, engine='pyarrow')
    print("Columns:", df.columns.tolist())
    print(f"Shape: {df.shape}")

    # Export to CSV only if --csv flag is passed
    if '--csv' in sys.argv:
        csv_path = os.path.join(script_dir, 'data', 'yellow_tripdata_2024-01.csv')
        df.to_csv(csv_path, index=False)
        print(f"Exported CSV to {csv_path}")


if __name__ == "__main__":
    main()
