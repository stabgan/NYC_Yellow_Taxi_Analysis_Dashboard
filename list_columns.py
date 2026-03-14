import pandas as pd
import sys


def main():
    """List column names from the NYC Yellow Taxi parquet file."""
    file_path = 'data/yellow_tripdata_2024-01.parquet'

    df = pd.read_parquet(file_path, engine='pyarrow')
    print(df.columns.tolist())

    # Export to CSV only if --export flag is passed
    if '--export' in sys.argv:
        df.to_csv('data/yellow_tripdata_2024-01.csv', index=False)
        print("Exported to data/yellow_tripdata_2024-01.csv")


if __name__ == "__main__":
    main()
