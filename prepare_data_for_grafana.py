import os

import pandas as pd
from sqlalchemy import create_engine


def load_data(file_path):
    """
    Load data from a Parquet file.

    Args:
        file_path (str): Path to the Parquet file.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    return pd.read_parquet(file_path, engine='pyarrow')


def aggregate_data(df):
    """
    Aggregate the taxi data into hourly and daily statistics.

    Args:
        df (pd.DataFrame): Input DataFrame containing taxi trip data.

    Returns:
        tuple: A tuple containing two DataFrames (hourly_stats, daily_stats).
    """
    pickup = pd.to_datetime(df['tpep_pickup_datetime'])

    # Hourly statistics
    df = df.copy()
    df['hour'] = pickup.dt.hour
    hourly_stats = df.groupby('hour').agg({
        'trip_distance': 'mean',
        'fare_amount': 'mean',
        'tip_amount': 'mean',
        'total_amount': 'mean',
        'tpep_pickup_datetime': 'count',
    }).reset_index()
    hourly_stats.columns = [
        'hour', 'avg_distance', 'avg_fare', 'avg_tip', 'avg_total', 'trip_count',
    ]

    # Daily statistics
    df['date'] = pickup.dt.date
    daily_stats = df.groupby('date').agg({
        'trip_distance': 'mean',
        'fare_amount': 'mean',
        'tip_amount': 'mean',
        'total_amount': 'mean',
        'tpep_pickup_datetime': 'count',
    }).reset_index()
    daily_stats.columns = [
        'date', 'avg_distance', 'avg_fare', 'avg_tip', 'avg_total', 'trip_count',
    ]

    return hourly_stats, daily_stats


def load_to_postgres(data, table_name, engine):
    """
    Load a DataFrame into a PostgreSQL table.

    Args:
        data (pd.DataFrame): DataFrame to be loaded into PostgreSQL.
        table_name (str): Name of the table to be created or replaced.
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine for database connection.
    """
    data.to_sql(table_name, engine, if_exists='replace', index=False)


def main():
    """
    Main function to load taxi data, aggregate it, and store in PostgreSQL.

    Database credentials are read from environment variables:
        DB_USER  (default: postgres)
        DB_PASS  (default: empty string)
        DB_HOST  (default: localhost)
        DB_NAME  (default: nyc_taxi_db)
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'data', 'yellow_tripdata_2024-01.parquet')

    if not os.path.isfile(file_path):
        raise FileNotFoundError(
            f"Data file not found: {file_path}\n"
            "Download it from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
        )

    # Load the taxi data
    df = load_data(file_path)

    # Aggregate the data into hourly and daily statistics
    hourly_stats, daily_stats = aggregate_data(df)

    # Read PostgreSQL connection details from environment variables
    db_user = os.environ.get('DB_USER', 'postgres')
    db_pass = os.environ.get('DB_PASS', '')
    db_host = os.environ.get('DB_HOST', 'localhost')
    db_name = os.environ.get('DB_NAME', 'nyc_taxi_db')

    db_url = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

    engine = create_engine(db_url)

    try:
        load_to_postgres(hourly_stats, 'hourly_stats', engine)
        load_to_postgres(daily_stats, 'daily_stats', engine)
    except Exception as exc:
        raise ConnectionError(
            f"Could not connect to PostgreSQL at {db_host}/{db_name} as {db_user}. "
            "Set DB_USER, DB_PASS, DB_HOST, DB_NAME environment variables."
        ) from exc

    print("Data successfully loaded into PostgreSQL.")


if __name__ == "__main__":
    main()
