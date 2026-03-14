import pandas as pd
from sqlalchemy import create_engine
import os


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
    # Hourly statistics
    df['hour'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.hour
    hourly_stats = df.groupby('hour').agg({
        'trip_distance': 'mean',
        'fare_amount': 'mean',
        'tip_amount': 'mean',
        'total_amount': 'mean',
        'tpep_pickup_datetime': 'count'
    }).reset_index()
    hourly_stats.columns = ['hour', 'avg_distance', 'avg_fare', 'avg_tip', 'avg_total', 'trip_count']

    # Daily statistics
    df['date'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.date
    daily_stats = df.groupby('date').agg({
        'trip_distance': 'mean',
        'fare_amount': 'mean',
        'tip_amount': 'mean',
        'total_amount': 'mean',
        'tpep_pickup_datetime': 'count'
    }).reset_index()
    daily_stats.columns = ['date', 'avg_distance', 'avg_fare', 'avg_tip', 'avg_total', 'trip_count']

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
    """
    # Path to the Parquet file containing taxi data
    file_path = 'data/yellow_tripdata_2024-01.parquet'

    # Load the taxi data
    df = load_data(file_path)

    # Aggregate the data into hourly and daily statistics
    hourly_stats, daily_stats = aggregate_data(df)

    # PostgreSQL connection details
    db_user = 'postgres'
    db_pass = '1234'
    db_host = 'localhost'
    db_name = 'nyc_taxi_db'

    # Create SQLAlchemy engine for PostgreSQL connection
    engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}')

    # Load aggregated data into PostgreSQL
    load_to_postgres(hourly_stats, 'hourly_stats', engine)
    load_to_postgres(daily_stats, 'daily_stats', engine)

    print("Data successfully loaded into PostgreSQL.")


if __name__ == "__main__":
    main()
