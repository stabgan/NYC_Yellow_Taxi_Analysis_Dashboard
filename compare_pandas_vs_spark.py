import os
import time
import warnings

import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour

warnings.filterwarnings('ignore')


def load_data_pandas(file_path, sample_fraction=None):
    """
    Load data using Pandas, optionally sampling a fraction of the data.
    """
    start_time = time.time()
    df = pd.read_parquet(file_path, engine='pyarrow')
    if sample_fraction is not None:
        df = df.sample(frac=sample_fraction)
    elapsed = time.time() - start_time
    print(f"Pandas loading time: {elapsed:.2f} seconds")
    return df


def load_data_spark(spark, file_path, sample_ratio=1.0):
    """
    Load data using an existing Spark session, optionally sampling.
    """
    start_time = time.time()
    df = spark.read.parquet(file_path)
    if sample_ratio < 1.0:
        df = df.sample(fraction=sample_ratio)
    elapsed = time.time() - start_time
    print(f"Spark loading time: {elapsed:.2f} seconds")
    return df


def analyze_pandas(df, output_dir):
    """
    Perform analysis on the DataFrame using Pandas and generate a visualization.
    """
    start_time = time.time()

    df = df.copy()
    df['hour'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.hour
    hourly_fares = df.groupby('hour')['fare_amount'].mean().reset_index()

    fig = px.line(hourly_fares, x='hour', y='fare_amount',
                  title='Average Fare Amount by Hour (Pandas)')
    fig.write_html(os.path.join(output_dir, f"hourly_fares_pandas_{len(df)}.html"))

    elapsed = time.time() - start_time
    print(f"Pandas analysis time: {elapsed:.2f} seconds")


def analyze_spark(df, output_dir):
    """
    Perform analysis on the DataFrame using Spark and generate a visualization.
    """
    start_time = time.time()

    hourly_fares = (
        df.withColumn('hour', hour('tpep_pickup_datetime'))
        .groupBy('hour')
        .agg(avg('fare_amount').alias('avg_fare'))
        .orderBy('hour')
    )

    hourly_fares_pd = hourly_fares.toPandas()

    fig = px.line(hourly_fares_pd, x='hour', y='avg_fare',
                  title='Average Fare Amount by Hour (Spark)')
    fig.write_html(os.path.join(output_dir, f"hourly_fares_spark_{df.count()}.html"))

    elapsed = time.time() - start_time
    print(f"Spark analysis time: {elapsed:.2f} seconds")


def main():
    """
    Main function to orchestrate the data loading and analysis process.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'data', 'yellow_tripdata_2024-01.parquet')
    output_dir = os.path.join(script_dir, 'Analysis_output')
    os.makedirs(output_dir, exist_ok=True)

    if not os.path.isfile(file_path):
        raise FileNotFoundError(
            f"Data file not found: {file_path}\n"
            "Download it from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
        )

    # Create a single Spark session for the entire run
    spark = SparkSession.builder.appName("NYCTaxiAnalysis").getOrCreate()

    try:
        # --- Small dataset (10 % sample) ---
        print("Analyzing small dataset (10% sample):")

        print("  Pandas:")
        df_pandas_small = load_data_pandas(file_path, sample_fraction=0.1)
        analyze_pandas(df_pandas_small, output_dir)

        print("  Spark:")
        df_spark_small = load_data_spark(spark, file_path, sample_ratio=0.1)
        analyze_spark(df_spark_small, output_dir)

        # --- Full dataset ---
        print("\nAnalyzing full dataset:")

        print("  Pandas:")
        df_pandas_full = load_data_pandas(file_path)
        analyze_pandas(df_pandas_full, output_dir)

        print("  Spark:")
        df_spark_full = load_data_spark(spark, file_path)
        analyze_spark(df_spark_full, output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
