import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg
import time
import os
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')


def load_data_pandas(file_path, sample_fraction=None):
    """
    Load data using Pandas, optionally sampling a fraction of the data.
    """
    start_time = time.time()
    df = pd.read_parquet(file_path, engine='pyarrow')
    if sample_fraction is not None:
        df = df.sample(frac=sample_fraction)
    end_time = time.time()
    print(f"Pandas loading time: {end_time - start_time:.2f} seconds")
    return df


def load_data_spark(file_path, sample_ratio=1.0):
    """
    Load data using Spark, optionally sampling a fraction of the data.
    """
    spark = SparkSession.builder.appName("NYCTaxiAnalysis").getOrCreate()
    start_time = time.time()
    df = spark.read.parquet(file_path)
    if sample_ratio < 1.0:
        df = df.sample(sample_ratio)
    end_time = time.time()
    print(f"Spark loading time: {end_time - start_time:.2f} seconds")
    return df, spark


def analyze_pandas(df):
    """
    Perform analysis on the DataFrame using Pandas and generate a visualization.
    """
    start_time = time.time()

    # Calculate average fare amount by hour
    df['hour'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.hour
    hourly_fares = df.groupby('hour')['fare_amount'].mean().reset_index()

    # Create and save the visualization
    fig = px.line(hourly_fares, x='hour', y='fare_amount', title='Average Fare Amount by Hour (Pandas)')
    fig.write_html(f"Analysis_output/hourly_fares_pandas_{len(df)}.html")

    end_time = time.time()
    print(f"Pandas analysis time: {end_time - start_time:.2f} seconds")


def analyze_spark(df, spark):
    """
    Perform analysis on the DataFrame using Spark and generate a visualization.
    """
    start_time = time.time()

    # Calculate average fare amount by hour
    hourly_fares = df.withColumn('hour', hour('tpep_pickup_datetime')) \
        .groupBy('hour') \
        .agg(avg('fare_amount').alias('avg_fare')) \
        .orderBy('hour')

    # Convert to Pandas for visualization
    hourly_fares_pd = hourly_fares.toPandas()

    # Create and save the visualization
    fig = px.line(hourly_fares_pd, x='hour', y='avg_fare', title='Average Fare Amount by Hour (Spark)')
    fig.write_html(f"Analysis_output/hourly_fares_spark_{df.count()}.html")

    end_time = time.time()
    print(f"Spark analysis time: {end_time - start_time:.2f} seconds")


def main():
    """
    Main function to orchestrate the data loading and analysis process.
    """
    file_path = 'data/yellow_tripdata_2024-01.parquet'
    os.makedirs('Analysis_output', exist_ok=True)

    # Analyze small dataset (10% of full data)
    print("Analyzing small dataset:")

    print("Pandas:")
    df_pandas_small = load_data_pandas(file_path, sample_fraction=0.1)
    analyze_pandas(df_pandas_small)

    print("\nSpark:")
    df_spark_small, spark = load_data_spark(file_path, sample_ratio=0.1)
    analyze_spark(df_spark_small, spark)

    # Analyze full dataset
    print("\nAnalyzing full dataset:")

    print("Pandas:")
    df_pandas_full = load_data_pandas(file_path)
    analyze_pandas(df_pandas_full)

    print("\nSpark:")
    df_spark_full, spark = load_data_spark(file_path)
    analyze_spark(df_spark_full, spark)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
