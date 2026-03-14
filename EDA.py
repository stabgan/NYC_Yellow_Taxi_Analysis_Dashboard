import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import os


def load_data(file_path):
    """
    Load data from a Parquet file.
    """
    return pd.read_parquet(file_path, engine='pyarrow')


def analyze_column(df, column_name):
    """
    Analyze a single column of the DataFrame and print its statistics.
    """
    col_type = df[column_name].dtype
    col_stats = df[column_name].describe()

    print(f"\nColumn: {column_name}")
    print(f"Type: {col_type}")
    print(f"Stats:\n{col_stats}")

    # Calculate mode for numeric columns or most common value for object columns
    if col_type in ['int64', 'float64']:
        print(f"Mode: {df[column_name].mode().values[0]}")
    elif col_type == 'object':
        print(f"Most common value: {df[column_name].value_counts().index[0]}")

    # Print additional information about the column
    print(f"Null values: {df[column_name].isnull().sum()}")
    print(f"Unique values: {df[column_name].nunique()}")


def plot_distribution(df, column_name):
    """
    Create and save a histogram plot for a numeric column.
    """
    fig = px.histogram(df, x=column_name, title=f'Distribution of {column_name}')
    fig.write_html(f"EDA_output/distribution_{column_name}.html")


def main():
    """
    Main function to perform Exploratory Data Analysis on the NYC Taxi dataset.
    """
    file_path = 'data/yellow_tripdata_2024-01.parquet'
    df = load_data(file_path)

    # Create output directory if it doesn't exist
    os.makedirs('EDA_output', exist_ok=True)

    # Calculate and print file size and memory usage
    file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
    memory_usage = df.memory_usage(deep=True).sum() / (1024 * 1024)  # Size in MB

    print(f"File size: {file_size:.2f} MB")
    print(f"Memory usage: {memory_usage:.2f} MB")
    print(f"Number of rows: {len(df)}")
    print(f"Number of columns: {len(df.columns)}")

    # Analyze each column in the DataFrame
    for column in df.columns:
        analyze_column(df, column)

        # Create distribution plots for numeric columns
        if df[column].dtype in ['int64', 'float64']:
            plot_distribution(df, column)

    # Create and save correlation heatmap
    numeric_df = df.select_dtypes(include=['float64', 'int64'])
    corr_matrix = numeric_df.corr()
    fig = go.Figure(data=go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns,
        y=corr_matrix.index,
        colorscale='RdBu',
        zmin=-1, zmax=1
    ))
    fig.update_layout(title='Correlation Heatmap')
    fig.write_html("EDA_output/correlation_heatmap.html")


if __name__ == "__main__":
    main()
