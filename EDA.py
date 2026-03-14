import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


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

    if pd.api.types.is_numeric_dtype(df[column_name]):
        mode_vals = df[column_name].mode()
        if not mode_vals.empty:
            print(f"Mode: {mode_vals.iloc[0]}")
    elif pd.api.types.is_object_dtype(df[column_name]):
        vc = df[column_name].value_counts()
        if not vc.empty:
            print(f"Most common value: {vc.index[0]}")

    print(f"Null values: {df[column_name].isnull().sum()}")
    print(f"Unique values: {df[column_name].nunique()}")


def plot_distribution(df, column_name, output_dir):
    """
    Create and save a histogram plot for a numeric column.
    """
    fig = px.histogram(df, x=column_name, title=f'Distribution of {column_name}')
    fig.write_html(os.path.join(output_dir, f"distribution_{column_name}.html"))


def main():
    """
    Main function to perform Exploratory Data Analysis on the NYC Taxi dataset.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, 'data', 'yellow_tripdata_2024-01.parquet')

    if not os.path.isfile(file_path):
        raise FileNotFoundError(
            f"Data file not found: {file_path}\n"
            "Download it from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
        )

    df = load_data(file_path)

    output_dir = os.path.join(script_dir, 'EDA_output')
    os.makedirs(output_dir, exist_ok=True)

    file_size = os.path.getsize(file_path) / (1024 * 1024)
    memory_usage = df.memory_usage(deep=True).sum() / (1024 * 1024)

    print(f"File size: {file_size:.2f} MB")
    print(f"Memory usage: {memory_usage:.2f} MB")
    print(f"Number of rows: {len(df)}")
    print(f"Number of columns: {len(df.columns)}")

    for column in df.columns:
        analyze_column(df, column)

        if pd.api.types.is_numeric_dtype(df[column]):
            plot_distribution(df, column, output_dir)

    # Correlation heatmap
    numeric_df = df.select_dtypes(include=['number'])
    corr_matrix = numeric_df.corr(numeric_only=True)
    fig = go.Figure(data=go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns.tolist(),
        y=corr_matrix.index.tolist(),
        colorscale='RdBu',
        zmin=-1, zmax=1,
    ))
    fig.update_layout(title='Correlation Heatmap')
    fig.write_html(os.path.join(output_dir, "correlation_heatmap.html"))


if __name__ == "__main__":
    main()
