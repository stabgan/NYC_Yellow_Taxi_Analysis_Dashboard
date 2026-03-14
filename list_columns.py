import pandas as pd

# Load the data
df = pd.read_parquet('data/yellow_tripdata_2024-01.parquet', engine='pyarrow')
df.to_csv('data/yellow_tripdata_2024-01.csv')

# Print column names
print(df.columns.tolist())