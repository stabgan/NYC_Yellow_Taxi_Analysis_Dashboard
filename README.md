# NYC Yellow Taxi Analysis Dashboard

End-to-end data analysis pipeline for NYC Yellow Taxi trip data (Jan 2024), with EDA, Pandas vs. Spark benchmarking, and a Grafana dashboard powered by PostgreSQL.

> IIT Madras — Data Science Project (CH23M514)

## What It Does

This project analyzes the [NYC TLC Yellow Taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and provides:

- Exploratory Data Analysis (EDA) with interactive Plotly visualizations (distributions, correlation heatmaps)
- A performance comparison between Pandas and PySpark for data loading and aggregation at different scales (10% sample vs. full dataset)
- Data preparation pipeline that aggregates hourly and daily trip statistics and loads them into PostgreSQL for Grafana dashboards

## Project Structure

```
├── EDA.py                        # Exploratory data analysis (column stats, distributions, correlation)
├── compare_pandas_vs_spark.py    # Pandas vs. PySpark performance benchmark
├── prepare_data_for_grafana.py   # Aggregates data and loads into PostgreSQL for Grafana
├── list_columns.py               # Utility: prints columns and exports CSV
├── requirements.txt              # Python dependencies
├── LICENSE                       # MIT
```

## Tech Stack

| Layer          | Technology                          |
|----------------|-------------------------------------|
| Language       | Python 3                            |
| Data I/O       | Pandas, PyArrow (Parquet)           |
| Big Data       | PySpark                             |
| Visualization  | Plotly                              |
| Database       | PostgreSQL (via SQLAlchemy, psycopg2)|
| Dashboard      | Grafana                             |

## Setup & Run

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Get the data

Download the January 2024 Yellow Taxi Parquet file from the [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and place it at:

```
data/yellow_tripdata_2024-01.parquet
```

### 3. Run EDA

```bash
python EDA.py
```

Outputs interactive HTML plots to `EDA_output/`.

### 4. Benchmark Pandas vs. Spark

```bash
python compare_pandas_vs_spark.py
```

Outputs comparison charts to `Analysis_output/`. Requires a working Spark/Java installation.

### 5. Load data into PostgreSQL for Grafana

```bash
python prepare_data_for_grafana.py
```

Expects a local PostgreSQL instance with database `nyc_taxi_db`. Default credentials are hardcoded in the script — update them before running.

Then connect Grafana to the `nyc_taxi_db` database and build dashboards from the `hourly_stats` and `daily_stats` tables.

## Known Issues

- **Hardcoded DB credentials** — `prepare_data_for_grafana.py` has plaintext `postgres`/`1234` credentials. Use environment variables or a `.env` file instead.
- **No data included** — The Parquet data file is not in the repo. You must download it manually.
- **No Grafana config** — Dashboard JSON/provisioning files are not included; Grafana setup is manual.
- **Spark dependency** — `compare_pandas_vs_spark.py` requires Java and a Spark installation, which is not documented.
- **No `.gitignore`** — Output directories and data files are not excluded from version control.

## License

MIT — see [LICENSE](LICENSE).
