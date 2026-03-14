# NYC Yellow Taxi Analysis Dashboard

End-to-end data pipeline for NYC Yellow Taxi trip records (Jan 2024) — EDA, Pandas-vs-Spark benchmarking, and a Grafana dashboard backed by PostgreSQL.

> IIT Madras — Data Science Project (CH23M514)

## What It Does

1. **Exploratory Data Analysis** — column statistics, distribution histograms, and a correlation heatmap, all rendered as interactive Plotly HTML files.
2. **Pandas vs PySpark Benchmark** — times data loading and hourly-fare aggregation at 10 % and 100 % scale to compare single-node Pandas with distributed Spark.
3. **Grafana Pipeline** — aggregates hourly and daily trip stats and loads them into PostgreSQL so you can build live Grafana dashboards.

## Project Structure

```
├── EDA.py                        # Exploratory data analysis
├── compare_pandas_vs_spark.py    # Pandas ↔ PySpark benchmark
├── prepare_data_for_grafana.py   # Aggregate → PostgreSQL loader
├── list_columns.py               # Utility: list columns / export CSV
├── requirements.txt              # Python dependencies
├── .gitignore
└── LICENSE                       # MIT
```

## 🛠 Tech Stack

| Layer | Technology |
|---|---|
| 🐍 Language | Python 3.9+ |
| 📊 Data I/O | Pandas, PyArrow |
| ⚡ Big Data | PySpark |
| 📈 Visualization | Plotly |
| 🗄️ Database | PostgreSQL (SQLAlchemy + psycopg2) |
| 📉 Dashboard | Grafana |

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Get the data

Download the **January 2024** Yellow Taxi Parquet file from the [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and place it at:

```
data/yellow_tripdata_2024-01.parquet
```

### 3. Run EDA

```bash
python EDA.py
```

Interactive HTML plots are written to `EDA_output/`.

### 4. Benchmark Pandas vs Spark

```bash
python compare_pandas_vs_spark.py
```

Requires a working Java + Spark installation. Output goes to `Analysis_output/`.

### 5. Load into PostgreSQL for Grafana

Set your database credentials via environment variables, then run:

```bash
export DB_USER=postgres
export DB_PASS=yourpassword
export DB_HOST=localhost
export DB_NAME=nyc_taxi_db

python prepare_data_for_grafana.py
```

Connect Grafana to `nyc_taxi_db` and query the `hourly_stats` / `daily_stats` tables.

## ⚠️ Known Issues

- **No data included** — the Parquet file must be downloaded manually (~50 MB).
- **No Grafana provisioning** — dashboard JSON is not included; Grafana setup is manual.
- **Spark dependency** — `compare_pandas_vs_spark.py` needs Java 8+ and a Spark installation.

## License

MIT — see [LICENSE](LICENSE).
