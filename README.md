# 🚕 NYC Yellow Taxi Analysis Dashboard

End-to-end data analysis pipeline for NYC Yellow Taxi trip data (January 2024), featuring interactive EDA, Pandas vs. PySpark benchmarking, and a Grafana dashboard backed by PostgreSQL.

> IIT Madras — Data Science Project (CH23M514)

---

## 📖 Description

This project processes and visualizes the [NYC TLC Yellow Taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) through three stages:

1. **Exploratory Data Analysis** — column-level statistics, distribution histograms, and a correlation heatmap, all rendered as interactive Plotly HTML files.
2. **Performance Benchmarking** — side-by-side comparison of Pandas and PySpark for data loading and aggregation on both a 10 % sample and the full dataset.
3. **Dashboard Pipeline** — aggregates hourly and daily trip statistics and loads them into PostgreSQL so Grafana can serve live dashboards.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| 🐍 Language | Python 3.10+ |
| 📊 Data I/O | Pandas, PyArrow (Parquet) |
| ⚡ Big Data | PySpark |
| 📈 Visualization | Plotly |
| 🐘 Database | PostgreSQL (SQLAlchemy + psycopg2) |
| 📉 Dashboard | Grafana |

---

## 📦 Dependencies

Listed in `requirements.txt`:

```
pandas>=2.0
pyarrow>=14.0
pyspark>=3.5
sqlalchemy>=2.0
psycopg2-binary>=2.9
plotly>=5.18
```

PySpark also requires a **Java 11+** runtime.

---

## 🚀 How to Run

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Download the data

Grab the **January 2024** Yellow Taxi Parquet file from the [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and place it at:

```
data/yellow_tripdata_2024-01.parquet
```

### 3. Run Exploratory Data Analysis

```bash
python EDA.py
```

Interactive HTML plots are saved to `EDA_output/`.

### 4. Benchmark Pandas vs. PySpark

```bash
python compare_pandas_vs_spark.py
```

Comparison charts are saved to `Analysis_output/`. Requires a working Java + Spark installation.

### 5. Load data into PostgreSQL for Grafana

Set your database credentials via environment variables (or accept the defaults):

```bash
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=changeme
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=nyc_taxi_db
```

Then run:

```bash
python prepare_data_for_grafana.py
```

Connect Grafana to `nyc_taxi_db` and build dashboards from the `hourly_stats` and `daily_stats` tables.

### 6. Utility — list columns

```bash
python list_columns.py            # print column names
python list_columns.py --export   # also export full CSV
```

---

## ⚠️ Known Issues

- **No data included** — the Parquet file is not in the repo; download it manually from the NYC TLC site.
- **No Grafana provisioning** — dashboard JSON and datasource configs are not included; Grafana setup is manual.
- **Spark / Java dependency** — `compare_pandas_vs_spark.py` requires Java 11+ and a Spark installation, which must be configured separately.

---

## 📄 License

MIT — see [LICENSE](LICENSE).
