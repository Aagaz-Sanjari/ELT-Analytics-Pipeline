# ELT Analytics Pipeline

A multi-source ELT pipeline that collects live data from public APIs, stores it untouched in PostgreSQL, and transforms it into analytics-ready tables with dbt. New data sources plug in through a small connector class and one config entry, so the loader, the raw table, and the scheduling never change.

Currently running with two live sources: **crypto market data** (CoinGecko) and **weather** (Open-Meteo), with a two-page Power BI dashboard on top.

## Dashboard

**Crypto page:** coin selector, price vs 3-reading moving average, rolling volatility, latest price.

![Crypto page](docs/dashboard-crypto.png)

**Weather page:** city selector, temperature vs moving average, latest temperature, observed rainfall.

![Weather page](docs/dashboard-weather.png)

**Pipeline Health page:** total runs, success rate, last run time, run duration by source, and a run history table fed by the `ops.pipeline_runs` log.

![Pipeline Health page](docs/dashboard-pipeline-health.png)

## Architecture

```
sources.yml (which sources to run)
      |
connectors/  (one small class per source)
      |
ingest/  (generic loader: hash, dedupe, batch id)
      |
PostgreSQL  raw.ingested_records  (original JSON, never modified)
      |
dbt  staging views  ->  mart tables  (+ data tests)
      |
Power BI dashboard
```

The pipeline is scheduled with Windows Task Scheduler (`run_pipeline.bat`), which runs ingestion, `dbt run`, and `dbt test` in order.

## Key design decisions

- **ELT, not ETL.** Raw API responses are loaded first and transformed inside the database. The raw table is the source of truth, so every downstream table can be rebuilt at any time.
- **One generic raw table.** All sources land in `raw.ingested_records` with `source`, `dataset`, a JSONB `payload`, a `payload_hash`, a `batch_id`, and `ingested_at`.
- **Idempotent loads.** A unique constraint on `(source, dataset, payload_hash)` means re-running the pipeline never creates duplicate records. This fixed a real problem in the first version of this project, where repeated runs inserted the same CoinGecko snapshot several times and skewed the rolling metrics.
- **Connector pattern.** Each source is a class with a `fetch()` method. `sources.yml` decides which connectors run and with which parameters.
- **Transformations in SQL.** Returns, moving averages, and volatility are computed with dbt window functions instead of Pandas.

## Data sources

| Source | Dataset | What is collected |
|---|---|---|
| CoinGecko | markets | Price, market cap, volume, 24h change for Bitcoin, Ethereum, Solana, Cardano, Polkadot |
| Open-Meteo | current | Temperature, humidity, wind speed, precipitation for Mumbai, Delhi, Pune, Bengaluru |

## dbt models

| Layer | Model | Description |
|---|---|---|
| Staging (view) | `stg_coingecko_markets` | Typed, validated crypto records extracted from the JSON payload |
| Staging (view) | `stg_open_meteo_current` | Typed weather observations per city |
| Mart (table) | `fct_coin_metrics` | Price return vs the previous reading, 3-point moving average, rolling volatility per coin |
| Mart (table) | `fct_city_weather` | Temperature change, 3-point moving average and max, rain flag per city |

Data tests (9) cover not-null checks, accepted values for coin IDs and city names, and required timestamps.

## Tech stack

Python, PostgreSQL, dbt (dbt-postgres), SQLAlchemy, Requests, PyYAML, Power BI, Windows Task Scheduler, Git.

## Project structure

```
connectors/        source connectors (base class, CoinGecko, Open-Meteo)
ingest/            generic loader, runner, and backfill script
crypto_dbt/        dbt project (staging and mart models, tests)
docs/              dashboard screenshots
sources.yml        list of active sources and their parameters
run_pipeline.bat   one-command run: ingest, dbt run, dbt test
```

The first version of the project (Pandas ETL scripts in `extract/`, `transform/`, `load/`) is kept for reference and is replaced by the structure above.

## Setup

**1. Requirements:** Python 3.12, PostgreSQL, and a database named `crypto_analytics`.

**2. Install dependencies**

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**3. Create a `.env` file in the project root**

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crypto_analytics
DB_USER=your_user
DB_PASSWORD=your_password
```

**4. Create the raw table**

```sql
create schema if not exists raw;

create table if not exists raw.ingested_records (
    id bigserial primary key,
    source text not null,
    dataset text not null,
    payload jsonb not null,
    payload_hash text not null,
    batch_id uuid not null,
    ingested_at timestamptz not null default now(),
    unique (source, dataset, payload_hash)
);
```

**5. Run the pipeline**

```powershell
python -m ingest.run
cd crypto_dbt
Get-Content ..\.env | ForEach-Object { if ($_ -match '^\s*([^#=]+)=(.*)$') { Set-Item -Path "Env:$($matches[1].Trim())" -Value $matches[2].Trim() } }
dbt run --profiles-dir .
dbt test --profiles-dir .
```

Or run everything with `run_pipeline.bat`, which is also what the scheduled task executes.

**6. Dashboard:** build the Power BI report on `analytics.fct_coin_metrics`, `analytics.fct_city_weather`, and `analytics.fct_pipeline_health` (screenshots above).

## Adding a new data source

1. Create a class in `connectors/` that extends `Connector`, sets `source` and `dataset`, and returns a list of records from `fetch()`.
2. Add an entry for it in `sources.yml`.
3. Add a dbt staging model that extracts typed columns from the JSON payload, plus a test file.

No change to the loader, the raw table, or the scheduler is needed.

## Limitations

- Data is collected as scheduled snapshots, not a continuous stream.
- The database and scheduler run locally, so collection only happens while the machine is on.
- Rolling metrics use the last 3 readings, so they become more meaningful as history accumulates.

## Roadmap


- Docker and Airflow for orchestration
- GitHub Actions to run dbt tests on each push

## Author

Aagaz Sanjari
[GitHub](https://github.com/Aagaz-Sanjari) | [LinkedIn](https://linkedin.com/in/aagaz-sanjari-b3a03b283)
