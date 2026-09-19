# Airflow Sales ETL

A batch ETL pipeline for sales data on Apache Airflow 2.x: CSV + a
supplier API → cleaning/enrichment → a PostgreSQL warehouse with SCD Type 2
history → a daily aggregation mart. Python 3.12, pandas, SQLAlchemy,
Docker Compose.

This is a companion to
[sales-data-pipeline](https://github.com/Nightroudt/sales-data-pipeline)
(the same "sales" domain on Java/Spring/Kafka) — same problem space, two
different orchestration paradigms: **streaming** there, **batch** here.

## Features

- **Extract**: reads every CSV currently in `data/incoming/`, and fetches
  product data from a FastAPI supplier stub in one batched call
- **Transform**: cleans/deduplicates transactions, validates/normalizes the
  supplier's product payload
- **Load**: SCD Type 2 upsert into `dim_product` (keyed on
  `product_id` + `updated_at`), idempotent upsert into `fact_sales`
- **Mart**: `mart_daily_sales` — revenue, quantity and top product per day,
  rebuilt idempotently from the warehouse
- Runs every 6 hours (`0 */6 * * *`), 3 retries with exponential backoff on
  every task, a structured-log alert on task failure
- Idempotent: re-running the pipeline against the same source data never
  duplicates a `fact_sales` row or a `mart_daily_sales` day — proven by a
  test that runs the whole DAG twice and asserts those two row counts don't
  change. `dim_product` is the one deliberate exception: SCD Type 2 means
  each run appends a new *version* per product (the demo API stub hands
  back a fresh `updated_at` every call), so its row count is *supposed* to
  grow — what stays stable there is the distinct product count, not the
  version count

## Architecture

```
dags/sales_etl_dag.py       the DAG: wiring, schedule, retries, alerting

plugins/
  hooks/                     SupplierApiHook — wraps the supplier API stub
  operators/                 thin Airflow wrappers around etl/* — no real
                             logic lives here, just glue

etl/
  extract/                    csv_source.py, api_source.py — pure functions,
                             no Airflow dependency, directly unit-testable
  transform/                  cleaning.py, enrichment.py
  load/                       scd_loader.py, fact_loader.py, mart_builder.py
                             — raw upsert SQL via SQLAlchemy Core

db/schema.py                 SQLAlchemy Core Table definitions
alembic/                     schema migrations
api_stub/                    FastAPI stand-in for the supplier's API
```

`plugins/operators/*` depend on `etl/*`, never the other way around — the
same "thin adapter, real logic underneath" split used across this
portfolio's other projects. Everything in `etl/` can be tested with plain
pytest and no Airflow runtime at all.

### Data flow between tasks

```
extract_transactions_csv ─┐
                          ├─> transform_clean_dedup ────────────┐
extract_products_api ─────┤                                     ├─> load_fact_sales ──┐
                          └─> transform_enrich_products ─> load_dim_product_scd2 ┘     ├─> build_daily_mart
                                                                                        ┘
```

Intermediate results move between tasks as **parquet file paths** pushed
through XCom, not as DataFrames — XCom isn't meant for bulk payloads.
`load_fact_sales` and `load_dim_product_scd2` both have to finish before
`build_daily_mart` runs, since the mart joins facts to the *current*
dimension version.

### SCD Type 2, concretely

`dim_product` has a unique key on `(product_id, source_updated_at)`.
Loading is two steps, both idempotent:

1. `INSERT ... ON CONFLICT (product_id, source_updated_at) DO NOTHING` —
   re-inserting a supplier snapshot the pipeline has already seen inserts
   nothing new.
2. A window-function `UPDATE` recomputes `valid_to`/`is_current` for every
   affected product from its own version history
   (`LEAD(source_updated_at) OVER (PARTITION BY product_id ORDER BY
   source_updated_at)`) — a pure function of what's already in the table,
   so re-running it always converges to the same result.

The supplier API stub returns a fresh `updated_at` (and often different
`category`/`unit_cost`) on every call — that's deliberate, so every
pipeline run has a genuinely new product version to track.

### Retries and alerting

Both are Airflow's own mechanisms, not custom code:
`default_args={"retries": 3, "retry_delay": ..., "retry_exponential_backoff":
True}` for retries, and an `on_failure_callback` that logs a structured
error for alerting. This setup has no SMTP server, so a log line stands in
for email — switching to real email needs no code change, just
`default_args={"email_on_failure": True, "email": [...]}` plus the
`AIRFLOW__SMTP__*` environment variables.

## A note on Apache Airflow and Windows

Airflow's own docs say it isn't tested on Windows outside WSL2/containers.
In practice, on this dev machine, `pip install apache-airflow` (with the
official constraints file), defining DAGs and custom operators, and
`dag.test()` (Airflow's own mechanism for running a whole DAG synchronously
without a scheduler) all work natively — so the whole test suite, including
the full-DAG integration test, runs directly with `pytest`, no Docker or
WSL2 required. Docker Compose is still how you get the real webserver UI
and a live 6-hourly schedule.

The one rough edge: on a machine whose Windows timezone name isn't in
`pendulum`'s mapping table, log timestamp formatting throws noisy (but
harmless) `--- Logging error ---` tracebacks. Safe to ignore — it never
affects task results or test outcomes.

## Running locally (no Docker)

```bash
python -m venv .venv
.venv/Scripts/activate  # or source .venv/bin/activate on Linux/macOS

pip install -r requirements-dev.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"

cp .env.example .env  # point WAREHOUSE_DATABASE_URL at your own Postgres

alembic upgrade head
```

Then, with `AIRFLOW_HOME` set to a local directory and `airflow db migrate`
run once (creates Airflow's own SQLite metadata DB):

```bash
export AIRFLOW_HOME=./airflow_home
airflow db migrate
python -c "from etl.extract.csv_source import generate_sample_transactions_csv as g; g('data/incoming/sample.csv', n=200)"
python -c "
import dags.sales_etl_dag as m
m.dag.test()
"
```

## Running with Docker Compose

```bash
docker compose up --build
```

This starts Postgres (with two databases — `airflow` for Airflow's own
metadata, `warehouse` for our tables), the API stub, runs both init jobs
(`airflow db migrate` + admin user; `alembic upgrade head` for the
warehouse), then the webserver (http://localhost:8090, `admin`/`admin`)
and scheduler. Drop a CSV into `data/incoming/` (or generate one with
`etl.extract.csv_source.generate_sample_transactions_csv`) before or after
the first scheduled run, then trigger the DAG from the UI or:

```bash
docker exec <scheduler-container> airflow dags trigger sales_etl
```

## Tests

```bash
ruff check .
pytest -v
```

- `tests/unit/` — transform layer on plain pandas DataFrames, the sample
  CSV generator — no database, no Airflow
- `tests/integration/` — SCD2/fact/mart loaders against a real Postgres
  (window functions and `ON CONFLICT` are Postgres-specific — SQLite
  wouldn't actually prove this layer works), and the full-DAG test via
  `dag.test()` against that same Postgres plus a real running instance of
  the API stub

Integration tests need a Postgres reachable at `TEST_WAREHOUSE_DATABASE_URL`
(defaults to `localhost:5432`, matching what GitHub Actions' `services:
postgres:` container publishes — CI already provides this, just run a
throwaway one locally: `docker run -p 5432:5432 -e POSTGRES_USER=warehouse
-e POSTGRES_PASSWORD=warehouse -e POSTGRES_DB=warehouse postgres:16-alpine`).
