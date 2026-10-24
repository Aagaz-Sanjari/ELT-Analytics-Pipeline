import importlib
import time
import uuid
from datetime import datetime, timezone

import yaml
from sqlalchemy import text

from ingest.loader import build_rows, insert_rows, engine

create_schema_stmt = text("create schema if not exists ops")

create_table_stmt = text(
    "create table if not exists ops.pipeline_runs ("
    "id bigserial primary key, "
    "source text not null, "
    "dataset text not null, "
    "started_at timestamptz not null, "
    "finished_at timestamptz not null, "
    "duration_seconds numeric not null, "
    "status text not null, "
    "records_fetched int not null default 0, "
    "records_inserted int not null default 0, "
    "error_message text)"
)

count_stmt = text(
    "select count(*) from raw.ingested_records "
    "where source = :source and dataset = :dataset"
)

log_stmt = text(
    "insert into ops.pipeline_runs "
    "(source, dataset, started_at, finished_at, duration_seconds, status, "
    "records_fetched, records_inserted, error_message) "
    "values (:source, :dataset, :started_at, :finished_at, :duration_seconds, "
    ":status, :records_fetched, :records_inserted, :error_message)"
)


def ensure_ops_table():
    with engine.begin() as conn:
        conn.execute(create_schema_stmt)
        conn.execute(create_table_stmt)


def build_connector(entry):
    module_path, class_name = entry["connector"].rsplit(".", 1)
    connector_class = getattr(importlib.import_module(module_path), class_name)
    return connector_class(entry.get("params"))


def count_rows(source, dataset):
    with engine.connect() as conn:
        return conn.execute(
            count_stmt, {"source": source, "dataset": dataset}
        ).scalar()


def log_run(fields):
    with engine.begin() as conn:
        conn.execute(log_stmt, fields)


def run_connector(entry):
    connector = build_connector(entry)
    started_at = datetime.now(timezone.utc)
    timer = time.perf_counter()
    fetched = 0
    inserted = 0
    status = "success"
    error = None

    try:
        before = count_rows(connector.source, connector.dataset)
        records = connector.fetch()
        rows = build_rows(
            connector.source, connector.dataset, records, str(uuid.uuid4())
        )
        insert_rows(rows)
        fetched = len(rows)
        inserted = count_rows(connector.source, connector.dataset) - before
    except Exception as exc:
        status = "failed"
        error = str(exc)[:500]

    log_run(
        {
            "source": connector.source,
            "dataset": connector.dataset,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc),
            "duration_seconds": round(time.perf_counter() - timer, 2),
            "status": status,
            "records_fetched": fetched,
            "records_inserted": inserted,
            "error_message": error,
        }
    )
    print(
        f"{connector.source}/{connector.dataset}: {status}, "
        f"fetched {fetched}, new {inserted}"
    )
    return status


def main():
    ensure_ops_table()

    with open("sources.yml") as f:
        config = yaml.safe_load(f)

    statuses = [run_connector(entry) for entry in config["sources"]]
    if "failed" in statuses:
        raise SystemExit(1)


if __name__ == "__main__":
    main()