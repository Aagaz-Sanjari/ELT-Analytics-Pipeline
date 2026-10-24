from ingest.loader import engine
from sqlalchemy import text

query = text(
    "select source, dataset, status, records_fetched, records_inserted, "
    "duration_seconds, started_at "
    "from ops.pipeline_runs order by started_at desc limit 10"
)

with engine.connect() as conn:
    for row in conn.execute(query):
        print(row)