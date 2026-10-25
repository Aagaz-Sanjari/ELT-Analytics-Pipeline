from ingest.loader import engine
from sqlalchemy import text

query = text(
    "select source, started_at, error_message "
    "from ops.pipeline_runs where status = 'failed' order by started_at desc"
)

with engine.connect() as conn:
    for row in conn.execute(query):
        print(row)