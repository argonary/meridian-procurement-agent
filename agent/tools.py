from databricks import sql as dbsql
import time
import logging
from config import DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN

ROW_LIMIT = 100
CATALOG = "workspace"
SCHEMA = "default"

logger = logging.getLogger(__name__)


def execute_sql(query: str) -> list[dict]:
    """Execute a SQL query against the Databricks Unity Catalog and return rows as a list of dicts.

    Caps output at ROW_LIMIT rows. Raises a descriptive RuntimeError on bad SQL
    so the model can read the error and self-correct.
    """
    start = time.perf_counter()
    try:
        conn = dbsql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
            catalog=CATALOG,
            schema=SCHEMA,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                raw_rows = cur.fetchmany(ROW_LIMIT)
                rows = [dict(zip(columns, row)) for row in raw_rows]
        finally:
            conn.close()
    except dbsql.exc.DatabaseError as e:
        raise RuntimeError(
            f"SQL execution failed.\nQuery: {query}\nError: {e}"
        ) from e

    elapsed = time.perf_counter() - start
    logger.info("SQL executed | rows=%d | time=%.3fs | query=%s", len(rows), elapsed, query)
    return rows


# Tool definition in Anthropic tool-use format — imported by agent_raw.py and graph.py
TOOL_DEFINITION = {
    "name": "execute_sql",
    "description": (
        "Execute a SQL query against the Meridian Industrial procurement database. "
        "Returns up to 100 rows as a JSON array of objects. "
        "If the query fails, the error message will be returned so you can correct the SQL and retry."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "A valid SQL query against the Meridian procurement database.",
            }
        },
        "required": ["query"],
    },
}
