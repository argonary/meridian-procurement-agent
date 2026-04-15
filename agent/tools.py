import sqlite3
import json
import time
import logging
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "meridian.db"
ROW_LIMIT = 100

logger = logging.getLogger(__name__)


def execute_sql(query: str) -> list[dict]:
    """Execute a SQL query against meridian.db and return rows as a list of dicts.

    Caps output at ROW_LIMIT rows. Raises a descriptive RuntimeError on bad SQL
    so the model can read the error and self-correct.
    """
    start = time.perf_counter()
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(query)
            rows = [dict(r) for r in cur.fetchmany(ROW_LIMIT)]
        finally:
            conn.close()
    except sqlite3.OperationalError as e:
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
                "description": "A valid SQLite query against the Meridian procurement database.",
            }
        },
        "required": ["query"],
    },
}
