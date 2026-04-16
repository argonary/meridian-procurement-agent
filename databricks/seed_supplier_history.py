"""
seed_supplier_history.py — Simulate three supplier change events against
workspace.default.suppliers to seed Delta time travel history.

Run from project root:
    python databricks/seed_supplier_history.py

Each UPDATE/INSERT commits a new Delta version. Use TIMESTAMP AS OF or
VERSION AS OF in subsequent queries to read historical states.
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path so config is importable.
_project_root = Path(__file__).parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from databricks import sql as dbsql
from config import DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN

CATALOG = "workspace"
SCHEMA = "default"


def run_events():
    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
        catalog=CATALOG,
        schema=SCHEMA,
    )

    try:
        with conn.cursor() as cur:

            # ── Event 1 — 2024-07-15 supplier tier review ─────────────────────
            cur.execute("""
                UPDATE suppliers
                SET tier = 'preferred'
                WHERE supplier_id = 'SUP-022'
            """)
            print("[2024-07-15] Event 1 complete: SUP-022 Palisade Pneumatics tier updated approved → preferred")

            # ── Event 2 — 2024-07-15 new strategic supplier onboarded ─────────
            cur.execute("""
                INSERT INTO suppliers
                    (supplier_id, supplier_name, tier, country, category_focus, is_active, onboarded_date)
                VALUES
                    ('SUP-051', 'Meridian Alloy Backup Co', 'strategic', 'USA', 'Raw Materials', TRUE, '2024-07-15')
            """)
            print("[2024-07-15] Event 2 complete: SUP-051 Meridian Alloy Backup Co inserted as strategic supplier")

            # ── Event 3 — 2025-02-01 overbilling response ─────────────────────
            cur.execute("""
                UPDATE suppliers
                SET tier = 'spot',
                    is_active = FALSE
                WHERE supplier_id = 'SUP-018'
            """)
            print("[2025-02-01] Event 3 complete: SUP-018 Hartwell Industrial Supply demoted to spot and deactivated")

    finally:
        conn.close()

    print("\nAll 3 events committed. Delta version history is now queryable via TIMESTAMP AS OF.")


if __name__ == "__main__":
    run_events()
