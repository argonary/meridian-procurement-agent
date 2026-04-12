import pathlib
import sqlite3

import pandas as pd

DB_PATH  = pathlib.Path("meridian.db")
DATA_DIR = pathlib.Path("data")

# ── Step 1: Load CSVs into meridian.db ────────────────────────────────────────
DB_PATH.unlink(missing_ok=True)
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

TABLE_MAP = {
    "suppliers":       DATA_DIR / "suppliers.csv",
    "purchase_orders": DATA_DIR / "purchase_orders.csv",
    "po_line_items":   DATA_DIR / "po_line_items.csv",
    "goods_receipts":  DATA_DIR / "goods_receipts.csv",
    "invoices":        DATA_DIR / "invoices.csv",
}

print("Loading CSVs into meridian.db ...")
for table, path in TABLE_MAP.items():
    df = pd.read_csv(path)
    df.to_sql(table, conn, if_exists="replace", index=False)
    print(f"  {table:<20} {len(df):>6} rows")
print()

# ── Step 2: Validation checks ─────────────────────────────────────────────────
results = []

def check(n, label, passed, detail=""):
    status = "PASS" if passed else f"FAIL  — {detail}"
    print(f"Check {n}  {label}: {status}")
    results.append(passed)


# Check 1 — Row counts
counts = dict(conn.execute("""
    SELECT 'suppliers'       AS t, COUNT(*) FROM suppliers
    UNION ALL SELECT 'purchase_orders',  COUNT(*) FROM purchase_orders
    UNION ALL SELECT 'po_line_items',    COUNT(*) FROM po_line_items
    UNION ALL SELECT 'goods_receipts',   COUNT(*) FROM goods_receipts
    UNION ALL SELECT 'invoices',         COUNT(*) FROM invoices
""").fetchall())

c1_ok = (
    40   <= counts["suppliers"]      <= 60    and
           counts["purchase_orders"] == 2000  and
    5000 <= counts["po_line_items"]  <= 12000 and
    1700 <= counts["goods_receipts"] <= 1900  and
    1700 <= counts["invoices"]       <= 1900
)
check(1, "Row counts",
      c1_ok,
      f"suppliers={counts['suppliers']}, purchase_orders={counts['purchase_orders']}, "
      f"po_line_items={counts['po_line_items']}, goods_receipts={counts['goods_receipts']}, "
      f"invoices={counts['invoices']}")


# Check 2 — OTIF by supplier tier
# goods_receipts has supplier_id denormalized; join directly to suppliers
rows = conn.execute("""
    SELECT s.tier,
           AVG(CASE WHEN gr.on_time = 1 AND gr.in_full = 1 THEN 1.0 ELSE 0.0 END) AS otif
    FROM goods_receipts gr
    JOIN suppliers s ON gr.supplier_id = s.supplier_id
    GROUP BY s.tier
""").fetchall()
otif = {r["tier"]: r["otif"] for r in rows}

tier_bounds = {
    "strategic": (0.90, 1.00),
    "preferred": (0.83, 0.91),
    "approved":  (0.73, 0.83),
    "spot":      (0.60, 0.70),
}
c2_fail = []
for tier, (lo, hi) in tier_bounds.items():
    val = otif.get(tier)
    if val is None or not (lo <= val <= hi):
        c2_fail.append(
            f"{tier}={val:.4f} (want {lo:.2f}–{hi:.2f})" if val is not None
            else f"{tier}=missing"
        )
check(2, "OTIF by supplier tier", not c2_fail, ", ".join(c2_fail))


# Check 3 — Price variance by category
# Generator categories: direct_materials (sigma 0.025), mro (0.12),
# indirect (0.15), services (0.15). No 'tail_spend' category exists.
# Thresholds: direct_materials ≤ 5%; indirect ≥ 10% (actual ~12%)
rows = conn.execute("""
    SELECT category,
           AVG(ABS((unit_price - standard_cost) / standard_cost)) AS abs_var
    FROM po_line_items
    GROUP BY category
""").fetchall()
var = {r["category"]: r["abs_var"] for r in rows}

dm_ok       = var.get("direct_materials", 999) <= 0.05
indirect_ok = var.get("indirect",         0)   >= 0.10
c3_ok       = dm_ok and indirect_ok
detail3 = []
if not dm_ok:
    v = var.get("direct_materials")
    detail3.append(
        f"direct_materials={v:.4f} (want ≤0.05)" if v is not None else "direct_materials=missing"
    )
if not indirect_ok:
    v = var.get("indirect")
    detail3.append(
        f"indirect={v:.4f} (want ≥0.10)" if v is not None else "indirect=missing"
    )
check(3, "Price variance by category", c3_ok, "; ".join(detail3))


# Check 4 — Overbilling anomaly (SUP-005)
# invoices has supplier_id and po_total_value denormalized; no extra joins needed
row = conn.execute("""
    SELECT
        COUNT(*)                                                                AS total,
        SUM(CASE WHEN invoice_amount > po_total_value THEN 1 ELSE 0 END)       AS overbilled,
        AVG(invoice_amount / po_total_value)                                    AS avg_ratio
    FROM invoices
    WHERE supplier_id = 'SUP-005'
""").fetchone()

if row["total"] == 0:
    check(4, "Overbilling anomaly SUP-005", False, "no invoices found for SUP-005")
else:
    pct   = row["overbilled"] / row["total"]
    ratio = row["avg_ratio"]
    c4_ok = pct >= 0.80 and 1.03 <= ratio <= 1.09
    check(4, "Overbilling anomaly SUP-005", c4_ok,
          f"overbill_pct={pct:.4f} (want ≥0.80), avg_ratio={ratio:.4f} (want 1.03–1.09)")


# Check 5 — Near-duplicate supplier names
names = {r[0] for r in conn.execute("SELECT supplier_name FROM suppliers").fetchall()}
has_supply   = "Hartwell Industrial Supply"   in names
has_supplies = "Hartwell Industrial Supplies" in names
c5_ok = has_supply and has_supplies
check(5, "Near-duplicate supplier names", c5_ok,
      f"'Hartwell Industrial Supply'={'found' if has_supply else 'MISSING'}, "
      f"'Hartwell Industrial Supplies'={'found' if has_supplies else 'MISSING'}")


# Check 6 — Compliance gap (corporate BU, closed PO, no receipt)
# Note: business_unit values are lowercase in the data ('corporate')
missing_cnt = conn.execute("""
    SELECT COUNT(*) AS cnt
    FROM purchase_orders po
    WHERE po.status        = 'closed'
      AND po.business_unit = 'corporate'
      AND NOT EXISTS (
          SELECT 1 FROM goods_receipts gr WHERE gr.po_id = po.po_id
      )
""").fetchone()["cnt"]

check(6, "Compliance gap (corporate, no receipt)", missing_cnt >= 5,
      f"found {missing_cnt} qualifying POs (want ≥5)")


# ── Summary ───────────────────────────────────────────────────────────────────
conn.close()
passed = sum(results)
total  = len(results)
print(f"\n{passed}/{total} checks passed")
