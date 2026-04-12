# Meridian – Session Primer
Last updated: 2026-04-12

## Phase status
- [x] Phase 1: generator complete — all 5 CSVs produced and verified
- [x] Phase 2: all 6 validation checks passing
- [ ] Phase 3: raw API agent working end-to-end
- [ ] Phase 4: LangGraph refactor complete
- [ ] Phase 5: Streamlit UI deployed and shareable
- [ ] Phase 6: Databricks swap complete

## What was built this session
`generator/generate_data.py` — fully implemented, all five functions wired
through `main()`. Running `python generate_data.py` from the `generator/`
directory writes all five CSVs to `data/`.

### Functions implemented
1. `generate_suppliers(n=50)` — 50 suppliers, tier distribution 5/10/20/15
   (strategic/preferred/approved/spot), near-duplicate pair at SUP-018/SUP-019
   (Hartwell Industrial Supply vs Hartwell Industrial Supplies).

2. `generate_purchase_orders(suppliers, n=2000)` — 2,000 POs, Jan 2024–Mar 2026
   (extended from Dec 2025 to produce open POs with a 60-day cutoff against
   today 2026-04-12). Power-law supplier weights, top-10 = 75.2% of POs.
   Status: 1,842 closed / 105 open / 53 cancelled.

3. `generate_po_line_items(purchase_orders)` — 7,731 lines across 1,947
   non-cancelled POs (avg 3.97 lines/PO). `total_value` written back to
   `purchase_orders` as sum of `line_total` per PO.

4. `generate_goods_receipts(purchase_orders, suppliers, line_items)` — 1,830
   receipts (one per closed PO, minus 12 dropped for the anomaly). Returns a
   tuple `(df, missing_ids)`.

5. `generate_invoices(purchase_orders, suppliers)` — 1,842 invoices (one per
   closed PO). Returns a tuple `(df, overbilling_supplier_id)`.

### Key design decisions
- **SEED = 108**. Each function uses `SEED + N` offset (1–4) so RNG streams
  are independent.
- **PO date range Jan 2024 – Mar 2026** — extended from the blueprint's Dec 2025
  because today is 2026-04-12; the original range produced zero open POs.
- **Overbilling supplier: SUP-005** Cardinal Fluid Systems (approved/services).
  Selected as the approved-tier supplier with the most closed POs (230 invoices).
  84.8% overbilled, avg ratio 1.0515, range 0.99–1.08. All other suppliers avg
  ratio 1.0000 with zero invoices > 1.03.
- **Missing receipt anomaly**: 12 closed corporate POs have no goods_receipt row.
  Selected as first 12 closed corporate POs in po_id order. IDs:
  PO-00039, PO-00043, PO-00045, PO-00053, PO-00081, PO-00082,
  PO-00090, PO-00119, PO-00120, PO-00142, PO-00148, PO-00150
  These are hard-coded in a comment inside `generate_goods_receipts()`.

## CSV row counts (SEED=108, verified)
| File                | Rows  | Cols |
|---------------------|-------|------|
| suppliers.csv       |    50 |    7 |
| purchase_orders.csv | 2,000 |    9 |
| po_line_items.csv   | 7,731 |    8 |
| goods_receipts.csv  | 1,830 |    9 |
| invoices.csv        | 1,842 |    9 |

## Exact next action
Phase 2: create `tests/validate.py`. Load the five CSVs into an in-memory
SQLite database and run all six validation queries from BUILD_BLUEPRINT.docx
Section 6. All six must pass before touching the agent.

Checks to implement:
1. Pareto — top 10 suppliers ≥ 75% of total spend
2. Tier vs OTIF — strategic > preferred > approved > spot, strategic ~0.94, spot ~0.65
3. Price variance — direct_materials ~2–3%, tail spend ~20–30%
4. Overbilling — exactly one supplier with avg invoice/PO ratio > 1.03
5. Missing receipts — 10–15 closed POs with no goods_receipt, all from corporate BU
6. Referential integrity — zero NULL foreign keys across all three join checks

## Validation decisions (Phase 2)
- `validate_data.py` loads all 5 CSVs from `data/` into persistent `meridian.db`
  (overwriting any existing file), then runs 6 validation checks against it.
- **OTIF fix**: `generate_goods_receipts()` now uses a single combined OTIF draw
  per receipt instead of two independent Bernoulli draws. Failed receipts are
  assigned one of three failure modes with equal weight:
  late+complete, on-time+short, late+short.
- A post-generation floor correction was applied for the strategic tier
  (small sample edge case: n≈83 receipts with SEED=108 produced a realized
  rate below the 0.90 floor; the minimum number of failing rows are flipped
  deterministically using the seeded rng).
- All 6 checks passing with SEED=108.

## Open blockers
None.
