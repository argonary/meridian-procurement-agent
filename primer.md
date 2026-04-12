# Meridian – Session Primer
Last updated: 2026-04-12

## Phase status
- [x] Phase 1: generator complete — all 5 CSVs produced and verified
- [ ] Phase 2: all 6 validation checks passing
- [ ] Phase 3: raw API agent working end-to-end
- [ ] Phase 4: LangGraph refactor complete
- [ ] Phase 5: Streamlit UI deployed and shareable
- [ ] Phase 6: Databricks swap complete

## What was done last session
Built `generator/generate_data.py` end-to-end. All five generator functions
implemented and wired through `main()`. Key decisions:

- **SEED = 108** throughout; each function uses `SEED + N` offset to keep
  random streams independent.
- **PO date range extended to Jan 2024 – Mar 2026** (blueprint said Dec 2025
  but today is 2026-04-12, so everything would have been `closed` with a
  60-day cutoff — extended to produce ~105 open POs).
- **Overbilling supplier**: `SUP-005` Cardinal Fluid Systems (approved/services),
  selected as the approved-tier supplier with the most closed POs (230 invoices,
  84.8% overbilled, avg ratio 1.0515). Picked by volume so Check 4 has signal.
- **Missing receipt anomaly**: 12 closed corporate POs have no goods_receipt row.
  IDs hard-coded in comment inside `generate_goods_receipts()`:
  PO-00039, PO-00043, PO-00045, PO-00053, PO-00081, PO-00082,
  PO-00090, PO-00119, PO-00120, PO-00142, PO-00148, PO-00150

## CSV row counts (SEED=108)
| File                | Rows  |
|---------------------|-------|
| suppliers.csv       |    50 |
| purchase_orders.csv | 2,000 |
| po_line_items.csv   | 7,731 |
| goods_receipts.csv  | 1,830 |
| invoices.csv        | 1,842 |

## Exact next action
Phase 2: load the five CSVs into SQLite and run the 6 validation queries
from BUILD_BLUEPRINT.docx Section 6. All 6 must pass before moving to
the agent. Create `tests/validate.py` (or a notebook) that loads
`data/*.csv` into an in-memory SQLite DB and runs each check.

## Open blockers
None.
