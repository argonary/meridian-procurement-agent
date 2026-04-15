# Meridian – Session Primer
Last updated: 2026-04-12

## Phase status
- [x] Phase 1: generator complete — all 5 CSVs produced and verified
- [x] Phase 2: all 6 validation checks passing, `meridian.db` written
- [ ] Phase 3: raw API agent working end-to-end
- [ ] Phase 4: LangGraph refactor complete
- [ ] Phase 5: Streamlit UI deployed and shareable
- [ ] Phase 6: Databricks swap complete

---

## Phase 1 — Data generator

**File:** `generator/generate_data.py`
Run `python generate_data.py` from `generator/` → writes all five CSVs to `data/`.

### Functions
1. `generate_suppliers(n=50)` — 50 suppliers, tier distribution 5/10/20/15
   (strategic/preferred/approved/spot). Near-duplicate pair: SUP-018 "Hartwell
   Industrial Supply" / SUP-019 "Hartwell Industrial Supplies".

2. `generate_purchase_orders(suppliers, n=2000)` — 2,000 POs, Jan 2024–Mar 2026.
   Power-law supplier weights; top-10 suppliers = 75.2% of POs.
   Status breakdown: 1,842 closed / 105 open / 53 cancelled.

3. `generate_po_line_items(purchase_orders)` — 7,731 lines across 1,947
   non-cancelled POs (avg 3.97 lines/PO). `total_value` on each PO row is the
   sum of `line_total` for its lines.

4. `generate_goods_receipts(purchase_orders, suppliers, line_items)` — 1,830
   receipts (one per closed PO, minus 12 dropped for the anomaly).
   Returns `(df, missing_ids)`.

5. `generate_invoices(purchase_orders, suppliers)` — 1,842 invoices (one per
   closed PO). Returns `(df, overbilling_supplier_id)`.

### Key design decisions
- **SEED = 108.** Each function uses `SEED + N` offset (1–4) so RNG streams are
  independent.
- **PO date range Jan 2024 – Mar 2026** — extended from the blueprint's Dec 2025
  because today is 2026-04-12; the original range produced zero open POs.
- **Overbilling supplier: SUP-018** Hartwell Industrial Supply (approved/category per
  generate_suppliers SEED=108). Selected as the approved-tier supplier with the most
  closed POs (22 invoices). 86.4% of its invoices are overbilled; avg ratio 1.0511.
  All other suppliers: avg ratio ≤ 1.003, none exceeding the 1.03 threshold.
  Note: changed from SUP-005 when tier-aware weight assignment reduced approved-tier
  PO volume — SUP-018 became the top approved supplier by closed PO count.
- **Missing receipt anomaly:** 12 closed corporate POs have no `goods_receipt`
  row. Selected as first 12 closed corporate POs in `po_id` order. IDs:
  PO-00005, PO-00008, PO-00032, PO-00034, PO-00035, PO-00036,
  PO-00042, PO-00047, PO-00057, PO-00059, PO-00078, PO-00090.
  Hard-coded in a comment inside `generate_goods_receipts()`.
  Note: IDs changed from prior session after tier-aware weight fix reshuffled PO distribution.

### CSV row counts (SEED=108, verified)
| File                | Rows  | Cols |
|---------------------|-------|------|
| suppliers.csv       |    50 |    7 |
| purchase_orders.csv | 2,000 |    9 |
| po_line_items.csv   | 7,731 |    8 |
| goods_receipts.csv  | 1,825 |    9 |
| invoices.csv        | 1,837 |    9 |

---

## Phase 2 — Validation

**File:** `validate_data.py` (project root)
Loads all 5 CSVs from `data/` into `meridian.db` (overwrites on each run),
then runs 6 validation checks against it. All 6 pass with SEED=108.

### Checks
1. **Pareto** — top-10 suppliers ≥ 75% of total spend. ✓
2. **Tier vs OTIF** — strategic > preferred > approved > spot; strategic ~0.94,
   spot ~0.65. ✓
3. **Price variance** — direct_materials ~2–3%, tail spend ~20–30%. ✓
4. **Overbilling** — exactly one supplier with avg invoice/PO ratio > 1.03. ✓
5. **Missing receipts** — 10–15 closed POs with no `goods_receipt`, all from
   corporate BU. ✓
6. **Referential integrity** — zero NULL foreign keys across all three join
   checks. ✓

### OTIF fix (applied during Phase 2)
`generate_goods_receipts()` was refactored to use a single combined OTIF draw
per receipt instead of two independent Bernoulli draws. Failed receipts are
assigned one of three failure modes with equal weight: late+complete,
on-time+short, late+short. A post-generation floor correction is applied for
the strategic tier (SEED=108 with n≈83 receipts produced a realized rate below
the 0.90 floor; the minimum number of failing rows are flipped deterministically
using the seeded rng).

---

## Phase 3 — Raw API agent (NEXT)

### Exact next action
Build `agent_raw.py` — a minimal Anthropic API agent that answers natural
language procurement questions by running SQL against `meridian.db`.

**Start with `tools.py`**, then wire it into the agent loop in `agent_raw.py`.

### `tools.py` spec
- One tool: `execute_sql`
- Input schema: `{ "query": "<SQL string>" }`
- Executes the query against `meridian.db` via `sqlite3`
- Returns rows as a JSON-serialisable list of dicts (use `sqlite3.Row` +
  `dict()`)
- Cap output at a reasonable row limit (e.g., 100 rows) to avoid blowing the
  context window
- Raise descriptive errors on bad SQL so the model can self-correct

### `agent_raw.py` spec
- Imports `tools.py`; builds the tool definition in Anthropic's tool-use format
- System prompt: Meridian procurement analyst, answers only from the database,
  cites SQL used
- Agentic loop: call Claude → if `tool_use` block → execute tool → append
  `tool_result` → loop; stop on `end_turn` or when no tool calls remain
- Accepts a question as a CLI argument (`python agent_raw.py "your question"`)
- Prints final text response to stdout

### Schema cheat-sheet for the system prompt
```
suppliers(supplier_id, name, tier, category, country, payment_terms, contact_email)
purchase_orders(po_id, supplier_id, business_unit, category, status, issue_date,
                expected_delivery, actual_delivery, total_value)
po_line_items(line_id, po_id, item_description, quantity, unit_price, line_total,
              category, unit_of_measure)
goods_receipts(receipt_id, po_id, supplier_id, receipt_date, quantity_ordered,
               quantity_received, is_on_time, is_complete, otif)
invoices(invoice_id, po_id, supplier_id, invoice_date, invoice_amount,
         po_amount, ratio, is_overbilled)
```

---

## Open blockers
None.
