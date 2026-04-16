# Meridian – Session Primer
Last updated: 2026-04-16

## Phase status
- [x] Phase 1: generator complete — all 5 CSVs produced and verified
- [x] Phase 2: all 6 validation checks passing, `meridian.db` written
- [x] Phase 3: raw API agent working end-to-end — all 4 demo questions pass, zero schema errors
- [x] Phase 4: LangGraph refactor complete — all 4 demo questions pass via `graph.py`
- [x] Phase 5: Streamlit UI built and running — all 4 demo questions render correctly in browser
- [x] Phase 6: Databricks swap complete — all 4 demo questions verified, results match SQLite exactly
- [x] Phase 7: Delta time travel complete — VERSION AS OF working, all 4 versions queryable, 2 example questions verified

**All phases complete. No next action.**

---

## What was built last session

### Phase 7 — Delta time travel extension

**Files changed:**

1. **`databricks/seed_supplier_history.py`** (new) — runs three mutations against
   `workspace.default.suppliers` via `databricks-sql-connector` to create 4 Delta versions.
   Imports credentials from `config.py`. Run once from project root:
   `python databricks/seed_supplier_history.py`

2. **`agent/graph.py`** — one paragraph added to `SCHEMA_CONTEXT` (the planner system prompt).
   Documents `VERSION AS OF` syntax, the version map, and when to use time travel.

3. **`streamlit_app.py`** — two time travel example questions added to the sidebar question list,
   plus a caption: "Time travel queries supported — ask about supplier state at any point since Jan 2024."

**No other files touched.**

### Delta version map

| Version | Event | Change |
|---------|-------|--------|
| 0 | Initial load | 50 suppliers baseline |
| 1 | 2024-07-15 supplier tier review | SUP-022 Palisade Pneumatics promoted `approved → preferred` |
| 2 | 2024-07-15 new strategic supplier | SUP-051 Meridian Alloy Backup Co inserted as `strategic` |
| 3 | 2025-02-01 overbilling response | SUP-018 Hartwell Industrial Supply demoted to `spot`, `is_active = FALSE` |

### Critical implementation note — VERSION AS OF, not TIMESTAMP AS OF

`TIMESTAMP AS OF` requires the table to predate the query timestamp. Because
`workspace.default.suppliers` was created today (2026-04-16), any `TIMESTAMP AS OF`
reference to a historical date (e.g. `'2024-07-15'`) fails — the table did not exist
at that timestamp. `VERSION AS OF` queries by Delta commit version number, which is
always valid regardless of when the table was created. This is the correct approach
for this project.

The planner system prompt explicitly uses `VERSION AS OF` with the version map so the
model generates correct queries without needing to discover this limitation through
the retry loop.

### Verified time travel questions

| Question | Behavior |
|---|---|
| What tier was Hartwell Industrial Supply before they were flagged? | Queries VERSION AS OF 2; returns `approved` |
| How has our supplier roster changed since the initial load? | Compares VERSION AS OF 0 vs VERSION AS OF 3 |

---

## Phase 1 — Data generator

**File:** `generator/generate_data.py`
Run `python generate_data.py` from `generator/` → writes all five CSVs to `data/`.

### Functions
1. `generate_suppliers(n=50)` — 50 suppliers, tier distribution 5/10/20/15
   (strategic/preferred/approved/spot). Near-duplicate pair: SUP-018 "Hartwell
   Industrial Supply" / SUP-019 "Hartwell Industrial Supplies".

2. `generate_purchase_orders(suppliers, n=2000)` — 2,000 POs, Jan 2024–Mar 2026.
   Tier-aware Zipf weights: strategic indices get the highest ranks, then preferred,
   approved, spot. Within each tier, order is shuffled. Top-10 suppliers scaled to
   exactly 0.75 of total PO weight.

3. `generate_po_line_items(purchase_orders, suppliers)` — 7,731 lines across all
   non-cancelled POs. Accepts `suppliers` as a second argument. Applies a
   tier-aware cost ceiling multiplier to `cost_hi` per PO:
   strategic 1.00 | preferred 0.85 | approved 0.60 | spot 0.25.
   `total_value` on each PO row is the sum of `line_total` for its lines.

4. `generate_goods_receipts(purchase_orders, suppliers, line_items)` — 1,825
   receipts (one per closed PO, minus 12 dropped for the anomaly).
   Returns `(df, missing_ids)`.

5. `generate_invoices(purchase_orders, suppliers)` — 1,837 invoices (one per
   closed PO). Returns `(df, overbilling_supplier_id)`.

### Key design decisions
- **SEED = 108.** Each function uses `SEED + N` offset (1–4) so RNG streams are independent.
- **PO date range Jan 2024 – Mar 2026** — extended from the blueprint's Dec 2025 because
  today is 2026-04-16; the original range produced zero open POs.
- **Tier-aware weight fix:** original generator used a random Zipf shuffle with no tier
  awareness — spot suppliers reached 63.2% of total spend. Fix: strategic indices receive
  the highest Zipf ranks, descending by tier. Result: spot now at 3.9% of spend.
- **Tier-aware cost ceiling:** spot POs were producing avg PO values above strategic.
  Fix: `cost_hi` multiplied by tier factor in `generate_po_line_items()`.
- **Overbilling supplier: SUP-018** Hartwell Industrial Supply (approved tier).
  Selected dynamically as the approved-tier supplier with the most closed POs.
  22 invoices; 86.4% overbilled; avg ratio 1.0511.
- **Missing receipt anomaly:** 12 closed corporate POs have no `goods_receipt` row.
  Selected as first 12 closed corporate POs in `po_id` order. Current IDs:
  PO-00005, PO-00008, PO-00032, PO-00034, PO-00035, PO-00036,
  PO-00042, PO-00047, PO-00057, PO-00059, PO-00078, PO-00090.
  Hard-coded in a comment inside `generate_goods_receipts()`.

### CSV row counts (SEED=108, verified)
| File                | Rows  | Cols |
|---------------------|-------|------|
| suppliers.csv       |    50 |    7 |
| purchase_orders.csv | 2,000 |    9 |
| po_line_items.csv   | 7,731 |    8 |
| goods_receipts.csv  | 1,825 |    9 |
| invoices.csv        | 1,837 |    9 |

### Tier spend breakdown (verified)
| Tier      |   POs | Avg PO Value |   Total Spend | % of Total |
|-----------|------:|-------------:|--------------:|-----------:|
| strategic | 1,156 |     $448,129 |  $518,037,580 |      67.3% |
| approved  |   259 |     $484,945 |  $125,600,660 |      16.3% |
| preferred |   414 |     $233,257 |   $96,568,380 |      12.5% |
| spot      |   118 |     $251,214 |   $29,643,260 |       3.9% |

---

## Phase 2 — Validation

**File:** `validate_data.py` (project root)
Loads all 5 CSVs from `data/` into `meridian.db` (overwrites on each run),
then runs 6 validation checks. All 6 pass with SEED=108.

### Checks
1. **Pareto** — top-10 suppliers ≥ 75% of total spend. ✓
2. **Tier vs OTIF** — strategic > preferred > approved > spot; strategic ~0.94, spot ~0.65. ✓
3. **Price variance** — direct_materials ~2–3%, tail spend ~20–30%. ✓
4. **Overbilling** — exactly one approved-tier supplier with avg ratio > 1.03. ✓
   Dynamically finds the top overbilling approved-tier supplier (SUP-018).
5. **Missing receipts** — 10–15 closed POs with no `goods_receipt`, all corporate. ✓
6. **Referential integrity** — zero NULL foreign keys across all join checks. ✓

### OTIF fix (applied Phase 2)
`generate_goods_receipts()` uses a single combined OTIF draw per receipt. Failed
receipts get one of three failure modes with equal weight: late+complete,
on-time+short, late+short. Post-generation floor correction flips the minimum
number of failing rows for any tier that falls below its floor (strategic floor 0.90).

---

## Phase 3 — Raw API agent

**Status: complete. All 4 demo questions answered correctly, zero schema errors.**

### Files built
- **`config.py`** (project root) — `ANTHROPIC_API_KEY` from `.env` + `MODEL = "claude-sonnet-4-6"`.
  Also loads `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` (added Phase 6).
  `MODEL` is defined here and imported everywhere; never hardcoded in agent files.
- **`agent/tools.py`** — `execute_sql(query)` via Databricks SQL connector (swapped Phase 6).
  100-row cap (`fetchmany`), raises `RuntimeError` with failed query + error text for model
  self-correction. `TOOL_DEFINITION` dict in Anthropic format — used only by `agent_raw.py`.
- **`agent/agent_raw.py`** — full agentic loop.
  - `run_agent(user_question, history=None)` — `history=None` with None-guard inside.
  - Loop: call Claude → `tool_use` → `execute_sql` → append `tool_result` → repeat.
  - `MAX_TURNS = 6`, `MAX_RETRIES = 2` per failing query, graceful error on exhaustion.
  - CLI: `python agent/agent_raw.py "your question"` from project root.
  - Logs every SQL call with row count and execution time.

### Exact schema (verified against Databricks DESCRIBE)
```
suppliers(supplier_id, supplier_name, tier, category_focus, country, is_active, onboarded_date)
purchase_orders(po_id, supplier_id, business_unit, category, po_date,
                expected_delivery_date, total_value, payment_terms, status)
po_line_items(line_id, po_id, item_name, category, standard_cost,
              unit_price, quantity, line_total)
goods_receipts(receipt_id, po_id, supplier_id, received_date, qty_ordered,
               qty_received, qty_rejected, on_time, in_full)
invoices(invoice_id, po_id, supplier_id, invoice_date, due_date, paid_date,
         invoice_amount, po_total_value, status)
```
Status values are lowercase throughout: `closed`, `open`, `cancelled`, `paid`,
`pending`, `overdue`, `disputed`. `on_time` and `in_full` are native `BOOLEAN` in
Databricks Delta (were 1/0 integers in SQLite).

---

## Phase 4 — LangGraph refactor

**Status: complete. All 4 demo questions pass via `graph.py`, answers match `agent_raw.py`.**

### File: `agent/graph.py`

- **`AgentState(TypedDict)`** — keys: `question`, `plan`, `results`, `answer`, `retry_count`.
- **`planner` node** — calls Claude (`max_tokens=2048`) with a JSON-mode prompt; returns a list of
  `{description, sql}` steps. Two-stage parse: tries `json.loads()` directly; on failure,
  collapses literal newlines to spaces and retries. Falls back to empty plan with error log.
- **`executor` node** — iterates the plan, calls `execute_sql()` per step. On `RuntimeError`,
  sends the failed SQL + error to Claude for a corrected query and retries once.
- **`synthesizer` node** — calls Claude with the original question + all step results
  (capped at 50 rows per step), produces the final natural language answer.
- Graph: `planner → executor → synthesizer → END`.
- **`run_graph(question) → str`** — public entry point used by the Streamlit app.
- **`_strip_fences(text)`** — removes markdown code fences from any model response.

### Critical config note
Planner `max_tokens` is **2048** (raised from 1024 in Phase 5). Do not lower it.
At 1024, complex multi-step plans with large composite JOINs are truncated mid-JSON,
causing a silent 0-step fallback and hallucinated synthesizer output.

---

## Phase 5 — Streamlit UI

**Status: complete. All demo questions render correctly in browser. No errors.**

### File: `streamlit_app.py` (project root)

- Wide layout, `st.chat_input` / `st.chat_message`, session state history
- Sidebar: 6 example questions (4 standard + 2 time travel) + schema reference + glossary
- Each user message calls `run_graph()` under `st.spinner`; answers rendered as markdown
- Error handling: bare `except Exception` surfaces errors in chat window, no crash

**Run:** `streamlit run streamlit_app.py` → http://localhost:8501

**Env note:** Required `pip install "protobuf>=4.21,<5"` to fix a `google.protobuf` import
error on this machine (Anaconda environment with conflicting protobuf version).

---

## Phase 6 — Databricks swap

**Status: complete. All 4 demo questions verified against Databricks, results match SQLite exactly.**

**Files changed:** `.env`, `config.py`, `agent/tools.py` only.

**Key facts:**
- Warehouse type: Serverless SQL Warehouse on AWS (`.cloud.databricks.com`)
- Unity Catalog path: `workspace.default.<table>`
- `dbsql.connect()` takes `catalog="workspace"`, `schema="default"` — unqualified table names resolve correctly
- Boolean dialect: `on_time = 1` fails; `on_time = TRUE` works — retry loop handles transparently
- `dbsql.exc.DatabaseError` is the caught error type (replaces `sqlite3.OperationalError`)

**Verified results (Databricks, match SQLite baseline):**

| Question | Result |
|---|---|
| Which suppliers = 80% of spend? | 6 suppliers; Delta Alloy Partners 54.4% |
| OTIF by tier? | Strategic 93.3% → Preferred 89.3% → Approved 79.6% → Spot 66.1% |
| Which supplier overbills? | SUP-018 Hartwell; 19 invoices; avg +5.95% |
| Closed POs with no receipt? | 12 POs, all corporate, correct IDs |

---

## Phase 7 — Delta time travel

**Status: complete. VERSION AS OF working, all 4 versions queryable, 2 example questions verified.**

**Files changed:** `databricks/seed_supplier_history.py` (new), `agent/graph.py`, `streamlit_app.py`.

**Key facts:**
- Syntax: `VERSION AS OF` — NOT `TIMESTAMP AS OF`
- `TIMESTAMP AS OF` fails because the table was created 2026-04-16; historical timestamps
  predate the table's existence. `VERSION AS OF` is always valid.
- Version map hardcoded in planner system prompt so model generates correct queries without retry

**Version map:**

| Version | Event | Change |
|---------|-------|--------|
| 0 | Initial load | 50 suppliers baseline |
| 1 | Tier review | SUP-022 Palisade Pneumatics `approved → preferred` |
| 2 | New supplier | SUP-051 Meridian Alloy Backup Co inserted as `strategic` |
| 3 | Overbilling response | SUP-018 Hartwell `→ spot`, `is_active = FALSE` |

**To recreate version history on a fresh table:**
`python databricks/seed_supplier_history.py` from project root.

---

## Open blockers
None. All phases complete.
