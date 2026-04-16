# Meridian – Session Primer
Last updated: 2026-04-15

## Phase status
- [x] Phase 1: generator complete — all 5 CSVs produced and verified
- [x] Phase 2: all 6 validation checks passing, `meridian.db` written
- [x] Phase 3: raw API agent working end-to-end — all 4 demo questions pass, zero schema errors
- [x] Phase 4: LangGraph refactor complete — all 4 demo questions pass via `graph.py`
- [x] Phase 5: Streamlit UI built and running — all 4 demo questions render correctly in browser
- [ ] Phase 6: Databricks swap complete

---

## What was built this session

### 1. `streamlit_app.py` (project root) — Phase 5

Single-page chat UI wrapping `run_graph()` from `agent/graph.py`.

**Layout:**
- `st.chat_input` / `st.chat_message` — standard Streamlit chat pattern
- `st.session_state.history` — list of `{role, content}` dicts, persists across reruns
- Each question calls `run_graph()` under `st.spinner`; answer rendered as `st.markdown`
- `except Exception as exc` wraps `run_graph()` — surfaces errors in chat rather than crashing

**Sidebar:**
- 4 example questions (italic list)
- Full schema reference — all 5 tables with column lists
- Glossary footer: OTIF definition, overbilling threshold, tier order

**Import path:**
Both project root and `agent/` added to `sys.path` before importing `from agent.graph import run_graph`,
so `from config import` and `from tools import` inside `graph.py` resolve correctly when launched
via `streamlit run streamlit_app.py`.

**Run command:** `streamlit run streamlit_app.py` from project root → http://localhost:8501

**Streamlit version:** 1.56.0. Required `protobuf` downgrade to `4.25.9` to fix
`ImportError: cannot import name 'descriptor' from 'google.protobuf'` on this environment.

### 2. Planner `max_tokens` fix — `agent/graph.py` line ~99

Raised planner `max_tokens` from `1024` → `2048`.

**Why:** Complex questions (e.g. "Who are our worst performing suppliers?") cause the planner to
emit a 5-step plan with a large composite JOIN in step 5. At 1024 tokens the JSON response was
truncated mid-string — both `json.loads()` parse attempts failed, plan fell back to 0 steps,
executor ran nothing, synthesizer hallucinated a generic non-answer. At 2048 tokens the full JSON
parses cleanly every time.

---

## Key decisions made

- **Planner token budget is the critical lever.** The planner's output is structured JSON containing
  full SQL strings. Multi-step plans with subquery-heavy SQL easily exceed 1024 tokens. 2048 is
  the right floor; if more complex questions arise, raise to 4096 before changing architecture.

- **No architectural change needed for the truncation bug.** Option B (simpler planner + combiner
  step) was considered but rejected — Option A (raise max_tokens) is less complexity for the same
  result. The current planner/executor/synthesizer structure is sound.

- **Streamlit `run_graph` import uses package-style path** (`from agent.graph import run_graph`)
  rather than manipulating `sys.path` to make `agent/` the root. This avoids a name collision
  where `graph` could shadow stdlib or other modules.

---

## Questions answered this session (verified)

| Question | Result |
|---|---|
| Which suppliers = 80% of spend? | 6 suppliers; Delta Alloy Partners 54.4% |
| OTIF by tier? | Strategic 93.3% → Preferred 89.3% → Approved 79.6% → Spot 66.1% |
| Which supplier overbills? | SUP-018 Hartwell; 19 invoices; avg +5.95% |
| Closed POs with no receipt? | 12 POs, all corporate, correct IDs |
| Who are our worst performing suppliers? | SUP-018 Hartwell (score 30.93, 86.4% overbilling) |
| Which supplier has the most rejected goods? | SUP-049 Thunderbolt Hydraulics, 76 units |

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
  today is 2026-04-14; the original range produced zero open POs.
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
  `MODEL` is defined here and imported everywhere; never hardcoded in agent files.
- **`agent/tools.py`** — `execute_sql(query)` via sqlite3, 100-row cap (`fetchmany`),
  raises `RuntimeError` with failed query + error text for model self-correction.
  `TOOL_DEFINITION` dict in Anthropic format — used only by `agent_raw.py`.
  `DB_PATH = Path(__file__).parent.parent / "meridian.db"` (project root).
- **`agent/agent_raw.py`** — full agentic loop.
  - `run_agent(user_question, history=None)` — `history=None` with None-guard inside.
    Using `history=[]` as default would bleed conversation state across calls.
  - Loop: call Claude → `tool_use` → `execute_sql` → append `tool_result` → repeat.
  - `MAX_TURNS = 6`, `MAX_RETRIES = 2` per failing query, graceful error on exhaustion.
  - CLI: `python agent/agent_raw.py "your question"` from project root.
  - Logs every SQL call with row count and execution time.

### DB path — critical note
`validate_data.py` writes `meridian.db` to the **project root** (uses relative
`pathlib.Path("meridian.db")`), not `data/`. `tools.py` resolves to project root
via `__file__`. Both point to the same file. `data/meridian.db` exists but is empty.

### Exact schema in system prompt (verified against PRAGMA table_info)
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
`pending`, `overdue`, `disputed`. `on_time` and `in_full` are SQLite booleans (1/0).

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
Planner `max_tokens` is **2048** (raised from 1024 this session). Do not lower it.
At 1024, complex multi-step plans with large composite JOINs are truncated mid-JSON,
causing a silent 0-step fallback and hallucinated synthesizer output.

---

## Phase 5 — Streamlit UI

**Status: complete. All demo questions render correctly in browser. No errors.**

### File: `streamlit_app.py` (project root)

- Wide layout, `st.chat_input` / `st.chat_message`, session state history
- Sidebar: 4 example questions + full schema reference + glossary
- Each user message calls `run_graph()` under `st.spinner`; answers rendered as markdown
- Error handling: bare `except Exception` surfaces errors in chat window, no crash

**Run:** `streamlit run streamlit_app.py` → http://localhost:8501

**Env note:** Required `pip install "protobuf>=4.21,<5"` to fix a `google.protobuf` import
error on this machine (Anaconda environment with conflicting protobuf version).

---

## Phase 6 — Databricks swap (NEXT)

### Exact next action
Open `agent/tools.py`. Replace the `sqlite3` connection block with `databricks-sql-connector`.
`execute_sql()` signature and return type (`list[dict]`) must stay identical — nothing else moves.

### What changes in `tools.py`
```python
# Replace this:
import sqlite3
conn = sqlite3.connect(DB_PATH)
...

# With this (sketch):
from databricks import sql as dbsql
conn = dbsql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"],
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
)
```
Qualify table names with `{catalog}.{schema}.` prefix if Unity Catalog is enabled.

### Env vars needed
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_CATALOG` / `DATABRICKS_SCHEMA`

### Acceptance test
Run all 4 demo questions via `python agent/graph.py "..."` and confirm answers match
the SQLite baseline before calling Phase 6 complete.

---

## Open blockers
None.
