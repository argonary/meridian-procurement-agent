"""
Meridian procurement agent — LangGraph planner/executor/synthesizer.

Architecture:
    planner  — Claude decomposes the question into [{description, sql}, ...] steps
    executor — runs each SQL via execute_sql(), single retry with LLM fix on error
    synthesizer — Claude reads question + all results, produces the final answer

Usage (from project root):
    PYTHONPATH=agent python agent/graph.py "Which suppliers account for 80% of our total spend?"

Windows:
    set PYTHONPATH=agent && python agent/graph.py "your question"
"""

import sys
import json
import logging
import argparse
import re
from pathlib import Path
from typing import TypedDict

# Ensure project root is on sys.path so `config` is importable when this script
# is invoked as `python agent/graph.py` from the project root.
_project_root = Path(__file__).parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import anthropic
from langgraph.graph import StateGraph, END

from config import ANTHROPIC_API_KEY, MODEL
from tools import execute_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ── Shared schema context ──────────────────────────────────────────────────────

SCHEMA_CONTEXT = """\
Database schema (SQLite):
  suppliers(supplier_id, supplier_name, tier, category_focus, country, is_active, onboarded_date)
  purchase_orders(po_id, supplier_id, business_unit, category, po_date,
                  expected_delivery_date, total_value, payment_terms, status)
  po_line_items(line_id, po_id, item_name, category, standard_cost,
                unit_price, quantity, line_total)
  goods_receipts(receipt_id, po_id, supplier_id, received_date, qty_ordered,
                 qty_received, qty_rejected, on_time, in_full)
  invoices(invoice_id, po_id, supplier_id, invoice_date, due_date, paid_date,
           invoice_amount, po_total_value, status)

Column notes:
  purchase_orders.status: 'closed', 'open', 'cancelled'  (lowercase)
  invoices.status: 'paid', 'pending', 'overdue', 'disputed'  (lowercase)
  goods_receipts.on_time, in_full: 1=true 0=false  (SQLite booleans)
  OTIF = on_time=1 AND in_full=1

Domain glossary:
  OTIF          — On Time In Full. Receipt is OTIF when on_time=1 AND in_full=1.
  Price variance — (unit_price - standard_cost) / standard_cost. Positive = paid above benchmark.
  DPO           — Days Payable Outstanding. Avg days between invoice_date and paid_date.
  Overbilling   — invoice_amount > po_total_value; flagged when ratio > 1.03.
  Tier          — strategic > preferred > approved > spot.\
"""

# ── State ──────────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    question: str
    plan: list[dict]    # [{description: str, sql: str}, ...]
    results: list[dict] # [{description, sql, rows, error}, ...]
    answer: str
    retry_count: int    # reserved for future conditional branching


# ── Helper ─────────────────────────────────────────────────────────────────────

def _strip_fences(text: str) -> str:
    """Remove optional markdown code fences from a model response."""
    text = re.sub(r"^```(?:json|sql)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


# ── Nodes ──────────────────────────────────────────────────────────────────────

def planner(state: AgentState) -> dict:
    """Decompose the question into a JSON list of {description, sql} query steps."""
    question = state["question"]
    logger.info("Planner | question=%r", question)

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=(
            "You are a SQL planning assistant for a procurement analytics database.\n"
            "Given a natural language question, output a JSON array of query steps.\n"
            "Each step must be an object with exactly two string keys:\n"
            '  "description": plain English label for this step\n'
            '  "sql": a valid, complete SQLite SELECT query — written on ONE LINE (no literal newlines inside the string)\n'
            "Output ONLY the JSON array — no explanation, no markdown fences, no extra text.\n"
            "CRITICAL: SQL values must not contain literal newline characters; use spaces to separate clauses.\n\n"
            + SCHEMA_CONTEXT
        ),
        messages=[{"role": "user", "content": question}],
    )

    raw = _strip_fences(response.content[0].text)
    logger.info("Planner raw | %s", raw[:400])

    try:
        plan = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: collapse literal newlines to spaces so multi-line SQL values
        # embedded in the JSON string become valid single-line strings.
        sanitized = " ".join(raw.splitlines())
        try:
            plan = json.loads(sanitized)
        except json.JSONDecodeError as exc:
            logger.error("Planner JSON parse failed after sanitization: %s | raw=%r", exc, raw)
            plan = []

    if not isinstance(plan, list):
        plan = [plan]

    logger.info("Planner | %d step(s)", len(plan))
    return {"plan": plan, "results": [], "retry_count": 0}


def executor(state: AgentState) -> dict:
    """Execute each SQL step. On error, ask Claude for a corrected query and retry once."""
    results: list[dict] = []

    for i, step in enumerate(state["plan"]):
        description = step.get("description", f"Step {i + 1}")
        sql = step.get("sql", "")
        logger.info("Executor | step %d: %s", i + 1, description)

        try:
            rows = execute_sql(sql)
            results.append({"description": description, "sql": sql, "rows": rows, "error": None})
            logger.info("Executor | step %d ok | %d row(s)", i + 1, len(rows))

        except RuntimeError as first_err:
            logger.warning("Executor | step %d failed (attempt 1): %s", i + 1, first_err)

            # Single retry: ask Claude to fix the SQL
            fix_response = client.messages.create(
                model=MODEL,
                max_tokens=512,
                system=(
                    "You are a SQL debugging assistant. Fix the broken SQLite query below.\n"
                    "Return ONLY the corrected SQL — no explanation, no markdown fences.\n\n"
                    + SCHEMA_CONTEXT
                ),
                messages=[
                    {
                        "role": "user",
                        "content": (
                            f"Step: {description}\n"
                            f"Failed SQL:\n{sql}\n\n"
                            f"Error: {first_err}\n\n"
                            "Return ONLY the corrected SQL."
                        ),
                    }
                ],
            )
            fixed_sql = _strip_fences(fix_response.content[0].text)
            logger.info("Executor | step %d retry SQL: %s", i + 1, fixed_sql[:200])

            try:
                rows = execute_sql(fixed_sql)
                results.append({"description": description, "sql": fixed_sql, "rows": rows, "error": None})
                logger.info("Executor | step %d retry ok | %d row(s)", i + 1, len(rows))
            except RuntimeError as second_err:
                logger.error("Executor | step %d retry also failed: %s", i + 1, second_err)
                results.append({
                    "description": description,
                    "sql": fixed_sql,
                    "rows": [],
                    "error": str(second_err),
                })

    return {"results": results}


def synthesizer(state: AgentState) -> dict:
    """Produce the final natural language answer from the question and all query results."""
    question = state["question"]
    results = state["results"]
    logger.info("Synthesizer | %d result step(s)", len(results))

    # Build a compact evidence block
    sections = []
    for i, r in enumerate(results):
        header = f"Step {i + 1}: {r['description']}"
        sql_line = f"SQL: {r['sql']}"
        if r["error"]:
            data_line = f"ERROR: {r['error']}"
        else:
            # Cap at 50 rows to stay within token limits
            data_line = f"Rows ({len(r['rows'])} returned): {json.dumps(r['rows'][:50], default=str)}"
        sections.append(f"{header}\n{sql_line}\n{data_line}")

    evidence = "\n\n".join(sections)

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=(
            "You are a procurement analytics assistant for Meridian Industrial. "
            "Answer the user's question using ONLY the data provided below. "
            "Never fabricate numbers. Keep the answer concise and business-readable. "
            "Use bullet points for lists. Cite the SQL query used for key figures."
        ),
        messages=[
            {
                "role": "user",
                "content": f"Question: {question}\n\nQuery results:\n{evidence}",
            }
        ],
    )

    answer = response.content[0].text.strip()
    logger.info("Synthesizer | answer=%d chars", len(answer))
    return {"answer": answer}


# ── Graph assembly ─────────────────────────────────────────────────────────────

def _build_graph():
    g = StateGraph(AgentState)
    g.add_node("planner", planner)
    g.add_node("executor", executor)
    g.add_node("synthesizer", synthesizer)
    g.set_entry_point("planner")
    g.add_edge("planner", "executor")
    g.add_edge("executor", "synthesizer")
    g.add_edge("synthesizer", END)
    return g.compile()


_app = _build_graph()


# ── Public interface ───────────────────────────────────────────────────────────

def run_graph(question: str) -> str:
    """Run the LangGraph agent on a single question and return the final answer string."""
    initial_state: AgentState = {
        "question": question,
        "plan": [],
        "results": [],
        "answer": "",
        "retry_count": 0,
    }
    final_state = _app.invoke(initial_state)
    return final_state["answer"]


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    # Reconfigure stdout to UTF-8 so em-dashes, bullets, etc. print on Windows.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Meridian procurement agent (LangGraph)")
    parser.add_argument("question", help="Natural language procurement question")
    args = parser.parse_args()

    answer = run_graph(args.question)
    print("\n" + answer)


if __name__ == "__main__":
    main()
