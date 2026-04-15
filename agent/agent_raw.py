"""
Meridian procurement agent — raw Anthropic API implementation.

Usage:
    python agent_raw.py "Which suppliers account for 80% of our total spend?"

The agent runs an agentic loop: call Claude → if tool_use → execute SQL →
append tool_result → repeat. Stops on end_turn or when no tool calls remain.
"""

import sys
import json
import logging
import argparse

import anthropic

from config import ANTHROPIC_API_KEY, MODEL
from tools import execute_sql, TOOL_DEFINITION

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

MAX_TURNS = 6
MAX_RETRIES = 2

SYSTEM_PROMPT = """You are a procurement analytics assistant for Meridian Industrial, \
a fictional industrial manufacturing company. You answer natural language questions \
about procurement data by writing and executing SQL queries against a SQLite database.

Rules:
- Answer ONLY from data returned by execute_sql. Never make up numbers.
- Always cite the SQL query you used in your final answer.
- If a query returns an error, read the error message, correct the SQL, and retry.
- After 2 failed attempts on the same query, respond with a clear error message.
- Keep answers concise and business-readable. Use bullet points for lists.

Database schema:
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
  purchase_orders.status values: 'closed', 'open', 'cancelled' (all lowercase)
  invoices.status values: 'paid', 'pending', 'overdue', 'disputed' (all lowercase)
  goods_receipts.on_time, in_full: 1=true, 0=false (SQLite booleans)
  OTIF = on_time=1 AND in_full=1

Domain glossary:
  OTIF — On Time In Full. A receipt is OTIF when on_time=1 AND in_full=1.
  Price variance — (unit_price - standard_cost) / standard_cost. Positive = paid above benchmark.
  DPO — Days Payable Outstanding. Average days between invoice_date and paid_date.
  Overbilling — invoice_amount > po_total_value, typically flagged when invoice_amount/po_total_value > 1.03.
  Tier — supplier classification: strategic > preferred > approved > spot.
"""


def run_agent(user_question: str, history: list | None = None) -> tuple[str, list]:
    """Run the procurement agent on a single question.

    Args:
        user_question: Natural language procurement question.
        history: Prior conversation turns (list of Anthropic message dicts).
                 Pass None (default) for a fresh conversation.

    Returns:
        (answer_text, updated_history)
    """
    if history is None:
        history = []

    messages = history + [{"role": "user", "content": user_question}]
    fail_counts: dict[str, int] = {}
    turns = 0

    while turns < MAX_TURNS:
        turns += 1

        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=[TOOL_DEFINITION],
            messages=messages,
        )

        logger.info("Turn %d | stop_reason=%s | blocks=%d", turns, response.stop_reason, len(response.content))

        if response.stop_reason == "end_turn":
            # Extract the final text block
            text_blocks = [b for b in response.content if b.type == "text"]
            answer = text_blocks[-1].text if text_blocks else "(no text response)"
            messages.append({"role": "assistant", "content": response.content})
            return answer, messages

        if response.stop_reason == "tool_use":
            tool_results = []
            messages.append({"role": "assistant", "content": response.content})

            for block in response.content:
                if block.type != "tool_use":
                    continue

                query = block.input.get("query", "")
                logger.info("Tool call | id=%s | query=%s", block.id, query)

                try:
                    rows = execute_sql(query)
                    result_content = json.dumps(rows, default=str)
                    fail_counts.pop(query, None)
                except RuntimeError as e:
                    fail_counts[query] = fail_counts.get(query, 0) + 1
                    if fail_counts[query] >= MAX_RETRIES:
                        result_content = (
                            f"ERROR (max retries reached): {e}. "
                            "Please respond with a graceful error message."
                        )
                    else:
                        result_content = f"ERROR: {e}. Please correct the SQL and try again."
                    logger.warning("SQL error (attempt %d): %s", fail_counts[query], e)

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_content,
                })

            messages.append({"role": "user", "content": tool_results})
            continue

        # Unexpected stop reason
        logger.warning("Unexpected stop_reason: %s", response.stop_reason)
        break

    return "Agent reached the maximum turn limit without producing a final answer.", messages


def main() -> None:
    parser = argparse.ArgumentParser(description="Meridian procurement agent")
    parser.add_argument("question", help="Natural language procurement question")
    args = parser.parse_args()

    answer, _ = run_agent(args.question)
    print("\n" + answer)


if __name__ == "__main__":
    main()
