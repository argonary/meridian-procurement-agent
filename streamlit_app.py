"""
Meridian Industrial — Procurement Analytics Agent
Streamlit chat UI. Wraps agent/graph.py via run_graph().

Run from project root:
    streamlit run streamlit_app.py
"""

import sys
from pathlib import Path

# Ensure agent/ is on the path so `from tools import` inside graph.py resolves.
_root = Path(__file__).parent
_agent_dir = _root / "agent"
for _p in [str(_root), str(_agent_dir)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import streamlit as st
from agent.graph import run_graph  # noqa: E402  (import after sys.path setup)

# ── Page config ────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Meridian Procurement Agent",
    page_icon="🏭",
    layout="wide",
)

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🏭 Meridian Industrial")
    st.caption("Procurement Analytics Agent")

    st.divider()

    st.subheader("Example questions")
    examples = [
        "Which suppliers account for 80% of our total spend?",
        "What is our OTIF rate broken down by supplier tier?",
        "Which supplier is overbilling us, and by how much?",
        "Show me all closed POs that have no goods receipt.",
        "What tier was Hartwell Industrial Supply before they were flagged?",
        "How has our supplier roster changed since the initial load?",
    ]
    for q in examples:
        st.markdown(f"- *{q}*")

    st.caption("Time travel queries supported — ask about supplier state at any point since Jan 2024.")

    st.divider()

    st.subheader("Schema reference")
    st.markdown("""
**suppliers**
`supplier_id`, `supplier_name`, `tier`, `category_focus`, `country`, `is_active`, `onboarded_date`

**purchase_orders**
`po_id`, `supplier_id`, `business_unit`, `category`, `po_date`, `expected_delivery_date`, `total_value`, `payment_terms`, `status`

**po_line_items**
`line_id`, `po_id`, `item_name`, `category`, `standard_cost`, `unit_price`, `quantity`, `line_total`

**goods_receipts**
`receipt_id`, `po_id`, `supplier_id`, `received_date`, `qty_ordered`, `qty_received`, `qty_rejected`, `on_time`, `in_full`

**invoices**
`invoice_id`, `po_id`, `supplier_id`, `invoice_date`, `due_date`, `paid_date`, `invoice_amount`, `po_total_value`, `status`
""")

    st.divider()
    st.caption("Tiers: strategic › preferred › approved › spot")
    st.caption("OTIF = on_time=1 AND in_full=1")
    st.caption("Overbilling = invoice_amount > po_total_value × 1.03")

# ── Session state ──────────────────────────────────────────────────────────────

if "history" not in st.session_state:
    st.session_state.history: list[dict] = []

# ── Main panel ─────────────────────────────────────────────────────────────────

st.title("Procurement Analytics Agent")
st.caption("Ask a natural language question about Meridian's procurement data.")

# Render existing conversation
for msg in st.session_state.history:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Chat input
if prompt := st.chat_input("Ask a procurement question…"):
    # Show the user's message immediately
    with st.chat_message("user"):
        st.markdown(prompt)
    st.session_state.history.append({"role": "user", "content": prompt})

    # Run the agent with a spinner
    with st.chat_message("assistant"):
        with st.spinner("Querying the database…"):
            try:
                answer = run_graph(prompt)
            except Exception as exc:
                answer = f"**Error:** {exc}"

        st.markdown(answer)

    st.session_state.history.append({"role": "assistant", "content": answer})
