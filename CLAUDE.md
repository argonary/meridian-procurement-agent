# Meridian Industrial — Procurement Analytics Agent

## Project overview
Conversational AI agent that answers natural language procurement questions
against a causally-structured synthetic dataset for a fictional company.

## Tech stack
Python · SQLite (Tier 1) · Databricks Delta (Tier 2) · Anthropic API
Model: claude-sonnet-4-6 · LangGraph · Streamlit

## Build philosophy
Finish line first. Working SQLite demo before any Databricks work.
Do not start Tier 2 until Tier 1 checklist is fully complete.

## Phase order
1. Data generator (generate_data.py)
2. Anomaly seeding + goods_receipts + invoices
3. Validation queries (all 6 must pass)
4. Raw API agent (agent_raw.py)
5. LangGraph refactor (graph.py)
6. Streamlit UI (streamlit_app.py)
7. Databricks swap (tools.py only)

## Session rule
At the end of every session, rewrite primer.md completely.
Include: what was built this session, exact state of each phase,
the single next action, and any open blockers.
