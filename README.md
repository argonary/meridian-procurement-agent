# Meridian Industrial: Procurement Analytics Agent

A conversational AI agent that answers natural language procurement questions against a causally-structured synthetic dataset. Ask it about supplier performance, spend concentration, overbilling anomalies, or how the supplier roster has changed over time.

Built as a portfolio project to demonstrate end-to-end AI agent architecture, data engineering discipline, and cloud database integration.

## Business motivation

Procurement is one of the least digitized functions in most industrial companies. Analysts spend hours writing SQL or waiting for reports to answer questions like which suppliers are underperforming, where spend is concentrated, or whether invoices match PO values. The goal of this project is to make that analysis conversational: a procurement manager should be able to ask a question in plain English and get a structured, sourced answer in seconds.

The dataset is synthetic but the problems are real. Overbilling, supplier duplication, single-source concentration risk, and missing goods receipts are all common issues in industrial procurement that cost companies money and create audit exposure.

## What it does

The agent accepts plain English questions and returns structured analysis:

- "Which suppliers make up 80% of our spend?" -> Pareto analysis with tier breakdown
- "Which supplier is overbilling us?" -> Statistical anomaly detection across 1,837 invoices
- "What is the OTIF rate by supplier tier?" -> Delivery performance with causal explanation
- "What tier was Hartwell Industrial Supply before they were flagged?" -> Delta time travel query across version history

## Architecture

The agent uses a three-node LangGraph graph: a Planner that generates a multi-step SQL query plan, an Executor that runs each query via execute_sql() and retries on failure, and a Synthesizer that interprets results and produces a natural language answer.

**Single DB-aware function.** execute_sql() in agent/tools.py is the only place that knows what database is running. Swapping from SQLite to Databricks meant changing one function -- graph.py, streamlit_app.py, and agent_raw.py were untouched.

**LangGraph planner/executor/synthesizer separation.** Each node has one job. The retry loop handles dialect differences between SQLite and Databricks automatically -- it caught boolean type mismatches and missing functions without any hardcoded fixes.

**Causal synthetic data.** The dataset is not random noise. Supplier tier drives OTIF rates (strategic ~93%, spot ~66%), category type drives price variance, and spend follows a power law. Four intentional anomalies are baked in.

**Delta time travel.** The suppliers table on Databricks has version history across 4 commits. The agent queries any version using VERSION AS OF syntax, enabling audit trail questions that are impossible against a traditional database.

## Tech stack

| Layer | Technology |
|---|---|
| Agent framework | LangGraph |
| LLM | Anthropic API (claude-sonnet-4-6) |
| Database (Tier 1) | SQLite |
| Database (Tier 2) | Databricks Delta / Unity Catalog |
| UI | Streamlit |
| Language | Python |

## Running locally

Requires Python 3.11+, an Anthropic API key, and the packages in requirements.txt.

Clone the repo and install dependencies:
    git clone https://github.com/argonary/meridian-procurement-agent
    cd meridian-procurement-agent
    pip install -r requirements.txt

Add your Anthropic API key to .env:
    ANTHROPIC_API_KEY=your-key-here

Generate the dataset and load it into SQLite:
    python generator/generate_data.py
    python validate_data.py

Run the agent from the command line:
    python agent/graph.py "Which supplier is overbilling us?"

Or launch the Streamlit UI:
    streamlit run streamlit_app.py

Databricks credentials are optional -- the agent runs fully on SQLite without them.

## Project structure

    agent/
      tools.py                    the only DB-aware function
      graph.py                    LangGraph planner/executor/synthesizer
      agent_raw.py                raw Anthropic API agent without LangGraph
    generator/
      generate_data.py            synthetic dataset with causal logic
    databricks/
      seed_supplier_history.py    seeds Delta version history
    streamlit_app.py              chat UI
    validate_data.py              6 validation checks against generated data
    config.py                     constants and environment variables

## The four anomalies

| Anomaly | Description |
|---|---|
| Overbilling | SUP-018 Hartwell Industrial Supply bills 4-8% above PO value on 85% of invoices |
| Near-duplicates | SUP-018 Hartwell Industrial Supply vs SUP-019 Hartwell Industrial Supplies |
| Concentration risk | Three high-spend categories are single-sourced |
| Compliance gap | Corporate BU has closed POs with no goods receipt |
