# Meridian Industrial: Procurement Analytics Platform

A portfolio project demonstrating end-to-end procurement analytics across data modeling,
SQL analysis, AI agent development, and executive dashboarding. Built on a causally
structured synthetic dataset covering the full procure-to-pay lifecycle.

Ask the conversational agent about supplier performance, spend concentration, or overbilling
anomalies in plain English. Explore the same data through six standalone SQL analyses and
an executive Lakeview dashboard.

---

## Business motivation

Procurement is one of the least digitized functions in most industrial companies. Analysts
spend hours writing SQL or waiting for reports to answer questions like which suppliers are
underperforming, where spend is concentrated, or whether invoices match PO values.

This project approaches that problem from three angles: a structured data warehouse with
documented governance, a library of standalone SQL analyses covering the core procurement
audit questions, and a conversational AI agent that makes the same analysis accessible to
non-technical users.

The dataset is synthetic but the problems are real. Overbilling, supplier duplication,
single-source concentration risk, and missing goods receipts are common issues in industrial
procurement that cost companies money and create audit exposure.

---

## What this project covers

### 1. Data warehouse and schema design

A 5-table relational schema modeling the full procure-to-pay lifecycle: supplier master,
purchase orders, line-item spend, goods receipts, and accounts payable invoices. The schema
is causally structured -- supplier tier predicts OTIF performance, spend category predicts
price variance, and four intentional anomalies are baked into the data. Fully documented in
`analysis/data_architecture.md`.

Delta Lake time travel is implemented on the suppliers table across four versioned states,
enabling point-in-time vendor master queries and supplier audit trail analysis.

### 2. SQL analyses

Six standalone SQL analyses in `analysis/sql/` answer the operational questions a
procurement manager or VP would actually ask:

1. Where is spend concentrated, and is that concentration a risk?
2. Are higher-tier suppliers actually outperforming on delivery?
3. Where are we paying above-contract pricing?
4. Which invoices show signs of overbilling?
5. Which supplier relationships carry the most combined risk?
6. How complete is our three-way match coverage?

Each query is written with a business-language header explaining the procurement context
and analytical rationale.

### 3. Executive dashboard

A Lakeview dashboard on Databricks surfaces four executive KPIs (Total Active Spend,
Suppliers with HIGH Risk, Overbilling Exposure, and Invoices Failing Three-Way Match)
alongside three operational charts covering spend concentration, supplier OTIF by tier,
and price variance by item.

### 4. Conversational AI agent

A LangGraph agent accepts plain English questions and returns structured procurement
analysis. Example queries:

- "Which suppliers make up 80% of our spend?" -> Pareto analysis with tier breakdown
- "Which supplier is overbilling us?" -> Anomaly detection across invoices
- "What is the OTIF rate by supplier tier?" -> Delivery performance with causal explanation
- "What tier was Hartwell before they were flagged?" -> Delta time travel query

The agent uses a three-node graph: a Planner that generates a SQL query plan, an Executor
that runs each query and retries on failure, and a Synthesizer that interprets results and
produces a plain-English answer.

---

## Tech stack

| Layer | Technology |
|---|---|
| Agent framework | LangGraph |
| LLM | Anthropic API (claude-sonnet-4-6) |
| Database (Tier 1) | SQLite |
| Database (Tier 2) | Databricks Delta / Unity Catalog |
| Dashboard | Databricks Lakeview |
| UI | Streamlit |
| Language | Python |

---

## Project structure

agent/
  tools.py                    the only DB-aware function
  graph.py                    LangGraph planner/executor/synthesizer
  agent_raw.py                raw Anthropic API agent without LangGraph
analysis/
  sql/                        six standalone procurement SQL analyses
  erd/                        entity-relationship diagram
  data_architecture.md        full schema design and governance documentation
  data_dictionary.md          column-level reference
  README.md                   analytical framing and threshold rationale
generator/
  generate_data.py            synthetic dataset with causal logic
databricks/
  seed_supplier_history.py    seeds Delta version history
streamlit_app.py              chat UI
validate_data.py              6 validation checks against generated data
config.py                     constants and environment variables

---

## Running locally

Requires Python 3.11+, an Anthropic API key, and the packages in requirements.txt.

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

---

## The four anomalies

| Anomaly | Description |
|---|---|
| Overbilling | SUP-018 Hartwell Industrial Supply is the primary overbilling offender, triggering a tier demotion and deactivation in Version 3 of the vendor master |
| Near-duplicates | SUP-018 Hartwell Industrial Supply vs SUP-019 Hartwell Industrial Supplies -- spend is split, performance is masked |
| Concentration risk | Three high-spend categories are single-sourced |
| Compliance gap | Corporate BU has closed POs with no goods receipt confirmation |