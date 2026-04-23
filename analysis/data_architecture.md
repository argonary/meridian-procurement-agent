# Data Architecture: Meridian Industrial Procurement Analytics

**Document type:** Data architecture & design specification  
**Scope:** Procurement analytics platform — source-to-pay domain  
**Schema version:** 2.0 (5-table core + Delta Lake time travel on suppliers)  
**Build status:** Complete — all phases including time travel operational  
**Last updated:** April 2026

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Business Context & Problem Statement](#2-business-context--problem-statement)
3. [Schema Overview](#3-schema-overview)
4. [Entity-Relationship Diagram](#4-entity-relationship-diagram)
5. [Table-by-Table Design Rationale](#5-table-by-table-design-rationale)
6. [Relationships & Cardinality](#6-relationships--cardinality)
7. [Data Dictionary](#7-data-dictionary)
8. [Grain & Aggregation Logic](#8-grain--aggregation-logic)
9. [Causal Logic & Intentional Anomalies](#9-causal-logic--intentional-anomalies)
10. [Normalization Decisions](#10-normalization-decisions)
11. [Slowly Changing Dimensions & Time Travel](#11-slowly-changing-dimensions--time-travel)
12. [Key Analytical Domains](#12-key-analytical-domains)
13. [Data Quality & Validation Framework](#13-data-quality--validation-framework)
14. [At Enterprise Scale: Production Architecture](#14-at-enterprise-scale-production-architecture)
15. [Design Tradeoffs & Alternatives Considered](#15-design-tradeoffs--alternatives-considered)
16. [Appendix: Glossary](#16-appendix-glossary)

---

## 1. Executive Summary

Meridian Industrial is a fictional mid-large heavy industrial manufacturer — the kind of company where procurement is operationally critical, not merely a cost center. A missed delivery on direct materials can halt a production line. A single-source category represents genuine supply chain exposure. An overbilling supplier at volume is a material financial loss.

This document describes the data architecture underlying Meridian's procurement analytics platform: a conversational AI agent that allows business users to ask natural language questions about procurement activity and receive data-backed, business-readable answers.

The core schema is a **5-table relational model** covering the full source-to-pay lifecycle — supplier master data, purchase orders, line-item spend, goods receipts, and accounts payable invoices. The dataset is **causally simulated**: supplier tier predicts delivery performance, spend category predicts price variance, and business unit predicts receipt compliance. These structures produce realistic anomalies that mirror real-world procurement risk.

The schema extends beyond the 5-table core with a **Delta Lake time travel layer on the `suppliers` table**, enabling point-in-time queries against four historical versions of the vendor master. This models a real and common enterprise requirement: understanding how supplier classifications, status changes, and onboarding events have evolved over time, and how those changes correlate with procurement outcomes.

**Build status:** All phases complete and operational, including the time travel extension. The agent handles time-aware queries via a hardcoded version map in the planner system prompt, ensuring correct `VERSION AS OF` syntax generation without retry overhead.

---

## 2. Business Context & Problem Statement

### The company profile

Meridian Industrial is modeled on a mid-to-large industrial manufacturer — roughly $2–5B in annual revenue, 500–1,000 active suppliers at full enterprise scale, and multi-plant operations spanning manufacturing, facilities, logistics, IT, and corporate functions. In the current build, the synthetic dataset represents a focused slice of this: 50 active suppliers, ~2,000 purchase orders over 24 months (January 2024 – December 2025), and ~6,000–8,000 line items across five business units.

This profile is intentional. Industrial manufacturing is a domain where:

- **Direct materials spend is high-stakes** — contracted, volume-sensitive, and production-line-dependent
- **OTIF failures have operational consequences** — a late delivery from a sole-source supplier does not just affect a metric, it affects throughput
- **Supplier tier management is active** — vendors are reviewed, reclassified, and occasionally deactivated in response to performance
- **Procurement data is complex** — it spans ERP systems, supplier portals, AP platforms, and contract management tools, none of which natively talk to each other

### The procurement analytics problem

Procurement is one of the highest-value analytical domains in industrial manufacturing, yet it is consistently underserved by traditional BI tooling. The reasons are structural:

- **Spend data is fragmented** across ERP systems, supplier portals, and AP platforms
- **Key metrics require multi-table joins** — OTIF requires the PO and the goods receipt; three-way match requires the PO, the receipt, and the invoice
- **Anomalies are buried in volume** — an overbilling supplier or a missing delivery confirmation is invisible in a standard dashboard until it surfaces as a month-end variance
- **Business users cannot write SQL** — the people who most need procurement insights (category managers, VP of Supply Chain) are furthest from the data

### What this architecture enables

The Meridian schema supports a conversational analytics agent that bridges this gap. A business user asks *"which suppliers are consistently invoicing above their PO amounts?"* and the agent generates the appropriate multi-table SQL, executes it, and returns a plain-English answer — including the specific supplier, the magnitude of the overbilling, and the number of invoices affected. With time travel enabled, a user can also ask *"what was our supplier base before the tier review?"* and the agent queries the correct historical version of the vendor master.

This requires a schema that is normalized enough to avoid data integrity problems, denormalized enough that common analytical joins are performant and straightforward, semantically clear enough for an LLM to reason about column meanings without excessive disambiguation, and temporally aware — capable of answering questions that reference how the data has changed over time.

---

## 3. Schema Overview

### Layer architecture

The schema is organized into three logical layers plus a temporal extension:

```
┌──────────────────────────────────────────────────────────────────────┐
│  REFERENCE LAYER                                                     │
│  suppliers  [+ Delta Lake time travel — 4 versions]                  │
│  Vendor master — the anchor for all spend and performance analysis   │
└──────────────────────────┬───────────────────────────────────────────┘
                           │ (1-to-many)
┌──────────────────────────▼───────────────────────────────────────────┐
│  TRANSACTION LAYER                                                   │
│  purchase_orders  →  po_line_items                                   │
│  goods_receipts                                                      │
│  The full buy-and-receive cycle                                      │
└──────────────────────────┬───────────────────────────────────────────┘
                           │ (1-to-1 per PO)
┌──────────────────────────▼───────────────────────────────────────────┐
│  FINANCIAL LAYER                                                     │
│  invoices                                                            │
│  Accounts payable — DPO, three-way match, overbilling detection      │
└──────────────────────────────────────────────────────────────────────┘
```

### Table summary

| Table | Layer | Grain | Row count (approx.) | Primary analytical use |
|---|---|---|---|---|
| `suppliers` | Reference | One row per vendor (current version) | 51 (v3: SUP-051 added) | Vendor master, tier segmentation, SCD analysis |
| `purchase_orders` | Transaction | One row per PO | 2,000 | Spend analysis, category management |
| `po_line_items` | Transaction | One row per line item | 6,000–8,000 | Price variance, item-level spend |
| `goods_receipts` | Transaction | One row per delivery event | ~1,800 | OTIF, delivery performance, defect rate |
| `invoices` | Financial | One row per invoice | ~1,800 | DPO, overbilling detection, AP aging |

---

## 4. Entity-Relationship Diagram

```
suppliers  [VERSION AS OF 0|1|2|3]
─────────────────────────────────────
PK  supplier_id         VARCHAR
    supplier_name       VARCHAR
    tier                VARCHAR       [strategic|preferred|approved|spot]
    category_focus      VARCHAR
    country             VARCHAR
    is_active           BOOLEAN
    onboarded_date      DATE
         │
         │ 1
         │
         │ many
         ▼
purchase_orders                              po_line_items
──────────────────────────────────────       ──────────────────────────────────────
PK  po_id               VARCHAR        1    PK  line_id             VARCHAR
FK  supplier_id         VARCHAR   ◄────────  FK  po_id               VARCHAR
    business_unit       VARCHAR         many     item_name           VARCHAR
    category            VARCHAR              category            VARCHAR
    po_date             DATE                 standard_cost       DECIMAL
    expected_delivery_date  DATE             unit_price          DECIMAL
    total_value         DECIMAL              quantity            INTEGER
    payment_terms       VARCHAR              line_total          DECIMAL
    status              VARCHAR
         │                    │
         │ 1                  │ 1
         │                    │
         │ 1                  │ 1
         ▼                    ▼
goods_receipts              invoices
──────────────────────────  ──────────────────────────────────────
PK  receipt_id  VARCHAR      PK  invoice_id          VARCHAR
FK  po_id       VARCHAR      FK  po_id               VARCHAR
FK  supplier_id VARCHAR      FK  supplier_id         VARCHAR
    received_date   DATE         invoice_date        DATE
    qty_ordered     INTEGER      due_date            DATE
    qty_received    INTEGER      paid_date           DATE
    qty_rejected    INTEGER      invoice_amount      DECIMAL
    on_time         BOOLEAN      po_total_value      DECIMAL
    in_full         BOOLEAN      status              VARCHAR
```

### Relationship summary

| Relationship | Type | Business meaning |
|---|---|---|
| `suppliers` → `purchase_orders` | One-to-many | A supplier can have many POs |
| `purchase_orders` → `po_line_items` | One-to-many | A PO contains 2–6 line items |
| `purchase_orders` → `goods_receipts` | One-to-one (ideal) / Zero-to-one (anomaly) | Each closed PO should have exactly one receipt; 10–15 corporate POs intentionally have none |
| `purchase_orders` → `invoices` | One-to-one (ideal) | Each closed PO should generate exactly one invoice |
| `suppliers` → `goods_receipts` | One-to-many (denormalized FK) | Convenience join — supplier_id is also on the PO |
| `suppliers` → `invoices` | One-to-many (denormalized FK) | Same rationale |

---

## 5. Table-by-Table Design Rationale

### 5.1 `suppliers` — The anchor table and slowly changing dimension

**Why it exists:** Every transaction in the schema traces back to a supplier. Without a clean vendor master, spend analysis becomes unreliable — you cannot aggregate by tier, measure supplier-level performance, or segment by geography. In real procurement systems, poor supplier master data is one of the most common root causes of reporting inaccuracy.

**Design decisions:**

- `tier` is the single most analytically important column in the entire dataset. It drives OTIF distributions in `goods_receipts` and is the primary segmentation dimension for supplier performance analysis. It is stored as a business classification, not derived as a calculated metric. It is also the column most likely to change over time, making it the natural candidate for time travel.
- `category_focus` at the supplier level differs subtly from `category` at the PO level. A supplier may have a primary focus (direct materials) but occasionally fill MRO orders. This tension reflects real procurement complexity.
- `is_active` supports vendor hygiene analysis. In production systems, inactive vendors remaining on the approved vendor list is a compliance risk. In the time travel layer, `is_active` changes in Version 3 when Hartwell is deactivated following the overbilling response.
- Two supplier names are intentionally near-duplicates (edit distance 1–2), creating a realistic deduplication exercise — a common real-world data quality problem.

**Time travel:** The `suppliers` table is the only table with Delta Lake time travel enabled. See Section 11 for the full version map and rationale.

**What is not here:** Payment terms live on `purchase_orders`, not `suppliers`, because terms can vary by transaction. Contact information, certifications, and ESG ratings would be added in a production schema expansion.

---

### 5.2 `purchase_orders` — The central fact

**Why it exists:** The purchase order is the atomic unit of procurement commitment. Everything downstream — receipts, invoices, line items — is a child of the PO. This table is the hub of the schema.

**Design decisions:**

- `total_value` is denormalized from `po_line_items`. This is deliberate controlled redundancy — the sum of line item `line_total` values equals `total_value`. Storing it here allows spend queries to run against a single table without joining to line items every time. The generator computes line items first, then writes the total back to the PO.
- `expected_delivery_date` lives here, not on `goods_receipts`, because the delivery expectation is set at order time. The receipt records the actual outcome. This separation is what makes OTIF calculation possible.
- `status` (open/closed/cancelled) is stored explicitly for query convenience and to mirror how ERP systems expose this field.
- `business_unit` enables spend allocation analysis. The `corporate` BU has a specific anomaly seeded into it: 10–15 closed POs with no corresponding goods receipt.

---

### 5.3 `po_line_items` — The price variance engine

**Why it exists:** Aggregate PO-level spend hides the most analytically important signals. Price variance only becomes visible at the line item level. This is where maverick buying patterns surface.

**Design decisions:**

- `standard_cost` is the benchmark "should cost." `unit_price` is what was actually paid. The delta is price variance, causally structured by category: direct materials show 2–3% variance (locked contracts), tail spend shows 20–30% (off-contract purchasing).
- `line_total = unit_price × quantity` is computed and stored explicitly, avoiding repeated calculation and reflecting standard ERP practice.
- `category` at the line item level can differ from `category` at the PO level and from `category_focus` at the supplier level. This three-level category hierarchy mirrors how real procurement systems classify spend.

---

### 5.4 `goods_receipts` — The delivery performance record

**Why it exists:** A purchase order is a commitment to buy. A goods receipt is confirmation that what was ordered actually arrived, on time, and in the right quantity. Without this table, you can measure spend but not performance.

**Design decisions:**

- `on_time` and `in_full` are stored as booleans rather than derived at query time. This mirrors ERP practice and simplifies OTIF queries significantly.
- `supplier_id` is denormalized from `purchase_orders`, allowing delivery performance queries by supplier without a three-table join.
- `qty_rejected` enables defect rate analysis, distinct from OTIF. A delivery can be on time and in full but still have quality failures.
- The `corporate` BU anomaly is implemented here: 10–15 closed POs have no corresponding `goods_receipt` record.

---

### 5.5 `invoices` — The accounts payable layer

**Why it exists:** The invoice is the financial close of the procurement cycle — what triggers payment and the primary input for AP analysis: days payable outstanding, payment timing, and overbilling detection.

**Design decisions:**

- `po_total_value` is denormalized from `purchase_orders` to enable direct billing ratio comparison (`invoice_amount / po_total_value`) without a join. This is the core calculation for overbilling detection.
- `paid_date` can be NULL, representing invoices not yet paid. This supports AP aging analysis.
- `status` (paid/pending/disputed/overdue) is stored explicitly, reflecting real system behavior.
- One supplier (SUP-018 Hartwell, later deactivated in Version 3) is seeded to overbill: `invoice_amount = po_total_value × Uniform(1.04, 1.08)` on 85%+ of their invoices. The agent surfacing this supplier by name — with the specific overbilling range and invoice count — is the most effective demo moment for non-technical interviewers.

---

## 6. Relationships & Cardinality

### Primary relationships

```
suppliers (1) ──────────────────────── (many) purchase_orders
```
Every PO belongs to exactly one supplier. All 50 (v0–v1) or 51 (v2–v3) suppliers have at least one PO; volume is power-law distributed so top suppliers have substantially more.

```
purchase_orders (1) ────────────────── (many) po_line_items
```
Every line item belongs to exactly one PO. Each PO has 2–6 line items. No orphaned line items exist — referential integrity is validated in the generation suite.

```
purchase_orders (1) ────────────────── (0 or 1) goods_receipts
```
The ideal state is one receipt per closed PO. The intentional anomaly breaks this: 10–15 corporate POs are closed and paid with no corresponding receipt.

```
purchase_orders (1) ────────────────── (0 or 1) invoices
```
Each closed PO should have exactly one invoice. The schema does not model partial invoicing (multiple invoices per PO), which is a deliberate simplification relative to real enterprise AP systems.

### Denormalized foreign keys

`goods_receipts.supplier_id` and `invoices.supplier_id` are denormalized from `purchase_orders`. This reduces join depth for the most common analytical queries. The tradeoff — potential inconsistency on supplier reassignment — is acceptable in an immutable analytics schema.

### Fan-out warning

Joining `purchase_orders` to `po_line_items` fans out: each PO row becomes multiple rows (one per line item). Aggregating `total_value` after this join produces inflated spend totals. Always aggregate spend from `purchase_orders.total_value` directly, or pre-aggregate line items in a CTE before joining.

---

## 7. Data Dictionary

### `suppliers`

| Column | Type | Nullable | Description | Example values |
|---|---|---|---|---|
| `supplier_id` | VARCHAR | No | Primary key. Format: SUP-001 through SUP-050 (v0); SUP-051 added in v2 | `SUP-001`, `SUP-051` |
| `supplier_name` | VARCHAR | No | Company name. Two records are intentional near-duplicates | `Acme Industrial Supply` |
| `tier` | VARCHAR | No | Vendor classification driving OTIF distributions. Values: strategic, preferred, approved, spot. Changes across versions — see Section 11 | `strategic` |
| `category_focus` | VARCHAR | No | Primary spend category. Values: direct_materials, mro, indirect, services | `direct_materials` |
| `country` | VARCHAR | No | Country of origin. ~60% USA, ~20% Mexico/Canada, ~20% Europe/Asia | `USA`, `Germany` |
| `is_active` | BOOLEAN | No | Active vendor flag. Set to FALSE for SUP-018 Hartwell in v3 | `true` |
| `onboarded_date` | DATE | No | Date supplier was approved. Range: 3–8 years prior to data start | `2018-03-14` |

### `purchase_orders`

| Column | Type | Nullable | Description | Example values |
|---|---|---|---|---|
| `po_id` | VARCHAR | No | Primary key. Format: PO-00001 through PO-02000 | `PO-00001` |
| `supplier_id` | VARCHAR | No | Foreign key → `suppliers.supplier_id` | `SUP-007` |
| `business_unit` | VARCHAR | No | Requesting BU. Values: manufacturing, facilities, logistics, it, corporate | `manufacturing` |
| `category` | VARCHAR | No | PO-level spend category | `direct_materials` |
| `po_date` | DATE | No | Date PO was issued. Range: Jan 2024 – Dec 2025 | `2024-03-15` |
| `expected_delivery_date` | DATE | No | Contracted delivery date. Used in OTIF: `received_date <= expected_delivery_date` | `2024-04-10` |
| `total_value` | DECIMAL | No | Total PO value in USD. Sum of line item `line_total` values, written back after line item generation | `47,320.00` |
| `payment_terms` | VARCHAR | No | Values: net_30, net_60, net_90. Strategic suppliers skew toward net_60/90 | `net_60` |
| `status` | VARCHAR | No | Values: open, closed, cancelled. Closed for POs >60 days old; ~3% cancelled | `closed` |

### `po_line_items`

| Column | Type | Nullable | Description | Example values |
|---|---|---|---|---|
| `line_id` | VARCHAR | No | Primary key | `LINE-00001` |
| `po_id` | VARCHAR | No | Foreign key → `purchase_orders.po_id` | `PO-00001` |
| `item_name` | VARCHAR | No | Item description | `Hydraulic Seal Kit` |
| `category` | VARCHAR | No | Item-level category. May differ from PO-level category | `mro` |
| `standard_cost` | DECIMAL | No | Benchmark "should cost." Ranges: direct_materials $50–$5,000; mro $20–$2,000 | `150.00` |
| `unit_price` | DECIMAL | No | Actual price paid. `standard_cost × (1 + variance)` where variance is category-driven | `154.50` |
| `quantity` | INTEGER | No | Units ordered. Direct materials: 10–500 units | `200` |
| `line_total` | DECIMAL | No | Computed: `unit_price × quantity`. Stored explicitly | `30,900.00` |

### `goods_receipts`

| Column | Type | Nullable | Description | Example values |
|---|---|---|---|---|
| `receipt_id` | VARCHAR | No | Primary key | `GR-00001` |
| `po_id` | VARCHAR | No | Foreign key → `purchase_orders.po_id` | `PO-00001` |
| `supplier_id` | VARCHAR | No | Denormalized FK → `suppliers.supplier_id` | `SUP-007` |
| `received_date` | DATE | No | Actual delivery date | `2024-04-08` |
| `qty_ordered` | INTEGER | No | Denormalized from PO line items | `200` |
| `qty_received` | INTEGER | No | Actual quantity received | `200` |
| `qty_rejected` | INTEGER | No | Quantity rejected at receiving. Non-zero for ~8% spot, ~3% approved, ~1% strategic/preferred | `0` |
| `on_time` | BOOLEAN | No | Whether delivery arrived by `expected_delivery_date`. Bernoulli draw, p driven by supplier tier | `true` |
| `in_full` | BOOLEAN | No | Whether ≥95% of ordered quantity was received. Bernoulli draw, p driven by supplier tier | `true` |

### `invoices`

| Column | Type | Nullable | Description | Example values |
|---|---|---|---|---|
| `invoice_id` | VARCHAR | No | Primary key. Format: INV-00001 | `INV-00001` |
| `po_id` | VARCHAR | No | Foreign key → `purchase_orders.po_id` | `PO-00001` |
| `supplier_id` | VARCHAR | No | Denormalized FK → `suppliers.supplier_id` | `SUP-007` |
| `invoice_date` | DATE | No | Date invoice was submitted. `po_date + Uniform(7, 30 days)` | `2024-04-20` |
| `due_date` | DATE | No | Payment due date. `invoice_date + payment_terms_days` | `2024-05-20` |
| `paid_date` | DATE | Yes | Actual payment date. NULL if unpaid (~5%). ~80% paid early, ~15% overdue | `2024-05-18` |
| `invoice_amount` | DECIMAL | No | Amount billed. Normal: `po_total_value × (1 + N(0, 0.005))`. Overbilling supplier: `× Uniform(1.04, 1.08)` | `47,560.00` |
| `po_total_value` | DECIMAL | No | Denormalized from `purchase_orders.total_value` for direct billing ratio calculation | `47,320.00` |
| `status` | VARCHAR | No | Values: paid, pending, overdue, disputed | `paid` |

---

## 8. Grain & Aggregation Logic

### Grain definitions

| Table | Grain | What one row represents |
|---|---|---|
| `suppliers` | Vendor (at a point in time) | One entry per approved vendor per version |
| `purchase_orders` | PO | One procurement commitment event |
| `po_line_items` | PO × Item | One distinct item within a PO |
| `goods_receipts` | Delivery event | One physical receipt of goods per closed PO |
| `invoices` | Invoice document | One billing document submitted by a supplier |

### Common aggregation patterns

**Spend by supplier** — aggregate at the PO grain, not line item:
```sql
SELECT supplier_id, SUM(total_value) AS total_spend
FROM purchase_orders
GROUP BY supplier_id
ORDER BY total_spend DESC
```

**Price variance** — only meaningful at the line item grain:
```sql
SELECT category,
  AVG((unit_price - standard_cost) / standard_cost) AS avg_variance_pct
FROM po_line_items
GROUP BY category
ORDER BY avg_variance_pct DESC
```

**OTIF rate by supplier tier:**
```sql
SELECT s.tier,
  AVG(CASE WHEN g.on_time AND g.in_full THEN 1.0 ELSE 0 END) AS otif_rate
FROM goods_receipts g
JOIN suppliers s ON g.supplier_id = s.supplier_id
GROUP BY s.tier
ORDER BY otif_rate DESC
```

**DPO (Days Payable Outstanding):**
```sql
SELECT AVG(DATEDIFF(paid_date, invoice_date)) AS avg_dpo
FROM invoices
WHERE paid_date IS NOT NULL
```

**Supplier state at a historical point in time:**
```sql
SELECT supplier_id, supplier_name, tier
FROM suppliers VERSION AS OF 1
WHERE tier = 'strategic'
```

---

## 9. Causal Logic & Intentional Anomalies

### Causal structure 1: Supplier tier -> OTIF performance

| Tier | OTIF center | Std deviation | Procurement interpretation |
|---|---|---|---|
| Strategic | 94% | ±3% | Contracted, SLA-bound, closely managed. Consistent delivery. |
| Preferred | 87% | ±5% | Approved and trusted, not strategic. Occasional failures. |
| Approved | 78% | ±7% | On the vendor list, not preferred. Material failure rate. |
| Spot | 65% | ±12% | One-off purchases, no SLA, highly unpredictable. |

### Causal structure 2: Spend category -> Price variance

| Category | Variance range | Procurement reason |
|---|---|---|
| Direct materials | ±2-3% | High-volume, contracted, price locked |
| MRO | ±8-15% | Mostly contracted, frequent urgent spot buys |
| Indirect/Services | ±10-20% | Low contract coverage, high discretion |
| Tail spend | ±20-30% | Maverick buying -- off-contract, off-catalog |

### Causal structure 3: PO volume -> Pareto distribution

Supplier PO volume is assigned via a power-law distribution: approximately 20% of suppliers account for ~80% of total spend. This is validated as part of the generation suite (Check 1).

### Intentional anomalies

| Anomaly | Where | Business risk |
|---|---|---|
| **Overbilling supplier** (SUP-018 Hartwell) | `invoices` | Systematic overbilling concentrated in a small number of suppliers. Dataset-wide overbilling rate is calibrated to 6.2% of invoices, reflecting realistic AP conditions. SUP-018 is the primary offender and triggers v3 supplier changes. |
| **Duplicate supplier names** | `suppliers` | Two vendors with near-identical names -- spend is split, performance is masked. |
| **Single-source categories** | `purchase_orders` | Three high-spend categories with one active supplier each -- no competitive pricing, supply chain exposure. |
| **Missing receipts** | `goods_receipts` | 10-15 closed, fully-paid corporate POs with no delivery confirmation -- three-way match failure. |

Note: SUP-018 Hartwell's overbilling behavior is what triggers its tier demotion and deactivation in Version 3 of the suppliers table. The causal link between the invoice anomaly and the SCD change is a deliberate narrative thread across the schema.


---

## 10. Normalization Decisions

The schema sits between 2NF and 3NF with deliberate, documented denormalizations.

### What is normalized

- Supplier attributes (tier, category_focus, country) live only in `suppliers`
- PO attributes (po_date, expected_delivery_date, payment_terms) live only in `purchase_orders`
- Line item detail (standard_cost, unit_price, quantity) lives only in `po_line_items`

### What is deliberately denormalized

| Denormalized field | Where duplicated | Justification |
|---|---|---|
| `purchase_orders.total_value` | Computed from `po_line_items.line_total` | Avoids line item join for every spend query |
| `goods_receipts.supplier_id` | Available via PO join | Enables supplier performance queries without a three-table join |
| `invoices.supplier_id` | Available via PO join | Same rationale |
| `invoices.po_total_value` | Available via PO join | Enables `invoice_amount / po_total_value` without a join |
| `goods_receipts.qty_ordered` | Available via line item aggregation | Simplifies fill-rate and OTIF queries |
| `goods_receipts.on_time` | Derivable from dates | Mirrors ERP behavior; reduces query complexity |

### Normalization tradeoff summary

This schema prioritizes **query simplicity and analytical performance** over strict normalization. In an OLTP system, strict normalization prevents update anomalies. In an analytics schema built on immutable generated data, the risk of update anomalies is zero and the benefit of denormalization is real. This is consistent with how analytical schemas in modern data warehouses are designed.

---

## 11. Slowly Changing Dimensions & Time Travel

### The SCD problem in procurement

Supplier attributes are not static. Tier classifications change in response to performance reviews. Vendors are onboarded and deactivated. In a standard relational schema, the current record overwrites the historical one — meaning you lose the ability to answer questions like *"what tier was this supplier when we ran this PO?"* or *"how did our OTIF look before we promoted this vendor to preferred?"*

This is the **slowly changing dimension (SCD)** problem. It is one of the most common data modeling challenges in enterprise analytics, and it is particularly acute in procurement where tier classification is both analytically important and actively managed.

### Delta Lake time travel as the solution

Rather than implementing a traditional SCD Type 2 pattern (adding `valid_from`, `valid_to`, and `is_current` columns), this schema uses **Delta Lake native time travel** via `VERSION AS OF`. Each time the `suppliers` table is modified, Delta Lake writes a new version while preserving all prior versions in its transaction log. Queries can target any specific version directly.

This approach is simpler to query than SCD Type 2 — no need to filter on `is_current = true` or manage date ranges. It is automatically maintained by Delta Lake at the storage layer with no additional ETL logic. And it is natively supported in Databricks SQL — `VERSION AS OF` works in all query contexts including the agent's SQL tool.

### Implementation

The supplier version history is seeded by `databricks/seed_supplier_history.py`. Running this script from the project root creates four distinct versions of the `suppliers` table by executing a series of targeted updates, with Delta Lake writing a new version at each commit.

To recreate version history on a fresh table:
```bash
python databricks/seed_supplier_history.py
```

### Critical implementation note: `VERSION AS OF` vs `TIMESTAMP AS OF`

Both are valid Delta Lake time travel syntaxes, but only `VERSION AS OF` is used in this build. `TIMESTAMP AS OF` fails because the Delta table was created on 2026-04-16, and the historical events being modeled predate that creation timestamp — Delta Lake cannot return a snapshot from before the table existed. `VERSION AS OF` references the transaction log sequence number, which is always valid regardless of when the table was created.

This constraint is hardcoded into the agent's planner system prompt via a version map, ensuring the agent generates correct syntax on the first attempt without retry overhead. In a production system where the table has been live long enough for timestamps to be meaningful, `TIMESTAMP AS OF` would be the more natural interface for business users.

### Version map

| Version | Event | Change | Business context |
|---|---|---|---|
| 0 | Initial load | 50 suppliers baseline | Starting vendor master at project initialization |
| 1 | Tier review | SUP-022 Palisade Pneumatics: `approved → preferred` | Palisade demonstrated improved OTIF and was promoted following a quarterly performance review |
| 2 | New supplier onboarding | SUP-051 Meridian Alloy Backup Co inserted as `strategic` | New strategic supplier added — likely a response to single-source risk in a critical direct materials category |
| 3 | Overbilling response | SUP-018 Hartwell: tier `→ spot`, `is_active = FALSE` | Hartwell's consistent overbilling (the invoice anomaly) triggers demotion and deactivation |

### Example time travel queries

**What tier was Palisade Pneumatics before the review?**
```sql
SELECT supplier_id, supplier_name, tier
FROM suppliers VERSION AS OF 0
WHERE supplier_name LIKE '%Palisade%'
```

**How many strategic suppliers did we have before the new onboarding?**
```sql
SELECT COUNT(*) AS strategic_count
FROM suppliers VERSION AS OF 1
WHERE tier = 'strategic'
```

**Which suppliers were active before Hartwell was deactivated?**
```sql
SELECT supplier_id, supplier_name, tier, is_active
FROM suppliers VERSION AS OF 2
ORDER BY tier, supplier_name
```

**How did OTIF compare for Palisade under its old tier vs. current?**
```sql
SELECT s.supplier_name, s.tier AS historical_tier,
  AVG(CASE WHEN g.on_time AND g.in_full THEN 1.0 ELSE 0 END) AS otif_rate
FROM goods_receipts g
JOIN (SELECT * FROM suppliers VERSION AS OF 0) s
  ON g.supplier_id = s.supplier_id
WHERE s.supplier_name LIKE '%Palisade%'
GROUP BY s.supplier_name, s.tier
```

### Narrative thread across the schema

The time travel layer is not isolated from the rest of the schema — it creates a causal narrative spanning multiple tables. Hartwell's consistent overbilling is detectable in `invoices`. The agent surfaces it via the overbilling detection query. The procurement team responds by demoting and deactivating the vendor — captured in Version 3 of `suppliers`. This end-to-end story, from data anomaly to analytical discovery to vendor management action, is one of the strongest interview narratives the project supports.

---

## 12. Key Analytical Domains

The schema supports seven primary analytical domains, each corresponding to a standalone SQL analysis in the `analysis/sql/` folder. Together these queries power the Meridian Procurement Analytics Lakeview dashboard, which surfaces four executive KPIs (Total Active Spend, Suppliers with HIGH Risk, Overbilling Exposure, and Invoices Failing Three-Way Match) alongside three operational charts.

### 12.1 Spend analysis

**Business question:** Where is money going, and is it going to the right suppliers?

**Primary tables:** `purchase_orders`, `suppliers`

**Key metrics:** Total spend by supplier / category / business unit / period. Pareto concentration (what % of suppliers account for 80% of spend). Spend trend over time.

---

### 12.2 Supplier performance (OTIF)

**Business question:** Are suppliers delivering what was promised, when it was promised?

**Primary tables:** `goods_receipts`, `suppliers`, `purchase_orders`

**Key metrics:** OTIF rate by supplier / tier / category. On-time rate vs. in-full rate independently. Late delivery distribution. Defect rate (`qty_rejected / qty_received`).

---

### 12.3 Price variance & contract compliance

**Business question:** Are we paying what we should be paying?

**Primary tables:** `po_line_items`, `purchase_orders`, `suppliers`

**Key metrics:** Average price variance by category. Total variance cost in dollars (`(unit_price - standard_cost) x quantity`). Tail spend identification. Maverick buying rate (% of spend with >15% variance).

---

### 12.4 Invoice & AP analysis (three-way match)

**Business question:** Are invoices accurate? Are we paying on time?

**Primary tables:** `invoices`, `purchase_orders`, `goods_receipts`

**Key metrics:** Billing ratio (`invoice_amount / po_total_value`) by supplier. Overbilling detection. DPO. Overdue invoice aging. Three-way match failure rate. A 5% tolerance is applied to amount variance checks to account for legitimate variances including freight, taxes, and rounding.

---

### 12.5 Supply chain risk

**Business question:** Where are we exposed?

**Primary tables:** `purchase_orders`, `suppliers`, `goods_receipts`

**Key metrics:** Single-source spend concentration. Geographic concentration. Spot supplier dependency. High-spend + low-OTIF risk matrix.

---

### 12.6 Compliance & audit

**Business question:** Are procurement processes being followed?

**Primary tables:** `goods_receipts`, `purchase_orders`, `invoices`

**Key metrics:** Missing receipt rate on closed POs by business unit. Invoices paid before receipt confirmation. POs without corresponding line items.

---

### 12.7 Vendor master history & SCD analysis

**Business question:** How has our supplier base changed, and what drove those changes?

**Primary tables:** `suppliers VERSION AS OF [n]`

**Key metrics:** Tier distribution at each point in time. Supplier count by tier across versions. Individual supplier tier trajectory. OTIF correlation with tier classification changes. Active vs. inactive supplier count over time.

---

## 13. Data Quality & Validation Framework

The generator includes a mandatory 6-check validation suite that must pass before any downstream work proceeds. These checks verify that the causal logic encoded in the generator held through execution.

### Check 1 — Pareto (spend concentration)

```sql
SELECT supplier_id, SUM(total_value) AS spend
FROM purchase_orders
GROUP BY supplier_id
ORDER BY spend DESC
```
**Expected:** Top 10 suppliers (20% of 50) account for ≥75% of total spend.

---

### Check 2 — Tier vs OTIF (causal relationship)

```sql
SELECT s.tier,
  AVG(CASE WHEN g.on_time THEN 1.0 ELSE 0 END) AS otif_rate
FROM goods_receipts g
JOIN suppliers s ON g.supplier_id = s.supplier_id
GROUP BY s.tier
ORDER BY otif_rate DESC
```
**Expected:** strategic > preferred > approved > spot. Strategic near 0.94, spot near 0.65.

---

### Check 3 — Price variance by category

```sql
SELECT category,
  AVG((unit_price - standard_cost) / standard_cost) AS avg_variance
FROM po_line_items
GROUP BY category
ORDER BY avg_variance DESC
```
**Expected:** Direct materials ~2–3% variance. Tail spend ~20–30%.

---

### Check 4 — Overbilling anomaly

```sql
SELECT supplier_id,
  AVG(invoice_amount / po_total_value) AS avg_ratio,
  COUNT(*) AS invoice_count
FROM invoices
GROUP BY supplier_id
HAVING avg_ratio > 1.03
ORDER BY avg_ratio DESC
```
**Expected:** Exactly one supplier with a ratio consistently between 1.04 and 1.08.

---

### Check 5 — Missing receipts anomaly

```sql
SELECT po.po_id, po.business_unit
FROM purchase_orders po
LEFT JOIN goods_receipts gr ON po.po_id = gr.po_id
WHERE po.status = 'closed'
  AND gr.receipt_id IS NULL
```
**Expected:** 10–15 rows, all from the `corporate` business unit.

---

### Check 6 — Referential integrity

Three LEFT JOIN checks across `purchase_orders → suppliers`, `po_line_items → purchase_orders`, and `invoices → purchase_orders`. All must return zero NULL foreign key matches.

**Failure protocol:** If any check fails, fix the generator and regenerate all five tables from scratch. CSV patching is prohibited — it breaks the causal consistency the dataset depends on.

---

## 14. At Enterprise Scale: Production Architecture

This section describes how the Meridian schema would extend into a full production data architecture at a large industrial manufacturer — a company with 500–1,000 active suppliers, 100,000+ POs per year, and multi-plant procurement operations across multiple geographies.

### 14.1 Source systems & ingestion

In a real enterprise, this data does not originate from a single system:

| Source system | Data produced | Integration method |
|---|---|---|
| ERP (SAP S/4HANA, Oracle) | POs, line items, goods receipts, supplier master | API or JDBC batch extract → Bronze layer |
| AP platform (Coupa, Ariba) | Invoices, payment records | API extraction → Bronze layer |
| Supplier portal | Certifications, self-reported data | API or file-based → Bronze layer |
| Contract management system | Contract terms, pricing schedules | File-based extract → Bronze layer |
| Logistics/WMS | Actual delivery records | API → Bronze layer |

The Python generator replaces all of these — producing the same five tables with the same causal structure, in a form that loads directly into Databricks Delta.

### 14.2 Medallion architecture

At enterprise scale, the 5-table schema would live in the Gold layer of a medallion architecture:

```
Bronze (Raw)             Silver (Cleaned)         Gold (Curated)
────────────────────     ────────────────────      ───────────────────────────────
Raw ERP extract      →   Deduplicated,         →   dim_suppliers  [time travel]
Raw AP extract           standardized,              dim_date
Raw portal data          validated,                 fact_purchase_orders
                         conformed                  fact_po_line_items
                         (dbt models)               fact_goods_receipts
                                                    fact_invoices
                                                    agg_supplier_performance
                                                    agg_spend_by_category
                                                    agg_ap_aging
```

The Meridian Tier 3 extensions (medallion pipeline, dbt models) are designed to add exactly this structure on top of the existing core.

### 14.3 Schema expansion

The 5-table core covers source-to-pay. A full enterprise procurement schema would add:

| Additional domain | Tables | What it enables |
|---|---|---|
| Contracting | `contracts`, `contract_line_items` | Contract compliance, pricing schedule adherence, coverage rate |
| Sourcing | `rfq_events`, `bids`, `awards` | Competitive sourcing analysis, bid-to-award rate, savings tracking |
| Supplier performance | `supplier_scorecards`, `kpi_targets` | Periodic ratings, trend analysis, target vs. actual |
| Risk | `risk_events`, `financial_health` | Force majeure flags, geopolitical exposure, supplier financial distress signals |
| Catalog | `catalog_items`, `catalog_compliance` | On-contract vs. off-catalog (maverick) spend at the item level |
| Approval workflow | `approval_events` | PO approval routing, delegation of authority compliance |

Each of these extends naturally from the existing primary key structure — `supplier_id` and `po_id` are the join anchors throughout. New tables can be added without structural rework to the existing five.

### 14.4 Time travel at scale

At enterprise scale, Delta Lake time travel on `suppliers` would evolve into a full SCD management process:

- Automated version triggers when tier review results are published (e.g., a quarterly scorecard process writes a new version)
- A version metadata table mapping version numbers to business events, review dates, and approving stakeholders
- Extended retention policy — Delta Lake default is 30-day log retention; enterprise environments extend this to 2–3 years to support audit and compliance queries
- `TIMESTAMP AS OF` becomes viable once the table's creation date is sufficiently far in the past relative to the historical events being queried — at that point, the timestamp-based interface is more natural for business users than version numbers

The same design pattern extends to other slowly changing reference data in an industrial manufacturer: category hierarchies, cost centers, plant master data, approved vendor lists, and standard cost schedules.

### 14.5 Governance & access controls

In a production environment, procurement data requires tiered access managed via Unity Catalog:

| Role | Access level | Rationale |
|---|---|---|
| Category manager | Own category spend + supplier performance | Competitive pricing data is commercially sensitive |
| VP Supply Chain | All spend + performance, no HR/salary data | Business-level oversight |
| AP analyst | Invoices and payment data only | Functional scope |
| Internal auditor | Full read access, all tables and all versions | Compliance requirement |
| Data engineer | Schema and pipeline access, no business data in prod | Least privilege |

Row-level security is implemented via Unity Catalog dynamic views — filtering `purchase_orders` by `category` or `business_unit` based on the authenticated user's role. Column-level masking would apply to sensitive fields like `invoice_amount` for roles without AP access.

### 14.6 Performance at scale

At enterprise scale (~100K POs/year, multi-year history), query performance requires:

- **Partitioning:** `purchase_orders` and `invoices` partitioned by `po_date` year/month
- **Z-ordering:** `goods_receipts` z-ordered on `supplier_id` for supplier performance queries; `po_line_items` z-ordered on `po_id`
- **Aggregation tables:** Pre-computed `agg_supplier_performance` and `agg_spend_by_category` for dashboard queries
- **Delta Lake log compaction:** Regular `OPTIMIZE` runs to compact small files from high-frequency writes
- **Time travel retention:** Extended log retention for the `suppliers` table to support multi-year SCD queries

At 5 tables and ~50K rows, Meridian requires none of these. The value of running on Databricks at this scale is toolchain familiarity and demonstrating architectural judgment — knowing what you would do at scale, and being honest that the current data volume does not require it.

---

## 15. Design Tradeoffs & Alternatives Considered

### 15.1 Why not a star schema?

A traditional data warehouse star schema would have a central fact table (`fact_procurement_transactions`) with dimension tables (`dim_supplier`, `dim_date`, `dim_category`, `dim_business_unit`). This was considered and rejected for two reasons.

First, the procurement lifecycle spans multiple discrete events — order, receipt, invoice — that do not compress cleanly into a single fact table without losing analytical resolution or requiring a wide table with many NULLs.

Second, the agent needs to reason about table semantics. Tables named `purchase_orders`, `goods_receipts`, and `invoices` are more semantically transparent to an LLM than `fact_procurement` with a `transaction_type` discriminator column. For a conversational analytics use case, semantic clarity in the schema directly improves SQL generation quality.

For a pure BI dashboard use case, a star schema would be preferable. For an agent that generates its own SQL, the normalized operational schema is the better choice.

### 15.2 Why Delta Lake time travel over SCD Type 2?

SCD Type 2 is the traditional solution: add `valid_from`, `valid_to`, and `is_current` columns and insert new rows for each change rather than updating existing ones. This was considered and rejected for three reasons.

SCD Type 2 requires filtering on `is_current = true` in every query that wants the current state — a common source of bugs. `VERSION AS OF` gives you the current state by default and historical states on explicit request. SCD Type 2 requires additional ETL logic to manage validity windows correctly, while Delta Lake handles versioning automatically at the storage layer. And for a portfolio project, demonstrating Delta Lake-native time travel is a more distinctive and current signal than SCD Type 2, which has been standard practice for two decades.

The tradeoff: Delta Lake time travel is tied to Databricks and has retention limits. SCD Type 2 is database-agnostic and permanently preserved. For a production system with regulatory retention requirements, SCD Type 2 would likely be preferred. For an analytics platform on Databricks, Delta Lake time travel is the natural choice.

### 15.3 Why `VERSION AS OF` and not `TIMESTAMP AS OF`?

Both are valid Delta Lake time travel syntaxes. `TIMESTAMP AS OF` is often more intuitive — you can ask "what did the supplier table look like on January 1st?" rather than needing to know the version number.

In this build, `TIMESTAMP AS OF` fails: the Delta table was created on 2026-04-16, and the historical events being modeled predate that creation timestamp. `VERSION AS OF` references the transaction log sequence, which is always valid. This constraint is hardcoded into the agent's planner system prompt so the model never attempts `TIMESTAMP AS OF` and never needs to retry.

### 15.4 Why denormalize `supplier_id` onto receipts and invoices?

The alternative — requiring all supplier lookups to go through `purchase_orders` — is cleaner from a normalization standpoint. It was rejected because the most common analytical queries need supplier context, and requiring a three-table join for every supplier performance or billing query adds complexity and cost that compounds at scale.

### 15.5 Why 5 tables and not more?

The schema deliberately excludes contracts, approvals, RFQ events, and supplier scorecards that would appear in a full source-to-pay schema. This is a portfolio scoping decision: 5 tables supports all analytical domains with realistic complexity while remaining buildable and explainable in an interview. The enterprise scale section above demonstrates awareness of what would be added. As the schema expands over time, new tables can extend from the existing `supplier_id` and `po_id` join anchors without structural rework.

---

## 16. Appendix: Glossary

| Term | Definition |
|---|---|
| **OTIF** | On-Time In-Full. The percentage of deliveries that arrived by the expected date AND met the minimum quantity threshold (≥95% of ordered quantity). The primary procurement delivery performance metric. |
| **DPO** | Days Payable Outstanding. Average days between invoice receipt and payment. Higher DPO (within terms) is generally favorable for cash flow. |
| **Three-way match** | Matching a purchase order, goods receipt, and supplier invoice before approving payment. A control that prevents payment for goods never received. |
| **Price variance** | The difference between actual price paid (`unit_price`) and the benchmark "should cost" (`standard_cost`). Positive variance means paying more than expected. |
| **Maverick buying** | Purchases made outside contracted channels — off-catalog, off-contract. Associated with high price variance and compliance risk. |
| **Tail spend** | Low-value, high-volume, often off-contract transactions. Characterized by wide price variance and limited management attention. |
| **Supplier tier** | A classification of vendors by relationship depth and contract status: strategic (SLA-bound, high value), preferred (approved, trusted), approved (on the vendor list), spot (one-off, no contract). |
| **Pareto concentration** | The observation that approximately 20% of suppliers account for 80% of total spend in most procurement datasets. |
| **Standard cost** | The expected or benchmark price for a purchased item, derived from contracts, market data, or historical averages. The basis for price variance calculation. |
| **Single source** | A spend category where only one approved supplier exists. Creates supply chain risk and eliminates competitive pricing leverage. |
| **SCD (Slowly Changing Dimension)** | A dimension table whose attributes change over time. Managing historical states of SCDs is a core data modeling challenge. |
| **SCD Type 2** | The traditional SCD pattern: when an attribute changes, insert a new row with `valid_from` / `valid_to` dates and an `is_current` flag rather than updating the existing row. |
| **Delta Lake time travel** | A Delta Lake feature that preserves all historical versions of a table in its transaction log, enabling `VERSION AS OF` and `TIMESTAMP AS OF` queries against prior states. |
| **VERSION AS OF** | Delta Lake syntax for querying a specific version of a table by transaction log sequence number. Always valid regardless of when the table was created. |
| **TIMESTAMP AS OF** | Delta Lake syntax for querying a table as it existed at a specific timestamp. Fails if the timestamp predates the table's creation date. |
| **Grain** | The level of detail represented by one row in a table. Essential for writing correct aggregation queries across tables with different granularity levels. |
| **Fan-out** | What happens when joining a parent table to a child table with a one-to-many relationship: each parent row produces multiple result rows. A common source of incorrect aggregations. |
| **Medallion architecture** | A data lakehouse design pattern with Bronze (raw), Silver (cleaned), and Gold (curated) layers. Each layer adds transformation and business logic. |
| **Unity Catalog** | Databricks' unified governance layer. Enables fine-grained access control, data lineage, and auditing across catalogs, schemas, and tables. |
| **Z-ordering** | A Delta Lake optimization that co-locates related data in the same files, reducing data scanned for queries filtering on z-ordered columns. |

---

*This document is intended as a standalone portfolio artifact and interview reference. It describes a synthetic dataset built for analytical and educational purposes. All company names, supplier names, and financial figures are fictional.*
