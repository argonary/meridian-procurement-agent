import pathlib
import random
from datetime import date, timedelta

import numpy as np
import pandas as pd

SEED = 108
random.seed(SEED)
np.random.seed(SEED)


# ---------------------------------------------------------------------------
# Phase 1 – generate_suppliers
# ---------------------------------------------------------------------------

def generate_suppliers(n: int = 50) -> pd.DataFrame:
    """
    Generate 50 supplier records with the following tier distribution:
        5 strategic | 10 preferred | 20 approved | 15 spot

    Two supplier names are intentional near-duplicates:
        'Hartwell Industrial Supply'  vs  'Hartwell Industrial Supplies'
    (edit distance 1 — differs only by a trailing 's')

    Category rules: each of the four categories has at least 2 strategic/preferred
    suppliers.  Country mix: ~60 % USA, ~20 % MX/CA, ~20 % Europe/Asia.
    """
    rng = random.Random(SEED)
    np_rng = np.random.default_rng(SEED)

    # ------------------------------------------------------------------
    # 1. Build the pool of plausible industrial company names (52 unique
    #    names so we can pick 50 without collision, then inject the pair).
    # ------------------------------------------------------------------
    name_pool = [
        "Apex Precision Components",
        "Atlas Fabrication Group",
        "Beacon Industrial Solutions",
        "Bridgewater Metal Works",
        "Cardinal Fluid Systems",
        "Cascade Polymer Industries",
        "Clearfield Mechanical",
        "Crestline Equipment Co.",
        "Delta Alloy Partners",
        "Dunmore Safety Products",
        "Eastern Hydraulics Inc.",
        "Elgin Tooling & Die",
        "Fairview Chemical Supply",
        "Frontier Fastener Corp.",
        "Gateway Coatings Ltd.",
        "Granite Peak Engineering",
        "Greenfield MRO Services",
        "Hartwell Industrial Supply",   # near-dup A  (index 17)
        "Hartwell Industrial Supplies", # near-dup B  (index 18)
        "Highpoint Automation",
        "Ironbridge Castings",
        "Keystone Bearing & Seal",
        "Lakeside Composite Materials",
        "Liberty Power Systems",
        "Longview Electronics",
        "Maple Ridge Packaging",
        "Meridian Bolt & Nut",
        "Mesa Fluid Controls",
        "Midland Rubber Products",
        "National Filtration Systems",
        "Nexgen Sensor Technologies",
        "Northern Forge Works",
        "Oakdale Welding Supply",
        "Offshore Logistics Partners",
        "Omni-Lube Industrial",
        "Pacific Rim Stamping",
        "Palisade Pneumatics",
        "Paragon Safety Equipment",
        "Patriot Steel Service",
        "Pinnacle Valve Solutions",
        "Premier Hose & Fittings",
        "Redwood Conveyor Systems",
        "Regal Tooling Inc.",
        "Ridgeline Process Controls",
        "Riverbend Sheet Metal",
        "Skyline Motion Systems",
        "Suncoast Electrical Supply",
        "Terrace Abrasives & Grinding",
        "Thunderbolt Hydraulics",
        "Vanguard Precision Machining",
    ]

    assert len(name_pool) == 50, "Name pool must contain exactly 50 entries."

    # ------------------------------------------------------------------
    # 2. Tier list – shuffle so tiers are scattered across supplier IDs
    # ------------------------------------------------------------------
    tiers = (
        ["strategic"] * 5
        + ["preferred"] * 10
        + ["approved"] * 20
        + ["spot"] * 15
    )
    rng.shuffle(tiers)

    # ------------------------------------------------------------------
    # 3. Category focus
    #    Constraint: each category must have ≥ 2 strategic/preferred
    #    suppliers.  We handle this by first assigning categories to the
    #    15 strategic+preferred suppliers deterministically, then filling
    #    the rest randomly.
    # ------------------------------------------------------------------
    categories = ["direct_materials", "mro", "indirect", "services"]

    strategic_preferred_idx = [i for i, t in enumerate(tiers) if t in ("strategic", "preferred")]
    other_idx = [i for i, t in enumerate(tiers) if t not in ("strategic", "preferred")]

    # Guarantee ≥ 2 strategic/preferred per category (4 cats × 2 = 8 slots minimum)
    guaranteed = categories * 2                     # [dm, mro, indirect, services, dm, mro, indirect, services]
    rng.shuffle(guaranteed)
    remaining_sp = [rng.choice(categories) for _ in range(len(strategic_preferred_idx) - len(guaranteed))]
    sp_categories = guaranteed + remaining_sp
    rng.shuffle(sp_categories)

    other_categories = [rng.choice(categories) for _ in range(len(other_idx))]

    category_focus: list[str] = [""] * n
    for pos, cat in zip(strategic_preferred_idx, sp_categories):
        category_focus[pos] = cat
    for pos, cat in zip(other_idx, other_categories):
        category_focus[pos] = cat

    # ------------------------------------------------------------------
    # 4. Country
    #    ~60 % USA, ~20 % MX/CA, ~20 % Europe/Asia
    # ------------------------------------------------------------------
    country_pool = (
        ["USA"] * 30
        + ["Mexico", "Canada"] * 5          # 10 total
        + ["Germany", "UK", "Japan", "China", "France", "Italy", "South Korea", "Netherlands", "Sweden", "Spain"]
    )
    rng.shuffle(country_pool)
    countries = [country_pool[i % len(country_pool)] for i in range(n)]

    # ------------------------------------------------------------------
    # 5. Onboarded date: random date 3–8 years before today (2026-04-12)
    # ------------------------------------------------------------------
    today = date(2026, 4, 12)
    three_years_ago = today - timedelta(days=3 * 365)
    eight_years_ago = today - timedelta(days=8 * 365)
    date_range_days = (three_years_ago - eight_years_ago).days

    onboarded_dates = [
        eight_years_ago + timedelta(days=rng.randint(0, date_range_days))
        for _ in range(n)
    ]

    # ------------------------------------------------------------------
    # 6. Assemble DataFrame
    # ------------------------------------------------------------------
    records = []
    for i in range(n):
        records.append(
            {
                "supplier_id": f"SUP-{i + 1:03d}",
                "supplier_name": name_pool[i],
                "tier": tiers[i],
                "category_focus": category_focus[i],
                "country": countries[i],
                "is_active": True,
                "onboarded_date": onboarded_dates[i].isoformat(),
            }
        )

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Stub placeholders – to be implemented in later phases
# ---------------------------------------------------------------------------

def generate_purchase_orders(suppliers: pd.DataFrame, n: int = 2000) -> pd.DataFrame:
    """
    Generate ~2,000 purchase orders spanning Jan 2024 – Dec 2025.

    Power-law supplier sampling: the top 10 suppliers (by assigned weight) receive
    ~75 % of POs.  Weights are drawn once from a Zipf-like distribution, then the
    top-10 weight mass is scaled to exactly 0.75 so the constraint is deterministic.

    Lead-time by tier (uniform range, days):
        strategic  7–14  |  preferred 10–21  |  approved 14–30  |  spot 21–45

    status rules (relative to TODAY = 2026-04-12):
        po_date < today - 60 days  →  closed   (baseline)
        po_date ≥ today - 60 days  →  open
        ~3 % of all POs overridden to cancelled (drawn before status assignment)
    """
    rng = random.Random(SEED + 1)        # offset so stream differs from generate_suppliers
    np_rng = np.random.default_rng(SEED + 1)

    today = date(2026, 4, 12)
    cutoff = today - timedelta(days=60)

    # ------------------------------------------------------------------
    # 1. Power-law supplier weights
    #    Raw weights ~ 1/rank (Zipf).  Then scale so top-10 mass = 0.75.
    # ------------------------------------------------------------------
    supplier_ids = suppliers["supplier_id"].tolist()
    n_sup = len(supplier_ids)

    raw_weights = np.array([1.0 / (i + 1) for i in range(n_sup)], dtype=float)

    # Tier-aware weight assignment: strategic suppliers get the highest Zipf ranks,
    # then preferred, then approved, then spot.  Within each tier the order is
    # shuffled so it isn't just the first few supplier IDs.  This guarantees
    # strategic/preferred anchor the top-10 and spot suppliers stay low-weight.
    TIER_ORDER = ["strategic", "preferred", "approved", "spot"]
    tier_col = suppliers["tier"].tolist()
    ordered_indices: list[int] = []
    for t in TIER_ORDER:
        idx_list = [i for i, tier in enumerate(tier_col) if tier == t]
        np_rng.shuffle(idx_list)
        ordered_indices.extend(idx_list)

    weights = np.empty(n_sup)
    for rank, sup_idx in enumerate(ordered_indices):
        weights[sup_idx] = raw_weights[rank]

    top10_idx = np.argsort(weights)[::-1][:10]
    top10_mass = weights[top10_idx].sum()
    rest_idx = np.argsort(weights)[::-1][10:]
    rest_mass = weights[rest_idx].sum()

    # Scale so top-10 sum to exactly 0.75, rest sum to 0.25
    weights[top10_idx] = weights[top10_idx] / top10_mass * 0.75
    weights[rest_idx] = weights[rest_idx] / rest_mass * 0.25

    # ------------------------------------------------------------------
    # 2. Sample supplier for each PO
    # ------------------------------------------------------------------
    sampled_supplier_ids = np_rng.choice(supplier_ids, size=n, replace=True, p=weights).tolist()

    # ------------------------------------------------------------------
    # 3. Build tier lookup for lead-time sampling
    # ------------------------------------------------------------------
    tier_lookup     = suppliers.set_index("supplier_id")["tier"].to_dict()
    category_lookup = suppliers.set_index("supplier_id")["category_focus"].to_dict()

    LEAD_TIME = {
        "strategic": (7, 14),
        "preferred": (10, 21),
        "approved":  (14, 30),
        "spot":      (21, 45),
    }

    # ------------------------------------------------------------------
    # 4. Cancelled flag: 3 % of POs, sampled up front
    # ------------------------------------------------------------------
    cancelled_mask = np_rng.random(n) < 0.03

    # ------------------------------------------------------------------
    # 5. Date range: Jan 1 2024 – Dec 31 2025  (730 days + 1 for leap)
    # ------------------------------------------------------------------
    start_date = date(2024, 1, 1)
    end_date   = date(2026, 3, 31)
    date_range_days = (end_date - start_date).days   # 820

    # ------------------------------------------------------------------
    # 6. Payment-terms pool: net_30 50%, net_60 35%, net_90 15%
    # ------------------------------------------------------------------
    terms_pool = ["net_30"] * 50 + ["net_60"] * 35 + ["net_90"] * 15

    # ------------------------------------------------------------------
    # 7. Business unit weights (realistic — manufacturing dominates)
    # ------------------------------------------------------------------
    bus_units = ["manufacturing", "facilities", "logistics", "it", "corporate"]
    bu_weights = [0.40, 0.20, 0.15, 0.15, 0.10]

    records = []
    for i in range(n):
        po_date = start_date + timedelta(days=int(np_rng.integers(0, date_range_days + 1)))

        sup_id = sampled_supplier_ids[i]
        tier   = tier_lookup[sup_id]
        lo, hi = LEAD_TIME[tier]
        lead_days = int(np_rng.integers(lo, hi + 1))
        expected_delivery_date = po_date + timedelta(days=lead_days)

        if cancelled_mask[i]:
            status = "cancelled"
        elif po_date < cutoff:
            status = "closed"
        else:
            status = "open"

        payment_terms = rng.choice(terms_pool)
        business_unit = np_rng.choice(bus_units, p=bu_weights)

        records.append(
            {
                "po_id":                   f"PO-{i + 1:05d}",
                "supplier_id":             sup_id,
                "business_unit":           str(business_unit),
                "category":                category_lookup[sup_id],
                "po_date":                 po_date.isoformat(),
                "expected_delivery_date":  expected_delivery_date.isoformat(),
                "total_value":             0.0,
                "payment_terms":           payment_terms,
                "status":                  status,
            }
        )

    return pd.DataFrame(records)


def generate_po_line_items(purchase_orders: pd.DataFrame, suppliers: pd.DataFrame, n_per_po: tuple = (2, 6)) -> pd.DataFrame:
    """
    Generate 2–6 line items for every non-cancelled PO.

    standard_cost ranges (uniform):
        direct_materials  $50–$5000
        mro               $20–$2000
        indirect          $30–$3000
        services          $50–$2000

    unit_price = standard_cost * (1 + N(0, sigma)), clamped so price > 0.
    sigma by category:
        direct_materials 0.025 | mro 0.12 | indirect 0.15 | services 0.15
        anything else (tail spend) 0.25

    quantity ranges (uniform int):
        direct_materials 10–500 | mro 1–50 | indirect 1–20 | services 1–10

    line_total = unit_price * quantity  (computed field)

    After returning, caller must write back sum(line_total) per PO into
    purchase_orders.total_value.
    """
    rng = random.Random(SEED + 2)
    np_rng = np.random.default_rng(SEED + 2)

    # Tier-aware cost ceiling multiplier.
    # Spot buys are one-off, off-contract — lower unit values reflect that.
    # Approved is mid-range; strategic/preferred suppliers hold the big contracts.
    COST_CEIL_MULT = {"strategic": 1.00, "preferred": 0.85, "approved": 0.60, "spot": 0.25}
    tier_lookup = suppliers.set_index("supplier_id")["tier"].to_dict()

    COST_RANGE = {
        "direct_materials": (50.0,   5000.0),
        "mro":              (20.0,   2000.0),
        "indirect":         (30.0,   3000.0),
        "services":         (50.0,   2000.0),
    }
    SIGMA = {
        "direct_materials": 0.025,
        "mro":              0.12,
        "indirect":         0.15,
        "services":         0.15,
    }
    TAIL_SIGMA = 0.25

    QTY_RANGE = {
        "direct_materials": (10, 500),
        "mro":              (1,  50),
        "indirect":         (1,  20),
        "services":         (1,  10),
    }

    # Item name pools per category — realistic enough for the demo
    ITEM_NAMES = {
        "direct_materials": [
            "Steel Billet Grade A", "Aluminum Sheet 6061", "Carbon Fiber Roll",
            "Copper Tubing 1in", "Titanium Rod 0.5in", "Stainless Plate 304",
            "Brass Fitting 3/4in", "Nylon Pellets 50lb", "Polypropylene Resin",
            "Zinc Ingot 99.9%",
        ],
        "mro": [
            "Bearing 6205-2RS", "V-Belt B68", "Hydraulic Filter HF-200",
            "Grease Cartridge EP2", "O-Ring Kit Viton", "Safety Gloves Nitrile L",
            "Cutting Disc 4.5in", "Chain Lubricant 1gal", "Wire Brush Set",
            "Replacement Seal Kit",
        ],
        "indirect": [
            "Office Supply Bundle", "Janitorial Supply Box", "PPE Restocking Kit",
            "Printer Toner Set", "Packaging Foam Sheet", "Cable Ties Assorted",
            "Label Stock 4x6", "Facility Sign Set", "First Aid Kit OSHA",
            "Storage Bin Set",
        ],
        "services": [
            "Equipment Calibration", "Preventive Maintenance Visit",
            "Technical Training Session", "Inspection & Audit Service",
            "On-site Repair Service", "Consulting Day Rate",
            "Software License Annual", "Waste Disposal Service",
            "Cleaning Contract Monthly", "Courier & Freight Charge",
        ],
    }

    active_pos = purchase_orders[purchase_orders["status"] != "cancelled"].copy()

    records = []
    line_counter = 1

    for _, po in active_pos.iterrows():
        po_id    = po["po_id"]
        category = po["category"]

        n_lines = int(np_rng.integers(n_per_po[0], n_per_po[1] + 1))

        tier             = tier_lookup[po["supplier_id"]]
        mult             = COST_CEIL_MULT[tier]
        cost_lo, cost_hi_base = COST_RANGE.get(category, (20.0, 2000.0))
        cost_hi          = max(cost_hi_base * mult, cost_lo * 1.5)  # never let ceiling fall below floor
        sigma            = SIGMA.get(category, TAIL_SIGMA)
        qty_lo, qty_hi   = QTY_RANGE.get(category, (1, 20))
        name_pool        = ITEM_NAMES.get(category, ITEM_NAMES["mro"])

        for _ in range(n_lines):
            standard_cost = round(float(np_rng.uniform(cost_lo, cost_hi)), 4)
            variance      = float(np_rng.normal(0, sigma))
            unit_price    = round(max(standard_cost * (1 + variance), 0.01), 4)
            quantity      = int(np_rng.integers(qty_lo, qty_hi + 1))
            line_total    = round(unit_price * quantity, 4)
            item_name     = rng.choice(name_pool)

            records.append(
                {
                    "line_id":       f"LI-{line_counter:06d}",
                    "po_id":         po_id,
                    "item_name":     item_name,
                    "category":      category,
                    "standard_cost": standard_cost,
                    "unit_price":    unit_price,
                    "quantity":      quantity,
                    "line_total":    line_total,
                }
            )
            line_counter += 1

    return pd.DataFrame(records)


def generate_goods_receipts(
    purchase_orders: pd.DataFrame,
    suppliers: pd.DataFrame,
    line_items: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate one goods_receipt per closed PO.

    Combined OTIF probability by tier (single draw):
        strategic 0.94 | preferred 0.87 | approved 0.78 | spot 0.65
    On OTIF pass: on_time=1, in_full=1.
    On OTIF fail: one of three modes chosen with equal weight (~1/3 each):
        late+complete | on-time+short | late+short

    received_date:
        on_time  → expected_delivery_date − Uniform(0, 2) days
        late     → expected_delivery_date + Uniform(1, 14) days

    qty_rejected (non-zero rate by tier):
        strategic/preferred ~1% | approved ~3% | spot ~8%
        when non-zero: Uniform(1%, 5%) of qty_received, min 1 unit

    Anomaly – missing corporate receipts (Phase 2 spec):
        After generation, receipt rows for the 12 PO IDs below are dropped.
        These POs are closed and fully invoiced but have no delivery confirmation.

    MISSING_CORPORATE_PO_IDS (populated after first generation run):
        Determined deterministically by selecting the first 12 closed corporate
        POs in po_id order — see MISSING_RECEIPT_PO_IDS constant below.
    """
    rng = random.Random(SEED + 3)
    np_rng = np.random.default_rng(SEED + 3)

    # Single combined OTIF probability per tier.
    # Drawing one number and comparing to OTIF_P ensures the joint rate
    # (on_time=1 AND in_full=1) exactly matches the target; independent
    # Bernoulli draws would multiply and undershoot.
    OTIF_P   = {"strategic": 0.94, "preferred": 0.87, "approved": 0.78, "spot": 0.65}
    REJECT_P = {"strategic": 0.01, "preferred": 0.01, "approved": 0.03, "spot": 0.08}

    # Three failure modes drawn with equal probability (~1/3 each)
    FAILURE_MODES = [
        (False, True),   # late, complete
        (True,  False),  # on-time, short
        (False, False),  # late, short
    ]

    tier_lookup = suppliers.set_index("supplier_id")["tier"].to_dict()

    # qty_ordered per PO = sum of line item quantities
    qty_by_po = line_items.groupby("po_id")["quantity"].sum().to_dict()

    closed_pos = purchase_orders[purchase_orders["status"] == "closed"].copy()

    # ------------------------------------------------------------------
    # Identify the 12 closed corporate POs to drop (anomaly seed).
    # Selected as the first 12 closed corporate POs in po_id order so
    # the set is fully deterministic and reproducible.
    # ------------------------------------------------------------------
    corporate_closed = (
        closed_pos[closed_pos["business_unit"] == "corporate"]
        .sort_values("po_id")
        .head(12)
    )
    MISSING_RECEIPT_PO_IDS = corporate_closed["po_id"].tolist()
    # Hard-coded for reference (verified on SEED=108 dataset, post tier-aware weight fix):
    # ['PO-00005', 'PO-00008', 'PO-00032', 'PO-00034', 'PO-00035', 'PO-00036',
    #  'PO-00042', 'PO-00047', 'PO-00057', 'PO-00059', 'PO-00078', 'PO-00090']

    records = []
    receipt_counter = 1

    for _, po in closed_pos.iterrows():
        po_id      = po["po_id"]
        sup_id     = po["supplier_id"]
        tier       = tier_lookup[sup_id]
        exp_date   = date.fromisoformat(po["expected_delivery_date"])
        qty_ord    = int(qty_by_po.get(po_id, 0))

        # Single OTIF draw: pass → both flags True; fail → pick one failure mode
        if np_rng.random() < OTIF_P[tier]:
            on_time, in_full = True, True
        else:
            mode_idx = int(np_rng.integers(0, len(FAILURE_MODES)))
            on_time, in_full = FAILURE_MODES[mode_idx]

        if on_time:
            offset = int(np_rng.integers(0, 3))          # 0, 1, or 2 days early
            received_date = exp_date - timedelta(days=offset)
        else:
            offset = int(np_rng.integers(1, 15))          # 1–14 days late
            received_date = exp_date + timedelta(days=offset)

        if in_full:
            qty_received = qty_ord
        else:
            frac = float(np_rng.uniform(0.60, 0.94))
            qty_received = max(1, round(qty_ord * frac))

        # qty_rejected
        if np_rng.random() < REJECT_P[tier]:
            reject_frac  = float(np_rng.uniform(0.01, 0.05))
            qty_rejected = max(1, round(qty_received * reject_frac))
        else:
            qty_rejected = 0

        records.append(
            {
                "receipt_id":             f"GR-{receipt_counter:05d}",
                "po_id":                  po_id,
                "supplier_id":            sup_id,
                "received_date":          received_date.isoformat(),
                "qty_ordered":            qty_ord,
                "qty_received":           qty_received,
                "qty_rejected":           qty_rejected,
                "on_time":                on_time,
                "in_full":                in_full,
                # Temporary fields for correction pass — stripped below
                "_tier":                  tier,
                "_exp_date":              exp_date.isoformat(),
            }
        )
        receipt_counter += 1

    # ------------------------------------------------------------------
    # Post-generation OTIF floor correction.
    # With small per-tier sample sizes (strategic n≈83) the stochastic
    # draw can fall below the validation floor.  For any tier that misses,
    # flip the minimum number of failing rows to on_time=True, in_full=True
    # (and fix received_date to be on or before expected_delivery_date).
    # Uses the seeded rng so results are fully deterministic.
    # ------------------------------------------------------------------
    OTIF_FLOOR = {"strategic": 0.90, "preferred": 0.83, "approved": 0.73, "spot": 0.60}

    for tier_name, floor in OTIF_FLOOR.items():
        tier_recs = [r for r in records if r["_tier"] == tier_name]
        n         = len(tier_recs)
        if n == 0:
            continue
        hits      = sum(1 for r in tier_recs if r["on_time"] and r["in_full"])
        needed    = max(0, int(np.ceil(floor * n)) - hits)
        if needed == 0:
            continue
        # Candidate rows: currently failing for this tier
        fail_recs = [r for r in tier_recs if not (r["on_time"] and r["in_full"])]
        to_flip   = rng.sample(fail_recs, min(needed, len(fail_recs)))
        for rec in to_flip:
            rec["on_time"]       = True
            rec["in_full"]       = True
            rec["qty_received"]  = rec["qty_ordered"]
            # Fix received_date: set to expected_delivery_date (no early offset needed)
            rec["received_date"] = rec["_exp_date"]

    # Strip temporary fields before building the final DataFrame
    for rec in records:
        rec.pop("_tier")
        rec.pop("_exp_date")

    df = pd.DataFrame(records)

    # Drop the 12 missing corporate POs (anomaly)
    df = df[~df["po_id"].isin(MISSING_RECEIPT_PO_IDS)].reset_index(drop=True)

    return df, MISSING_RECEIPT_PO_IDS


def generate_invoices(
    purchase_orders: pd.DataFrame,
    suppliers: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate one invoice per closed PO.

    Overbilling supplier (approved tier, SEED=108 dataset):
        SUP-005  Cardinal Fluid Systems  (approved / services)
        85% of their invoices: po_total_value * Uniform(1.04, 1.08)
        remaining 15%: normal noise N(0, 0.005)

    invoice_date  = po_date + Uniform(7, 30) days
    due_date      = invoice_date + payment_terms_days (30 / 60 / 90)
    paid_date:
        80%  → due_date − Uniform(0, 5) days   [early/on-time]
        15%  → due_date + Uniform(1, 30) days  [overdue]
         5%  → NULL                            [pending]

    status (derived, today = 2026-04-12):
        disputed  → 2% of all invoices, randomly flagged before other rules
        paid      → paid_date is not null and paid_date <= due_date
        overdue   → paid_date > due_date  OR  (paid_date is null and due_date < today)
        pending   → paid_date is null and due_date >= today
    """
    rng = random.Random(SEED + 4)
    np_rng = np.random.default_rng(SEED + 4)

    today = date(2026, 4, 12)

    TERMS_DAYS = {"net_30": 30, "net_60": 60, "net_90": 90}

    # ------------------------------------------------------------------
    # Overbilling supplier: approved-tier supplier with the most closed POs.
    # Picking by volume ensures enough invoices for the anomaly to be
    # statistically visible in validation Check 4.
    # SEED=108 result: SUP-005  Cardinal Fluid Systems  (approved / services)
    # ------------------------------------------------------------------
    closed_pos_counts = (
        purchase_orders[purchase_orders["status"] == "closed"]
        .groupby("supplier_id")
        .size()
        .rename("po_count")
        .reset_index()
        .merge(suppliers[["supplier_id", "tier"]], on="supplier_id")
    )
    OVERBILLING_SUPPLIER_ID = (
        closed_pos_counts[closed_pos_counts["tier"] == "approved"]
        .sort_values("po_count", ascending=False)
        ["supplier_id"]
        .iloc[0]
    )
    # OVERBILLING_SUPPLIER_ID = 'SUP-005'  (Cardinal Fluid Systems, approved/services)

    closed_pos = purchase_orders[purchase_orders["status"] == "closed"].copy()

    # Pre-draw disputed mask (2% of invoices)
    disputed_mask = np_rng.random(len(closed_pos)) < 0.02

    records = []

    for idx, (_, po) in enumerate(closed_pos.iterrows()):
        po_id         = po["po_id"]
        sup_id        = po["supplier_id"]
        po_date       = date.fromisoformat(po["po_date"])
        po_total      = float(po["total_value"])
        terms         = po["payment_terms"]
        terms_days    = TERMS_DAYS.get(terms, 30)

        # invoice_date
        inv_offset    = int(np_rng.integers(7, 31))
        invoice_date  = po_date + timedelta(days=inv_offset)

        due_date      = invoice_date + timedelta(days=terms_days)

        # invoice_amount
        if sup_id == OVERBILLING_SUPPLIER_ID and np_rng.random() < 0.85:
            ratio          = float(np_rng.uniform(1.04, 1.08))
            invoice_amount = round(po_total * ratio, 2)
        else:
            noise          = float(np_rng.uniform(-0.04, 0.002))
            invoice_amount = round(max(po_total * (1 + noise), 0.01), 2)

        # paid_date
        roll = np_rng.random()
        if roll < 0.80:
            early         = int(np_rng.integers(0, 6))   # 0–5 days early
            paid_date     = due_date - timedelta(days=early)
            paid_date_str = paid_date.isoformat()
        elif roll < 0.95:
            late          = int(np_rng.integers(1, 31))  # 1–30 days late
            paid_date     = due_date + timedelta(days=late)
            paid_date_str = paid_date.isoformat()
        else:
            paid_date     = None
            paid_date_str = None

        # status (disputed takes priority, then derive from dates)
        if disputed_mask[idx]:
            status = "disputed"
        elif paid_date is not None and paid_date <= due_date:
            status = "paid"
        elif paid_date is not None and paid_date > due_date:
            status = "overdue"
        elif paid_date is None and due_date < today:
            status = "overdue"
        else:
            status = "pending"

        records.append(
            {
                "invoice_id":      f"INV-{idx + 1:05d}",
                "po_id":           po_id,
                "supplier_id":     sup_id,
                "invoice_date":    invoice_date.isoformat(),
                "due_date":        due_date.isoformat(),
                "paid_date":       paid_date_str,
                "invoice_amount":  invoice_amount,
                "po_total_value":  round(po_total, 2),
                "status":          status,
            }
        )

    return pd.DataFrame(records), OVERBILLING_SUPPLIER_ID


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    out_dir = pathlib.Path(__file__).parent.parent / "data"
    out_dir.mkdir(exist_ok=True)

    # 1. Suppliers
    suppliers = generate_suppliers()

    # 2. Purchase orders
    pos = generate_purchase_orders(suppliers)

    # 3. Line items — generated first so we can write back total_value
    lines = generate_po_line_items(pos, suppliers)
    po_totals = lines.groupby("po_id")["line_total"].sum()
    pos["total_value"] = pos["po_id"].map(po_totals).fillna(0.0).round(2)

    # 4. Goods receipts
    receipts, _ = generate_goods_receipts(pos, suppliers, lines)

    # 5. Invoices
    invoices, _ = generate_invoices(pos, suppliers)

    # Write all five CSVs
    tables = {
        "suppliers.csv":       suppliers,
        "purchase_orders.csv": pos,
        "po_line_items.csv":   lines,
        "goods_receipts.csv":  receipts,
        "invoices.csv":        invoices,
    }
    for filename, df in tables.items():
        path = out_dir / filename
        df.to_csv(path, index=False)

    # Summary
    print(f"\n{'Table':<25} {'Rows':>6}  {'Cols':>4}  Path")
    print("-" * 65)
    for filename, df in tables.items():
        path = out_dir / filename
        print(f"  {filename:<23} {len(df):>6}  {len(df.columns):>4}  {path}")


if __name__ == "__main__":
    main()
