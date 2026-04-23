-- ============================================================
-- Spend Concentration Analysis
-- ============================================================
-- BUSINESS QUESTION:
--   What share of total spend flows through our top suppliers?
--   Is our supplier base dangerously concentrated?
--
-- PROCUREMENT CONTEXT:
--   A healthy supply base typically follows a power law —
--   some concentration is expected. The risk flag is when
--   the top 3-5 suppliers hold >60% of spend in a category,
--   creating single-source dependency.
--
-- OUTPUT:
--   Supplier rank, cumulative spend share, concentration tier
--
-- DASHBOARD KPI: Total Active Spend
--   The Lakeview counter widget derives total active spend
--   directly from this query's source data:
--
--   SELECT ROUND(SUM(total_value), 2) AS total_active_spend
--   FROM workspace.default.purchase_orders
--   WHERE status != 'cancelled'
-- ============================================================

WITH supplier_spend AS (
    SELECT
        s.supplier_id,
        s.supplier_name,
        s.tier,
        SUM(po.total_value) AS total_spend
    FROM purchase_orders po
    JOIN suppliers s ON po.supplier_id = s.supplier_id
    WHERE po.status != 'cancelled'
    GROUP BY 1, 2, 3
),
ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_spend DESC)             AS spend_rank,
        ROUND(
            100.0 * total_spend / SUM(total_spend) OVER (),
            2
        )                                                   AS spend_pct,
        ROUND(
            100.0 * SUM(total_spend) OVER (
                ORDER BY total_spend DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / SUM(total_spend) OVER (),
            2
        )                                                   AS cumulative_pct
    FROM supplier_spend
)
SELECT
    spend_rank,
    supplier_name,
    tier,
    total_spend,
    spend_pct,
    cumulative_pct,
    CASE
        WHEN cumulative_pct <= 50 THEN 'Core (top 50%)'
        WHEN cumulative_pct <= 80 THEN 'Secondary (50-80%)'
        ELSE 'Tail'
    END                                                     AS concentration_tier
FROM ranked
WHERE spend_rank <= 15
ORDER BY spend_rank ASC;