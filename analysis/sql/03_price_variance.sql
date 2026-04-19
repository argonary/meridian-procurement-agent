-- ============================================================
-- Price Variance Analysis (Maverick Spend Detection)
-- ============================================================
-- BUSINESS QUESTION:
--   Where are we paying above standard cost? Which items,
--   categories, or suppliers show the largest price premium
--   over the established standard?
--
-- PROCUREMENT CONTEXT:
--   Standard cost is what procurement negotiated and locked
--   in. Unit price is what was actually paid per line item.
--   A consistent premium signals contract leakage, maverick
--   buying, or supplier price creep that hasn't been caught
--   in reviews. Even a 5% variance at volume is a material
--   financial exposure.
--
-- OUTPUT:
--   Item-level variance, ranked by total overspend impact.
-- ============================================================

WITH variance AS (
    SELECT
        li.item_name,
        li.category,
        s.supplier_name,
        s.tier,
        COUNT(li.line_id)                                   AS line_count,
        ROUND(AVG(li.standard_cost), 2)                     AS avg_standard_cost,
        ROUND(AVG(li.unit_price), 2)                        AS avg_unit_price,
        ROUND(AVG(li.unit_price - li.standard_cost), 2)     AS avg_price_premium,
        ROUND(100.0 * AVG(li.unit_price - li.standard_cost)
              / NULLIF(AVG(li.standard_cost), 0), 2)        AS variance_pct,
        ROUND(SUM((li.unit_price - li.standard_cost)
              * li.quantity), 2)                            AS total_overspend
    FROM po_line_items li
    JOIN purchase_orders po
        ON li.po_id = po.po_id
    JOIN suppliers s
        ON po.supplier_id = s.supplier_id
    GROUP BY li.item_name, li.category, s.supplier_name, s.tier
)
SELECT *
FROM variance
WHERE avg_price_premium > 0
ORDER BY total_overspend DESC
LIMIT 50;