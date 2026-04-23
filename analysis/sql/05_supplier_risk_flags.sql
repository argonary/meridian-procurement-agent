-- ============================================================
-- Supplier Risk Flags (Multi-Dimensional Risk Scoring)
-- ============================================================
-- BUSINESS QUESTION:
--   Which suppliers carry the most combined risk across
--   delivery performance, overbilling history, and spend
--   concentration?
--
-- PROCUREMENT CONTEXT:
--   No single metric tells the full risk story. A supplier
--   can have perfect OTIF but systematically overbill.
--   Another might be reliable but represent dangerous spend
--   concentration. This query surfaces a composite risk
--   view -- the kind a category manager would use to
--   prioritize supplier reviews.
--
-- THRESHOLD RATIONALE:
--   Delivery risk thresholds (HIGH < 65%, MEDIUM < 80%) are
--   calibrated to the actual OTIF distribution in the data,
--   where strategic suppliers cluster at 90%+ and spot
--   suppliers range from 22-70%. A HIGH flag at 65% captures
--   genuine chronic underperformers rather than suppliers
--   with occasional misses.
--
--   Billing risk thresholds (HIGH > 20%, MEDIUM > 10%) are
--   set conservatively given that the dataset overbilling
--   rate is 6.2% overall. A supplier exceeding 20% overbill
--   rate is a clear outlier warranting immediate review.
--
-- OUTPUT:
--   One row per supplier with risk indicators across three
--   dimensions, plus a composite risk flag.
--
-- DASHBOARD KPI: Suppliers with HIGH Risk
--   The Lakeview counter widget uses the query below to
--   count suppliers flagged HIGH on at least one dimension:
--
--   WITH delivery AS (
--       SELECT supplier_id,
--           ROUND(100.0 * SUM(CASE WHEN on_time AND in_full
--               THEN 1 ELSE 0 END) / COUNT(receipt_id), 2)
--           AS otif_rate
--       FROM workspace.default.goods_receipts
--       GROUP BY supplier_id
--   ),
--   billing AS (
--       SELECT supplier_id,
--           ROUND(100.0 * SUM(CASE WHEN invoice_amount > po_total_value
--               THEN 1 ELSE 0 END) / COUNT(*), 2)
--           AS overbill_rate
--       FROM workspace.default.invoices
--       GROUP BY supplier_id
--   ),
--   spend AS (
--       SELECT supplier_id, SUM(total_value) AS total_spend
--       FROM workspace.default.purchase_orders
--       WHERE status != 'cancelled'
--       GROUP BY supplier_id
--   ),
--   total_spend AS (
--       SELECT SUM(total_value) AS grand_total
--       FROM workspace.default.purchase_orders
--       WHERE status != 'cancelled'
--   ),
--   risk_flags AS (
--       SELECT s.supplier_id,
--           CASE WHEN sp.total_spend / ts.grand_total > 0.10
--               THEN 'HIGH' ELSE 'OK' END AS concentration_risk,
--           CASE WHEN d.otif_rate < 65 THEN 'HIGH'
--               WHEN d.otif_rate < 80 THEN 'MEDIUM'
--               ELSE 'OK' END AS delivery_risk,
--           CASE WHEN b.overbill_rate > 20 THEN 'HIGH'
--               WHEN b.overbill_rate > 10 THEN 'MEDIUM'
--               ELSE 'OK' END AS billing_risk
--       FROM workspace.default.suppliers s
--       JOIN spend sp ON s.supplier_id = sp.supplier_id
--       JOIN delivery d ON s.supplier_id = d.supplier_id
--       JOIN billing b ON s.supplier_id = b.supplier_id
--       CROSS JOIN total_spend ts
--   )
--   SELECT COUNT(*) AS high_risk_supplier_count
--   FROM risk_flags
--   WHERE concentration_risk = 'HIGH'
--      OR delivery_risk = 'HIGH'
--      OR billing_risk = 'HIGH'
-- ============================================================

WITH delivery AS (
    SELECT
        supplier_id,
        COUNT(receipt_id)                                   AS total_receipts,
        ROUND(100.0 * SUM(CASE WHEN on_time AND in_full
                               THEN 1 ELSE 0 END)
              / COUNT(receipt_id), 2)                       AS otif_rate
    FROM goods_receipts
    GROUP BY supplier_id
),
billing AS (
    SELECT
        supplier_id,
        COUNT(*)                                            AS total_invoices,
        SUM(CASE WHEN invoice_amount > po_total_value
                 THEN 1 ELSE 0 END)                        AS overbill_count,
        ROUND(100.0 * SUM(CASE WHEN invoice_amount > po_total_value
                               THEN 1 ELSE 0 END)
              / COUNT(*), 2)                               AS overbill_rate
    FROM invoices
    GROUP BY supplier_id
),
spend AS (
    SELECT
        supplier_id,
        ROUND(SUM(total_value), 2)                         AS total_spend
    FROM purchase_orders
    WHERE status != 'cancelled'
    GROUP BY supplier_id
),
total_spend AS (
    SELECT SUM(total_value) AS grand_total
    FROM purchase_orders
    WHERE status != 'cancelled'
)
SELECT
    s.supplier_id,
    s.supplier_name,
    s.tier,
    s.category_focus,
    s.country,
    ROUND(sp.total_spend, 2)                               AS total_spend,
    ROUND(100.0 * sp.total_spend
          / ts.grand_total, 2)                             AS spend_share_pct,
    d.otif_rate,
    b.overbill_rate,
    CASE WHEN sp.total_spend / ts.grand_total > 0.10
         THEN 'HIGH' ELSE 'OK' END                         AS concentration_risk,
    CASE WHEN d.otif_rate < 65
         THEN 'HIGH' WHEN d.otif_rate < 80
         THEN 'MEDIUM' ELSE 'OK' END                       AS delivery_risk,
    CASE WHEN b.overbill_rate > 20
         THEN 'HIGH' WHEN b.overbill_rate > 10
         THEN 'MEDIUM' ELSE 'OK' END                       AS billing_risk
FROM suppliers s
JOIN spend sp       ON s.supplier_id = sp.supplier_id
JOIN delivery d     ON s.supplier_id = d.supplier_id
JOIN billing b      ON s.supplier_id = b.supplier_id
CROSS JOIN total_spend ts
ORDER BY total_spend DESC;