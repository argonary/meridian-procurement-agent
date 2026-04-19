-- ============================================================
-- Supplier Performance by Tier
-- ============================================================
-- BUSINESS QUESTION:
--   Are higher-tier suppliers actually outperforming on
--   delivery? Does tier predict OTIF (On-Time In-Full) rate?
--
-- PROCUREMENT CONTEXT:
--   Supplier tiers (e.g. Preferred, Approved, Restricted)
--   are supposed to reflect performance history. This query
--   validates whether the tier structure is doing its job
--   or whether low-tier suppliers are punching above their
--   weight -- or vice versa.
--
-- OUTPUT:
--   OTIF rate, fill rate, and rejection rate by supplier
--   tier, ordered from best to worst performing.
-- ============================================================

SELECT
    s.tier,
    COUNT(DISTINCT gr.supplier_id)                          AS supplier_count,
    COUNT(gr.receipt_id)                                    AS total_receipts,
    ROUND(100.0 * SUM(CASE WHEN gr.on_time AND gr.in_full
                           THEN 1 ELSE 0 END)
          / COUNT(gr.receipt_id), 2)                        AS otif_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN gr.on_time
                           THEN 1 ELSE 0 END)
          / COUNT(gr.receipt_id), 2)                        AS on_time_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN gr.in_full
                           THEN 1 ELSE 0 END)
          / COUNT(gr.receipt_id), 2)                        AS in_full_rate_pct,
    ROUND(100.0 * SUM(gr.qty_rejected)
          / NULLIF(SUM(gr.qty_received), 0), 2)             AS rejection_rate_pct
FROM goods_receipts gr
JOIN suppliers s
    ON gr.supplier_id = s.supplier_id
GROUP BY s.tier
ORDER BY otif_rate_pct DESC;