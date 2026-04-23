-- ============================================================
-- Overbilling Detection
-- ============================================================
-- BUSINESS QUESTION:
--   Which invoices exceed the value of their originating
--   purchase order? By how much, and which suppliers are
--   responsible?
--
-- PROCUREMENT CONTEXT:
--   An invoice should never exceed the PO it references
--   without an approved change order. When it does, it is
--   either an error or intentional overbilling. At scale,
--   even small systematic overcharges compound into
--   significant financial leakage. This is one of the most
--   common findings in a procurement audit.
--
-- OUTPUT:
--   Invoice-level overbilling flags with dollar variance
--   and supplier context, ranked by overcharge amount.
--
-- DASHBOARD KPI: Overbilling Exposure
--   The Lakeview counter widget derives total overbilling
--   exposure directly from this query's source data:
--
--   SELECT ROUND(SUM(invoice_amount - po_total_value), 2)
--       AS total_overbilling_exposure
--   FROM workspace.default.invoices
--   WHERE invoice_amount > po_total_value
-- ============================================================

SELECT
    i.invoice_id,
    i.supplier_id,
    s.supplier_name,
    s.tier,
    i.po_id,
    i.invoice_date,
    i.status                                                AS invoice_status,
    ROUND(i.po_total_value, 2)                              AS po_value,
    ROUND(i.invoice_amount, 2)                              AS invoice_amount,
    ROUND(i.invoice_amount - i.po_total_value, 2)           AS overcharge_amount,
    ROUND(100.0 * (i.invoice_amount - i.po_total_value)
          / NULLIF(i.po_total_value, 0), 2)                 AS overcharge_pct
FROM invoices i
JOIN suppliers s
    ON i.supplier_id = s.supplier_id
WHERE i.invoice_amount > i.po_total_value
ORDER BY overcharge_amount DESC;