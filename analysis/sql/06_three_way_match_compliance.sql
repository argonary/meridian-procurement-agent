-- ============================================================
-- Three-Way Match Compliance
-- ============================================================
-- BUSINESS QUESTION:
--   For each invoice, do we have a matching PO and a
--   matching goods receipt? Where are the gaps?
--
-- PROCUREMENT CONTEXT:
--   Three-way match is the gold standard control in accounts
--   payable: PO issued, goods received, invoice matches both.
--   When any leg is missing, payment should be held pending
--   investigation. Gaps indicate either process failures
--   (goods received but not logged) or control failures
--   (invoices paid without receipt confirmation). This is a
--   standard internal audit finding.
--
-- TOLERANCE RATIONALE:
--   A 5% tolerance is applied to the amount variance check
--   to account for legitimate invoice variances such as
--   freight charges, taxes, minor quantity adjustments, and
--   rounding across line items. In the Meridian dataset,
--   invoice amounts carry a small noise band by design to
--   reflect real-world AP conditions. A fixed dollar
--   tolerance is inappropriate at this spend scale; a
--   percentage-based threshold is more robust.
--
-- OUTPUT:
--   Invoice-level match status across all three legs, with
--   a compliance flag and gap description.
--
-- DASHBOARD KPI: Invoices Failing Three-Way Match
--   The Lakeview counter widget uses the query below to
--   count invoices that fail match on any dimension:
--
--   WITH match AS (
--       SELECT
--           i.invoice_id,
--           i.po_id,
--           i.supplier_id,
--           po.po_id IS NOT NULL AS has_po,
--           gr.po_id IS NOT NULL AS has_receipt,
--           ROUND(i.invoice_amount, 2) AS invoiced_amount,
--           ROUND(po.total_value, 2) AS po_amount,
--           ROUND(i.invoice_amount - po.total_value, 2) AS amount_variance
--       FROM workspace.default.invoices i
--       LEFT JOIN workspace.default.purchase_orders po
--           ON i.po_id = po.po_id
--       LEFT JOIN workspace.default.goods_receipts gr
--           ON i.po_id = gr.po_id
--           AND i.supplier_id = gr.supplier_id
--   ),
--   flagged AS (
--       SELECT *,
--           CASE
--               WHEN has_po AND has_receipt
--                    AND ABS(amount_variance) / NULLIF(po_amount, 0) < 0.05
--                   THEN 'MATCHED'
--               WHEN has_po AND has_receipt
--                    AND ABS(amount_variance) / NULLIF(po_amount, 0) >= 0.05
--                   THEN 'AMOUNT MISMATCH'
--               WHEN has_po AND NOT has_receipt
--                   THEN 'MISSING RECEIPT'
--               WHEN NOT has_po
--                   THEN 'NO PO FOUND'
--               ELSE 'REVIEW REQUIRED'
--           END AS match_status
--       FROM match
--   )
--   SELECT COUNT(*) AS invoices_failing_match
--   FROM flagged
--   WHERE match_status != 'MATCHED'
-- ============================================================

WITH match AS (
    SELECT
        i.invoice_id,
        i.po_id,
        i.supplier_id,
        i.invoice_date,
        i.invoice_amount,
        i.status                                            AS invoice_status,
        po.po_id                                            IS NOT NULL
                                                            AS has_po,
        gr.po_id                                            IS NOT NULL
                                                            AS has_receipt,
        ROUND(i.invoice_amount, 2)                          AS invoiced_amount,
        ROUND(po.total_value, 2)                            AS po_amount,
        ROUND(i.invoice_amount - po.total_value, 2)         AS amount_variance
    FROM workspace.default.invoices i
    LEFT JOIN workspace.default.purchase_orders po
        ON i.po_id = po.po_id
    LEFT JOIN workspace.default.goods_receipts gr
        ON i.po_id = gr.po_id
        AND i.supplier_id = gr.supplier_id
)
SELECT
    invoice_id,
    po_id,
    supplier_id,
    invoice_date,
    invoice_status,
    has_po,
    has_receipt,
    invoiced_amount,
    po_amount,
    amount_variance,
    CASE
        WHEN has_po AND has_receipt
             AND ABS(amount_variance) / NULLIF(po_amount, 0) < 0.05
            THEN 'MATCHED'
        WHEN has_po AND has_receipt
             AND ABS(amount_variance) / NULLIF(po_amount, 0) >= 0.05
            THEN 'AMOUNT MISMATCH'
        WHEN has_po AND NOT has_receipt
            THEN 'MISSING RECEIPT'
        WHEN NOT has_po
            THEN 'NO PO FOUND'
        ELSE 'REVIEW REQUIRED'
    END                                                     AS match_status
FROM match
ORDER BY
    CASE match_status
        WHEN 'NO PO FOUND'       THEN 1
        WHEN 'MISSING RECEIPT'   THEN 2
        WHEN 'AMOUNT MISMATCH'   THEN 3
        WHEN 'REVIEW REQUIRED'   THEN 4
        ELSE 5
    END;