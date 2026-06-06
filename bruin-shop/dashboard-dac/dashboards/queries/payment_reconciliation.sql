SELECT
  FORMAT_DATE('%Y-%m-%d', report_date) AS report_date,
  shopify_order_attempts,
  shopify_successful_orders,
  stripe_payment_intents,
  stripe_succeeded_intents,
  shopify_partially_refunded_orders,
  stripe_refunds,
  stripe_refund_amount_usd,
  successful_order_gap,
  refund_record_gap
FROM `bruin-playground-arsalan.reports.rpt_payment_reconciliation`
WHERE report_date BETWEEN COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.start }}', '') AS DATE), DATE '2025-01-01')
    AND COALESCE(SAFE_CAST(NULLIF('{{ filters.date_range.end }}', '') AS DATE), DATE '2026-06-05')
ORDER BY report_date DESC
LIMIT 60
