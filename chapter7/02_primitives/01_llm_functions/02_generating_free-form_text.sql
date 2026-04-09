-- Summarizing support tickets - shape inputs (one row per product)
-- tag::ticket_summary_inline[]
WITH tickets_for_summary AS (
  SELECT ticket_id, product_id, created_at, subject, body
  FROM `oreilly_support.support_tickets`
  WHERE created_at >= TIMESTAMP_SUB('2024-01-14', INTERVAL 7 DAY)
),
grouped AS (
  SELECT
    product_id,
    COUNT(*) AS ticket_count,
    STRING_AGG(
      CONCAT('Subject: ', subject, '\nBody: ', body),
      '\n\n---\n\n'
      ORDER BY created_at DESC
    ) AS ticket_text
  FROM tickets_for_summary
  GROUP BY product_id
)
-- end::ticket_summary_inline[]

-- Summarizing support tickets - generate summaries (one model call per product)
-- tag::ticket_summary_generate[]
SELECT
  product_id,
  ticket_count,
  (SELECT ml_generate_text_llm_result FROM ML.GENERATE_TEXT(
    MODEL `oreilly_support.gemini_flash`,
    (SELECT FORMAT(
      CONCAT(
        'You are a support triage assistant. ',
        'Summarize the main issues customers report for product %s ',
        'over the last week in 3 bullet points.\n\n',
        'Tickets:\n%s'
      ),
      product_id,
      ticket_text
    ) AS prompt),
    STRUCT(256 AS max_output_tokens, TRUE AS flatten_json_output)
  )) AS issue_summary
FROM grouped;
-- end::ticket_summary_generate[]
