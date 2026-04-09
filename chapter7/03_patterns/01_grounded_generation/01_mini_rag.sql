-- Mini-RAG - context CTE (packing retrieved chunks)
-- tag::context_cte[]
WITH context AS (
  SELECT
    run_id,
    ticket_id,
    product_id,
    STRING_AGG(
      CONCAT('[', article_id, ' | ', chunk_id, ']\n', content),
      '\n\n---\n\n'
      ORDER BY distance
    ) AS context_text,
    ARRAY_AGG(chunk_id ORDER BY distance) AS retrieved_chunk_ids,
    ARRAY_AGG(DISTINCT article_id ORDER BY article_id) AS retrieved_article_ids
  FROM `oreilly_support.ticket_kb_retrieval`
  GROUP BY run_id, ticket_id, product_id
),
-- end::context_cte[]

-- Mini-RAG - ticket CTE (attaching the ticket text)
-- tag::ticket_cte[]
ticket AS (
  SELECT CONCAT(subject, '\n', body) AS ticket_text
  FROM `oreilly_support.support_tickets`
  WHERE ticket_id = (SELECT ticket_id FROM context)
)
-- end::ticket_cte[]

-- Mini-RAG - generating a grounded draft reply (final `SELECT`)
-- tag::generate_draft[]
SELECT
  c.run_id,
  c.ticket_id,
  (SELECT ml_generate_text_llm_result FROM ML.GENERATE_TEXT(
    MODEL `oreilly_support.gemini_flash`,
    (SELECT FORMAT(
      CONCAT(
        'Draft a helpful support reply using ONLY the context.\n\n',
        'Ticket:\n%s\n\n',
        'Context:\n%s\n\n',
        'Requirements:\n',
        '- 3–6 sentences max.\n',
        '- End with: \"Sources: <comma-separated chunk_id list>\".\n'
      ),
      (SELECT ticket_text FROM ticket),
      c.context_text
    ) AS prompt),
    STRUCT(
      0.2 AS temperature,
      256 AS max_output_tokens,
      TRUE AS flatten_json_output
    )
  )) AS draft_reply,
  c.retrieved_chunk_ids,
  c.retrieved_article_ids
FROM context c;
-- end::generate_draft[]
