-- Token budget caps per prompt
-- tag::cost_caps[]
CREATE TABLE IF NOT EXISTS
  `oreilly_support.prompt_cost_caps` (
  prompt_id STRING,
  prompt_version INT64,
  daily_token_cap INT64,
  owner STRING,
  created_at TIMESTAMP
);

SELECT
  d.run_date,
  d.prompt_id,
  d.prompt_version,
  d.tokens_in + d.tokens_out AS tokens_total,
  c.daily_token_cap
FROM `oreilly_support.llm_calls_daily` d
JOIN `oreilly_support.prompt_cost_caps` c
  USING (prompt_id, prompt_version)
WHERE d.run_date = CURRENT_DATE()
  AND d.tokens_in + d.tokens_out > c.daily_token_cap;
-- end::cost_caps[]

-- Cache view for latest successful outputs
-- tag::llm_cache_view[]
CREATE OR REPLACE VIEW
  `oreilly_support.llm_cache_latest` AS
SELECT
  prompt_id,
  prompt_version,
  model,
  params_json,
  input_hash,
  output_text,
  created_at
FROM `oreilly_support.llm_calls`
WHERE COALESCE(status, 'ok') = 'ok'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY
    prompt_id,
    prompt_version,
    model,
    params_json,
    input_hash
  ORDER BY created_at DESC
) = 1;
-- end::llm_cache_view[]

-- Cache miss identification before generation
-- tag::llm_cache_misses[]
WITH prompts AS (
  SELECT
    ticket_id,
    CONCAT('Summarize ticket: ', subject, '\n', body) AS prompt,
    TO_HEX(SHA256(
      CONCAT('Summarize ticket: ', subject, '\n', body)
    )) AS input_hash
  FROM `oreilly_support.support_tickets_llm_input`
  WHERE status IN ('open', 'in_progress')
  LIMIT 100
),
cache AS (
  SELECT input_hash, output_text
  FROM `oreilly_support.llm_cache_latest`
  WHERE prompt_id = 'ticket_summary'
    AND prompt_version = 1
    AND model = 'gemini-2.5-flash'
    AND params_json = TO_JSON_STRING(
      STRUCT(0.2 AS temperature, 256 AS max_output_tokens)
    )
),
misses AS (
  SELECT p.ticket_id, p.prompt, p.input_hash
  FROM prompts p
  LEFT JOIN cache c USING (input_hash)
  WHERE c.input_hash IS NULL
)
SELECT * FROM misses;
-- end::llm_cache_misses[]

-- Generate for misses and merge with cache (continues previous CTE)
-- tag::llm_cache_generate[]
generated AS (
  SELECT input_hash, ml_generate_text_llm_result AS output_text
  FROM ML.GENERATE_TEXT(
    MODEL `oreilly_support.gemini_flash`,
    (SELECT input_hash, prompt FROM misses),
    STRUCT(
      0.2 AS temperature,
      256 AS max_output_tokens,
      TRUE AS flatten_json_output
    )
  )
)
SELECT
  p.ticket_id,
  COALESCE(c.output_text, g.output_text) AS summary_text
FROM prompts p
LEFT JOIN cache c USING (input_hash)
LEFT JOIN generated g USING (input_hash);
-- end::llm_cache_generate[]

