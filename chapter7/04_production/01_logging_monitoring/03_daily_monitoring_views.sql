-- Daily monitoring view for LLM calls
-- tag::llm_calls_daily[]
CREATE OR REPLACE VIEW
  `oreilly_support.llm_calls_daily` AS
SELECT
  DATE(created_at) AS run_date,
  prompt_id,
  prompt_version,
  model,
  COUNT(*) AS call_count,
  COUNTIF(COALESCE(status, 'ok') != 'ok') AS error_count,
  SUM(COALESCE(tokens_in, 0)) AS tokens_in,
  SUM(COALESCE(tokens_out, 0)) AS tokens_out,
  AVG(latency_ms) AS avg_latency_ms
FROM `oreilly_support.llm_calls`
GROUP BY run_date, prompt_id, prompt_version, model;
-- end::llm_calls_daily[]

-- Daily monitoring view for retrieval runs
-- tag::retrieval_runs_daily[]
CREATE OR REPLACE VIEW
  `oreilly_support.retrieval_runs_daily` AS
SELECT
  DATE(created_at) AS run_date,
  retrieval_config_id,
  embedding_model,
  embedding_version,
  COUNT(*) AS run_count,
  AVG(ARRAY_LENGTH(retrieved)) AS avg_top_k,
  AVG((
    SELECT MIN(distance)
    FROM UNNEST(retrieved)
  )) AS avg_best_distance
FROM `oreilly_support.retrieval_runs`
GROUP BY
  run_date,
  retrieval_config_id,
  embedding_model,
  embedding_version;
-- end::retrieval_runs_daily[]

-- Alert queries for failures and retrieval drift
-- tag::alert_queries[]
SELECT *
FROM `oreilly_support.llm_calls_daily`
WHERE run_date = CURRENT_DATE()
  AND error_count > 0;

SELECT *
FROM `oreilly_support.retrieval_runs_daily`
WHERE run_date = CURRENT_DATE()
  AND avg_best_distance > 0.35;
-- end::alert_queries[]

