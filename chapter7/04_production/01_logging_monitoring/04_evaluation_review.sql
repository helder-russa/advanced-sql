-- Minimal evaluation set table
-- tag::eval_sets[]
CREATE TABLE IF NOT EXISTS
  `oreilly_support.eval_sets` (
  eval_set_id STRING,
  row_id STRING,
  input_text STRING,
  expected_json STRING,
  created_at TIMESTAMP
);
-- end::eval_sets[]

-- Minimal evaluation results table
-- tag::eval_results[]
CREATE TABLE IF NOT EXISTS
  `oreilly_support.eval_results` (
  eval_run_id STRING,
  eval_set_id STRING,
  row_id STRING,
  metric_name STRING,
  metric_value FLOAT64,
  passed BOOL,
  prompt_id STRING,
  prompt_version INT64,
  retrieval_config_id STRING,
  model STRING,
  created_at TIMESTAMP
)
CLUSTER BY eval_set_id, metric_name, prompt_id;
-- end::eval_results[]

-- Minimal human review tables for RAG outputs
-- tag::human_review_loop[]
CREATE TABLE IF NOT EXISTS
  `oreilly_support.rag_answers` (
  run_id STRING,
  ticket_id STRING,
  created_at TIMESTAMP,
  answer_text STRING,
  retrieved_chunk_ids ARRAY<STRING>,
  prompt_id STRING,
  prompt_version INT64,
  model STRING
);

CREATE TABLE IF NOT EXISTS
  `oreilly_support.rag_answer_reviews` (
  run_id STRING,
  reviewer STRING,
  decision STRING,
  notes STRING,
  created_at TIMESTAMP
)
CLUSTER BY decision;

CREATE OR REPLACE VIEW
  `oreilly_support.rag_review_queue` AS
SELECT
  a.run_id,
  a.ticket_id,
  a.created_at,
  a.answer_text,
  a.retrieved_chunk_ids,
  a.prompt_id,
  a.prompt_version,
  a.model
FROM `oreilly_support.rag_answers` a
LEFT JOIN `oreilly_support.rag_answer_reviews` r
  USING (run_id)
WHERE r.run_id IS NULL
ORDER BY a.created_at DESC
LIMIT 100;
-- end::human_review_loop[]
