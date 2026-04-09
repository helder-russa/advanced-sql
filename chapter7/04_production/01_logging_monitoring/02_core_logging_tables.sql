-- Core logging table for LLM calls
-- tag::llm_calls[]
CREATE TABLE IF NOT EXISTS
  `oreilly_support.llm_calls` (
  run_id STRING,
  parent_run_id STRING,
  created_at TIMESTAMP,
  prompt_id STRING,
  prompt_version INT64,
  model STRING,
  params_json STRING,
  input_hash STRING,
  retrieval_run_id STRING,
  tokens_in INT64,
  tokens_out INT64,
  latency_ms INT64,
  status STRING,
  output_text STRING
);
-- end::llm_calls[]

-- Core logging table for retrieval runs
-- tag::retrieval_runs[]
CREATE TABLE IF NOT EXISTS
  `oreilly_support.retrieval_runs` (
  retrieval_run_id STRING,
  created_at TIMESTAMP,
  retrieval_config_id STRING,
  embedding_model STRING,
  embedding_version STRING,
  query_hash STRING,
  retrieved ARRAY<STRUCT<
    chunk_id STRING,
    distance FLOAT64
  >>
);
-- end::retrieval_runs[]

