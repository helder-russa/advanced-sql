-- Prompt version registry table
-- tag::prompt_versions[]
CREATE TABLE IF NOT EXISTS
  `oreilly_support.prompt_versions` (
  prompt_id STRING,
  prompt_version INT64,
  description STRING,
  template_text STRING,
  created_at TIMESTAMP,
  owner STRING,
  is_active BOOL
);
-- end::prompt_versions[]

