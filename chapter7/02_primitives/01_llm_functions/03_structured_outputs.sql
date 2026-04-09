-- A safe input view for ticket triage
-- tag::ticket_triage_input_view[]
CREATE OR REPLACE VIEW `oreilly_support.support_tickets_llm_input` AS
SELECT
  ticket_id,
  created_at,
  channel,
  product_id,
  status,
  subject,
  REGEXP_REPLACE(
    body,
    r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}',
    '[REDACTED_EMAIL]'
  ) AS body
FROM `oreilly_support.support_tickets`;
-- end::ticket_triage_input_view[]

-- Ticket triage - building `prompt_data`
-- tag::ticket_triage_structured[]
CREATE TEMP TABLE triage_prompt_data AS
SELECT
  ticket_id,
  CONCAT(
    'You are a support triage system. ',
    'Analyze the ticket and respond with ONLY valid JSON.\n\n',
    'Required fields:\n',
    '- issue_category: must be exactly one of: ',
    '"login_timeout", "performance", "notifications", "other"\n',
    '- severity: must be exactly one of: "low", "medium", "high"\n',
    '- needs_engineering: must be true or false\n',
    '- short_rationale: a brief one-sentence explanation ',
    '(keep under 100 characters)\n\n',
    'Ticket Details:\n',
    'Product ID: ',
    REGEXP_REPLACE(IFNULL(product_id, 'N/A'), r'[\n\r\t"]', ' '),
    '\n',
    'Subject: ', REGEXP_REPLACE(IFNULL(subject, 'N/A'), r'[\n\r\t"]', ' '), '\n',
    'Body: ', REGEXP_REPLACE(IFNULL(body, 'N/A'), r'[\n\r\t"]', ' '), '\n\n',
    'Respond with only the JSON object, no additional text.'
  ) AS prompt
FROM `oreilly_support.support_tickets_llm_input`
WHERE status IN ('open', 'in_progress');
-- end::ticket_triage_structured[]

-- Ticket triage - generating typed rows with `AI.GENERATE_TABLE`
-- tag::ticket_triage_generate_table[]
CREATE TEMP TABLE triage_generated AS
SELECT *
FROM AI.GENERATE_TABLE(
  MODEL `oreilly_support.gemini_flash`,
  (SELECT ticket_id, prompt FROM triage_prompt_data),
  STRUCT(
    '''issue_category STRING, severity STRING, 
    needs_engineering BOOL, short_rationale STRING''' as output_schema,
    512 AS max_output_tokens,
    0.1 AS temperature
  )
);
-- end::ticket_triage_generate_table[]

-- Ticket triage - enforcing allowlists and defaults in SQL
-- tag::ticket_triage_enforce_allowlists[]
SELECT
  ticket_id,
  IF(
    issue_category IN ('login_timeout','performance','notifications','other'),
    issue_category,
    'other'
  ) AS issue_category,
  IF(
    severity IN ('low','medium','high'),
    severity,
    'medium'
  ) AS severity,
  IFNULL(needs_engineering, FALSE) AS needs_engineering,
  SUBSTR(
    IFNULL(short_rationale, 'Error processing ticket'),
    1,
    100
  ) AS short_rationale
FROM triage_generated;
-- end::ticket_triage_enforce_allowlists[]
