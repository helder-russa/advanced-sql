
-- Schema for Chapter 7 running example tables.
-- BigQuery-style SQL; adjust dataset names as needed.

-- Create a dataset to hold support-related tables.
CREATE SCHEMA IF NOT EXISTS oreilly_support;

-- Customer support tickets raised by users.
CREATE OR REPLACE TABLE oreilly_support.support_tickets (
  ticket_id        STRING NOT NULL,
  customer_id      STRING,
  created_at       TIMESTAMP,
  channel          STRING,   -- e.g. 'email', 'chat', 'phone'
  subject          STRING,
  body             STRING,
  product_id       STRING,
  status           STRING,   -- e.g. 'open', 'in_progress', 'resolved'
  resolution_code  STRING    -- e.g. 'known_issue', 'config_change', 'bug_fix'
);

-- Knowledge base articles used by support agents.
CREATE OR REPLACE TABLE oreilly_support.support_kb_articles (
  article_id   STRING NOT NULL,
  title        STRING,
  body         STRING,
  product_ids  ARRAY<STRING>,  -- one or more related product_ids
  language     STRING,         -- e.g. 'en', 'de', 'pt-BR'
  updated_at   TIMESTAMP
);

-- Product catalog with basic metadata referenced by tickets and KB articles.
CREATE OR REPLACE TABLE oreilly_support.product_catalog (
  product_id      STRING NOT NULL,
  product_name    STRING,
  product_family  STRING,
  tier            STRING,  -- e.g. 'free', 'standard', 'enterprise'
  is_active       BOOL
);
----

