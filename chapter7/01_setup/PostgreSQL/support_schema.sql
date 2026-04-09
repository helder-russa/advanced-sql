-- Schema for Chapter 7 running example tables.
-- PostgreSQL SQL; adjust schema names as needed.

-- Create a schema to hold support-related tables.
CREATE SCHEMA IF NOT EXISTS oreilly_support;

-- Customer support tickets raised by users.
CREATE TABLE IF NOT EXISTS oreilly_support.support_tickets (
  ticket_id       TEXT PRIMARY KEY,
  customer_id     TEXT,
  created_at      TIMESTAMPTZ,
  channel         TEXT,   -- e.g. 'email', 'chat', 'phone'
  subject         TEXT,
  body            TEXT,
  product_id      TEXT,
  status          TEXT,   -- e.g. 'open', 'in_progress', 'resolved'
  resolution_code TEXT    -- e.g. 'known_issue', 'config_change', 'bug_fix'
);

-- Knowledge base articles used by support agents.
CREATE TABLE IF NOT EXISTS oreilly_support.support_kb_articles (
  article_id  TEXT PRIMARY KEY,
  title       TEXT,
  body        TEXT,
  product_ids TEXT[],  -- one or more related product_ids
  language    TEXT,    -- e.g. 'en', 'de', 'pt-BR'
  updated_at  TIMESTAMPTZ
);

-- Product catalog with basic metadata referenced by tickets and KB articles.
CREATE TABLE IF NOT EXISTS oreilly_support.product_catalog (
  product_id     TEXT PRIMARY KEY,
  product_name   TEXT,
  product_family TEXT,
  tier           TEXT,  -- e.g. 'free', 'standard', 'enterprise'
  is_active      BOOLEAN
);
