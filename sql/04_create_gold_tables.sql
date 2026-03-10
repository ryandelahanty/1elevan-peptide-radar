-- Peptide Radar — Phase 0, Step 2c: Gold tables
-- Run in Databricks SQL Editor
-- Idempotent: safe to re-run

CREATE TABLE IF NOT EXISTS peptide_radar.gold.weekly_digest_items (
  peptide_id            STRING NOT NULL,
  digest_week           DATE NOT NULL,
  canonical_name        STRING,
  composite_score       FLOAT,
  score_delta_7d        FLOAT,
  regulatory_status     STRING,
  top_signal_summary    STRING,   -- 2-3 sentences from Haiku
  last_three_events     STRING,   -- JSON — preserve anatomy
  tokens_in             INT,
  tokens_out            INT,
  cost_usd              FLOAT
) USING DELTA;

CREATE TABLE IF NOT EXISTS peptide_radar.gold.monthly_brief (
  brief_month     DATE NOT NULL,
  top_peptide_ids ARRAY<STRING>,
  synthesis       STRING,
  triggered_by    STRING,
  tokens_in       INT,
  tokens_out      INT,
  cost_usd        FLOAT,
  created_at      TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

CREATE TABLE IF NOT EXISTS peptide_radar.gold.llm_costs (
  call_id        STRING NOT NULL,
  job_name       STRING,
  model          STRING,
  tokens_in      INT,
  tokens_out     INT,
  cost_usd       FLOAT,
  triggered_by   STRING,
  call_timestamp TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;
