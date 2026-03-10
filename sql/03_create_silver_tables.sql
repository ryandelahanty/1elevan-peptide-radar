-- Peptide Radar — Phase 0, Step 2b: Silver tables
-- Run in Databricks SQL Editor
-- Idempotent: safe to re-run

CREATE TABLE IF NOT EXISTS peptide_radar.silver.peptides (
  peptide_id          STRING NOT NULL,
  canonical_name      STRING NOT NULL,   -- lowercase: 'vasopressin', 'mots-c'
  aliases             ARRAY<STRING>,
  mechanism           STRING,
  indication_tags     ARRAY<STRING>,
  strategic_fit_score FLOAT,
  watchlist_active    BOOLEAN DEFAULT TRUE,
  seed_source         STRING,            -- 'peptide_database_raw' or 'manual'
  last_updated        TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS peptide_radar.silver.peptide_aliases (
  alias_id          STRING NOT NULL,
  peptide_id        STRING NOT NULL,
  canonical_name    STRING NOT NULL,
  alias             STRING NOT NULL,
  alias_type        STRING,    -- 'code_name','chemical_name','spelling_variant','expanded_name'
  source_confidence STRING,    -- 'high', 'medium', 'low'
  added_by          STRING,    -- 'seed', 'manual_review', 'resolver'
  added_at          TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS peptide_radar.silver.fda_category_mapping (
  raw_value          STRING NOT NULL,
  normalized_status  STRING NOT NULL,
  confidence         STRING,
  mapping_version    STRING,
  notes              STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS peptide_radar.silver.regulatory_status (
  peptide_id              STRING NOT NULL,
  status_503a             STRING,
  status_503b             STRING,
  on_safety_risk_list     BOOLEAN DEFAULT FALSE,
  approved_drug_nda       STRING,
  on_shortage_list        BOOLEAN DEFAULT FALSE,
  source_snapshot_id      STRING,
  effective_date          DATE,
  last_checked_timestamp  TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS peptide_radar.silver.signals (
  signal_id       STRING NOT NULL,
  peptide_id      STRING,           -- NULL if unresolved
  event_date      DATE NOT NULL,
  source_type     STRING NOT NULL,
  event_type      STRING NOT NULL,
  event_value     STRING,           -- JSON
  event_direction STRING,           -- 'positive', 'negative', 'neutral'
  severity        STRING,           -- 'critical', 'high', 'medium', 'low'
  raw_ref         STRING,
  source_hash     STRING,
  sent_to_gold    BOOLEAN DEFAULT FALSE,
  signal_date     TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
PARTITIONED BY (source_type, event_date);

CREATE TABLE IF NOT EXISTS peptide_radar.silver.opportunity_scores (
  peptide_id              STRING NOT NULL,
  score_date              DATE NOT NULL,
  regulatory_score        FLOAT,
  evidence_score          FLOAT,
  ip_whitespace_score     FLOAT,
  supply_score            FLOAT,
  strategic_fit_score     FLOAT,
  composite_score         FLOAT,
  score_delta_7d          FLOAT,
  score_delta_30d         FLOAT,
  convergence_count_30d   INT,
  last_three_events       STRING,   -- JSON array of signal_ids
  alert_threshold_hit     BOOLEAN DEFAULT FALSE,
  regulatory_change       BOOLEAN DEFAULT FALSE
) USING DELTA;

CREATE TABLE IF NOT EXISTS peptide_radar.silver.manual_review_queue (
  review_id         STRING NOT NULL,
  raw_text          STRING NOT NULL,
  source_type       STRING,
  source_ref        STRING,
  candidate_aliases ARRAY<STRING>,
  status            STRING DEFAULT 'pending',
  resolved_to       STRING,
  reviewer_note     STRING,
  created_at        TIMESTAMP DEFAULT current_timestamp(),
  reviewed_at       TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS peptide_radar.silver.internal_discrepancies (
  discrepancy_id    STRING NOT NULL,
  peptide_id        STRING,
  peptide_name      STRING,
  field_name        STRING,        -- 'fda_category', 'patent_status', etc.
  internal_value    STRING,        -- from elevanbio_dev bronze
  external_value    STRING,        -- from external source
  external_source   STRING,
  severity          STRING,
  detected_at       TIMESTAMP DEFAULT current_timestamp(),
  reviewed          BOOLEAN DEFAULT FALSE
) USING DELTA;
