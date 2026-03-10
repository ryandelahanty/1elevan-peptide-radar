-- Peptide Radar — Phase 0, Step 2a: Bronze tables
-- Run in Databricks SQL Editor
-- Idempotent: safe to re-run

CREATE TABLE IF NOT EXISTS peptide_radar.bronze.raw_snapshots (
  snapshot_id     STRING NOT NULL,   -- SHA-256 of raw_content
  source          STRING NOT NULL,   -- 'fda_503a', 'fda_503b', 'ct_gov', 'pubmed', etc.
  source_url      STRING,
  raw_content     BINARY,
  content_type    STRING,            -- 'pdf', 'json', 'html', 'xml'
  fetch_timestamp TIMESTAMP NOT NULL,
  prior_hash      STRING,
  changed         BOOLEAN NOT NULL,  -- snapshot_id != prior_hash
  http_status     INT,
  parser_version  STRING
) USING DELTA
PARTITIONED BY (source)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
