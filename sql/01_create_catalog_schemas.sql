-- Peptide Radar — Phase 0, Step 1: Create catalog and schemas
-- Run in Databricks SQL Editor
-- Idempotent: safe to re-run

-- The peptide_radar catalog must already exist in Unity Catalog.
-- If not, a workspace admin must run: CREATE CATALOG IF NOT EXISTS peptide_radar;

CREATE SCHEMA IF NOT EXISTS peptide_radar.bronze;
CREATE SCHEMA IF NOT EXISTS peptide_radar.silver;
CREATE SCHEMA IF NOT EXISTS peptide_radar.gold;
CREATE SCHEMA IF NOT EXISTS peptide_radar.config;
