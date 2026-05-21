# Databricks notebook source
# Title: Peptide Radar — Full Deploy & Activate
# Run this ONCE in Databricks. It:
#   1. Syncs the repo to Databricks Repos
#   2. Seeds the watchlist (fixed names)
#   3. Seeds the FDA category mapping
#   4. Verifies counts
#   5. Creates all 5 Databricks Workflow jobs with correct schedules
#   6. Triggers Job 1 immediately to start collecting data
#
# Prerequisites:
#   - peptide_radar catalog + all schemas + all tables already created
#     (sql/01-04 already run from 2026-03-10 session)
#   - Databricks Git Folder synced to github.com/ryandelahanty/1elevan-peptide-radar
#   - Secret scope 'peptide-radar' exists
#   - This notebook running on a cluster with WRITE access to peptide_radar catalog

# COMMAND ----------
# =============================================================
# STEP 1 — Verify tables exist from prior session
# =============================================================

print("=== Verifying catalog structure ===")
schemas = spark.sql("SHOW SCHEMAS IN peptide_radar").collect()
print(f"Schemas found: {[r.databaseName for r in schemas]}")

tables_bronze = spark.sql("SHOW TABLES IN peptide_radar.bronze").collect()
tables_silver = spark.sql("SHOW TABLES IN peptide_radar.silver").collect()
tables_gold   = spark.sql("SHOW TABLES IN peptide_radar.gold").collect()

print(f"Bronze: {[r.tableName for r in tables_bronze]}")
print(f"Silver: {[r.tableName for r in tables_silver]}")
print(f"Gold:   {[r.tableName for r in tables_gold]}")

# Expected silver: peptides, peptide_aliases, fda_category_mapping,
#   regulatory_status, signals, opportunity_scores,
#   manual_review_queue, internal_discrepancies

# COMMAND ----------
# =============================================================
# STEP 2 — Seed watchlist (FIXED — corrected name mismatches)
# =============================================================

print("\n=== Seeding watchlist ===")

seed_data = [
    # (canonical_name, seed_source, strategic_fit_score, notes)
    # 8 FDA PreCheck compounds
    ('vasopressin',                        'peptide_database_raw', 0.90, 'FDA PreCheck; 503A bulk'),
    ('desmopressin',                       'peptide_database_raw', 0.88, 'FDA PreCheck; synthetic ADH analog'),
    ('oxytocin',                           'peptide_database_raw', 0.87, 'FDA PreCheck; 503A bulk'),
    ('glucagon',                           'peptide_database_raw', 0.85, 'FDA PreCheck; emergency hypoglycemia'),
    ('leuprolide',                         'peptide_database_raw', 0.83, 'FDA PreCheck; GnRH agonist'),
    ('octreotide',                         'peptide_database_raw', 0.82, 'FDA PreCheck; somatostatin analog'),
    ('bivalirudin',                        'peptide_database_raw', 0.80, 'FDA PreCheck; direct thrombin inhibitor'),
    ('liraglutide',                        'peptide_database_raw', 0.79, 'FDA PreCheck; GLP-1 agonist'),
    # High-value research peptides (corrected names)
    ('sermorelin',                         'peptide_database_raw', 0.75, 'GHRH analog'),
    ('gonadorelin',                        'peptide_database_raw', 0.72, 'GnRH; fertility'),
    ('thymosin alpha-1',                   'peptide_database_raw', 0.74, 'Immune modulation'),
    ('ipamorelin',                         'peptide_database_raw', 0.71, 'Selective GH secretagogue'),
    ('cjc-1295',                           'peptide_database_raw', 0.70, 'GHRH analog; combined w/ ipamorelin'),
    ('bpc-157',                            'peptide_database_raw', 0.68, 'Tissue repair; regulatory unclear'),
    ('tb-500 (thymosin beta-4 fragment)',   'peptide_database_raw', 0.67, 'Tissue repair; wound healing'),
    ('delta sleep inducing peptide (dsip)', 'peptide_database_raw', 0.55, 'Sleep peptide; niche'),
    ('selank',                             'peptide_database_raw', 0.60, 'Anxiolytic; growing US interest'),
    ('semax',                              'peptide_database_raw', 0.58, 'Nootropic; ACTH analog; intranasal'),
    ('epithalon',                          'peptide_database_raw', 0.62, 'Telomerase activator; longevity'),
    ('mots-c (mitochondrial orf peptide)', 'peptide_database_raw', 0.65, 'Mitochondrial; metabolic; longevity'),
    ('collagen tripeptide ghk-cu',         'peptide_database_raw', 0.58, 'Copper peptide; skin/wound'),
    ('kpv (lys-pro-val)',                  'peptide_database_raw', 0.60, 'Anti-inflammatory; IBD; skin'),
    ('kisspeptin-54 / kisspeptin-10',      'peptide_database_raw', 0.63, 'Reproductive endocrinology'),
    ('aod-9604',                           'peptide_database_raw', 0.66, 'Lipolytic HGH fragment'),
    ('melanotan ii',                       'peptide_database_raw', 0.50, 'Melanocyte; regulatory risk'),
    ('bremelanotide',                      'peptide_database_raw', 0.55, 'PT-141; FDA-approved; HSDD'),
    ('ll-37 (cathelicidin)',               'peptide_database_raw', 0.62, 'Antimicrobial; wound care'),
    ('secretin (human)',                   'peptide_database_raw', 0.52, 'GI peptide; diagnostic'),
    ('somatostatin',                       'peptide_database_raw', 0.60, 'Hormone inhibitor; oncology'),
    ('terlipressin',                       'peptide_database_raw', 0.65, 'Vasopressin analog; hepatorenal'),
    ('cetrorelix',                         'peptide_database_raw', 0.58, 'GnRH antagonist; fertility'),
    ('ganirelix',                          'peptide_database_raw', 0.57, 'GnRH antagonist; fertility'),
    ('alarelin',                           'peptide_database_raw', 0.54, 'GnRH analog; veterinary'),
    ('hexarelin',                          'peptide_database_raw', 0.60, 'GH secretagogue; cardiac'),
    ('tesamorelin',                        'peptide_database_raw', 0.69, 'FDA-approved GHRH; lipodystrophy'),
]

import hashlib
from datetime import datetime, timezone

def peptide_id(name: str) -> str:
    return hashlib.md5(name.encode()).hexdigest()

now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

rows = [{
    "peptide_id":          peptide_id(name),
    "canonical_name":      name,
    "seed_source":         src,
    "strategic_fit_score": float(score),
    "watchlist_active":    True,
    "last_updated":        now_ts,
    "notes":               notes,
} for name, src, score, notes in seed_data]

seed_df = spark.createDataFrame(rows)

# MERGE (idempotent re-runs)
seed_df.createOrReplaceTempView("_seed_tmp")

spark.sql("""
MERGE INTO peptide_radar.silver.peptides AS target
USING _seed_tmp AS source
ON target.canonical_name = source.canonical_name
WHEN MATCHED THEN UPDATE SET
  target.seed_source         = source.seed_source,
  target.strategic_fit_score = source.strategic_fit_score,
  target.watchlist_active    = TRUE,
  target.last_updated        = source.last_updated,
  target.notes               = source.notes
WHEN NOT MATCHED THEN INSERT *
""")

count = spark.sql("SELECT COUNT(*) AS n FROM peptide_radar.silver.peptides").first()["n"]
print(f"Peptides in watchlist: {count}  (expected 36)")
assert count == 36, f"Seed count wrong: {count}"

# COMMAND ----------
# =============================================================
# STEP 3 — Seed FDA category mapping
# =============================================================

print("\n=== Seeding FDA category mapping ===")

mapping_data = [
    ('Approved',              'approved'),
    ('Approved/503A',         'approved'),
    ('Approved/OTC/503A',     'approved'),
    ('Approved (iPLEDGE)',    'approved'),
    ('Approved/Supplement',   'approved'),
    ('503A Bulk',             '503a_bulk'),
    ('503A',                  '503a_bulk'),
    ('503A (not FDA approved)','503a_bulk'),
    ('503A Eval.',            '503a_eval'),
    ('OTC/503A',              'cosmetic_or_otc'),
    ('OTC/Rx/503A',           'cosmetic_or_otc'),
    ('Cosmetic/503A',         'cosmetic_or_otc'),
    ('503A/Cosmetic',         'cosmetic_or_otc'),
    ('GRAS/503A',             'cosmetic_or_otc'),
    ('Supplement',            'supplement'),
    ('Supplement/503A',       'supplement'),
    ('Schedule II/503A',      'controlled'),
    ('Schedule III/503A',     'controlled'),
    ('Schedule IV/503A',      'controlled'),
    ('Investigational/503A',  'investigational'),
]

map_rows = [{
    "raw_value":         raw,
    "normalized_status": norm,
    "confidence":        "high",
    "mapping_version":   "v1",
    "notes":             None,
} for raw, norm in mapping_data]

map_df = spark.createDataFrame(map_rows)
map_df.createOrReplaceTempView("_map_tmp")

spark.sql("""
MERGE INTO peptide_radar.silver.fda_category_mapping AS target
USING _map_tmp AS source
ON target.raw_value = source.raw_value
WHEN MATCHED THEN UPDATE SET
  target.normalized_status = source.normalized_status,
  target.confidence        = source.confidence,
  target.mapping_version   = source.mapping_version
WHEN NOT MATCHED THEN INSERT *
""")

map_count = spark.sql(
    "SELECT COUNT(*) AS n FROM peptide_radar.silver.fda_category_mapping"
).first()["n"]
print(f"FDA mapping rows: {map_count}  (expected 20)")
assert map_count == 20, f"Mapping count wrong: {map_count}"

# COMMAND ----------
# =============================================================
# STEP 4 — Create Databricks Workflow jobs via REST API
# =============================================================
# Uses the workspace token — no personal API key needed.
# Adjust REPO_PATH below to match your Databricks Git Folder path.

import json, urllib.request, urllib.error

WORKSPACE_HOST = spark.conf.get(
    "spark.databricks.workspaceUrl",
    "adb-252904149011683.3.azuredatabricks.net"
)
if not WORKSPACE_HOST.startswith("https://"):
    WORKSPACE_HOST = "https://" + WORKSPACE_HOST
WORKSPACE_HOST = WORKSPACE_HOST.rstrip("/")

TOKEN = spark.conf.get("spark.databricks.token")

# Path where the Git Folder is synced in Databricks Repos
REPO_PATH = "/Workspace/Users/ryan.delahanty@1elevan.com/1elevan-peptide-radar"

CLUSTER_SPEC = {
    "num_workers": 0,
    "spark_version": "15.4.x-scala2.12",   # adjust to current LTS runtime
    "node_type_id":  "Standard_DS3_v2",
    "driver_node_type_id": "Standard_DS3_v2",
    "autotermination_minutes": 10,
    "aws_attributes": {},
    "azure_attributes": {
        "first_on_demand": 1,
        "availability":    "SPOT_WITH_FALLBACK_AZURE",
    },
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
    },
}

JOB_DEFINITIONS = [
    {
        "name": "peptide_radar_job1_fda_bulks",
        "schedule": {"quartz_cron_expression": "0 0 6 ? * MON", "timezone_id": "UTC"},
        "script": f"{REPO_PATH}/jobs/job_fda_bulks.py",
        "description": "Job 1: FDA 503A/503B bulk list differ. No LLM. $0/run.",
    },
    {
        "name": "peptide_radar_job2_clinical_trials",
        "schedule": {"quartz_cron_expression": "0 0 6 ? * TUE", "timezone_id": "UTC"},
        "script": f"{REPO_PATH}/jobs/job_clinical_trials.py",
        "description": "Job 2: ClinicalTrials.gov poller. No LLM. $0/run.",
    },
    {
        "name": "peptide_radar_job3_pubmed_biorxiv",
        "schedule": {"quartz_cron_expression": "0 0 6 ? * WED", "timezone_id": "UTC"},
        "script": f"{REPO_PATH}/jobs/job_pubmed_biorxiv.py",
        "description": "Job 3: PubMed + bioRxiv harvester. No LLM. $0/run.",
    },
    {
        "name": "peptide_radar_job4_nih_reporter",
        "schedule": {"quartz_cron_expression": "0 0 6 ? * THU", "timezone_id": "UTC"},
        "script": f"{REPO_PATH}/jobs/job_nih_reporter.py",
        "description": "Job 4: NIH RePORTER monitor. No LLM. $0/run.",
    },
    {
        "name": "peptide_radar_job5_opportunity_scorer",
        "schedule": {"quartz_cron_expression": "0 0 6 ? * FRI", "timezone_id": "UTC"},
        "script": f"{REPO_PATH}/jobs/job_opportunity_scorer.py",
        "description": "Job 5: Scorer + weekly digest. One Haiku call if threshold hit. ~$0.15 max/run.",
    },
]


def jobs_api(method: str, path: str, body: dict = None):
    url  = f"{WORKSPACE_HOST}/api/2.1/jobs/{path}"
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(
        url, data=data,
        headers={"Authorization": f"Bearer {TOKEN}",
                 "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Jobs API {method} {path}: {e.code} {e.read().decode()}")


print("\n=== Creating Databricks Workflow jobs ===")

# Check for existing jobs to avoid duplicates
existing = jobs_api("GET", "list").get("jobs", [])
existing_names = {j["settings"]["name"]: j["job_id"] for j in existing}

created_job_ids = {}

for jd in JOB_DEFINITIONS:
    job_payload = {
        "name": jd["name"],
        "description": jd["description"],
        "schedule": {**jd["schedule"], "pause_status": "UNPAUSED"},
        "tasks": [{
            "task_key": "main",
            "python_script_task": {
                "python_file": jd["script"],
            },
            "new_cluster": CLUSTER_SPEC,
            "timeout_seconds": 900,
            "max_retries": 1,
            "min_retry_interval_millis": 300_000,
        }],
        "max_concurrent_runs": 1,
        "tags": {"project": "peptide_radar", "version": "v1"},
    }

    if jd["name"] in existing_names:
        # Update existing
        job_id = existing_names[jd["name"]]
        jobs_api("POST", "update", {
            "job_id": job_id,
            "new_settings": job_payload,
        })
        print(f"  Updated job: {jd['name']} (id={job_id})")
    else:
        resp   = jobs_api("POST", "create", job_payload)
        job_id = resp["job_id"]
        print(f"  Created job: {jd['name']} (id={job_id})")

    created_job_ids[jd["name"]] = job_id

print(f"\nAll 5 jobs configured. IDs: {created_job_ids}")

# COMMAND ----------
# =============================================================
# STEP 5 — Trigger Job 1 immediately to start collecting data
# =============================================================

print("\n=== Triggering Job 1 (FDA Bulks) to run NOW ===")

job1_id = created_job_ids.get("peptide_radar_job1_fda_bulks")
if job1_id:
    run_resp = jobs_api("POST", "run-now", {"job_id": job1_id})
    run_id   = run_resp.get("run_id")
    print(f"Job 1 triggered. run_id={run_id}")
    print(f"Watch at: {WORKSPACE_HOST}/#job/{job1_id}/run/{run_id}")
else:
    print("Job 1 ID not found — trigger manually in Workflows UI.")

# COMMAND ----------
# =============================================================
# STEP 6 — Final verification
# =============================================================

print("\n=== Final verification ===")

spark.sql("""
    SELECT 'peptides'            AS tbl, COUNT(*) AS rows FROM peptide_radar.silver.peptides
    UNION ALL
    SELECT 'fda_category_mapping',     COUNT(*)          FROM peptide_radar.silver.fda_category_mapping
    UNION ALL
    SELECT 'signals',                  COUNT(*)          FROM peptide_radar.silver.signals
    UNION ALL
    SELECT 'llm_costs',                COUNT(*)          FROM peptide_radar.gold.llm_costs
""").show()

print("""
=== DEPLOYMENT COMPLETE ===

Jobs scheduled (all UTC, job clusters, spot, auto-terminate 10 min):
  Monday    06:00  job_fda_bulks          (running NOW — first snapshot baseline)
  Tuesday   06:00  job_clinical_trials
  Wednesday 06:00  job_pubmed_biorxiv
  Thursday  06:00  job_nih_reporter
  Friday    06:00  job_opportunity_scorer + weekly digest

Cost: $0/run for Jobs 1-4. ~$0.15 max for Job 5 if threshold hit.
      All LLM calls via Databricks Model Serving — burns DBU, not API credits.

Next step: Get Teams webhook URL from Scott Kalcic.
  Then: databricks secrets put-secret peptide-radar TEAMS_WEBHOOK_URL <url>
""")
