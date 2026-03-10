# PEPTIDE RADAR — MASTER BUILD SPECIFICATION v2.0
# Last updated: 2026-03-10 | Author: Ryan Delahanty / Claude

> **For Claude Code:** Read this entire file before writing any code. When done,
> summarize: (1) the three core rules, (2) the five v1 jobs in order, (3) the
> confirmed seed source table. Do not write any code until Ryan confirms your
> summary is correct. If anything is ambiguous — stop and ask. Do not guess.

**Platform:** Databricks on Azure, Unity Catalog  
**Workspace:** adb-252904149011683.3.azuredatabricks.net  
**Catalog (new):** `peptide_radar` — separate from `elevanbio_dev`  
**Seed source:** `elevanbio_dev.bronze.peptide_database_raw` (94 rows, peptide-only)  
**LLM provider:** Anthropic API (key in Databricks Secrets, scope `peptide-radar`)  
**Alerts:** Slack (webhook in Databricks Secrets, scope `peptide-radar`)  
**Budget ceiling:** $5/month hard limit (LLM + DBU combined)  
**Philosophy:** Deterministic first, LLM last. Only process deltas. Never run
expensive logic on static data.

---

## CORE RULES — OVERRIDE EVERYTHING ELSE

1. **Deduplication is always by content hash (SHA-256). Never by source ID or URL.**
2. **Every LLM call routes through `governed_llm_call()` in `utils/cost_guard.py`. No exceptions. No direct Anthropic client calls anywhere else.**
3. **Never loop LLM calls over individual items. Always batch. One call per job run maximum.**

---

## THE PRODUCT — THREE OUTPUTS, NOTHING ELSE

Lock these before touching the pipeline. The system produces exactly:

1. **Immediate Slack alert** — same-day, for FDA bulks list category changes only. No LLM. Deterministic message.
2. **Weekly ranked watchlist** — every Friday, top peptide score movers with dimension breakdown and delta. One Haiku call maximum.
3. **Monthly deep brief** — manual trigger only, Sonnet, top 3-5 opportunities with specific action flags.

Not a dashboard. Not a knowledge base. A ranked decision list pushed to Slack.

---

## CORE LOOP — Every Job, No Exceptions

```
fetch() → content_hash() → diff_against_prior() → score_delta() → llm_only_if_threshold_crossed()
```

If an LLM call is required before the diff step, the architecture is wrong. Rebuild the diff.

---

## CONSTANTS

Store in `peptide_radar.config.job_settings` Delta table and
Databricks Secrets. Not hardcoded in Python files.

```python
DEFAULT_MODEL              = "claude-haiku-4-5-20251001"   # weekly digest — do not substitute
REASONING_MODEL            = "claude-sonnet-4-6"            # monthly brief only, manual trigger
MONTHLY_TOKEN_LIMIT        = 50_000
DAILY_TOKEN_LIMIT_PER_JOB  = 5_000
MAX_ITEMS_PER_RUN          = 100
MAX_TOKENS_PER_LLM_CALL    = 1_500
LLM_TIMEOUT_SECONDS        = 30
CIRCUIT_BREAKER_HOURLY_USD = 0.50
SCORE_ALERT_THRESHOLD      = 0.72
SCORE_DELTA_THRESHOLD      = 0.15
CONVERGENCE_THRESHOLD      = 3    # distinct source_types with signals in 30 days
```

---

## DEPLOYMENT ARCHITECTURE

**Code lives here:**
```
GitHub (private repo: 1elevan-peptide-radar)
  ↕ git push / pull
Databricks Repos (synced, read by Workflows)
  ↕ job task points at .py file
Databricks Workflows (scheduled jobs, single-node, auto-terminate)
```

**No Databricks Asset Bundles (DABs) in v1.** Plain Python scripts, Workflows
defined manually in the UI. DABs = Databricks Asset Bundles, a CLI-based
deployment framework. Add it when multi-environment deployment becomes real.

**Cluster config for all jobs:**
- Type: Job cluster (never all-purpose — those stay running and you get billed)
- Node: `Standard_DS3_v2` or smallest available
- Workers: 0 (single-node / driver-only for all v1 jobs)
- Auto-terminate: 10 minutes
- Spot instances: yes

**Running a job manually:** Trigger via Databricks Workflows UI → "Run now".
No notebooks required. The monthly brief is a Workflow job you run on-demand.

---

## PROJECT STRUCTURE

```
peptide-radar/                        ← GitHub repo root
  PEPTIDE_RADAR_SPEC.md               ← this file
  PEPTIDE_RADAR_INSTRUCTIONS.md       ← build sequence for Claude Code
  README.md
  requirements.txt
  conf/
    scoring_weights.json              ← edit weights without code change
    fda_category_mapping.json         ← raw → normalized vocabulary
    watchlist_seed.csv                ← initial 30-40 peptides with metadata
  peptide_radar/
    __init__.py
    utils/
      cost_guard.py                   ← governed_llm_call() lives here
      diff_engine.py                  ← content_hash(), diff_structured_rows()
      slack_notifier.py
    resolvers/
      entity_resolver.py              ← alias matching, no LLM
    ingestors/
      fda_bulks.py                    ← Job 1
      clinical_trials.py              ← Job 2
      pubmed_biorxiv.py               ← Job 3
      nih_reporter.py                 ← Job 4
    scoring/
      opportunity_scorer.py           ← Job 5 core logic
      convergence_detector.py
  jobs/
    job_fda_bulks.py                  ← entry point for Job 1
    job_clinical_trials.py            ← entry point for Job 2
    job_pubmed_biorxiv.py             ← entry point for Job 3
    job_nih_reporter.py               ← entry point for Job 4
    job_opportunity_scorer.py         ← entry point for Job 5
    job_monthly_brief.py              ← manual trigger only
  sql/
    01_create_catalog_schemas.sql
    02_create_bronze_tables.sql
    03_create_silver_tables.sql
    04_create_gold_tables.sql
    05_seed_watchlist.sql
  tests/
    test_entity_resolver.py
    test_diff_engine.py
    test_scorer.py
    test_cost_guard.py
```

---

## CATALOG STRUCTURE

### Why `peptide_radar` is separate from `elevanbio_dev`

Peptide Radar has its own ingestion cadence, diff logic, alias resolution,
manual-review queue, and LLM-cost policy — operationally distinct from the
core platform. Coupling an experimental external-intelligence engine to
production data contracts this early is how refactors become platform migrations.

The boundary is simple: Radar **reads** from `elevanbio_dev` (seed only,
cross-catalog SELECT). Radar **writes** one narrow summary view back to
`elevanbio_dev.gold` for platform consumption. Nothing else crosses.

No dev/prod split for `peptide_radar` until all three are true:
1. Workflows run unattended for 3-4 weeks
2. At least one downstream consumer relies on the weekly digest
3. You have had at least one schema migration you would not test live

---

## SEED SOURCE — CONFIRMED

**Primary seed:** `elevanbio_dev.bronze.peptide_database_raw`
- 94 rows, 32 columns, purpose-built peptide table
- Contains all 8 FDA PreCheck compounds (Vasopressin, Desmopressin, Oxytocin,
  Glucagon, Leuprolide, Octreotide, Bivalirudin, Liraglutide)
- Clean `col_503a_bulk_compounding_category` and `primary_therapeutic_area` fields
- `fda_approved`, `patent_status`, `orphan_drug_status` usable directly

**Cross-reference (pricing/ratings only):** `elevanbio_dev.bronze.peptide_compound_master_raw`
- 327 rows, mostly small molecules — NOT the peptide entity authority
- Use only for: `safety_rating_1_5`, `efficacy_rating_1_5`, `average_sales_price_asp`,
  `us_demand_3yr_projection` where names match a watchlist peptide
- JOIN on normalized name; treat unmatched rows as missing, not errors

**DO NOT use** `combined_cgmp_peptide_raw` as a seed. Likely a duplicate sheet
from the same Excel source. Await bronze audit results before relying on it.

---

## DELTA TABLE SCHEMAS

### Create schemas first
```sql
CREATE SCHEMA IF NOT EXISTS peptide_radar.bronze;
CREATE SCHEMA IF NOT EXISTS peptide_radar.silver;
CREATE SCHEMA IF NOT EXISTS peptide_radar.gold;
CREATE SCHEMA IF NOT EXISTS peptide_radar.config;
```

### Bronze — Raw Immutable Snapshots

```sql
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
```

### Silver — Structured, Scored, Normalized

```sql
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

-- Internal vs external data discrepancies
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
```

### Gold — LLM-Enriched (Small, Gated)

```sql
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
```

---

## FDA CATEGORY VOCABULARY

Normalized values for `silver.fda_category_mapping.normalized_status`.

| Normalized value | Covers |
|---|---|
| `approved` | 'Approved', 'Approved/503A', 'Approved/OTC/503A', 'Approved (iPLEDGE)', 'Approved/Supplement' |
| `503a_bulk` | '503A Bulk', '503A', '503A (not FDA approved)' |
| `503a_eval` | '503A Eval.' |
| `cosmetic_or_otc` | 'OTC/503A', 'OTC/Rx/503A', 'Cosmetic/503A', '503A/Cosmetic', 'GRAS/503A' |
| `supplement` | 'Supplement', 'Supplement/503A' |
| `controlled` | 'Schedule II/503A', 'Schedule III/503A', 'Schedule IV/503A' |
| `investigational` | 'Investigational/503A' |
| `unknown` | everything else → manual review |

---

## EVENT TYPES

Use exactly these strings in `silver.signals.event_type`. No free-text variants.

```
fda_category_change          fda_safety_risk_added        fda_safety_risk_removed
fda_shortage_added           fda_shortage_removed         fda_warning_letter
new_trial_registered         trial_status_change          trial_phase_advance
trial_terminated_safety      trial_terminated_business    trial_withdrawn
new_publication              new_preprint                 preprint_velocity_spike
nih_grant_awarded            supplier_catalog_add         supplier_price_change
patent_filed                 patent_abandoned             sec_commercial_intent
internal_discrepancy
```

---

## UTILITY CODE

### `utils/diff_engine.py`
```python
import hashlib

def content_hash(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode()).hexdigest()

def diff_structured_rows(old_rows, new_rows, key_field, compare_fields):
    old_by_key = {r[key_field]: r for r in old_rows}
    new_by_key = {r[key_field]: r for r in new_rows}
    all_keys = set(old_by_key) | set(new_by_key)
    inserted, deleted, changed = [], [], []
    for key in all_keys:
        if key not in old_by_key:
            inserted.append(new_by_key[key])
        elif key not in new_by_key:
            deleted.append(old_by_key[key])
        else:
            old, new = old_by_key[key], new_by_key[key]
            if any(old.get(f) != new.get(f) for f in compare_fields):
                changed.append((old, new))
    return {'inserted': inserted, 'deleted': deleted, 'changed': changed}
```

### `resolvers/entity_resolver.py`
```python
import re
from difflib import SequenceMatcher

def normalize(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r'[^\w\s\-]', '', text)
    return re.sub(r'\s+', ' ', text)

def resolve_peptide(text: str, alias_index: dict) -> list[str]:
    normalized = normalize(text)
    matches = set()
    if normalized in alias_index:
        matches.update(alias_index[normalized])
    for alias, ids in alias_index.items():
        if alias in normalized:
            matches.update(ids)
    if not matches and len(text.split()) <= 5:
        for alias, ids in alias_index.items():
            if SequenceMatcher(None, normalized, alias).ratio() >= 0.85:
                matches.update(ids)
    return list(matches)   # [] means unresolved → caller writes to manual_review_queue

def build_alias_index(alias_rows: list[dict]) -> dict:
    index = {}
    for row in alias_rows:
        key = normalize(row['alias'])
        index.setdefault(key, []).append(row['canonical_name'])
    return index
```

### `utils/cost_guard.py` — governed_llm_call() only entry point
(Full implementation in COST GUARD section above)

---

## SCORING FORMULAS

`conf/scoring_weights.json`:
```json
{"regulatory": 0.30, "evidence": 0.25, "ip": 0.20, "supply": 0.15, "fit": 0.10}
```

```python
def regulatory_score(status_503a, on_safety_risk, approved_nda, on_shortage):
    if on_safety_risk: return 0.0
    if approved_nda and not on_shortage: return 0.05
    scores = {"controlled":0.10,"approved":0.20,"cosmetic_or_otc":0.40,
              "supplement":0.40,"503a_eval":0.50,"investigational":0.60,"503a_bulk":0.85}
    return scores.get(status_503a, 0.30)

def evidence_score(human_trials, animal_studies, recent_pubs_12m, has_phase2):
    raw = min(human_trials*0.15,0.45) + min(animal_studies*0.05,0.15) + min(recent_pubs_12m*0.02,0.20)
    return min(raw + (0.20 if has_phase2 else 0), 1.0)

def ip_whitespace_score(patent_count_5yr):
    for threshold, score in [(0,1.0),(2,0.85),(5,0.65),(15,0.40)]:
        if patent_count_5yr <= threshold: return score
    return 0.15

def supply_score(supplier_count, in_catalog):
    if supplier_count == 0: return 0.0
    if in_catalog: return 1.0
    return 0.70 if supplier_count >= 2 else 0.40

def composite_score(reg, evid, ip, supply, fit, weights):
    return (reg*weights["regulatory"] + evid*weights["evidence"] +
            ip*weights["ip"] + supply*weights["supply"] + fit*weights["fit"])

def should_escalate_to_gold(composite, delta_7d, convergence_count, regulatory_change):
    return (regulatory_change or composite >= 0.72 or
            delta_7d >= 0.15 or convergence_count >= 3)
```

---

## JOB SCHEDULE

| Job | Schedule | LLM | Est. cost/run |
|---|---|---|---|
| job_fda_bulks | Mon 06:00 UTC | Never | $0.00 |
| job_clinical_trials | Tue 06:00 UTC | Never | $0.00 |
| job_pubmed_biorxiv | Wed 06:00 UTC | Never | $0.00 |
| job_nih_reporter | Thu 06:00 UTC | Never | $0.00 |
| job_opportunity_scorer | Fri 06:00 UTC | Haiku if threshold hit | $0.00–$0.15 |
| job_monthly_brief | Manual only | Sonnet | ~$0.50 |

**Estimated monthly: $0.50–$1.50 LLM + minimal DBU**

---

## NON-NEGOTIABLE RULES

```
NEVER call LLM in a for-loop over individual items
NEVER use Sonnet except job_monthly_brief
NEVER skip content_hash — it is the canonical dedup key
NEVER process more than 100 Silver rows per job run
NEVER send more than 1,500 tokens in a single LLM call
NEVER call Anthropic API directly — always through governed_llm_call()
NEVER use all-purpose clusters for scheduled jobs
NEVER enable a source without passing the 2-week dry-run signal test
NEVER touch WHO ICTRP without legal sign-off first
NEVER reference USPTO Public PAIR — retired 2022/2023; use Patent Center API
ALWAYS write to bronze.raw_snapshots before any processing
ALWAYS diff before scoring — never recompute on unchanged data
ALWAYS preserve score anatomy (5 dimensions + last_three_events)
ALWAYS route unresolved entities to manual_review_queue
ALWAYS run internal reconciliation after FDA signals
Monthly brief: confirmation input() required every single run
```

---

## DEFINITION OF DONE — v1

1. Slack alert within 30 minutes of any FDA 503A/503B list change
2. Friday digest with ranked watchlist, dimension scores, and deltas
3. Monthly brief on demand resulting in a compounding decision

Monthly cost under $5. Everything else is Phase 2.
