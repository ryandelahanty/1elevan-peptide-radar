# PEPTIDE RADAR — BUILD INSTRUCTIONS v2.0
## Exactly what to type, in exactly this order

---

## BEFORE YOU OPEN CLAUDE CODE

Confirm these are true:

- [ ] GitHub repo `1elevan-peptide-radar` exists (private)
- [ ] Databricks Repos is connected to that GitHub repo
- [ ] Databricks Secrets scope `peptide-radar` exists with keys:
      `ANTHROPIC_API_KEY` and `SLACK_WEBHOOK_URL`
- [ ] Catalog `peptide_radar` has been created in Unity Catalog
- [ ] `PEPTIDE_RADAR_SPEC_v2.md` is in the repo root
- [ ] Python 3.10+ is available in the Databricks runtime

---

## HOW TO START EVERY CLAUDE CODE SESSION

Type this first. Every time. No exceptions:

```
Read PEPTIDE_RADAR_SPEC_v2.md. Summarize:
(1) the three core rules that override everything,
(2) the five v1 jobs in build order with their day/time schedule,
(3) the confirmed seed source table and why it was chosen.
Do not write any code until I confirm your summary is correct.
```

Wait. Read the summary. Correct anything wrong before proceeding.

---

## PHASE 0 — INFRASTRUCTURE
### Build these in order. One at a time. Do not skip ahead.

---

### STEP 1 — sql/01_create_catalog_schemas.sql

Type:
```
Build sql/01_create_catalog_schemas.sql.
Create the four schemas from SPEC:
  peptide_radar.bronze
  peptide_radar.silver
  peptide_radar.gold
  peptide_radar.config
Show me the complete file.
```

**Run in Databricks SQL Editor. Verify:**
```sql
SHOW SCHEMAS IN peptide_radar;
-- Expected: bronze, config, default, gold, information_schema, silver
```

If it passes: **"Schemas confirmed. Build 02_create_bronze_tables.sql."**

---

### STEP 2 — sql/02_create_bronze_tables.sql through 04_create_gold_tables.sql

Type:
```
Build sql/02_create_bronze_tables.sql, 03_create_silver_tables.sql,
and 04_create_gold_tables.sql using the exact schemas from SPEC_v2.
Include all tables in the spec, in the order listed.
Show me each file before moving to the next.
```

**Verify after each:**
```sql
SHOW TABLES IN peptide_radar.bronze;
SHOW TABLES IN peptide_radar.silver;
SHOW TABLES IN peptide_radar.gold;
```

Expected silver tables: peptides, peptide_aliases, fda_category_mapping,
regulatory_status, signals, opportunity_scores, manual_review_queue,
internal_discrepancies.

If all pass: **"All tables confirmed. Build sql/05_seed_watchlist.sql."**

---

### STEP 3 — sql/05_seed_watchlist.sql + conf/watchlist_seed.csv

Type:
```
Build conf/watchlist_seed.csv with 30-40 peptides.
Seed source is elevanbio_dev.bronze.peptide_database_raw (94 rows).
Include all 8 FDA PreCheck compounds:
  Vasopressin, Desmopressin, Oxytocin, Glucagon, Leuprolide,
  Octreotide, Bivalirudin, Liraglutide
Plus the highest-value research peptides from peptide_database_raw:
  Sermorelin, Gonadorelin, Thymosin Alpha-1, Ipamorelin, CJC-1295,
  BPC-157, TB-500, DSIP, Selank, Semax, Epithalon, MOTS-c,
  GHK-Cu, KPV, Kisspeptin, AOD-9604, Melanotan II, Bremelanotide

CSV columns:
  peptide_id, canonical_name, seed_source, strategic_fit_score, notes

Then build sql/05_seed_watchlist.sql to load this CSV into
peptide_radar.silver.peptides.
Show me both files.
```

**Also build conf/fda_category_mapping.json:**
```
Build conf/fda_category_mapping.json using the exact vocabulary
table from SPEC_v2. Include all 8 normalized values and all raw
source strings. This file loads into peptide_radar.silver.fda_category_mapping.
Show me the file.
```

**Verify:**
```sql
SELECT COUNT(*) FROM peptide_radar.silver.peptides;
-- Expected: 30-40
SELECT COUNT(*) FROM peptide_radar.silver.fda_category_mapping;
-- Expected: 8
```

If it passes: **"Seed data confirmed. Build entity_resolver.py."**

---

### STEP 4 — peptide_radar/resolvers/entity_resolver.py

Type:
```
Build peptide_radar/resolvers/entity_resolver.py using the exact
code in SPEC_v2. Copy it precisely — normalize(), resolve_peptide(),
build_alias_index(). Do not add extra logic.
Show me the complete file.
```

**Test:**
```python
from peptide_radar.resolvers.entity_resolver import resolve_peptide, build_alias_index, normalize

# Minimal test alias index
test_aliases = [
    {"alias": "PNB-0408", "canonical_name": "dihexa"},
    {"alias": "MOTS c", "canonical_name": "mots-c"},
    {"alias": "vasopressin", "canonical_name": "vasopressin"},
]
index = build_alias_index(test_aliases)

print(resolve_peptide("PNB-0408", index))       # Expected: ['dihexa']
print(resolve_peptide("MOTS c study", index))   # Expected: ['mots-c']
print(resolve_peptide("XQ-2847 novel", index))  # Expected: []
```

If it passes: **"Entity resolver confirmed. Build diff_engine.py."**

---

### STEP 5 — peptide_radar/utils/diff_engine.py

Type:
```
Build peptide_radar/utils/diff_engine.py using the exact code
in SPEC_v2: content_hash() and diff_structured_rows().
Show me the complete file.
```

**Test:**
```python
from peptide_radar.utils.diff_engine import content_hash, diff_structured_rows

# Hash normalization
assert content_hash("Vasopressin") == content_hash("vasopressin ")
print("Hash normalization: PASS")

# Diff logic
old = [{"name": "vasopressin", "category": "503a_bulk"},
       {"name": "octreotide",  "category": "approved"}]
new = [{"name": "vasopressin", "category": "approved"},     # changed
       {"name": "octreotide",  "category": "approved"},
       {"name": "sermorelin",  "category": "503a_bulk"}]    # inserted

result = diff_structured_rows(old, new, "name", ["category"])
assert result["changed"][0][1]["name"] == "vasopressin"
assert result["inserted"][0]["name"] == "sermorelin"
assert result["deleted"] == []
print("Diff logic: PASS")
```

If it passes: **"Diff engine confirmed. Build cost_guard.py."**

---

### STEP 6 — peptide_radar/utils/cost_guard.py

Type:
```
Build peptide_radar/utils/cost_guard.py.
governed_llm_call() is the ONLY entry point for LLM calls.
It must implement all four guards in order:
  1. check_circuit_breaker() — reads peptide_radar.gold.llm_costs
  2. Prompt size check — reject if > MAX_TOKENS_PER_LLM_CALL * 4
  3. Monthly ceiling — reject if monthly total + estimated > MONTHLY_TOKEN_LIMIT
  4. Daily job ceiling — reject if daily job total + estimated > DAILY_TOKEN_LIMIT_PER_JOB
Get ANTHROPIC_API_KEY from: dbutils.secrets.get("peptide-radar", "ANTHROPIC_API_KEY")
Log every call to peptide_radar.gold.llm_costs.
Return None (not raise) if any guard fires — callers must handle None gracefully.
Show me the complete file.
```

**Test (run in Databricks notebook):**
```python
import os
os.environ["ANTHROPIC_API_KEY"] = dbutils.secrets.get("peptide-radar", "ANTHROPIC_API_KEY")

from peptide_radar.utils.cost_guard import governed_llm_call, check_circuit_breaker

check_circuit_breaker()  # Should not raise
print("Circuit breaker: OK")

result = governed_llm_call("Say CONFIRMED and nothing else.", "test_job")
print("Result:", result)  # Expected: "CONFIRMED"

# Verify logged
spark.sql("SELECT * FROM peptide_radar.gold.llm_costs ORDER BY call_timestamp DESC LIMIT 1").show()
```

If it passes: **"cost_guard.py confirmed. Build slack_notifier.py."**

---

### STEP 7 — peptide_radar/utils/slack_notifier.py

Type:
```
Build peptide_radar/utils/slack_notifier.py with two functions:

send_alert(title, message, severity)
  severity: 'critical' (red), 'high' (orange), 'medium' (blue)
  Webhook from: dbutils.secrets.get("peptide-radar", "SLACK_WEBHOOK_URL")
  Fail silently — log error, never raise. A Slack outage must not
  crash a job.

send_digest(text)
  Posts the weekly digest string to the same webhook.
  Same fail-silent requirement.

Show me the complete file.
```

**Test:**
```python
from peptide_radar.utils.slack_notifier import send_alert
send_alert("Infrastructure Test", "Peptide Radar build — Phase 0 test. Ignore.", "medium")
```

Check Slack. If message arrives: **"Phase 0 complete."**

---

## PHASE 1 — Job 1: FDA Bulks Differ

**Build and fully test this before touching any other job.**
**This job must have zero LLM calls. All alerts are deterministic.**

Type:
```
Build peptide_radar/ingestors/fda_bulks.py and jobs/job_fda_bulks.py.

fda_bulks.py must handle three sources:
  503A list:     https://www.fda.gov/media/94155/download
  503B list:     https://www.fda.gov/drugs/human-drug-compounding/503b-bulk-drug-substances-list
  Safety risks:  https://www.fda.gov/drugs/human-drug-compounding/certain-bulk-drug-substances-use-compounding-may-present-significant-safety-risks

For each source:
  1. Fetch content
  2. Compute content_hash
  3. Compare to most recent snapshot_id in peptide_radar.bronze.raw_snapshots
     for that source
  4. If unchanged: log "no change", return immediately — zero further processing
  5. If changed:
     a. Write new snapshot to bronze.raw_snapshots (changed=True)
     b. Parse into structured rows: {peptide_name, category, status_text, effective_date}
     c. Load prior parsed rows from peptide_radar.silver.signals
        (most recent for this source_type)
     d. Run diff_structured_rows() on key_field="peptide_name",
        compare_fields=["category", "status_text"]
     e. Apply entity_resolver to each peptide_name
     f. Unresolved names → peptide_radar.silver.manual_review_queue
     g. Write delta rows to peptide_radar.silver.signals with correct event_type
     h. Cross-check against elevanbio_dev.bronze.peptide_database_raw —
        if FDA external category conflicts with internal fda_approved field,
        write to peptide_radar.silver.internal_discrepancies

job_fda_bulks.py:
  - Call send_alert() for event_type IN ('fda_category_change', 'fda_safety_risk_added')
  - NO governed_llm_call() anywhere in this file
  - Log run summary to console

Show me both complete files.
```

**Test sequence:**
```python
# First run — no prior snapshot, should store baseline
from jobs.job_fda_bulks import run
run()

# Verify bronze snapshot written
spark.sql("""
  SELECT source, changed, fetch_timestamp
  FROM peptide_radar.bronze.raw_snapshots
  ORDER BY fetch_timestamp DESC LIMIT 5
""").show()

# Second run — same content, should exit immediately
run()
# Verify: no new signals written, no LLM calls, cost = $0

spark.sql("SELECT COUNT(*) FROM peptide_radar.gold.llm_costs WHERE job_name='job_fda_bulks'").show()
# Expected: 0 rows
```

Schedule as Databricks Workflow: **Monday 06:00 UTC, job cluster, auto-terminate 10 min.**

Say: **"FDA bulks confirmed. Build job_clinical_trials."**

---

## PHASE 1 — Job 2: ClinicalTrials Poller

Type:
```
Build peptide_radar/ingestors/clinical_trials.py and jobs/job_clinical_trials.py.

API: https://clinicaltrials.gov/api/v2/studies
Params:
  filter.phase=PHASE1,PHASE2
  filter.status=RECRUITING,ACTIVE_NOT_RECRUITING,TERMINATED,WITHDRAWN
  query.intr=peptide
  fields=nctId,briefTitle,conditions,interventions,phase,overallStatus,
         whyStopped,primaryCompletionDate,lastUpdatePosted
  pageSize=100

Dedup rule: content_hash(nctId + lastUpdatePosted)
Only process rows where that hash has not been seen before.

For TERMINATED or WITHDRAWN: parse whyStopped field.
If it contains 'business', 'enrollment', 'funding', or 'financial':
  event_type = 'trial_terminated_business'   (orphaned molecule signal)
Otherwise:
  event_type = 'trial_terminated_safety'     (negative signal)

Apply entity_resolver to briefTitle + conditions text.
Unresolved → manual_review_queue.
No LLM. No alerts.

Show me both complete files.
```

Schedule: **Tuesday 06:00 UTC.**

Say: **"ClinicalTrials confirmed. Build job_pubmed_biorxiv."**

---

## PHASE 1 — Job 3: PubMed + bioRxiv Harvester

Type:
```
Build peptide_radar/ingestors/pubmed_biorxiv.py and jobs/job_pubmed_biorxiv.py.

PubMed (NCBI E-utilities API, no auth required for <3 req/sec):
  Base: https://eutils.ncbi.nlm.nih.gov/entrez/eutils/
  Query: ("peptide"[MeSH] OR "therapeutic peptides"[TW]) AND
         ("SLC6A1" OR "GABA transporter" OR "intranasal delivery" OR
          "CNS peptide" OR "mitochondrial peptide" OR "longevity peptide")
  Filter: last 90 days, humans or clinical trial publication type
  Fields: PMID, title, abstract, authors, journal, pub_date

bioRxiv (public API):
  https://api.biorxiv.org/details/biorxiv/[start_date]/[end_date]/
  Categories: pharmacology, neuroscience
  Filter client-side: title/abstract contains watchlist peptide name

DEDUP: content_hash(title + first_author + pub_year)
PMID and DOI are NOT dedup keys — same paper can have different IDs
across sources.

Only write to silver.signals if:
  - Paper is Phase I/II human study, OR
  - Preprint velocity ratio > 0.4 for that peptide in last 90 days

Store velocity ratio in event_value JSON:
  {"preprint_count": N, "peer_reviewed_count": M, "velocity_ratio": R}

No LLM. No alerts.

Show me both complete files.
```

Schedule: **Wednesday 06:00 UTC.**

Say: **"PubMed/bioRxiv confirmed. Build job_nih_reporter."**

---

## PHASE 1 — Job 4: NIH RePORTER Monitor

Type:
```
Build peptide_radar/ingestors/nih_reporter.py and jobs/job_nih_reporter.py.

API: https://api.reporter.nih.gov/v2/projects/search
Query body:
  {
    "criteria": {
      "advanced_text_search": {
        "operator": "and",
        "search_field": "all",
        "search_text": "peptide GABAergic SLC6A1 intranasal CNS delivery longevity mitochondrial"
      },
      "activity_codes": ["R01", "R21", "SBIR", "STTR"]
    },
    "offset": 0,
    "limit": 50,
    "sort_field": "project_start_date",
    "sort_order": "desc"
  }

Dedup: content_hash(project_num + fiscal_year)
event_type = 'nih_grant_awarded'
SBIR/STTR: severity = 'high' (commercial intent, limited IP filing)
R01/R21: severity = 'medium'

No LLM. No alerts.

Show me both complete files.
```

Schedule: **Thursday 06:00 UTC.**

Say: **"NIH RePORTER confirmed. Build opportunity scorer."**

---

## PHASE 1 — Job 5: Opportunity Scorer + Weekly Digest

Type:
```
Build peptide_radar/scoring/opportunity_scorer.py,
peptide_radar/scoring/convergence_detector.py,
and jobs/job_opportunity_scorer.py.

Use exact scoring formulas from SPEC_v2.
Load weights from conf/scoring_weights.json at runtime — not hardcoded.

Scoring logic:
  1. Load peptide_radar.silver.peptides WHERE watchlist_active = TRUE
  2. For each peptide, query silver.regulatory_status and
     silver.signals (last 30 days)
  3. Compute all 5 dimension scores
  4. Compute composite_score
  5. Load prior week composite from silver.opportunity_scores
  6. Compute score_delta_7d and score_delta_30d
  7. Count distinct source_types in signals last 30 days → convergence_count
  8. Set last_three_events = JSON array of last 3 signal_ids
  9. Set regulatory_change flag if any FDA signal in last 7 days
  10. Evaluate should_escalate_to_gold() for each peptide
  11. Write all scores to silver.opportunity_scores

Weekly digest LLM call — CRITICAL BATCHING RULE:
  - Collect ALL peptides where should_escalate_to_gold() = True
  - If none: log "nothing to digest this week", post minimal Slack summary, exit
  - If some: build ONE single prompt containing all elevated peptides
  - Make ONE governed_llm_call() — not one per peptide
  - Parse response into per-peptide summaries
  - Write to gold.weekly_digest_items
  - Call send_digest() with formatted weekly output

This is the ONE Haiku call per week maximum.
If you write governed_llm_call() inside a for-loop, that is a SPEC violation.

Show me all files.
```

**Verify:**
```python
# Confirm scoring math produces sensible values
from peptide_radar.scoring.opportunity_scorer import regulatory_score, composite_score
import json

weights = json.load(open("conf/scoring_weights.json"))
reg = regulatory_score("503a_bulk", False, None, False)
comp = composite_score(reg, 0.6, 0.8, 0.7, 0.9, weights)
print(f"Test composite: {comp:.3f}")  # Expected: > 0.60

# Confirm no LLM call happened from scoring alone
spark.sql("""
  SELECT COUNT(*) FROM peptide_radar.gold.llm_costs
  WHERE job_name = 'job_opportunity_scorer'
  AND date(call_timestamp) = current_date()
""").show()
# Expected: 0 unless a peptide crossed threshold
```

Schedule: **Friday 06:00 UTC.**

---

## PHASE 1 COMPLETE — End-to-End Verification

```python
# Inject a synthetic FDA signal to test the full pipeline
import uuid
from datetime import date
from peptide_radar.utils.diff_engine import content_hash

test_row = {
    "signal_id": str(uuid.uuid4()),
    "peptide_id": (spark.sql("SELECT peptide_id FROM peptide_radar.silver.peptides WHERE canonical_name='vasopressin'").first()["peptide_id"]),
    "event_date": date.today().isoformat(),
    "source_type": "fda_503a",
    "event_type": "fda_category_change",
    "event_value": '{"old":"503a_bulk","new":"approved"}',
    "event_direction": "negative",
    "severity": "critical",
    "raw_ref": "test",
    "source_hash": content_hash("test"),
    "sent_to_gold": False,
    "signal_date": "2026-03-10T00:00:00"
}

spark.createDataFrame([test_row]).write.mode("append").saveAsTable("peptide_radar.silver.signals")

# Run scorer manually
from jobs.job_opportunity_scorer import run
run()

# Verify:
# 1. Slack message arrived
# 2. Gold digest item written
spark.sql("SELECT * FROM peptide_radar.gold.weekly_digest_items ORDER BY digest_week DESC LIMIT 3").show()
# 3. Cost was logged
spark.sql("SELECT * FROM peptide_radar.gold.llm_costs ORDER BY call_timestamp DESC LIMIT 3").show()
```

If Slack message arrives and gold table has a row: **v1 is done.**

---

## IF CLAUDE CODE GETS CONFUSED

If it starts improvising or output looks wrong:
```
Stop. Re-read PEPTIDE_RADAR_SPEC_v2.md.
Tell me which rule you may have violated.
Do not continue until we agree on what went wrong.
```

If it calls LLM inside a for-loop:
```
Stop. Core Rule 3 violation: never loop LLM calls over individual items.
Delete this code entirely.
Build a single batched prompt for ALL elevated peptides.
One governed_llm_call() call per job run.
```

If it calls Anthropic API directly instead of governed_llm_call():
```
Stop. Core Rule 2 violation.
governed_llm_call() in cost_guard.py is the only LLM entry point.
Delete the direct call. Route through governed_llm_call().
```

If it writes any LLM call into job_fda_bulks.py:
```
Stop. job_fda_bulks.py must have zero LLM calls.
All FDA alerts are deterministic strings.
Remove the LLM call entirely.
```

---

## STARTING A NEW SESSION AFTER A BREAK

```
Read PEPTIDE_RADAR_SPEC_v2.md.

Phase 0 complete: schemas, tables, seed data, entity_resolver.py,
diff_engine.py, cost_guard.py, slack_notifier.py — all working.

We are currently on: [JOB NAME — e.g. "Job 3: pubmed_biorxiv_harvester"].

Read the [job name] section of the spec and tell me what you are
about to build before writing any code.
```

---

## PHASE 2 SOURCES — Build after v1 is stable

Use the 2-week dry-run rule for every new source:
  items_after_filter / items_fetched < 0.05 → disable and log, never enable.

Priority order:
1. Supplier catalogs (Bachem, GenScript) — HTML diff, no auth
2. SEC EDGAR — EFTS API, no auth
3. USPTO Patent Center — NOT Public PAIR (retired 2022/2023)
4. EU CTIS — validate coverage against watchlist first
5. Conference abstracts — annual, manual trigger only
6. WHO ICTRP — legal review required before any integration
