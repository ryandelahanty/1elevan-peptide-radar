# Peptide Radar Build — Session Handoff
**Date:** March 10, 2026  
**Prepared by:** Claude (claude.ai project session)  
**For:** Next Claude session continuing the Peptide Radar build  
**Ryan's instruction:** If anything in this handoff is unclear, stop and ask Ryan clarifying questions before writing any code.

---

## What Was Accomplished This Session

### Infrastructure (complete, verified live)
- `peptide_radar` catalog created in Databricks Unity Catalog (elevanbio_dev workspace: `adb-252904149011683.3.azuredatabricks.net`)
- Four schemas created and verified: `bronze`, `silver`, `gold`, `config`
- All Delta tables created directly via MCP connector (not Claude Code):
  - **Bronze:** `raw_snapshots` (1 table)
  - **Silver:** `peptides`, `peptide_aliases`, `fda_category_mapping`, `regulatory_status`, `signals`, `opportunity_scores`, `manual_review_queue`, `internal_discrepancies` (8 tables)
  - **Gold:** `weekly_digest_items`, `monthly_brief`, `llm_costs` (3 tables)
- GitHub repo created: `https://github.com/ryandelahanty/1elevan-peptide-radar`
- Databricks Git Folder connected at `/Workspace/Users/ryan.delahanty@1elevan.com/1elevan-peptide-radar`
- Local clone at `C:\elevanbio\1elevan-peptide-radar`
- Databricks CLI installed and configured on Ryan's Windows machine
- Secret scope `peptide-radar` created with two secrets:
  - `ANTHROPIC_API_KEY` — live, new key created for this project
  - `SLACK_WEBHOOK_URL` — stubbed (Teams webhook pending; Ryan uses Teams not Slack; Scott can help create channel)
- Claude Code v2.1.71 installed and confirmed working

### IMPORTANT: Delta DEFAULT values
DEFAULT column values were dropped from the DDL due to a Delta feature flag requirement (`delta.feature.allowColumnDefaults`). All Boolean and TIMESTAMP defaults that were in the original SQL (e.g., `watchlist_active = TRUE`, `sent_to_gold = FALSE`, `signal_date = current_timestamp()`) **must be set explicitly in Python ingestion code**, not at the DDL layer. Do not attempt to re-add DEFAULTs to the DDL without first enabling the feature flag per table.

---

## Where Claude Code Left Off

Claude Code completed Phase 0 Steps 1-2 (schema creation, table DDL) and was working on Step 3 (seed watchlist). It generated `05_seed_watchlist.sql` with 36 peptides and `conf/fda_category_mapping.json` with 20 raw-to-normalized mappings.

**Step 3 is NOT complete. The seed SQL has errors (see below).**

---

## Blocking Issue — Do Not Run 05_seed_watchlist.sql

The seed SQL has name mismatches and invented compounds. These were caught by cross-checking against the live source table. The seed SQL must be regenerated before running.

### Name mismatches (seed name vs. actual `LOWER(peptide_name_generic)` in source):

| Seed SQL `canonical_name` | Correct value from source |
|---|---|
| `ghk-cu` | `collagen tripeptide ghk-cu` |
| `dsip` | `delta sleep inducing peptide (dsip)` |
| `kpv` | `kpv (lys-pro-val)` |
| `kisspeptin` | `kisspeptin-54 / kisspeptin-10` |
| `ll-37` | `ll-37 (cathelicidin)` |
| `mots-c` | `mots-c (mitochondrial orf peptide)` |
| `tb-500` | `tb-500 (thymosin beta-4 fragment)` |
| `secretin` | `secretin (human)` (remove porcine variant for now) |

### Invented compounds (not in source — must change `seed_source` to `'manual'` or remove):
- `pentagastrin`
- `sincalide`

### Fix required from Claude Code:
Tell Claude Code exactly this:

> Stop. The seed SQL has name mismatches against the actual peptide_database_raw source. Do not run it. The canonical_name values in the seed must match LOWER(peptide_name_generic) from the source exactly. Corrections needed: [list above]. Recompute SHA-256 peptide_ids for all corrected names. Regenerate 05_seed_watchlist.sql. Show me the updated file before running anything.

**After Claude Code regenerates the file, paste the raw contents here (use `type C:\elevanbio\1elevan-peptide-radar\sql\05_seed_watchlist.sql` in terminal) and verify before running.**

---

## Verified Source Table Facts (do not re-derive)

### Seed source: `elevanbio_dev.bronze.peptide_database_raw`
- 94 rows, 32 columns
- Name field: `peptide_name_generic`
- Key fields for Radar: `col_503a_bulk_compounding_category`, `primary_therapeutic_area`, `fda_approved`, `patent_status`, `orphan_drug_status`
- Contains all 8 FDA PreCheck compounds

### Cross-reference source: `elevanbio_dev.bronze.peptide_compound_master_raw`
- 327 rows, 44 columns
- Name field: `name_category_1_peptides`
- Use only for these fields where names overlap: `average_sales_price_asp`, `average_wholesale_price_awp`, `wholesale_acquisition_cost_wac`, `national_average_drug_acquisition_cost_nadac`, `safety_rating_1_5`, `efficacy_rating_1_5`, `us_demand_3yr_projection`
- Confirmed real fields — verified against live schema

---

## 8 Confirmed Architectural Decisions (do not relitigate)

1. **Catalog isolation.** New catalog `peptide_radar`. All radar tables live there, separate from `elevanbio_dev`.
2. **Seed authority.** `peptide_database_raw` (94 rows) is the entity authority. `peptide_compound_master_raw` is cross-reference only for pricing/ratings fields where names overlap.
3. **`fda_category` normalization.** Handled in `peptide_radar.silver.fda_category_mapping`. Does not touch `elevanbio_dev`. Nine normalized values: `approved`, `503a_bulk`, `503a_eval`, `503a_not_approved`, `cosmetic_or_otc`, `supplement`, `controlled`, `investigational`, `unknown`.
4. **No dev/prod split yet.** Single `peptide_radar` catalog. Prod introduced only when: (a) workflows run unattended 3-4 weeks, (b) downstream consumer depends on output, (c) at least one schema migration you wouldn't test live.
5. **Deployment stack.** Plain Python scripts in GitHub repo, synced via Databricks Git Folder, run as Databricks Workflows on single-node job clusters with auto-terminate. No Databricks Asset Bundles in v1.
6. **Three non-negotiable rules.** (a) Deduplication by content hash only, never source ID. (b) All LLM calls through `governed_llm_call()`, no exceptions. (c) LLM calls always batched, never looped over individual items.
7. **Internal discrepancy signal.** External data conflicting with internal bronze ratings emits `signal_type = 'internal_discrepancy'` to `peptide_radar.silver.signals`. First-class signal type, not a log entry.
8. **v1 scope is exactly 5 jobs.** FDA bulks differ, ClinicalTrials poller, PubMed/bioRxiv harvester, NIH RePORTER monitor, opportunity scorer and weekly digest. Everything else is Phase 2.

---

## Three Non-Negotiable Code Rules (enforce in every Claude Code session)

1. Deduplication always by SHA-256 content hash, never source ID or URL
2. Every LLM call through `governed_llm_call()` in `utils/cost_guard.py`. No direct Anthropic client calls anywhere else.
3. Never loop LLM calls over individual items. Always batch. One call per job run maximum.

---

## Immediate Next Steps (in order)

1. Fix `05_seed_watchlist.sql` — see blocking issue above
2. Verify corrected seed SQL (paste raw contents, check before running)
3. Run corrected seed SQL in Databricks SQL Editor
4. Verify: `SELECT COUNT(*) FROM peptide_radar.silver.peptides;` — expected 36 (or adjusted count if pentagastrin/sincalide are removed)
5. Load `conf/fda_category_mapping.json` into `peptide_radar.silver.fda_category_mapping` — Claude Code offered to create `06_seed_fda_mapping.sql`; have it show the SQL before running
6. Phase 0 Step 4: `entity_resolver.py`
7. Phase 0 Step 5: `diff_engine.py`
8. Phase 0 Step 6: `cost_guard.py` (requires Databricks secret scope `peptide-radar` — already exists)
9. Phase 0 Step 7: `slack_notifier.py` (note: will be renamed to Teams notifier; stub webhook already in place)

---

## Known Pending Items (not blockers for build)

- **Teams webhook:** Ryan uses Teams not Slack. `SLACK_WEBHOOK_URL` secret is stubbed. Scott Kalcic can help create a Teams channel with an Incoming Webhook connector. When URL is obtained, update the secret: `databricks secrets put-secret peptide-radar SLACK_WEBHOOK_URL <real_url>`. The variable name `SLACK_WEBHOOK_URL` stays as-is in code for now; spec update happens after build is stable.
- **Workspace cleanup:** SQL files and notebooks in Databricks workspace are scattered (old queries, drafts, etc.). A cleanup/organization session is needed but is not a build blocker.
- **Bronze table audit:** A separate chat ran an audit prompt with Darrin. Pull results back into this project only if the audit changes what `peptide_database_raw` contains or surfaces a better seed source. Otherwise leave it in that chat.
- **`pentagastrin` and `sincalide`:** Not in `peptide_database_raw`. Decision needed: remove from seed entirely, or keep as `seed_source = 'manual'` with no source cross-reference.

---

## Key File Locations

| File | Location |
|---|---|
| Spec | `C:\elevanbio\1elevan-peptide-radar\PEPTIDE_RADAR_SPEC_v2.md` |
| Instructions | `C:\elevanbio\1elevan-peptide-radar\PEPTIDE_RADAR_INSTRUCTIONS_v2.md` |
| Bronze DDL | `C:\elevanbio\1elevan-peptide-radar\sql\02_create_bronze_tables.sql` |
| Silver DDL | `C:\elevanbio\1elevan-peptide-radar\sql\03_create_silver_tables.sql` |
| Gold DDL | `C:\elevanbio\1elevan-peptide-radar\sql\04_create_gold_tables.sql` |
| Seed SQL (needs fix) | `C:\elevanbio\1elevan-peptide-radar\sql\05_seed_watchlist.sql` |
| FDA mapping JSON | `C:\elevanbio\1elevan-peptide-radar\conf\fda_category_mapping.json` |
| Databricks workspace | `https://adb-252904149011683.3.azuredatabricks.net` |
| GitHub repo | `https://github.com/ryandelahanty/1elevan-peptide-radar` |

---

## Session Opener for Next Claude Code Session

Paste this at the start of every Claude Code session:

> Read PEPTIDE_RADAR_INSTRUCTIONS_v2.md and confirm you understand the three non-negotiable rules and the current build phase before writing any code. Current state: Phase 0 Step 3 in progress. Seed SQL needs correction per PEPTIDE_RADAR_HANDOFF_20260310.md. Do not run any SQL until I confirm it.

---

*End of handoff. If unclear on anything, ask Ryan before proceeding.*
