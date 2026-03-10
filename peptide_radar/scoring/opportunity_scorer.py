import json
import os
from datetime import datetime, timezone, date as date_type, timedelta

from peptide_radar.utils.cost_guard import governed_llm_call
from peptide_radar.utils.teams_notifier import send_digest as send_teams_digest
from peptide_radar.scoring.convergence_detector import get_convergence_count

# Thresholds from spec constants
SCORE_ALERT_THRESHOLD = 0.72
SCORE_DELTA_THRESHOLD = 0.15
CONVERGENCE_THRESHOLD = 3


def _load_weights():
    """Load scoring weights from conf/scoring_weights.json at runtime."""
    candidates = [
        os.path.join(os.path.dirname(__file__), "..", "..", "conf", "scoring_weights.json"),
        "conf/scoring_weights.json",
    ]
    for path in candidates:
        try:
            with open(os.path.normpath(path)) as f:
                return json.load(f)
        except (FileNotFoundError, OSError):
            continue
    # Fallback to spec defaults if file not found
    return {"regulatory": 0.30, "evidence": 0.25, "ip": 0.20, "supply": 0.15, "fit": 0.10}


# --- Exact scoring formulas from spec (verbatim) ---

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


# --- Data loading ---

def _load_active_peptides():
    """Load active watchlist peptides."""
    try:
        rows = spark.sql("""
            SELECT peptide_id, canonical_name, strategic_fit_score
            FROM peptide_radar.silver.peptides
            WHERE watchlist_active = TRUE
        """).collect()
        return [{"peptide_id": r["peptide_id"],
                 "canonical_name": r["canonical_name"],
                 "strategic_fit_score": float(r["strategic_fit_score"] or 0)}
                for r in rows]
    except NameError:
        return []


def _load_regulatory_status(peptide_id):
    """Load latest regulatory status for a peptide."""
    defaults = {"status_503a": None, "on_safety_risk": False,
                "approved_nda": False, "on_shortage": False}
    try:
        safe_id = peptide_id.replace("'", "''")
        row = spark.sql(f"""
            SELECT status_503a, on_safety_risk, approved_nda, on_shortage
            FROM peptide_radar.silver.regulatory_status
            WHERE peptide_id = '{safe_id}'
            ORDER BY effective_date DESC
            LIMIT 1
        """).first()
        if row:
            return {
                "status_503a": row["status_503a"],
                "on_safety_risk": bool(row["on_safety_risk"]),
                "approved_nda": bool(row["approved_nda"]),
                "on_shortage": bool(row["on_shortage"]),
            }
    except NameError:
        pass
    return defaults


def _load_signals(peptide_id, days):
    """Load signals for a peptide within last N days, ordered by signal_date DESC."""
    try:
        safe_id = peptide_id.replace("'", "''")
        rows = spark.sql(f"""
            SELECT signal_id, source_type, event_type, event_value,
                   severity, signal_date, event_date
            FROM peptide_radar.silver.signals
            WHERE peptide_id = '{safe_id}'
            AND event_date >= date_sub(current_date(), {days})
            ORDER BY signal_date DESC
        """).collect()
        return [r.asDict() for r in rows]
    except NameError:
        return []


def _load_prior_composite(peptide_id, days_ago):
    """Load composite score from N days ago."""
    try:
        safe_id = peptide_id.replace("'", "''")
        row = spark.sql(f"""
            SELECT composite_score
            FROM peptide_radar.silver.opportunity_scores
            WHERE peptide_id = '{safe_id}'
            AND score_date <= date_sub(current_date(), {days_ago})
            ORDER BY score_date DESC
            LIMIT 1
        """).first()
        if row and row["composite_score"] is not None:
            return float(row["composite_score"])
    except NameError:
        pass
    return None


def _derive_evidence_inputs(signals):
    """Derive evidence_score inputs from signal data."""
    human_trials = 0
    animal_studies = 0
    recent_pubs = 0
    has_phase2 = False

    for sig in signals:
        et = sig.get("event_type", "")
        if et in ("trial_active", "trial_terminated_business", "trial_terminated_safety"):
            human_trials += 1
            try:
                ev = sig.get("event_value", "{}")
                ev_data = json.loads(ev) if isinstance(ev, str) else ev
                phase = str(ev_data.get("phase", "")).upper()
                if "PHASE2" in phase or "PHASE 2" in phase:
                    has_phase2 = True
            except (json.JSONDecodeError, AttributeError):
                pass
        elif et == "literature_phase_study":
            human_trials += 1
            recent_pubs += 1
        elif et == "preprint_velocity":
            recent_pubs += 1
        elif et == "nih_grant_awarded":
            recent_pubs += 1

    return human_trials, animal_studies, recent_pubs, has_phase2


def _has_regulatory_change_7d(signals_30d):
    """Check if any FDA signal in last 7 days."""
    today = date_type.today()
    cutoff = today - timedelta(days=7)
    for sig in signals_30d:
        et = sig.get("event_type", "")
        if et.startswith("fda_"):
            ed = sig.get("event_date")
            if ed is not None:
                sig_date = ed.date() if hasattr(ed, "date") else ed
                if sig_date >= cutoff:
                    return True
    return False


def _get_last_three_event_ids(signals):
    """JSON array of last 3 signal_ids by signal_date DESC."""
    return json.dumps([s["signal_id"] for s in signals[:3]])


def _write_rows(table_name, rows):
    if not rows:
        return
    try:
        spark.createDataFrame(rows).write.mode("append").saveAsTable(table_name)
    except NameError:
        pass


# --- Main scoring pipeline ---

def score_all_peptides():
    """Run full scoring pipeline. Returns (score_rows, elevated_peptides)."""
    weights = _load_weights()
    peptides = _load_active_peptides()
    today = date_type.today()

    score_rows = []
    elevated = []

    for pep in peptides:
        pid = pep["peptide_id"]
        canonical = pep["canonical_name"]
        fit = pep["strategic_fit_score"]

        # Step 2: Load regulatory status and signals (last 30 days)
        reg_status = _load_regulatory_status(pid)
        signals_30d = _load_signals(pid, 30)

        # Step 3: Compute all 5 dimension scores
        reg = regulatory_score(
            reg_status["status_503a"],
            reg_status["on_safety_risk"],
            reg_status["approved_nda"],
            reg_status["on_shortage"],
        )

        human_trials, animal_studies, recent_pubs, has_phase2 = _derive_evidence_inputs(signals_30d)
        evid = evidence_score(human_trials, animal_studies, recent_pubs, has_phase2)

        ip = ip_whitespace_score(0)  # No patent data source yet
        sup = supply_score(1, False)  # Default: 1 supplier, not in catalog

        # Step 4: Composite score
        comp = composite_score(reg, evid, ip, sup, fit, weights)

        # Step 5-6: Load prior composites and compute deltas
        prior_7d = _load_prior_composite(pid, 7)
        prior_30d = _load_prior_composite(pid, 30)
        delta_7d = comp - prior_7d if prior_7d is not None else 0.0
        delta_30d = comp - prior_30d if prior_30d is not None else 0.0

        # Step 7: Convergence count
        try:
            conv_count = get_convergence_count(pid, spark, 30)
        except NameError:
            conv_count = 0

        # Step 8: Last three events
        last_three = _get_last_three_event_ids(signals_30d)

        # Step 9: Regulatory change in last 7 days
        reg_change = _has_regulatory_change_7d(signals_30d)

        # Step 10: Evaluate escalation
        escalate = should_escalate_to_gold(comp, delta_7d, conv_count, reg_change)

        score_rows.append({
            "peptide_id": pid,
            "score_date": today,
            "regulatory_score": float(reg),
            "evidence_score": float(evid),
            "ip_whitespace_score": float(ip),
            "supply_score": float(sup),
            "strategic_fit_score": float(fit),
            "composite_score": float(comp),
            "score_delta_7d": float(delta_7d),
            "score_delta_30d": float(delta_30d),
            "convergence_count_30d": int(conv_count),
            "last_three_events": last_three,
            "alert_threshold_hit": escalate,
            "regulatory_change": reg_change,
        })

        if escalate:
            elevated.append({
                "peptide_id": pid,
                "canonical_name": canonical,
                "composite": comp,
                "delta_7d": delta_7d,
                "convergence": conv_count,
                "regulatory_change": reg_change,
                "status_503a": reg_status["status_503a"],
                "last_three": last_three,
            })

    # Step 11: Write all scores to silver.opportunity_scores
    _write_rows("peptide_radar.silver.opportunity_scores", score_rows)

    return score_rows, elevated


# --- Weekly digest (ONE governed_llm_call — never in a loop) ---

def _build_digest_prompt(elevated_peptides):
    """Build single batched prompt for ALL elevated peptides."""
    lines = [
        "You are a peptide therapeutics analyst at 1ElevanBio, a compounding pharmacy "
        "focused on peptide API manufacturing. Summarize the following elevated peptides "
        "for a weekly executive digest. For each peptide, provide a 2-3 sentence summary "
        "explaining why it was flagged and what business actions to consider.",
        "",
        "Format your response as a JSON array:",
        '[{"canonical_name": "...", "summary": "..."}]',
        "",
        "Elevated peptides this week:",
    ]
    for p in elevated_peptides:
        lines.append(
            f"- {p['canonical_name']}: composite={p['composite']:.3f}, "
            f"delta_7d={p['delta_7d']:.3f}, convergence={p['convergence']}, "
            f"regulatory_change={p['regulatory_change']}, "
            f"status_503a={p.get('status_503a', 'unknown')}"
        )
    return "\n".join(lines)


def _parse_digest_response(response_text, elevated_peptides):
    """Parse LLM response into per-peptide summaries."""
    summaries = {}
    try:
        start = response_text.find("[")
        end = response_text.rfind("]") + 1
        if start >= 0 and end > start:
            parsed = json.loads(response_text[start:end])
            for item in parsed:
                name = item.get("canonical_name", "")
                summary = item.get("summary", "")
                summaries[name] = summary
    except (json.JSONDecodeError, KeyError):
        for p in elevated_peptides:
            summaries[p["canonical_name"]] = response_text[:500]
    return summaries


def generate_digest(elevated_peptides):
    """Generate weekly digest via ONE governed_llm_call. Returns digest items written."""
    if not elevated_peptides:
        print("  No peptides escalated — nothing to digest this week")
        send_teams_digest("Peptide Radar Weekly Digest: No peptides crossed alert thresholds this week.")
        return []

    # ONE prompt for ALL elevated peptides — never one per peptide
    prompt = _build_digest_prompt(elevated_peptides)

    # ONE governed_llm_call — Core Rule 3: never loop LLM calls
    response = governed_llm_call(prompt, "job_opportunity_scorer", triggered_by="weekly_digest")

    if response is None:
        print("  governed_llm_call returned None — digest skipped")
        send_teams_digest("Peptide Radar Weekly Digest: LLM call failed or budget exceeded. Manual review recommended.")
        return []

    summaries = _parse_digest_response(response, elevated_peptides)

    today = date_type.today()
    tokens_in_est = len(prompt) // 4
    tokens_out_est = len(response) // 4

    digest_items = []
    for p in elevated_peptides:
        canonical = p["canonical_name"]
        digest_items.append({
            "peptide_id": p["peptide_id"],
            "digest_week": today,
            "canonical_name": canonical,
            "composite_score": float(p["composite"]),
            "score_delta_7d": float(p["delta_7d"]),
            "regulatory_status": p.get("status_503a") or "unknown",
            "top_signal_summary": summaries.get(canonical, "No summary available"),
            "last_three_events": p["last_three"],
            "tokens_in": tokens_in_est,
            "tokens_out": tokens_out_est,
            "cost_usd": 0.0,  # Actual cost logged in gold.llm_costs by governed_llm_call
        })

    _write_rows("peptide_radar.gold.weekly_digest_items", digest_items)

    # Format and send Teams digest
    digest_lines = ["Peptide Radar Weekly Digest:", ""]
    for item in digest_items:
        digest_lines.append(f"{item['canonical_name']} (composite: {item['composite_score']:.3f})")
        digest_lines.append(f"  {item['top_signal_summary']}")
        digest_lines.append("")
    send_teams_digest("\n".join(digest_lines))

    return digest_items
