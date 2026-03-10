import uuid
import json
from datetime import datetime, timezone, date as date_type

import requests

from peptide_radar.utils.diff_engine import content_hash
from peptide_radar.resolvers.entity_resolver import resolve_peptide, build_alias_index, normalize

API_URL = "https://clinicaltrials.gov/api/v2/studies"
API_PARAMS = {
    "filter.phase": "PHASE1,PHASE2",
    "filter.overallStatus": "RECRUITING,ACTIVE_NOT_RECRUITING,TERMINATED,WITHDRAWN",
    "query.intr": "peptide",
    "fields": "nctId,briefTitle,conditions,interventions,phase,overallStatus,whyStopped,primaryCompletionDate,lastUpdatePosted",
    "pageSize": 100,
}

BUSINESS_TERMS = ["business", "enrollment", "funding", "financial"]


def fetch_studies():
    """Fetch studies from ClinicalTrials.gov API. Returns list of study dicts."""
    all_studies = []
    next_token = None
    try:
        while True:
            params = dict(API_PARAMS)
            if next_token:
                params["pageToken"] = next_token
            resp = requests.get(API_URL, params=params, timeout=60)
            if resp.status_code != 200:
                print(f"  ClinicalTrials API error: status={resp.status_code}")
                break
            data = resp.json()
            studies = data.get("studies", [])
            all_studies.extend(studies)
            next_token = data.get("nextPageToken")
            if not next_token or len(all_studies) >= 100:
                break
    except Exception as e:
        print(f"  fetch_studies error: {e}")
    return all_studies


def _get_seen_hashes():
    """Load all source_hash values already in signals for clinical_trials."""
    try:
        rows = spark.sql("""
            SELECT source_hash
            FROM peptide_radar.silver.signals
            WHERE source_type = 'clinical_trials'
        """).collect()
        return {r["source_hash"] for r in rows}
    except NameError:
        return set()


def _load_alias_index():
    try:
        rows = spark.sql("""
            SELECT alias, canonical_name
            FROM peptide_radar.silver.peptide_aliases
        """).collect()
        return build_alias_index([
            {"alias": r["alias"], "canonical_name": r["canonical_name"]} for r in rows
        ])
    except NameError:
        return {}


def _load_peptide_lookup():
    try:
        rows = spark.sql("""
            SELECT peptide_id, canonical_name
            FROM peptide_radar.silver.peptides
        """).collect()
        return {r["canonical_name"]: r["peptide_id"] for r in rows}
    except NameError:
        return {}


def _resolve_name(text, alias_index, peptide_lookup):
    resolved = resolve_peptide(text, alias_index)
    if resolved:
        for canonical in resolved:
            pid = peptide_lookup.get(canonical)
            if pid:
                return pid, canonical
    norm = normalize(text)
    for canonical, pid in peptide_lookup.items():
        if normalize(canonical) == norm:
            return pid, canonical
    return None, None


def _write_rows(table_name, rows):
    if not rows:
        return
    try:
        spark.createDataFrame(rows).write.mode("append").saveAsTable(table_name)
    except NameError:
        pass


def _extract_field(study, *keys):
    """Navigate nested study JSON to extract a field."""
    obj = study
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return None
    return obj


def _classify_termination(why_stopped):
    """Classify termination reason. Returns (event_type, direction, severity)."""
    if why_stopped:
        why_lower = why_stopped.lower()
        if any(term in why_lower for term in BUSINESS_TERMS):
            return "trial_terminated_business", "positive", "medium"
    return "trial_terminated_safety", "negative", "high"


def process_studies(studies):
    """Process fetched studies. Returns result dict."""
    result = {
        "trials_fetched": len(studies),
        "new_signals": 0,
        "unresolved_count": 0,
    }

    seen_hashes = _get_seen_hashes()
    alias_index = _load_alias_index()
    peptide_lookup = _load_peptide_lookup()

    signals = []
    unresolved = []
    now = datetime.now(timezone.utc)
    today = date_type.today()

    for study in studies:
        proto = study.get("protocolSection", {})
        id_mod = proto.get("identificationModule", {})
        status_mod = proto.get("statusModule", {})
        design_mod = proto.get("designModule", {})
        cond_mod = proto.get("conditionsModule", {})

        nct_id = id_mod.get("nctId", "")
        brief_title = id_mod.get("briefTitle", "")
        overall_status = status_mod.get("overallStatus", "")
        why_stopped = status_mod.get("whyStopped", "")
        last_update = status_mod.get("lastUpdatePostDateStruct", {}).get("date", "")
        phase_list = design_mod.get("phases", [])
        phase = phase_list[0] if phase_list else ""
        conditions = cond_mod.get("conditions", [])
        conditions_text = " ".join(conditions) if conditions else ""

        if not nct_id:
            continue

        # Dedup: content_hash(nctId + lastUpdatePosted)
        row_hash = content_hash(nct_id + last_update)
        if row_hash in seen_hashes:
            continue
        seen_hashes.add(row_hash)

        # Entity resolution on briefTitle + conditions
        resolve_text = f"{brief_title} {conditions_text}".strip()
        pid, canonical = _resolve_name(resolve_text, alias_index, peptide_lookup)

        if pid is None:
            unresolved.append({
                "review_id": str(uuid.uuid4()),
                "raw_text": resolve_text[:500],
                "source_type": "clinical_trials",
                "source_ref": nct_id,
                "candidate_aliases": [],
                "status": "pending",
                "resolved_to": None,
                "reviewer_note": None,
                "created_at": now,
                "reviewed_at": None,
            })
            continue

        # Classify event
        if overall_status in ("TERMINATED", "WITHDRAWN"):
            event_type, direction, severity = _classify_termination(why_stopped)
        else:
            event_type = "trial_active"
            direction = "positive"
            severity = "low"

        event_value = {
            "nct_id": nct_id,
            "brief_title": brief_title,
            "phase": phase,
            "overall_status": overall_status,
            "why_stopped": why_stopped,
            "conditions": conditions,
            "last_update": last_update,
        }

        signals.append({
            "signal_id": str(uuid.uuid4()),
            "peptide_id": pid,
            "event_date": today,
            "source_type": "clinical_trials",
            "event_type": event_type,
            "event_value": json.dumps(event_value),
            "event_direction": direction,
            "severity": severity,
            "raw_ref": nct_id,
            "source_hash": row_hash,
            "sent_to_gold": False,
            "signal_date": now,
        })

    _write_rows("peptide_radar.silver.signals", signals)
    _write_rows("peptide_radar.silver.manual_review_queue", unresolved)

    result["new_signals"] = len(signals)
    result["unresolved_count"] = len(unresolved)
    return result
