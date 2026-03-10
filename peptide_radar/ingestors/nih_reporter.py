import uuid
import json
from datetime import datetime, timezone, date as date_type

import requests

from peptide_radar.utils.diff_engine import content_hash
from peptide_radar.resolvers.entity_resolver import resolve_peptide, build_alias_index, normalize

API_URL = "https://api.reporter.nih.gov/v2/projects/search"

QUERY_BODY = {
    "criteria": {
        "advanced_text_search": {
            "operator": "and",
            "search_field": "all",
            "search_text": (
                '("peptide synthesis" OR "solid phase peptide synthesis" OR SPPS OR '
                '"peptide manufacturing" OR "cGMP peptide" OR "peptide formulation" OR '
                '"IND enabling peptide" OR "peptide process development" OR '
                '"lyophilized peptide" OR "outsourcing facility" OR "bulk drug substance")'
            ),
        },
        "activity_codes": ["R43", "R44", "R41", "R42", "UG3", "UH3", "U44", "SB1"],
    },
    "offset": 0,
    "limit": 50,
    "sort_field": "project_start_date",
    "sort_order": "desc",
}

HIGH_SEVERITY_CODES = {"R43", "R44", "R41", "R42"}
MEDIUM_SEVERITY_CODES = {"UG3", "UH3", "U44", "SB1"}


def fetch_grants():
    """Fetch grants from NIH RePORTER API. Returns list of project dicts."""
    projects = []
    try:
        resp = requests.post(API_URL, json=QUERY_BODY, timeout=60)
        if resp.status_code != 200:
            print(f"  NIH RePORTER API error: status={resp.status_code}")
            return projects

        data = resp.json()
        results = data.get("results", [])
        projects.extend(results)
        print(f"  NIH RePORTER: {len(projects)} projects returned")
    except Exception as e:
        print(f"  fetch_grants error: {e}")
    return projects


def _get_seen_hashes():
    """Load existing source_hash values for nih_reporter."""
    try:
        rows = spark.sql("""
            SELECT source_hash
            FROM peptide_radar.silver.signals
            WHERE source_type = 'nih_reporter'
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
    """Resolve grant text to (peptide_id, canonical_name) or (None, None)."""
    resolved = resolve_peptide(text, alias_index)
    if resolved:
        for canonical in resolved:
            pid = peptide_lookup.get(canonical)
            if pid:
                return pid, canonical
    norm = normalize(text)
    for canonical, pid in peptide_lookup.items():
        if normalize(canonical) in norm:
            return pid, canonical
    return None, None


def _write_rows(table_name, rows):
    if not rows:
        return
    try:
        spark.createDataFrame(rows).write.mode("append").saveAsTable(table_name)
    except NameError:
        pass


def _classify_severity(activity_code):
    """R43/R44/R41/R42 = high, UG3/UH3/U44/SB1 = medium, else low."""
    code = activity_code.upper() if activity_code else ""
    if code in HIGH_SEVERITY_CODES:
        return "high"
    if code in MEDIUM_SEVERITY_CODES:
        return "medium"
    return "low"


def process_grants(projects):
    """Process fetched NIH grants. Returns result dict."""
    result = {
        "grants_fetched": len(projects),
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

    for project in projects:
        project_num = project.get("project_num", "") or ""
        fiscal_year = str(project.get("fiscal_year", "") or "")
        project_title = project.get("project_title", "") or ""
        abstract_text = project.get("abstract_text", "") or ""
        activity_code = project.get("activity_code", "") or ""
        org_name = project.get("organization", {}).get("org_name", "") if project.get("organization") else ""
        pi_name = ""
        pi_list = project.get("principal_investigators", [])
        if pi_list and isinstance(pi_list, list) and len(pi_list) > 0:
            pi = pi_list[0]
            pi_name = f"{pi.get('first_name', '')} {pi.get('last_name', '')}".strip()

        project_start = project.get("project_start_date", "") or ""
        project_end = project.get("project_end_date", "") or ""
        award_amount = project.get("award_amount", None)

        if not project_num:
            continue

        # Dedup: content_hash(project_num + fiscal_year)
        row_hash = content_hash(f"{project_num}{fiscal_year}")
        if row_hash in seen_hashes:
            continue
        seen_hashes.add(row_hash)

        # Entity resolution on title + abstract
        resolve_text = f"{project_title} {abstract_text}".strip()
        pid, canonical = _resolve_name(resolve_text, alias_index, peptide_lookup)

        if pid is None:
            unresolved.append({
                "review_id": str(uuid.uuid4()),
                "raw_text": resolve_text[:500],
                "source_type": "nih_reporter",
                "source_ref": project_num,
                "candidate_aliases": [],
                "status": "pending",
                "resolved_to": None,
                "reviewer_note": None,
                "created_at": now,
                "reviewed_at": None,
            })
            continue

        severity = _classify_severity(activity_code)

        event_value = {
            "project_num": project_num,
            "project_title": project_title,
            "activity_code": activity_code,
            "fiscal_year": fiscal_year,
            "org_name": org_name,
            "pi_name": pi_name,
            "project_start": project_start,
            "project_end": project_end,
            "award_amount": award_amount,
        }

        signals.append({
            "signal_id": str(uuid.uuid4()),
            "peptide_id": pid,
            "event_date": today,
            "source_type": "nih_reporter",
            "event_type": "nih_grant_awarded",
            "event_value": json.dumps(event_value),
            "event_direction": "neutral",
            "severity": severity,
            "raw_ref": project_num,
            "source_hash": row_hash,
            "sent_to_gold": False,
            "signal_date": now,
        })

    _write_rows("peptide_radar.silver.signals", signals)
    _write_rows("peptide_radar.silver.manual_review_queue", unresolved)

    result["new_signals"] = len(signals)
    result["unresolved_count"] = len(unresolved)
    return result
