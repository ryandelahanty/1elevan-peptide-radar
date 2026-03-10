import uuid
import json
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, date as date_type, timedelta

import requests

from peptide_radar.utils.diff_engine import content_hash
from peptide_radar.resolvers.entity_resolver import resolve_peptide, build_alias_index, normalize

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
BIORXIV_URL = "https://api.biorxiv.org/details/biorxiv"

PUBMED_QUERY = (
    '("peptide"[MeSH] OR "therapeutic peptide"[TW]) AND '
    '("compounding"[TW] OR "503A"[TW] OR "503B"[TW] OR '
    '"vasopressin"[TW] OR "oxytocin"[TW] OR "sermorelin"[TW] OR '
    '"ipamorelin"[TW] OR "BPC-157"[TW] OR "semaglutide"[TW] OR '
    '"liraglutide"[TW] OR "tesamorelin"[TW] OR "thymosin"[TW] OR '
    '"epithalon"[TW] OR "selank"[TW] OR "semax"[TW] OR '
    '"melanotan"[TW] OR "kisspeptin"[TW] OR "CJC-1295"[TW] OR '
    '"TB-500"[TW] OR "GHK-Cu"[TW])'
)

BIORXIV_CATEGORIES = {"pharmacology", "neuroscience"}

PHASE_PATTERNS = [
    "phase i ", "phase ii ", "phase 1 ", "phase 2 ",
    "phase i/", "phase ii/", "phase 1/", "phase 2/",
    "phase i,", "phase ii,", "phase 1,", "phase 2,",
    "clinical trial",
]


def fetch_pubmed():
    """Fetch PubMed articles from last 90 days. Returns list of article dicts."""
    articles = []
    try:
        search_params = {
            "db": "pubmed",
            "term": PUBMED_QUERY,
            "retmax": 500,
            "retmode": "json",
            "datetype": "pdat",
            "reldate": 90,
        }
        resp = requests.get(ESEARCH_URL, params=search_params, timeout=60)
        if resp.status_code != 200:
            print(f"  PubMed esearch error: status={resp.status_code}")
            return articles

        data = resp.json()
        pmids = data.get("esearchresult", {}).get("idlist", [])
        if not pmids:
            print("  PubMed: 0 PMIDs returned")
            return articles

        print(f"  PubMed: {len(pmids)} PMIDs found")
        time.sleep(0.4)

        for i in range(0, len(pmids), 100):
            batch = pmids[i:i + 100]
            fetch_params = {
                "db": "pubmed",
                "id": ",".join(batch),
                "retmode": "xml",
            }
            resp = requests.get(EFETCH_URL, params=fetch_params, timeout=60)
            if resp.status_code != 200:
                print(f"  PubMed efetch error: status={resp.status_code}")
                continue
            articles.extend(_parse_pubmed_xml(resp.text))
            time.sleep(0.4)
    except Exception as e:
        print(f"  fetch_pubmed error: {e}")
    return articles


def _parse_pubmed_xml(xml_text):
    """Parse PubMed efetch XML into list of article dicts."""
    articles = []
    try:
        root = ET.fromstring(xml_text)
        for article_elem in root.findall(".//PubmedArticle"):
            medline = article_elem.find("MedlineCitation")
            if medline is None:
                continue

            pmid_elem = medline.find("PMID")
            pmid = pmid_elem.text if pmid_elem is not None else ""

            article = medline.find("Article")
            if article is None:
                continue

            title_elem = article.find("ArticleTitle")
            title = title_elem.text if title_elem is not None else ""

            abstract_parts = []
            abstract_elem = article.find("Abstract")
            if abstract_elem is not None:
                for at in abstract_elem.findall("AbstractText"):
                    if at.text:
                        abstract_parts.append(at.text)
            abstract = " ".join(abstract_parts)

            authors = []
            author_list = article.find("AuthorList")
            if author_list is not None:
                for author in author_list.findall("Author"):
                    last = author.find("LastName")
                    fore = author.find("ForeName")
                    if last is not None and last.text:
                        name = last.text
                        if fore is not None and fore.text:
                            name += f" {fore.text}"
                        authors.append(name)

            journal_elem = article.find("Journal/Title")
            journal = journal_elem.text if journal_elem is not None else ""

            pub_date_elem = article.find("Journal/JournalIssue/PubDate")
            pub_year = ""
            if pub_date_elem is not None:
                year_elem = pub_date_elem.find("Year")
                if year_elem is not None:
                    pub_year = year_elem.text

            pub_types = []
            for pt in article.findall("PublicationTypeList/PublicationType"):
                if pt.text:
                    pub_types.append(pt.text.lower())

            articles.append({
                "pmid": pmid,
                "title": title or "",
                "abstract": abstract,
                "first_author": authors[0] if authors else "",
                "authors": authors,
                "journal": journal or "",
                "pub_year": pub_year,
                "pub_types": pub_types,
                "source": "pubmed",
                "doi": "",
            })
    except Exception as e:
        print(f"  _parse_pubmed_xml error: {e}")
    return articles


def fetch_biorxiv(watchlist_names):
    """Fetch bioRxiv preprints from last 90 days matching watchlist. Returns list of article dicts."""
    articles = []
    today = date_type.today()
    start = today - timedelta(days=90)
    start_str = start.strftime("%Y-%m-%d")
    end_str = today.strftime("%Y-%m-%d")

    normalized_names = [normalize(n) for n in watchlist_names if n]

    cursor = 0
    try:
        while True:
            url = f"{BIORXIV_URL}/{start_str}/{end_str}/{cursor}"
            resp = requests.get(url, timeout=60)
            if resp.status_code != 200:
                print(f"  bioRxiv API error: status={resp.status_code}")
                break

            data = resp.json()
            collection = data.get("collection", [])
            if not collection:
                break

            for item in collection:
                category = item.get("category", "").lower()
                if category not in BIORXIV_CATEGORIES:
                    continue

                title = item.get("title", "")
                abstract = item.get("abstract", "")
                combined = normalize(f"{title} {abstract}")

                if not any(name in combined for name in normalized_names):
                    continue

                authors_str = item.get("authors", "")
                first_author = authors_str.split(";")[0].strip() if authors_str else ""

                pub_date = item.get("date", "")
                pub_year = pub_date[:4] if len(pub_date) >= 4 else ""

                articles.append({
                    "pmid": "",
                    "title": title,
                    "abstract": abstract,
                    "first_author": first_author,
                    "authors": [a.strip() for a in authors_str.split(";") if a.strip()],
                    "journal": "bioRxiv",
                    "pub_year": pub_year,
                    "pub_types": ["preprint"],
                    "source": "biorxiv",
                    "doi": item.get("doi", ""),
                })

            if len(collection) < 100:
                break
            cursor += 100
            time.sleep(0.4)
    except Exception as e:
        print(f"  fetch_biorxiv error: {e}")
    return articles


def _get_seen_hashes():
    """Load existing source_hash values for pubmed and biorxiv."""
    try:
        rows = spark.sql("""
            SELECT source_hash
            FROM peptide_radar.silver.signals
            WHERE source_type IN ('pubmed', 'biorxiv', 'pubmed_biorxiv')
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


def _load_watchlist_names():
    """Load canonical_names from silver.peptides for bioRxiv filtering."""
    try:
        rows = spark.sql("""
            SELECT canonical_name
            FROM peptide_radar.silver.peptides
        """).collect()
        return [r["canonical_name"] for r in rows]
    except NameError:
        return []


def _resolve_name(text, alias_index, peptide_lookup):
    """Resolve article text to (peptide_id, canonical_name) or (None, None)."""
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


def _is_phase_study(title, abstract, pub_types):
    """Check if article is a Phase I/II human study."""
    text = f"{title} {abstract}".lower()
    if "clinical trial" in " ".join(pub_types):
        return True
    return any(p in text for p in PHASE_PATTERNS)


def process_articles(pubmed_articles, biorxiv_articles):
    """Process all fetched articles. Returns result dict."""
    result = {
        "pubmed_fetched": len(pubmed_articles),
        "biorxiv_fetched": len(biorxiv_articles),
        "new_signals": 0,
        "unresolved_count": 0,
    }

    seen_hashes = _get_seen_hashes()
    alias_index = _load_alias_index()
    peptide_lookup = _load_peptide_lookup()

    all_articles = pubmed_articles + biorxiv_articles
    signals = []
    unresolved = []
    now = datetime.now(timezone.utc)
    today = date_type.today()

    # Track per-peptide counts for velocity ratio
    peptide_counts = {}

    for article in all_articles:
        title = article.get("title", "")
        first_author = article.get("first_author", "")
        pub_year = article.get("pub_year", "")
        source = article.get("source", "")

        # Dedup: content_hash(title + first_author + pub_year)
        row_hash = content_hash(f"{title}{first_author}{pub_year}")
        if row_hash in seen_hashes:
            continue
        seen_hashes.add(row_hash)

        # Entity resolution on title + abstract
        resolve_text = f"{title} {article.get('abstract', '')}".strip()
        pid, canonical = _resolve_name(resolve_text, alias_index, peptide_lookup)

        if pid is None:
            unresolved.append({
                "review_id": str(uuid.uuid4()),
                "raw_text": resolve_text[:500],
                "source_type": source,
                "source_ref": article.get("pmid") or article.get("doi", ""),
                "candidate_aliases": [],
                "status": "pending",
                "resolved_to": None,
                "reviewer_note": None,
                "created_at": now,
                "reviewed_at": None,
            })
            continue

        # Track counts for velocity ratio
        if pid not in peptide_counts:
            peptide_counts[pid] = {"preprint": 0, "peer_reviewed": 0, "canonical": canonical}
        if source == "biorxiv":
            peptide_counts[pid]["preprint"] += 1
        else:
            peptide_counts[pid]["peer_reviewed"] += 1

        # Only signal if Phase I/II human study
        is_phase = _is_phase_study(
            title, article.get("abstract", ""), article.get("pub_types", [])
        )
        if is_phase:
            event_value = {
                "title": title,
                "first_author": first_author,
                "journal": article.get("journal", ""),
                "pub_year": pub_year,
                "pmid": article.get("pmid", ""),
                "doi": article.get("doi", ""),
            }
            signals.append({
                "signal_id": str(uuid.uuid4()),
                "peptide_id": pid,
                "event_date": today,
                "source_type": source,
                "event_type": "literature_phase_study",
                "event_value": json.dumps(event_value),
                "event_direction": "positive",
                "severity": "medium",
                "raw_ref": article.get("pmid") or article.get("doi", ""),
                "source_hash": row_hash,
                "sent_to_gold": False,
                "signal_date": now,
            })

    # Velocity ratio signals — one per peptide where ratio > 0.4
    for pid, counts in peptide_counts.items():
        total = counts["preprint"] + counts["peer_reviewed"]
        if total == 0:
            continue
        ratio = counts["preprint"] / total
        if ratio > 0.4:
            event_value = {
                "preprint_count": counts["preprint"],
                "peer_reviewed_count": counts["peer_reviewed"],
                "velocity_ratio": round(ratio, 3),
            }
            vel_hash = content_hash(f"velocity_{pid}_{today.isoformat()}")
            if vel_hash not in seen_hashes:
                signals.append({
                    "signal_id": str(uuid.uuid4()),
                    "peptide_id": pid,
                    "event_date": today,
                    "source_type": "pubmed_biorxiv",
                    "event_type": "preprint_velocity",
                    "event_value": json.dumps(event_value),
                    "event_direction": "neutral",
                    "severity": "low",
                    "raw_ref": counts["canonical"],
                    "source_hash": vel_hash,
                    "sent_to_gold": False,
                    "signal_date": now,
                })

    _write_rows("peptide_radar.silver.signals", signals)
    _write_rows("peptide_radar.silver.manual_review_queue", unresolved)

    result["new_signals"] = len(signals)
    result["unresolved_count"] = len(unresolved)
    return result
