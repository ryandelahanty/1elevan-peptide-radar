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
    return list(matches)

def build_alias_index(alias_rows: list[dict]) -> dict:
    index = {}
    for row in alias_rows:
        key = normalize(row['alias'])
        index.setdefault(key, []).append(row['canonical_name'])
    return index
