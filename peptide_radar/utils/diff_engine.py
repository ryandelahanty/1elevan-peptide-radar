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
