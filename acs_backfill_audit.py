#!/usr/bin/env python3
"""
ACS Backfill Audit — exact normalized name matching only.

Loads the website dataset (camellias.json) and the ACS catalog
(acs_catalog_full.json), matches entries by normalized name, validates
candidate ACS URLs are live, and writes audit reports + updated data files.
"""

import json
import os
import re
import string
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# ── paths ────────────────────────────────────────────────────────────────────
REPO = Path(__file__).resolve().parent
DATA = REPO / "data"
CAMELLIAS_JSON = DATA / "camellias.json"
ACS_CATALOG = Path("/Users/lukewang/.openclaw/workspace/camellia-project/acs_catalog/acs_catalog_full.json")
INDEX_JSON = DATA / "index.json"

# ── normalisation ────────────────────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """Lowercase, strip diacritics, collapse punctuation/spaces/hyphens."""
    # NFD decompose then strip combining marks
    s = unicodedata.normalize("NFD", name)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    # Replace hyphens, underscores, dots with space
    s = re.sub(r"[-_.]+", " ", s)
    # Remove apostrophes and other punctuation except spaces
    s = re.sub(r"[^\w\s]", "", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ── URL validation ───────────────────────────────────────────────────────────

def validate_url(url: str, timeout: int = 15) -> bool:
    """Return True if url responds 200 with non-trivial body."""
    try:
        req = Request(url, method="GET", headers={"User-Agent": "CamelliaAudit/1.0"})
        with urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return False
            body = resp.read(2048)
            # ACS pages that exist have substantial HTML; empty / tiny = broken
            return len(body) > 500
    except (HTTPError, URLError, OSError, Exception):
        return False


def batch_validate(urls: list[str], workers: int = 8) -> dict[str, bool]:
    """Validate a list of URLs concurrently. Returns {url: is_valid}."""
    results = {}
    total = len(urls)
    done = 0
    start = time.time()
    last_progress = start

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(validate_url, u): u for u in urls}
        for fut in as_completed(futs):
            u = futs[fut]
            try:
                results[u] = fut.result()
            except Exception:
                results[u] = False
            done += 1
            now = time.time()
            if now - last_progress >= 30 or done == total:
                elapsed = now - start
                print(f"  [validate] {done}/{total} URLs checked  ({elapsed:.0f}s elapsed)")
                last_progress = now
    return results


# ── main audit ───────────────────────────────────────────────────────────────

def main():
    t0 = time.time()

    # 1. Load data
    print("Loading website dataset …")
    with open(CAMELLIAS_JSON) as f:
        entries = json.load(f)
    print(f"  {len(entries)} entries loaded.")

    print("Loading ACS catalog …")
    with open(ACS_CATALOG) as f:
        catalog = json.load(f)
    acs_records = catalog["records"]
    print(f"  {len(acs_records)} ACS records loaded.")

    # 2. Build normalised ACS lookup  {norm_name: record}
    acs_lookup: dict[str, dict] = {}
    acs_dupes = []
    for rec in acs_records:
        nk = normalize_name(rec["name"])
        if nk in acs_lookup:
            acs_dupes.append((rec["name"], acs_lookup[nk]["name"]))
        else:
            acs_lookup[nk] = rec

    if acs_dupes:
        print(f"  WARNING: {len(acs_dupes)} duplicate normalised ACS names (keeping first).")

    # 3. Classify each website entry
    added = []       # no acs_url → will add
    replaced = []    # has acs_url but differs from canonical → will replace
    kept = []        # has correct acs_url already
    unmatched = []   # no ACS match

    for entry in entries:
        nk = normalize_name(entry["name"])
        current = entry.get("acs_url") or None
        acs_rec = acs_lookup.get(nk)

        if acs_rec is None:
            # No exact ACS match
            unmatched.append({"name": entry["name"], "current_acs_url": current})
            continue

        canonical_url = acs_rec["url"]

        if current is None:
            added.append({
                "name": entry["name"],
                "acs_url": canonical_url,
                "acs_catalog_name": acs_rec["name"],
            })
        elif current == canonical_url:
            kept.append({"name": entry["name"], "acs_url": current})
        else:
            replaced.append({
                "name": entry["name"],
                "old_acs_url": current,
                "new_acs_url": canonical_url,
                "acs_catalog_name": acs_rec["name"],
            })

    print(f"\n── Classification ──")
    print(f"  Kept (already correct) : {len(kept)}")
    print(f"  To add                 : {len(added)}")
    print(f"  To replace             : {len(replaced)}")
    print(f"  Unmatched (no ACS)     : {len(unmatched)}")

    # 4. Validate candidate URLs (added + replaced new URLs)
    urls_to_check = list({r["acs_url"] for r in added} | {r["new_acs_url"] for r in replaced})
    print(f"\nValidating {len(urls_to_check)} unique candidate ACS URLs …")
    validity = batch_validate(urls_to_check, workers=10)
    valid_count = sum(1 for v in validity.values() if v)
    invalid_count = len(validity) - valid_count
    print(f"  Valid: {valid_count}  Invalid: {invalid_count}")

    # Filter to only valid
    added_valid = [r for r in added if validity.get(r["acs_url"], False)]
    added_invalid = [r for r in added if not validity.get(r["acs_url"], False)]
    replaced_valid = [r for r in replaced if validity.get(r["new_acs_url"], False)]
    replaced_invalid = [r for r in replaced if not validity.get(r["new_acs_url"], False)]

    print(f"\n── After validation ──")
    print(f"  Added (valid)          : {len(added_valid)}")
    print(f"  Added (invalid, skip)  : {len(added_invalid)}")
    print(f"  Replaced (valid)       : {len(replaced_valid)}")
    print(f"  Replaced (invalid,skip): {len(replaced_invalid)}")

    # 5. Apply changes to entries
    # Build quick lookup by normalised name for the actions
    add_map = {normalize_name(r["name"]): r["acs_url"] for r in added_valid}
    replace_map = {normalize_name(r["name"]): r["new_acs_url"] for r in replaced_valid}

    changes_made = 0
    for entry in entries:
        nk = normalize_name(entry["name"])
        if nk in add_map:
            entry["acs_url"] = add_map[nk]
            changes_made += 1
        elif nk in replace_map:
            entry["acs_url"] = replace_map[nk]
            changes_made += 1

    print(f"\n  Applied {changes_made} changes to in-memory dataset.")

    # 6. Write updated camellias.json
    print("Writing camellias.json …")
    with open(CAMELLIAS_JSON, "w") as f:
        json.dump(entries, f, indent=2, ensure_ascii=False)
        f.write("\n")

    # 7. Write per-letter files
    print("Writing per-letter JSON files …")
    letter_buckets: dict[str, list] = {}
    for entry in entries:
        first = entry["name"][0].upper() if entry["name"] else "Z"
        if first not in string.ascii_uppercase:
            first = "Z"  # fallback
        letter_buckets.setdefault(first, []).append(entry)

    for letter, bucket in sorted(letter_buckets.items()):
        path = DATA / f"{letter}.json"
        with open(path, "w") as f:
            json.dump(bucket, f, indent=2, ensure_ascii=False)
            f.write("\n")

    # 8. Rebuild index.json (name, species, chinese_name, japanese_name only)
    print("Writing index.json …")
    index = []
    for entry in entries:
        index.append({
            "name": entry["name"],
            "species": entry.get("species", ""),
            "chinese_name": entry.get("chinese_name", []),
            "japanese_name": entry.get("japanese_name", []),
        })
    with open(INDEX_JSON, "w") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)
        f.write("\n")

    # 9. Write audit reports
    print("Writing audit reports …")

    full_audit = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "total_entries": len(entries),
        "acs_catalog_size": len(acs_records),
        "kept": len(kept),
        "added_valid": len(added_valid),
        "added_invalid_skipped": len(added_invalid),
        "replaced_valid": len(replaced_valid),
        "replaced_invalid_skipped": len(replaced_invalid),
        "unmatched": len(unmatched),
        "kept_records": kept,
        "added_records": added_valid,
        "added_invalid_records": added_invalid,
        "replaced_records": replaced_valid,
        "replaced_invalid_records": replaced_invalid,
        "unmatched_records": unmatched,
    }
    with open(REPO / "acs_backfill_full_audit.json", "w") as f:
        json.dump(full_audit, f, indent=2, ensure_ascii=False)
        f.write("\n")

    with open(REPO / "acs_backfill_added.json", "w") as f:
        json.dump(added_valid, f, indent=2, ensure_ascii=False)
        f.write("\n")

    with open(REPO / "acs_backfill_replaced.json", "w") as f:
        json.dump(replaced_valid, f, indent=2, ensure_ascii=False)
        f.write("\n")

    with open(REPO / "acs_backfill_unmatched.json", "w") as f:
        json.dump(unmatched, f, indent=2, ensure_ascii=False)
        f.write("\n")

    summary_lines = [
        "ACS Backfill Audit Summary",
        "=" * 40,
        f"Date               : {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Website entries     : {len(entries)}",
        f"ACS catalog records : {len(acs_records)}",
        "",
        "Results:",
        f"  Already correct   : {len(kept)}",
        f"  Added (valid)     : {len(added_valid)}",
        f"  Added (invalid)   : {len(added_invalid)}",
        f"  Replaced (valid)  : {len(replaced_valid)}",
        f"  Replaced (invalid): {len(replaced_invalid)}",
        f"  Unmatched (no ACS): {len(unmatched)}",
        "",
        f"Total changes applied: {changes_made}",
        f"Runtime: {time.time() - t0:.1f}s",
    ]
    with open(REPO / "acs_backfill_summary.txt", "w") as f:
        f.write("\n".join(summary_lines) + "\n")

    print("\n" + "\n".join(summary_lines))
    print(f"\nDone in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
