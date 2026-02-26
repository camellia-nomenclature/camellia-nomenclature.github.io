#!/usr/bin/env python3
"""
Deterministic, resumable image enrichment for 301 backfill entries.

Priority order (strict exact-name match only):
  1. SoCal/ACCS direct URL check (HEAD request)
  2. ACCS search POST
  3. ACS encyclopedia page scrape
  4. ICR API (rate-limited: 1 req / 8s)

Saves progress after every item. Fully resumable.
"""
import json
import os
import re
import sys
import time
import glob
import urllib.parse
import urllib.request

# ─── paths ────────────────────────────────────────────────────────
REPO = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(REPO, "data")
PROJECT_DIR = os.path.join(REPO, "..", "camellia-project")
INPUT_FILE = os.path.join(REPO, "backfill_added_entries.json")
PROGRESS_FILE = os.path.join(REPO, "image_enrichment_progress.json")
OUT_ADDED = os.path.join(REPO, "image_enrichment_added.json")
OUT_SKIPPED = os.path.join(REPO, "image_enrichment_skipped_ambiguous.json")
OUT_MISSING = os.path.join(REPO, "image_enrichment_missing.json")
OUT_SUMMARY = os.path.join(REPO, "image_enrichment_summary.txt")

# ─── source URLs ──────────────────────────────────────────────────
SOCAL_BASE = "https://www.atlanticcoastcamelliasociety.org/Camelliae%20Floris%20Bibliotheca/images/"
ACCS_SEARCH_URL = "https://www.atlanticcoastcamelliasociety.org/Camelliae%20Floris%20Bibliotheca/ACS%20CFB-search.php?go"
ACCS_BASE_IMG = "https://www.atlanticcoastcamelliasociety.org/Camelliae%20Floris%20Bibliotheca/"
ICR_API_URL = "https://camellia.iflora.cn/Cutivars/SearchPhotosList"

# Cached data from camellia-project
ACS_INDEX_FILE = os.path.join(PROJECT_DIR, "data", "acs_all_entries.json")
SOCAL_CACHE_FILE = os.path.join(PROJECT_DIR, "data", "image_urls.json")
ICR_CACHE_FILE = os.path.join(PROJECT_DIR, "data", "image_urls_icr_full.json")

# ICR rate-limit
ICR_DELAY = 8.0


# ─── name normalization ──────────────────────────────────────────
def normalize_name(name):
    """Fix OCR title-casing artifacts: Aaron'S Ruby → Aaron's Ruby."""
    # Fix 'S, 'T, etc. (apostrophe + capital letter that should be lowercase)
    result = re.sub(r"'([A-Z])\b", lambda m: "'" + m.group(1).lower(), name)
    return result

# ─── logging ──────────────────────────────────────────────────────
def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)


# ─── cached lookups (avoid network when possible) ────────────────
_socal_cache = None
_icr_cache = None

def load_socal_cache():
    global _socal_cache
    if _socal_cache is not None:
        return _socal_cache
    _socal_cache = {}
    if os.path.exists(SOCAL_CACHE_FILE):
        with open(SOCAL_CACHE_FILE, "r") as f:
            raw = json.load(f)
        for k, v in raw.items():
            _socal_cache[k.lower().strip()] = v  # v is URL or None
    log(f"SoCal cache loaded: {len(_socal_cache)} entries")
    return _socal_cache


def load_icr_cache():
    global _icr_cache
    if _icr_cache is not None:
        return _icr_cache
    _icr_cache = {}
    if os.path.exists(ICR_CACHE_FILE):
        with open(ICR_CACHE_FILE, "r") as f:
            raw = json.load(f)
        for k, v in raw.items():
            _icr_cache[k.lower().strip()] = v
    log(f"ICR cache loaded: {len(_icr_cache)} entries")
    return _icr_cache


# ─── source 1: SoCal direct HEAD ─────────────────────────────────
def try_socal(name):
    """Check SoCal cache first, then HEAD request with normalized name."""
    norm = normalize_name(name)
    cache = load_socal_cache()

    # Check cache (case-insensitive)
    cached = cache.get(norm.lower().strip()) or cache.get(name.lower().strip())
    if cached:
        return cached, "socal"

    # Live HEAD request with normalized name
    for try_name in dict.fromkeys([norm, name]):  # deduplicate, preserve order
        url = SOCAL_BASE + urllib.parse.quote(try_name + ".jpg")
        try:
            req = urllib.request.Request(url, method="HEAD")
            req.add_header("User-Agent", "Mozilla/5.0")
            resp = urllib.request.urlopen(req, timeout=8)
            if resp.status == 200:
                return url, "socal"
        except Exception:
            pass
    return None, None


# ─── source 2: ACCS search POST ──────────────────────────────────
def try_accs_search(name):
    """POST search with normalized name; accept only exact alt-text match."""
    norm = normalize_name(name)
    for try_name in dict.fromkeys([norm, name]):
        try:
            data = urllib.parse.urlencode({"name": try_name, "submit": "Search"}).encode()
            req = urllib.request.Request(ACCS_SEARCH_URL, data=data, method="POST")
            req.add_header("Content-Type", "application/x-www-form-urlencoded")
            req.add_header("User-Agent", "Mozilla/5.0")
            resp = urllib.request.urlopen(req, timeout=12)
            html = resp.read().decode("utf-8", errors="ignore")

            imgs = re.findall(
                r'<img[^>]*src="(images/[^"]+\.jpg)"[^>]*alt="([^"]*)"',
                html,
                re.IGNORECASE,
            )
            if not imgs:
                continue

            name_lower = try_name.lower().strip()
            for src, alt in imgs:
                if alt.lower().strip() == name_lower:
                    return ACCS_BASE_IMG + src.replace(" ", "%20"), "accs"

            # Results exist but none matched exactly → ambiguous
            return "AMBIGUOUS", None
        except Exception:
            continue
    return None, None


# ─── source 3: ACS encyclopedia ──────────────────────────────────
_acs_index = None

def load_acs_index():
    global _acs_index
    if _acs_index is not None:
        return _acs_index
    _acs_index = {}
    if os.path.exists(ACS_INDEX_FILE):
        with open(ACS_INDEX_FILE, "r") as f:
            raw = json.load(f)
        for acs_name, acs_url in raw.items():
            _acs_index[acs_name.lower().strip()] = (acs_name, acs_url)
    log(f"ACS index loaded: {len(_acs_index)} entries")
    return _acs_index


def try_acs(name):
    """Look up ACS page, fetch it, and extract image (exact name match only)."""
    idx = load_acs_index()
    norm = normalize_name(name)
    key = norm.lower().strip()
    if key not in idx:
        # Try original name as fallback
        key = name.lower().strip()
        if key not in idx:
            return None, None

    _, page_url = idx[key]
    try:
        req = urllib.request.Request(page_url)
        req.add_header("User-Agent", "Mozilla/5.0")
        resp = urllib.request.urlopen(req, timeout=15)
        html = resp.read().decode("utf-8", errors="ignore")

        imgs = re.findall(r'<img[^>]*src="([^"]+\.(?:jpg|jpeg|png))"', html, re.IGNORECASE)
        for src in imgs:
            if any(skip in src.lower() for skip in [
                "logo", "icon", "social", "facebook", "twitter", "button",
                "banner", "header", "footer", "sprite", "gravatar",
                "sites/default/files/styles",
            ]):
                continue
            full_url = src if src.startswith("http") else page_url.rsplit("/", 1)[0] + "/" + src
            return full_url, "acs"
    except Exception:
        pass
    return None, None


# ─── source 4: ICR API ───────────────────────────────────────────
_last_icr_time = 0.0

def try_icr(name):
    """Check ICR cache first, then query API. Hard rate-limit: 1 req / 8s."""
    global _last_icr_time
    norm = normalize_name(name)
    cache = load_icr_cache()

    # Check cache
    cached = cache.get(norm.lower().strip()) or cache.get(name.lower().strip())
    if cached:
        return cached, "icr"

    # Live API request
    elapsed = time.time() - _last_icr_time
    if elapsed < ICR_DELAY:
        time.sleep(ICR_DELAY - elapsed)

    for try_name in dict.fromkeys([norm, name]):
        try:
            params = urllib.parse.urlencode({"page": 1, "latin": try_name})
            url = f"{ICR_API_URL}?{params}"
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            _last_icr_time = time.time()
            resp = urllib.request.urlopen(req, timeout=15)

            if resp.status in (403, 429):
                log(f"  ICR blocked ({resp.status}) for {try_name}")
                return None, None

            body = json.loads(resp.read().decode("utf-8"))
            photos = body.get("data")
            if photos:
                default = next((p for p in photos if p.get("IsDefaultPhoto")), photos[0])
                img_url = default.get("ImageUrl", "")
                if img_url:
                    return img_url, "icr"
        except Exception as e:
            log(f"  ICR error for {try_name}: {e}")
    return None, None


# ─── progress ─────────────────────────────────────────────────────
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)


# ─── sync to dataset ─────────────────────────────────────────────
def sync_to_dataset(added_entries):
    """Update image field in per-letter A-Z files, then regenerate aggregated files."""
    if not added_entries:
        log("No images to sync.")
        return

    # Build lookup: name → (image_url, source)
    updates = {e["name"]: (e["image"], e.get("imageSource", "")) for e in added_entries}

    updated_count = 0
    for letter_file in sorted(glob.glob(os.path.join(DATA_DIR, "*.json"))):
        fname = os.path.basename(letter_file)
        if fname in ("index.json", "camellias.json", "bugs.json"):
            continue
        letter = fname.replace(".json", "")
        if len(letter) != 1 or not letter.isalpha():
            continue

        with open(letter_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        changed = False
        for entry in data:
            name = entry["name"].strip()
            if name in updates:
                img_url, img_src = updates[name]
                entry["image"] = img_url
                if img_src:
                    entry["imageSource"] = img_src
                changed = True
                updated_count += 1

        if changed:
            data.sort(key=lambda e: e["name"].strip().lower())
            with open(letter_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.write("\n")

    log(f"Updated {updated_count} entries in A-Z files")

    # Regenerate data/camellias.json, root camellias.json, data/index.json
    all_camellias = []
    for letter_file in sorted(glob.glob(os.path.join(DATA_DIR, "*.json"))):
        fname = os.path.basename(letter_file)
        if fname in ("index.json", "camellias.json", "bugs.json"):
            continue
        letter = fname.replace(".json", "")
        if len(letter) != 1 or not letter.isalpha():
            continue
        with open(letter_file, "r", encoding="utf-8") as f:
            all_camellias.extend(json.load(f))

    # data/camellias.json
    with open(os.path.join(DATA_DIR, "camellias.json"), "w", encoding="utf-8") as f:
        json.dump(all_camellias, f, indent=2, ensure_ascii=False)
        f.write("\n")

    # root camellias.json
    with open(os.path.join(REPO, "camellias.json"), "w", encoding="utf-8") as f:
        json.dump(all_camellias, f, indent=2, ensure_ascii=False)
        f.write("\n")

    # data/index.json
    index_data = []
    for entry in all_camellias:
        idx = {"name": entry["name"], "species": entry["species"]}
        if entry.get("chinese_name"):
            idx["chinese_name"] = entry["chinese_name"]
        index_data.append(idx)
    with open(os.path.join(DATA_DIR, "index.json"), "w", encoding="utf-8") as f:
        json.dump(index_data, f, indent=2, ensure_ascii=False)
        f.write("\n")

    log(f"Regenerated camellias.json ({len(all_camellias)}), index.json ({len(index_data)})")


# ─── main ─────────────────────────────────────────────────────────
def main():
    log("=== IMAGE ENRICHMENT (STRICT EXACT-NAME MATCH) ===")

    with open(INPUT_FILE, "r") as f:
        entries = json.load(f)
    log(f"Input: {len(entries)} entries from backfill_added_entries.json")

    progress = load_progress()
    log(f"Resuming from progress: {len(progress)} already processed")

    added = []
    skipped_ambiguous = []
    missing = []

    stats = {"socal": 0, "accs": 0, "acs": 0, "icr": 0, "ambiguous": 0, "missing": 0}

    for i, entry in enumerate(entries):
        name = entry["name"].strip()

        # Already processed?
        if name in progress:
            result = progress[name]
            if result["status"] == "found":
                added.append({"name": name, "image": result["image"], "imageSource": result["source"]})
                stats[result["source"]] += 1
            elif result["status"] == "ambiguous":
                skipped_ambiguous.append({"name": name, "reason": result.get("reason", "ambiguous ACCS match")})
                stats["ambiguous"] += 1
            else:
                missing.append({"name": name})
                stats["missing"] += 1
            continue

        log(f"[{i+1}/{len(entries)}] {name}")

        # Source 1: SoCal HEAD
        img_url, source = try_socal(name)
        if img_url:
            log(f"  FOUND (socal): {img_url[:80]}")
            progress[name] = {"status": "found", "image": img_url, "source": source}
            added.append({"name": name, "image": img_url, "imageSource": source})
            stats["socal"] += 1
            save_progress(progress)
            continue

        # Source 2: ACCS search
        img_url, source = try_accs_search(name)
        if img_url == "AMBIGUOUS":
            log(f"  SKIPPED (ambiguous ACCS match)")
            progress[name] = {"status": "ambiguous", "reason": "ambiguous ACCS match"}
            skipped_ambiguous.append({"name": name, "reason": "ambiguous ACCS match"})
            stats["ambiguous"] += 1
            save_progress(progress)
            continue
        if img_url:
            log(f"  FOUND (accs): {img_url[:80]}")
            progress[name] = {"status": "found", "image": img_url, "source": source}
            added.append({"name": name, "image": img_url, "imageSource": source})
            stats["accs"] += 1
            save_progress(progress)
            continue

        # Source 3: ACS encyclopedia
        img_url, source = try_acs(name)
        if img_url:
            log(f"  FOUND (acs): {img_url[:80]}")
            progress[name] = {"status": "found", "image": img_url, "source": source}
            added.append({"name": name, "image": img_url, "imageSource": source})
            stats["acs"] += 1
            save_progress(progress)
            continue

        # Source 4: ICR API (rate-limited)
        img_url, source = try_icr(name)
        if img_url:
            log(f"  FOUND (icr): {img_url[:80]}")
            progress[name] = {"status": "found", "image": img_url, "source": source}
            added.append({"name": name, "image": img_url, "imageSource": source})
            stats["icr"] += 1
            save_progress(progress)
            continue

        # No image found
        log(f"  MISSING (no source had image)")
        progress[name] = {"status": "missing"}
        missing.append({"name": name})
        stats["missing"] += 1
        save_progress(progress)

    # ─── write output files ────────────────────────────────────────
    with open(OUT_ADDED, "w") as f:
        json.dump(added, f, indent=2, ensure_ascii=False)
        f.write("\n")
    with open(OUT_SKIPPED, "w") as f:
        json.dump(skipped_ambiguous, f, indent=2, ensure_ascii=False)
        f.write("\n")
    with open(OUT_MISSING, "w") as f:
        json.dump(missing, f, indent=2, ensure_ascii=False)
        f.write("\n")

    summary_lines = [
        "IMAGE ENRICHMENT SUMMARY",
        "=" * 50,
        f"Total input entries:  {len(entries)}",
        f"Images found:         {len(added)}",
        f"  - SoCal:            {stats['socal']}",
        f"  - ACCS search:      {stats['accs']}",
        f"  - ACS encyclopedia: {stats['acs']}",
        f"  - ICR API:          {stats['icr']}",
        f"Skipped (ambiguous):  {stats['ambiguous']}",
        f"Missing (no source):  {stats['missing']}",
        "=" * 50,
    ]
    summary_text = "\n".join(summary_lines)
    with open(OUT_SUMMARY, "w") as f:
        f.write(summary_text + "\n")
    log("\n" + summary_text)

    # ─── sync to dataset ───────────────────────────────────────────
    if added:
        log("\nSyncing images to dataset...")
        sync_to_dataset(added)

    log("\n=== DONE ===")


if __name__ == "__main__":
    main()
