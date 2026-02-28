#!/usr/bin/env python3
"""
Targeted ACS image backfill for entries whose ACS links were just fixed.

Scope: entries from acs_backfill_added.json and acs_backfill_replaced.json.
Only fills images that are missing/empty/null. Never overwrites existing images.
"""
import json
import os
import re
import glob
import time
import urllib.request

REPO = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(REPO, "data")

ADDED_FILE = os.path.join(REPO, "acs_backfill_added.json")
REPLACED_FILE = os.path.join(REPO, "acs_backfill_replaced.json")

OUT_SCOPED = os.path.join(REPO, "acs_image_backfill_scoped.json")
OUT_UPDATED = os.path.join(REPO, "acs_image_backfill_updated.json")
OUT_SKIPPED = os.path.join(REPO, "acs_image_backfill_skipped.json")
OUT_SUMMARY = os.path.join(REPO, "acs_image_backfill_summary.txt")

# Skip these patterns when extracting images from ACS pages
SKIP_PATTERNS = [
    "logo", "icon", "social", "facebook", "twitter", "button",
    "banner", "header", "footer", "sprite", "gravatar",
    "sites/default/files/styles", "content/images",
]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_acs_image(acs_url):
    """Fetch an ACS page and extract the camellia image URL."""
    try:
        req = urllib.request.Request(acs_url)
        req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        resp = urllib.request.urlopen(req, timeout=15)
        html = resp.read().decode("utf-8", errors="ignore")

        imgs = re.findall(r'<img[^>]*src="([^"]+\.(?:jpg|jpeg|png))"', html, re.IGNORECASE)
        for src in imgs:
            if any(skip in src.lower() for skip in SKIP_PATTERNS):
                continue
            full_url = src if src.startswith("http") else acs_url.rsplit("/", 1)[0] + "/" + src
            return full_url
    except Exception as e:
        log(f"  Error fetching {acs_url}: {e}")
    return None


def load_dataset_lookup():
    """Build name → (letter_file, entry) lookup from A-Z files."""
    lookup = {}
    for letter_file in sorted(glob.glob(os.path.join(DATA_DIR, "*.json"))):
        fname = os.path.basename(letter_file)
        if fname in ("index.json", "camellias.json", "bugs.json"):
            continue
        letter = fname.replace(".json", "")
        if len(letter) != 1 or not letter.isalpha():
            continue
        with open(letter_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        for entry in data:
            lookup[entry["name"].strip()] = (letter_file, entry)
    return lookup


def sync_to_dataset(updated_entries):
    """Update image fields in A-Z files, then regenerate aggregated files."""
    if not updated_entries:
        log("No images to sync.")
        return

    updates = {e["name"]: (e["image"], e["imageSource"]) for e in updated_entries}

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
                entry["imageSource"] = img_src
                changed = True
                updated_count += 1

        if changed:
            data.sort(key=lambda e: e["name"].strip().lower())
            with open(letter_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.write("\n")

    log(f"Updated {updated_count} entries in A-Z files")

    # Regenerate aggregated files
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

    with open(os.path.join(DATA_DIR, "camellias.json"), "w", encoding="utf-8") as f:
        json.dump(all_camellias, f, indent=2, ensure_ascii=False)
        f.write("\n")

    with open(os.path.join(REPO, "camellias.json"), "w", encoding="utf-8") as f:
        json.dump(all_camellias, f, indent=2, ensure_ascii=False)
        f.write("\n")

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


def main():
    log("=== ACS IMAGE BACKFILL (SCOPED TO FIXED ACS LINKS) ===")

    # Load scoped entries
    with open(ADDED_FILE, "r") as f:
        added = json.load(f)
    with open(REPLACED_FILE, "r") as f:
        replaced = json.load(f)

    # Build scoped list: name → acs_url
    scoped = {}
    for e in added:
        scoped[e["name"]] = e["acs_url"]
    for e in replaced:
        scoped[e["name"]] = e["new_acs_url"]

    log(f"Scoped entries: {len(scoped)} ({len(added)} added + {len(replaced)} replaced)")

    # Load dataset
    lookup = load_dataset_lookup()

    # Process each scoped entry
    scoped_out = []
    updated_out = []
    skipped_out = []

    for i, (name, acs_url) in enumerate(scoped.items(), 1):
        log(f"[{i}/{len(scoped)}] {name}")

        entry_info = lookup.get(name)
        if not entry_info:
            log(f"  NOT FOUND in dataset — skipping")
            skipped_out.append({"name": name, "reason": "not found in dataset"})
            scoped_out.append({"name": name, "acs_url": acs_url, "action": "not_found"})
            continue

        _, entry = entry_info
        current_image = entry.get("image")

        scoped_entry = {
            "name": name,
            "acs_url": acs_url,
            "current_image": current_image,
            "current_imageSource": entry.get("imageSource"),
        }

        # If image already exists, skip
        if current_image:
            log(f"  Image already exists ({entry.get('imageSource', 'unknown')}) — skipping")
            skipped_out.append({
                "name": name,
                "reason": "image already exists",
                "existing_image": current_image,
                "existing_source": entry.get("imageSource"),
            })
            scoped_entry["action"] = "skipped_has_image"
            scoped_out.append(scoped_entry)
            continue

        # Try to extract image from ACS page
        time.sleep(1)  # polite rate-limit
        img_url = fetch_acs_image(acs_url)

        if img_url:
            log(f"  FOUND: {img_url[:80]}")
            updated_out.append({
                "name": name,
                "image": img_url,
                "imageSource": "acs",
                "acs_url": acs_url,
            })
            scoped_entry["action"] = "updated"
            scoped_entry["new_image"] = img_url
        else:
            log(f"  No image on ACS page")
            skipped_out.append({
                "name": name,
                "reason": "no image on ACS page",
                "acs_url": acs_url,
            })
            scoped_entry["action"] = "no_acs_image"

        scoped_out.append(scoped_entry)

    # Write output files
    with open(OUT_SCOPED, "w") as f:
        json.dump(scoped_out, f, indent=2, ensure_ascii=False)
        f.write("\n")
    with open(OUT_UPDATED, "w") as f:
        json.dump(updated_out, f, indent=2, ensure_ascii=False)
        f.write("\n")
    with open(OUT_SKIPPED, "w") as f:
        json.dump(skipped_out, f, indent=2, ensure_ascii=False)
        f.write("\n")

    # Summary
    n_updated = len(updated_out)
    n_skipped_has_image = sum(1 for s in skipped_out if s.get("reason") == "image already exists")
    n_no_acs_image = sum(1 for s in skipped_out if s.get("reason") == "no image on ACS page")
    n_not_found = sum(1 for s in skipped_out if s.get("reason") == "not found in dataset")

    summary_lines = [
        "ACS IMAGE BACKFILL SUMMARY (SCOPED)",
        "=" * 50,
        f"Total scoped entries:     {len(scoped)}",
        f"  from acs_backfill_added:    {len(added)}",
        f"  from acs_backfill_replaced: {len(replaced)}",
        "",
        f"Images added:             {n_updated}",
        f"Skipped (already has image): {n_skipped_has_image}",
        f"Skipped (no ACS image):   {n_no_acs_image}",
        f"Skipped (not in dataset): {n_not_found}",
        "=" * 50,
    ]
    if updated_out:
        summary_lines.append("")
        summary_lines.append("Updated entries:")
        for u in updated_out:
            summary_lines.append(f"  - {u['name']}: {u['image'][:80]}")

    summary_text = "\n".join(summary_lines)
    with open(OUT_SUMMARY, "w") as f:
        f.write(summary_text + "\n")
    log("\n" + summary_text)

    # Sync to dataset
    if updated_out:
        log("\nSyncing images to dataset...")
        sync_to_dataset(updated_out)

    log("\n=== DONE ===")


if __name__ == "__main__":
    main()
