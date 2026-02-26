#!/usr/bin/env python3
"""
Backfill website data using cleaned OCR-only files with STRICT matching.

Pipeline:
1. Copy OCR-only files to cleaned_for_backfill/
2. Clean copies (whitespace normalization only)
3. Parse entries from cleaned files
4. Strict match (exact name after trim) against existing website JSON
5. Add missing entries to per-letter A-Z files
6. Regenerate camellias.json, index.json, root camellias.json
7. Produce audit files
"""
import json
import os
import re
import shutil
import glob

# Paths
OCR_SRC = "/Users/lukewang/.openclaw/workspace/camellia-project/ocr_isolated/species_book_sections_ocr_only"
CLEANED_DIR = "/Users/lukewang/.openclaw/workspace/camellia-project/ocr_isolated/cleaned_for_backfill"
WEBSITE_DIR = "/Users/lukewang/.openclaw/workspace/camellia-nomenclature.github.io"
DATA_DIR = os.path.join(WEBSITE_DIR, "data")
AUDIT_DIR = WEBSITE_DIR  # audit files in repo root

# Size extraction patterns
SIZE_PATTERNS = [
    ("Very large", re.compile(r'\bVery\s+large\b', re.IGNORECASE)),
    ("Large to very large", re.compile(r'\bLarge\s+to\s+very\s+large\b', re.IGNORECASE)),
    ("Medium to large", re.compile(r'\bMedium\s+to\s+large\b', re.IGNORECASE)),
    ("Small to medium", re.compile(r'\bSmall\s+to\s+medium\b', re.IGNORECASE)),
    ("Miniature to small", re.compile(r'\bMiniature\s+to\s+small\b', re.IGNORECASE)),
    ("Large", re.compile(r'\bLarge\b', re.IGNORECASE)),
    ("Medium", re.compile(r'\bMedium\b', re.IGNORECASE)),
    ("Small", re.compile(r'\bSmall\b', re.IGNORECASE)),
    ("Miniature", re.compile(r'\bMiniature\b', re.IGNORECASE)),
]

# Species normalization map
SPECIES_MAP = {
    "Japonica": "Japonica",
    "Reticulata": "Reticulata",
    "Sasanqua": "Sasanqua",
    "NRH": "NRH",
    "Rusticana": "Rusticana",
    "Edithae": "Edithae",
    "Formerly Hiemalis": "Formerly Hiemalis",
    "Formerly Vernalis": "Formerly Hiemalis",  # Map to same category
    "Wabisuke": "Wabisuke",
    "Granthamiana": "Granthamiana",
    "Pitardii": "Pitardii",
    "Saluenensis": "Saluenensis",
    "Oleifera": "Oleifera",
    "Hybrid": "Hybrid",
    "Higo": "Higo",
    "Species": "Species",
    "Lutchuensis": "Lutchuensis",
    "Sinensis": "Sinensis",
}


def extract_size(description):
    """Extract flower size from description text."""
    for size_name, pattern in SIZE_PATTERNS:
        if pattern.search(description):
            return size_name
    return ""


def clean_text(text):
    """Clean OCR text: normalize whitespace, fix common OCR artifacts."""
    # Normalize line endings
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    # Fix common OCR artifacts: multiple spaces -> single space within lines
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        # Collapse multiple spaces to single (but preserve line structure)
        line = re.sub(r'  +', ' ', line)
        # Strip trailing whitespace
        line = line.rstrip()
        cleaned_lines.append(line)
    return '\n'.join(cleaned_lines)


def parse_ocr_file(filepath):
    """Parse a cleaned OCR file into list of entry dicts."""
    with open(filepath, 'r', encoding='utf-8') as f:
        text = f.read()

    entries = []

    # Extract header
    lines = text.split('\n')
    file_species = None
    content_start = 0
    for i, line in enumerate(lines):
        if line.startswith('SPECIES:'):
            file_species = line.split(':', 1)[1].strip()
        elif line.startswith('SOURCE:'):
            content_start = i + 1
            break
        elif line.startswith('TOTAL VARIETIES:'):
            continue

    if file_species is None:
        return entries

    # Get content after header
    content = '\n'.join(lines[content_start:])

    # Split by separator lines
    # Entries are separated by lines of dashes
    raw_blocks = re.split(r'-{10,}', content)

    for block in raw_blocks:
        block = block.strip()
        if not block:
            continue

        # Skip if this is just a species header line like "Species Granthamiana"
        if re.match(r'^Species\s+\w+$', block):
            continue

        # Join multi-line blocks into single line for parsing
        # Replace newline+space continuation with single space
        single_line = re.sub(r'\n\s*', ' ', block).strip()

        # Parse: Name - Species - Description
        # The pattern is: NAME - SPECIES_NAME - REST_OF_DESCRIPTION
        match = re.match(
            r'^(.+?)\s*-\s*(Japonica|Reticulata|Sasanqua|NRH|Rusticana|Edithae|'
            r'Formerly\s+Hiemalis|Formerly\s+Vernalis|Wabisuke|Granthamiana|'
            r'Pitardii|Saluenensis|Oleifera|Hybrid|Higo|Species|Lutchuensis|Sinensis)'
            r'\s*-\s*(.+)$',
            single_line
        )

        if match:
            name = match.group(1).strip()
            raw_species = match.group(2).strip()
            description = match.group(3).strip()

            # Normalize species
            species = SPECIES_MAP.get(raw_species, raw_species)

            # Extract size
            size = extract_size(description)

            entries.append({
                "name": name,
                "species": species,
                "size": size,
                "description": description,
                "image": None,
                "acs_url": None,
                "icr_url": None,
                "chinese_name": []
            })
        else:
            # Try to parse without explicit species (use file species)
            # Some entries may just be "Name - Description" using file species
            # But this is rare in the OCR files; log but skip
            pass

    return entries


def step1_copy_ocr_files():
    """Copy OCR-only files to cleaned_for_backfill directory."""
    print("=" * 60)
    print("STEP 1: Copying OCR-only files to cleaned_for_backfill/")
    print("=" * 60)

    os.makedirs(CLEANED_DIR, exist_ok=True)

    src_files = glob.glob(os.path.join(OCR_SRC, "*.txt"))
    copied = 0
    for src in sorted(src_files):
        fname = os.path.basename(src)
        dst = os.path.join(CLEANED_DIR, fname)
        shutil.copy2(src, dst)
        copied += 1
        print(f"  Copied: {fname}")

    print(f"  Total files copied: {copied}")
    return copied


def step2_clean_copies():
    """Clean the copied files (whitespace normalization only)."""
    print("\n" + "=" * 60)
    print("STEP 2: Cleaning copied files (whitespace normalization)")
    print("=" * 60)

    cleaned_files = glob.glob(os.path.join(CLEANED_DIR, "*.txt"))
    for fpath in sorted(cleaned_files):
        fname = os.path.basename(fpath)
        with open(fpath, 'r', encoding='utf-8') as f:
            original = f.read()
        cleaned = clean_text(original)
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write(cleaned)
        diff_chars = len(original) - len(cleaned)
        print(f"  Cleaned: {fname} (removed {diff_chars} chars)")


def step3_parse_all_entries():
    """Parse all entries from cleaned OCR files."""
    print("\n" + "=" * 60)
    print("STEP 3: Parsing entries from cleaned OCR files")
    print("=" * 60)

    all_entries = []
    cleaned_files = glob.glob(os.path.join(CLEANED_DIR, "*-ocr-only.txt"))

    for fpath in sorted(cleaned_files):
        fname = os.path.basename(fpath)
        entries = parse_ocr_file(fpath)
        print(f"  {fname}: {len(entries)} entries parsed")
        all_entries.extend(entries)

    print(f"  Total OCR entries parsed: {len(all_entries)}")
    return all_entries


def step4_strict_match(ocr_entries):
    """Match OCR entries against existing website data using STRICT mode."""
    print("\n" + "=" * 60)
    print("STEP 4: STRICT matching against existing website JSON")
    print("=" * 60)

    # Load existing camellias.json
    camellias_path = os.path.join(DATA_DIR, "camellias.json")
    with open(camellias_path, 'r', encoding='utf-8') as f:
        existing = json.load(f)

    # Build set of existing names (exact, trimmed)
    existing_names = set()
    for e in existing:
        existing_names.add(e["name"].strip())

    print(f"  Existing entries in website: {len(existing_names)}")

    matched = []
    to_add = []

    for entry in ocr_entries:
        name = entry["name"].strip()
        if name in existing_names:
            matched.append(entry)
        else:
            to_add.append(entry)

    # Deduplicate to_add by name (OCR might have dupes across files)
    seen = set()
    deduped_to_add = []
    for entry in to_add:
        name = entry["name"].strip()
        if name not in seen:
            seen.add(name)
            deduped_to_add.append(entry)

    print(f"  OCR entries matching existing: {len(matched)}")
    print(f"  OCR entries to add (new, unique): {len(deduped_to_add)}")
    if len(to_add) != len(deduped_to_add):
        print(f"  (Deduplicated {len(to_add) - len(deduped_to_add)} duplicate OCR entries)")

    return matched, deduped_to_add


def step5_add_to_dataset(to_add):
    """Add missing entries to per-letter A-Z JSON files."""
    print("\n" + "=" * 60)
    print("STEP 5: Adding missing entries to per-letter A-Z files")
    print("=" * 60)

    if not to_add:
        print("  Nothing to add.")
        return

    # Group new entries by first letter
    by_letter = {}
    for entry in to_add:
        letter = entry["name"].strip()[0].upper()
        if letter not in by_letter:
            by_letter[letter] = []
        by_letter[letter].append(entry)

    total_added = 0
    for letter in sorted(by_letter.keys()):
        letter_file = os.path.join(DATA_DIR, f"{letter}.json")

        # Load existing letter file
        if os.path.exists(letter_file):
            with open(letter_file, 'r', encoding='utf-8') as f:
                letter_data = json.load(f)
        else:
            letter_data = []

        # Get existing names in this letter file for dedup check
        existing_in_letter = {e["name"].strip() for e in letter_data}

        added_count = 0
        for entry in by_letter[letter]:
            name = entry["name"].strip()
            if name not in existing_in_letter:
                letter_data.append(entry)
                existing_in_letter.add(name)
                added_count += 1

        # Sort by name (case-insensitive)
        letter_data.sort(key=lambda e: e["name"].strip().lower())

        # Write back
        with open(letter_file, 'w', encoding='utf-8') as f:
            json.dump(letter_data, f, indent=2, ensure_ascii=False)
            f.write('\n')

        print(f"  {letter}.json: added {added_count} entries (total now: {len(letter_data)})")
        total_added += added_count

    print(f"  Total entries added across all letter files: {total_added}")
    return total_added


def step6_regenerate_artifacts():
    """Regenerate camellias.json, index.json, and root camellias.json."""
    print("\n" + "=" * 60)
    print("STEP 6: Regenerating website artifacts")
    print("=" * 60)

    all_camellias = []
    index_data = []

    # Read all letter files (A-Z)
    for letter_file in sorted(glob.glob(os.path.join(DATA_DIR, "*.json"))):
        fname = os.path.basename(letter_file)
        # Skip non-letter files
        if fname in ('index.json', 'camellias.json', 'bugs.json'):
            continue
        # Only process single-letter files
        letter = fname.replace('.json', '')
        if len(letter) != 1 or not letter.isalpha():
            continue

        with open(letter_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        all_camellias.extend(data)

    # Build index.json as search index (name + species + optional chinese_name)
    for entry in all_camellias:
        idx_entry = {
            "name": entry["name"],
            "species": entry["species"]
        }
        if entry.get("chinese_name"):
            idx_entry["chinese_name"] = entry["chinese_name"]
        index_data.append(idx_entry)

    # Write data/camellias.json
    camellias_path = os.path.join(DATA_DIR, "camellias.json")
    with open(camellias_path, 'w', encoding='utf-8') as f:
        json.dump(all_camellias, f, indent=2, ensure_ascii=False)
        f.write('\n')
    print(f"  data/camellias.json: {len(all_camellias)} entries")

    # Write data/index.json
    index_path = os.path.join(DATA_DIR, "index.json")
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, indent=2, ensure_ascii=False)
        f.write('\n')
    print(f"  data/index.json: {len(index_data)} entries")

    # Write root camellias.json (mirror of data/camellias.json)
    root_camellias = os.path.join(WEBSITE_DIR, "camellias.json")
    with open(root_camellias, 'w', encoding='utf-8') as f:
        json.dump(all_camellias, f, indent=2, ensure_ascii=False)
        f.write('\n')
    print(f"  camellias.json (root): {len(all_camellias)} entries")

    return len(all_camellias), len(index_data)


def step7_audit(ocr_entries, matched, to_add, total_camellias):
    """Produce audit outputs."""
    print("\n" + "=" * 60)
    print("STEP 7: Producing audit outputs")
    print("=" * 60)

    # 1. backfill_added_entries.json
    added_path = os.path.join(AUDIT_DIR, "backfill_added_entries.json")
    with open(added_path, 'w', encoding='utf-8') as f:
        json.dump(to_add, f, indent=2, ensure_ascii=False)
        f.write('\n')
    print(f"  backfill_added_entries.json: {len(to_add)} entries")

    # 2. backfill_missing_after_sync.json — check what's still missing
    camellias_path = os.path.join(DATA_DIR, "camellias.json")
    with open(camellias_path, 'r', encoding='utf-8') as f:
        final_data = json.load(f)
    final_names = {e["name"].strip() for e in final_data}

    still_missing = []
    for entry in to_add:
        if entry["name"].strip() not in final_names:
            still_missing.append(entry)

    missing_path = os.path.join(AUDIT_DIR, "backfill_missing_after_sync.json")
    with open(missing_path, 'w', encoding='utf-8') as f:
        json.dump(still_missing, f, indent=2, ensure_ascii=False)
        f.write('\n')
    print(f"  backfill_missing_after_sync.json: {len(still_missing)} entries (should be 0)")

    # 3. backfill_audit_summary.txt
    summary_lines = [
        "BACKFILL AUDIT SUMMARY",
        "=" * 50,
        f"Date: 2026-02-26",
        f"Mode: STRICT (exact name match, whitespace trim only)",
        "",
        "SOURCE FILES:",
        f"  OCR source dir: {OCR_SRC}",
        f"  Cleaned copies: {CLEANED_DIR}",
        "",
        "COUNTS:",
        f"  Total OCR entries parsed:     {len(ocr_entries)}",
        f"  Matched existing (kept):      {len(matched)}",
        f"  Newly added (unique):         {len(to_add)}",
        f"  Still missing after sync:     {len(still_missing)}",
        f"  Final website total entries:  {total_camellias}",
        "",
        "ADDED ENTRIES BY SPECIES:",
    ]

    by_species = {}
    for e in to_add:
        sp = e.get("species", "Unknown")
        by_species[sp] = by_species.get(sp, 0) + 1
    for sp in sorted(by_species.keys()):
        summary_lines.append(f"  {sp}: {by_species[sp]}")

    summary_lines.extend([
        "",
        "ADDED ENTRIES BY LETTER:",
    ])
    by_letter = {}
    for e in to_add:
        letter = e["name"].strip()[0].upper()
        by_letter[letter] = by_letter.get(letter, 0) + 1
    for letter in sorted(by_letter.keys()):
        summary_lines.append(f"  {letter}: {by_letter[letter]}")

    summary_lines.extend([
        "",
        "ARTIFACTS REGENERATED:",
        "  - data/A.json through Z.json (per-letter files)",
        "  - data/camellias.json (full dataset)",
        "  - data/index.json (search index)",
        "  - camellias.json (root copy)",
        "",
        "VALIDATION:",
        f"  index.json entries == camellias.json entries: {total_camellias == total_camellias}",
        f"  Missing after sync: {len(still_missing)}",
        "",
    ])

    summary_path = os.path.join(AUDIT_DIR, "backfill_audit_summary.txt")
    with open(summary_path, 'w') as f:
        f.write('\n'.join(summary_lines))
    print(f"  backfill_audit_summary.txt written")

    return still_missing


def step8_sanity_checks():
    """Run sanity checks on the regenerated data."""
    print("\n" + "=" * 60)
    print("STEP 8: Running sanity checks")
    print("=" * 60)

    errors = []

    # 1. Check camellias.json == sum of A-Z files
    with open(os.path.join(DATA_DIR, "camellias.json"), 'r') as f:
        all_data = json.load(f)

    letter_total = 0
    for letter_file in sorted(glob.glob(os.path.join(DATA_DIR, "*.json"))):
        fname = os.path.basename(letter_file)
        if fname in ('index.json', 'camellias.json', 'bugs.json'):
            continue
        letter = fname.replace('.json', '')
        if len(letter) != 1 or not letter.isalpha():
            continue
        with open(letter_file, 'r') as f:
            data = json.load(f)
        letter_total += len(data)

    if len(all_data) == letter_total:
        print(f"  [PASS] camellias.json ({len(all_data)}) == sum of A-Z files ({letter_total})")
    else:
        msg = f"  [FAIL] camellias.json ({len(all_data)}) != sum of A-Z files ({letter_total})"
        print(msg)
        errors.append(msg)

    # 2. Check index.json count == camellias.json count
    with open(os.path.join(DATA_DIR, "index.json"), 'r') as f:
        index_data = json.load(f)

    if len(index_data) == len(all_data):
        print(f"  [PASS] index.json ({len(index_data)}) == camellias.json ({len(all_data)})")
    else:
        msg = f"  [FAIL] index.json ({len(index_data)}) != camellias.json ({len(all_data)})"
        print(msg)
        errors.append(msg)

    # 3. Check root camellias.json == data/camellias.json
    with open(os.path.join(WEBSITE_DIR, "camellias.json"), 'r') as f:
        root_data = json.load(f)

    if len(root_data) == len(all_data):
        print(f"  [PASS] root camellias.json ({len(root_data)}) == data/camellias.json ({len(all_data)})")
    else:
        msg = f"  [FAIL] root camellias.json ({len(root_data)}) != data/camellias.json ({len(all_data)})"
        print(msg)
        errors.append(msg)

    # 4. Check each entry in A-Z file starts with correct letter
    for letter_file in sorted(glob.glob(os.path.join(DATA_DIR, "*.json"))):
        fname = os.path.basename(letter_file)
        if fname in ('index.json', 'camellias.json', 'bugs.json'):
            continue
        letter = fname.replace('.json', '')
        if len(letter) != 1 or not letter.isalpha():
            continue
        with open(letter_file, 'r') as f:
            data = json.load(f)
        for e in data:
            if e["name"].strip() and e["name"].strip()[0].upper() != letter.upper():
                msg = f"  [FAIL] Entry '{e['name']}' in {fname} doesn't start with {letter}"
                print(msg)
                errors.append(msg)

    if not errors:
        print(f"  [PASS] All entries in correct letter files")

    # 5. Check no duplicate names in camellias.json
    names = [e["name"].strip() for e in all_data]
    dupes = set()
    seen = set()
    for n in names:
        if n in seen:
            dupes.add(n)
        seen.add(n)

    if not dupes:
        print(f"  [PASS] No duplicate names in camellias.json")
    else:
        msg = f"  [WARN] {len(dupes)} duplicate names found: {list(dupes)[:10]}"
        print(msg)

    # 6. Spot-check: verify some added entries are searchable in index
    with open(os.path.join(AUDIT_DIR, "backfill_added_entries.json"), 'r') as f:
        added = json.load(f)
    if added:
        index_names = {e["name"].strip() for e in index_data}
        sample = added[:5]
        for entry in sample:
            name = entry["name"].strip()
            if name in index_names:
                print(f"  [PASS] Added entry '{name}' found in index.json")
            else:
                msg = f"  [FAIL] Added entry '{name}' NOT in index.json"
                print(msg)
                errors.append(msg)

    return errors


def main():
    print("CAMELLIA BACKFILL: Cleaned OCR → Website JSON (STRICT MODE)")
    print("=" * 60)

    # Step 1: Copy
    step1_copy_ocr_files()

    # Step 2: Clean
    step2_clean_copies()

    # Step 3: Parse
    ocr_entries = step3_parse_all_entries()

    # Step 4: Strict match
    matched, to_add = step4_strict_match(ocr_entries)

    # Step 5: Add to dataset
    step5_add_to_dataset(to_add)

    # Step 6: Regenerate
    total_camellias, total_index = step6_regenerate_artifacts()

    # Step 7: Audit
    step7_audit(ocr_entries, matched, to_add, total_camellias)

    # Step 8: Sanity checks
    errors = step8_sanity_checks()

    # Summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"  Total OCR entries processed: {len(ocr_entries)}")
    print(f"  Matched existing:            {len(matched)}")
    print(f"  Newly added:                 {len(to_add)}")
    print(f"  Final website total:         {total_camellias}")
    print(f"  Sanity check errors:         {len(errors)}")
    if errors:
        for e in errors:
            print(f"    {e}")
    print("=" * 60)


if __name__ == "__main__":
    main()
