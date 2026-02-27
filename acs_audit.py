#!/usr/bin/env python3
"""
Full ACS link audit + fix for the camellia website dataset.

Checks every entry's acs_url for dead/broken/empty behavior.
Attempts recovery via deterministic slug lookup.
Clears acs_url if no valid link found.
Syncs all data artifacts and generates reports.
"""

import asyncio
import aiohttp
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from collections import defaultdict
from datetime import datetime

REPO = Path(__file__).parent
DATA_DIR = REPO / "data"
CAMELLIAS_FILE = DATA_DIR / "camellias.json"

ACS_BASE = "https://www.americancamellias.com/education-and-camellia-care/acs-camellia-encyclopedia"
CONCURRENCY = 20  # concurrent requests
TIMEOUT = 15  # seconds per request
PROGRESS_INTERVAL = 500  # report progress every N entries
EVENT_INTERVAL = 600  # openclaw event every N seconds (10 min)

# State file for resuming
STATE_FILE = REPO / "acs_audit_state.json"


def name_to_slug(name: str) -> str:
    """Convert cultivar name to ACS URL slug."""
    s = name.lower()
    s = s.replace("'", "")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    s = re.sub(r"-+", "-", s)
    return s


def name_to_acs_url(name: str) -> str:
    """Build deterministic ACS URL from cultivar name."""
    first_letter = name[0].lower()
    slug = name_to_slug(name)
    return f"{ACS_BASE}/camellias-beginning-with-{first_letter}/{slug}"


def send_progress_event(done: int, total: int):
    """Send progress event via openclaw CLI."""
    try:
        subprocess.run(
            ["openclaw", "system", "event", "--text",
             f"ACS audit progress: {done}/{total}", "--mode", "now"],
            capture_output=True, timeout=10
        )
    except Exception:
        pass


async def check_url(session: aiohttp.ClientSession, url: str) -> dict:
    """Check a single URL. Returns dict with status info."""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                               allow_redirects=True) as resp:
            # Read content length from headers or body
            content_length = resp.content_length
            if content_length is None:
                body = await resp.read()
                content_length = len(body)
            return {
                "status": resp.status,
                "content_length": content_length,
                "valid": resp.status == 200 and content_length > 0,
                "error": None,
            }
    except asyncio.TimeoutError:
        return {"status": None, "content_length": 0, "valid": False, "error": "timeout"}
    except aiohttp.ClientError as e:
        return {"status": None, "content_length": 0, "valid": False, "error": str(e)}
    except Exception as e:
        return {"status": None, "content_length": 0, "valid": False, "error": str(e)}


async def run_audit(data: list) -> dict:
    """Run the full audit on all entries with acs_url."""
    results = {}  # name -> audit result
    entries_with_url = [(i, e) for i, e in enumerate(data) if e.get("acs_url")]
    total = len(entries_with_url)
    done = 0
    last_event_time = time.time()

    print(f"Auditing {total} ACS URLs with concurrency={CONCURRENCY}...")

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, force_close=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def check_entry(idx: int, entry: dict):
            nonlocal done, last_event_time
            name = entry["name"]
            url = entry["acs_url"]

            async with semaphore:
                result = await check_url(session, url)

            result["index"] = idx
            result["name"] = name
            result["original_url"] = url
            result["action"] = "ok" if result["valid"] else "needs_fix"
            results[name] = result

            done += 1
            if done % PROGRESS_INTERVAL == 0 or done == total:
                elapsed = time.time() - last_event_time
                print(f"  [{done}/{total}] checked...")
                if elapsed >= EVENT_INTERVAL:
                    send_progress_event(done, total)
                    last_event_time = time.time()

        tasks = [check_entry(i, e) for i, e in entries_with_url]
        await asyncio.gather(*tasks)

    send_progress_event(done, total)
    return results


async def attempt_recovery(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore,
                           name: str, original_url: str) -> dict:
    """Try to recover a valid ACS URL for a cultivar name."""
    deterministic_url = name_to_acs_url(name)

    # Strategy 1: if deterministic differs from original, try it
    if deterministic_url != original_url:
        async with semaphore:
            result = await check_url(session, deterministic_url)
        if result["valid"]:
            return {
                "recovered": True,
                "new_url": deterministic_url,
                "method": "deterministic_slug",
            }

    # Strategy 2: Try some common slug variations
    slug = name_to_slug(name)
    first_letter = name[0].lower()
    base = f"{ACS_BASE}/camellias-beginning-with-{first_letter}"

    variations = set()

    # Try without parenthetical suffixes
    name_no_paren = re.sub(r"\s*\(.*?\)\s*", "", name).strip()
    if name_no_paren != name:
        variations.add(name_to_slug(name_no_paren))

    # Try replacing "Variegated" with "var"
    if "variegated" in slug:
        variations.add(slug.replace("variegated", "var"))

    # Try without trailing hyphens or with double hyphens
    if slug.endswith("-"):
        variations.add(slug.rstrip("-"))
    variations.add(slug + "-")

    # Try removing accented character artifacts
    clean = re.sub(r"[^a-z0-9-]", "", slug)
    if clean != slug:
        variations.add(clean)

    # Remove original slug from variations
    variations.discard(name_to_slug(name))
    variations.discard(original_url.split("/")[-1])

    for var_slug in variations:
        if not var_slug:
            continue
        var_url = f"{base}/{var_slug}"
        async with semaphore:
            result = await check_url(session, var_url)
        if result["valid"]:
            return {
                "recovered": True,
                "new_url": var_url,
                "method": f"variation:{var_slug}",
            }

    return {"recovered": False, "new_url": None, "method": None}


async def run_recovery(audit_results: dict) -> dict:
    """Attempt recovery for all broken URLs."""
    broken = {name: r for name, r in audit_results.items() if r["action"] == "needs_fix"}
    total = len(broken)
    if total == 0:
        print("No broken URLs to recover.")
        return {}

    print(f"\nAttempting recovery for {total} broken URLs...")
    recovery_results = {}
    done = 0

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, force_close=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def recover_one(name: str, audit_result: dict):
            nonlocal done
            rec = await attempt_recovery(session, semaphore, name, audit_result["original_url"])
            recovery_results[name] = rec
            done += 1
            if done % 100 == 0 or done == total:
                print(f"  [{done}/{total}] recovery attempts...")

        tasks = [recover_one(name, r) for name, r in broken.items()]
        await asyncio.gather(*tasks)

    return recovery_results


def apply_fixes(data: list, audit_results: dict, recovery_results: dict) -> tuple:
    """Apply fixes to the dataset. Returns (fixed_entries, cleared_entries)."""
    fixed = []
    cleared = []

    name_to_idx = {e["name"]: i for i, e in enumerate(data)}

    for name, audit in audit_results.items():
        if audit["action"] != "needs_fix":
            continue

        idx = name_to_idx.get(name)
        if idx is None:
            continue

        rec = recovery_results.get(name, {})
        if rec.get("recovered"):
            data[idx]["acs_url"] = rec["new_url"]
            fixed.append({
                "name": name,
                "old_url": audit["original_url"],
                "new_url": rec["new_url"],
                "method": rec["method"],
            })
        else:
            data[idx]["acs_url"] = None
            cleared.append({
                "name": name,
                "old_url": audit["original_url"],
                "error": audit.get("error"),
                "http_status": audit.get("status"),
            })

    return fixed, cleared


def sync_artifacts(data: list):
    """Sync camellias.json -> per-letter JSON files + index.json."""
    # Write camellias.json
    with open(CAMELLIAS_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  Updated {CAMELLIAS_FILE}")

    # Group by first letter
    by_letter = defaultdict(list)
    for entry in data:
        letter = entry["name"][0].upper()
        by_letter[letter].append(entry)

    # Write per-letter files
    for letter, entries in sorted(by_letter.items()):
        letter_file = DATA_DIR / f"{letter}.json"
        with open(letter_file, "w") as f:
            json.dump(entries, f, indent=2, ensure_ascii=False)
    print(f"  Updated {len(by_letter)} letter files")

    # Write index.json (subset of fields for search)
    index = []
    for entry in data:
        index.append({
            "name": entry["name"],
            "species": entry.get("species"),
            "chinese_name": entry.get("chinese_name", []),
            "japanese_name": entry.get("japanese_name", []),
        })
    with open(DATA_DIR / "index.json", "w") as f:
        json.dump(index, f, indent=2, ensure_ascii=False)
    print(f"  Updated index.json ({len(index)} entries)")


def generate_reports(audit_results: dict, fixed: list, cleared: list):
    """Generate audit report files."""
    # Full audit
    full_audit = []
    for name, r in sorted(audit_results.items()):
        full_audit.append({
            "name": name,
            "url": r["original_url"],
            "status": r.get("status"),
            "content_length": r.get("content_length"),
            "valid": r["valid"],
            "error": r.get("error"),
        })
    with open(REPO / "acs_audit_full.json", "w") as f:
        json.dump(full_audit, f, indent=2, ensure_ascii=False)
    print(f"  acs_audit_full.json ({len(full_audit)} entries)")

    # Fixed entries
    with open(REPO / "acs_audit_fixed.json", "w") as f:
        json.dump(fixed, f, indent=2, ensure_ascii=False)
    print(f"  acs_audit_fixed.json ({len(fixed)} entries)")

    # Cleared entries
    with open(REPO / "acs_audit_cleared.json", "w") as f:
        json.dump(cleared, f, indent=2, ensure_ascii=False)
    print(f"  acs_audit_cleared.json ({len(cleared)} entries)")

    # Summary
    total = len(audit_results)
    valid = sum(1 for r in audit_results.values() if r["valid"])
    broken = total - valid
    recovered = len(fixed)
    removed = len(cleared)

    summary = f"""ACS Link Audit Summary
======================
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Total entries with acs_url: {total}
Valid (live) links:         {valid}
Broken/dead links:          {broken}
  - Recovered (fixed):      {recovered}
  - Cleared (set to null):  {removed}

Recovery rate: {recovered}/{broken} ({100*recovered/broken:.1f}% of broken) if broken > 0 else N/A
Overall health: {valid}/{total} ({100*valid/total:.1f}%) valid before fix
Post-fix health: {valid + recovered}/{total} ({100*(valid+recovered)/total:.1f}%) valid after fix
"""
    with open(REPO / "acs_audit_summary.txt", "w") as f:
        f.write(summary)
    print(f"  acs_audit_summary.txt")
    print(summary)


async def main():
    print(f"=== ACS Link Audit ===")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Repo: {REPO}\n")

    # Load dataset
    with open(CAMELLIAS_FILE) as f:
        data = json.load(f)
    print(f"Loaded {len(data)} entries from camellias.json")

    # Phase 1: Audit all URLs
    print("\n--- Phase 1: URL Audit ---")
    audit_results = await run_audit(data)

    valid_count = sum(1 for r in audit_results.values() if r["valid"])
    broken_count = sum(1 for r in audit_results.values() if not r["valid"])
    print(f"\nAudit complete: {valid_count} valid, {broken_count} broken")

    # Phase 2: Recovery
    print("\n--- Phase 2: Recovery ---")
    recovery_results = await run_recovery(audit_results)

    recovered = sum(1 for r in recovery_results.values() if r.get("recovered"))
    print(f"Recovery complete: {recovered} recovered out of {broken_count} broken")

    # Phase 3: Apply fixes
    print("\n--- Phase 3: Apply Fixes ---")
    fixed, cleared = apply_fixes(data, audit_results, recovery_results)
    print(f"Fixed {len(fixed)} entries, cleared {len(cleared)} entries")

    # Phase 4: Sync artifacts
    print("\n--- Phase 4: Sync Artifacts ---")
    sync_artifacts(data)

    # Phase 5: Generate reports
    print("\n--- Phase 5: Reports ---")
    generate_reports(audit_results, fixed, cleared)

    # Completion event
    try:
        subprocess.run(
            ["openclaw", "system", "event", "--text", "ACS audit complete", "--mode", "now"],
            capture_output=True, timeout=10
        )
    except Exception:
        pass

    # Cleanup state file
    if STATE_FILE.exists():
        STATE_FILE.unlink()

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
