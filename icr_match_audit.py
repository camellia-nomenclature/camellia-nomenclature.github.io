#!/usr/bin/env python3
"""ICR presence/match audit for a list of Chinese camellia names.

Queries the ICR NameSearchList JSON API for each name, applies fuzzy
matching, and writes structured output files.  Resumable via progress JSON.
"""

import json, re, sys, time, subprocess, unicodedata
from pathlib import Path
from difflib import SequenceMatcher
from urllib.parse import quote
from collections import Counter

import requests

# ── configuration ──────────────────────────────────────────────────
BASE = Path(__file__).resolve().parent
PROGRESS_FILE = BASE / "icr_match_audit_progress.json"
FULL_JSON     = BASE / "icr_match_audit_full.json"
SUMMARY_TXT   = BASE / "icr_match_audit_summary.txt"
LINKS_TXT     = BASE / "icr_match_audit_links.txt"

API_URL = "https://camellia.iflora.cn/Cutivars/NameSearchList"
DETAIL_BASE = "https://camellia.iflora.cn/Cutivars/Detail?latin="
RATE_LIMIT_SECONDS = 8
TIMEOUT = 30

NAMES = [
    "超墨", "仙种五宝", "仙种五宝 - 白仙", "仙种五宝 - 黑仙",
    "仙种五宝-白仙", "伊予姬", "信义黄彩", "凌波仙子", "凤尾蝶",
    "十八学士", "十八学士 - 叶变", "十八学士 - 金边", "唐子咲云龙",
    "墨川 - 超墨", "复色粉红螺旋", "复色紫金山", "大锦茶梅 彩叶",
    "姬侘助", "婵", "彩槟榔", "彩霞", "抓破 - 嫦娥彩", "抓破 - 赤丹",
    "无名", "春待姬", "极光", "柳叶赛牡丹", "桃花公主", "楼兰王妃",
    "粉红螺旋", "粉霞", "紫琪", "紫螺旋", "紫螺旋 复色", "紫金山",
    "红胶仓 / 红屁屁", "至宝", "覆轮姬侘助", "覆轮桃色丹",
    "金玉满堂", "金边赛", "锦鲤", "雪精灵", "青叶黑贝", "香荷茶梅",
    "黑狮子金鱼", "日天", "紫八重咲云龙", "小果连蕊茶", "黄妃",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# ── helpers ────────────────────────────────────────────────────────

def normalize(text: str) -> str:
    """Normalize for fuzzy comparison: strip separators, NFKC."""
    t = unicodedata.normalize("NFKC", text)
    t = re.sub(r"[\s\-/·—–()（）]", "", t)
    return t.lower()


def fuzzy_score(a: str, b: str) -> float:
    return SequenceMatcher(None, normalize(a), normalize(b)).ratio()


def strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s).strip()


def query_api(latin: str, session: requests.Session) -> list:
    """POST to NameSearchList, return list of candidate dicts."""
    data = {"latin": latin, "page": "1", "limit": "50"}
    resp = session.post(API_URL, headers=HEADERS, data=data, timeout=TIMEOUT)
    resp.raise_for_status()
    body = resp.json()
    if body.get("code") == "0" and body.get("data"):
        return body["data"]
    return []


def search_icr(name: str, session: requests.Session) -> dict:
    """Query ICR for a single name. Returns dict with status info."""
    # Build query variants
    query_variants = [name]
    plain = re.sub(r"\s*[-/]\s*", "", name).strip()
    if plain != name:
        query_variants.append(plain)
    if " - " in name:
        query_variants.append(name.split(" - ")[-1].strip())
        query_variants.append(name.split(" - ")[0].strip())
    if " / " in name:
        for part in name.split(" / "):
            p = part.strip()
            if p and p not in query_variants:
                query_variants.append(p)
    # Also try with space stripped (e.g. "大锦茶梅 彩叶" -> "大锦茶梅彩叶")
    no_space = name.replace(" ", "")
    if no_space not in query_variants:
        query_variants.append(no_space)

    # De-dup while preserving order
    seen = set()
    unique_variants = []
    for v in query_variants:
        if v not in seen:
            seen.add(v)
            unique_variants.append(v)
    query_variants = unique_variants

    all_candidates = []
    queries_tried = []

    for vi, variant in enumerate(query_variants):
        queries_tried.append(variant)
        try:
            raw = query_api(variant, session)
        except Exception as e:
            return {
                "input_name": name,
                "status": "error",
                "error": str(e),
                "candidates": [],
                "best_match": None,
                "confidence": 0.0,
                "queries_tried": queries_tried,
            }

        for item in raw:
            sci = item.get("ScientificName", "")
            accepted = item.get("AcceptedName", "")
            status_html = item.get("Status", "")
            status_text = strip_html(status_html)
            key = (sci, accepted)
            if not any((c["scientific_name"], c["accepted_name"]) == key for c in all_candidates):
                all_candidates.append({
                    "scientific_name": sci,
                    "accepted_name": accepted,
                    "status": status_text,
                    "detail_url": DETAIL_BASE + quote(accepted) if accepted else "",
                })

        # Rate limit between variant queries
        if vi < len(query_variants) - 1:
            time.sleep(RATE_LIMIT_SECONDS)

    # ── matching logic ─────────────────────────────────────────
    if not all_candidates:
        return {
            "input_name": name,
            "status": "no_match",
            "candidates": [],
            "best_match": None,
            "confidence": 0.0,
            "queries_tried": queries_tried,
        }

    # Score each candidate by comparing ScientificName to input name
    scored = []
    for c in all_candidates:
        sci = c["scientific_name"]
        s1 = fuzzy_score(name, sci)
        s2 = fuzzy_score(plain, sci) if plain != name else 0
        best_s = max(s1, s2)
        scored.append({**c, "score": round(best_s, 4)})

    scored.sort(key=lambda x: x["score"], reverse=True)
    top = scored[0]

    if top["score"] >= 0.95:
        status = "exact"
    elif top["score"] >= 0.6:
        if len(scored) > 1 and scored[1]["score"] >= top["score"] - 0.05:
            status = "ambiguous"
        else:
            status = "fuzzy_match"
    else:
        status = "no_match"

    return {
        "input_name": name,
        "status": status,
        "candidates": scored[:10],
        "best_match": {
            "scientific_name": top["scientific_name"],
            "accepted_name": top["accepted_name"],
            "icr_status": top["status"],
            "detail_url": top["detail_url"],
        } if status in ("exact", "fuzzy_match", "ambiguous") else None,
        "confidence": top["score"],
        "queries_tried": queries_tried,
    }


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"completed": {}, "total": len(NAMES)}


def save_progress(progress: dict):
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False, indent=2))


def send_event(text: str):
    try:
        subprocess.run(
            ["openclaw", "system", "event", "--text", text, "--mode", "now"],
            capture_output=True, timeout=10,
        )
    except Exception:
        pass


def write_outputs(results: list):
    """Write all three output files from the full results list."""
    # 1. Full JSON
    FULL_JSON.write_text(json.dumps(results, ensure_ascii=False, indent=2))

    # 2. Summary TXT
    buckets = {"exact": [], "fuzzy_match": [], "ambiguous": [], "no_match": [], "error": []}
    for r in results:
        buckets.setdefault(r["status"], []).append(r)

    lines = [f"ICR Match Audit Summary  ({len(results)} names)", "=" * 50, ""]
    for bucket_name in ["exact", "fuzzy_match", "ambiguous", "no_match", "error"]:
        items = buckets.get(bucket_name, [])
        lines.append(f"[{bucket_name.upper()}] ({len(items)})")
        for r in items:
            bm = r.get("best_match")
            match_info = ""
            if bm:
                match_info = (
                    f" -> {bm.get('scientific_name', '?')} "
                    f"[{bm.get('accepted_name', '?')}] "
                    f"({bm.get('icr_status', '?')})  "
                    f"conf={r['confidence']:.2f}"
                )
            lines.append(f"  {r['input_name']}{match_info}")
        lines.append("")
    SUMMARY_TXT.write_text("\n".join(lines))

    # 3. Links TXT — one link per matched name
    link_lines = []
    for r in results:
        bm = r.get("best_match")
        if bm and bm.get("detail_url"):
            link_lines.append(f"{r['input_name']}\t{bm['detail_url']}")
    LINKS_TXT.write_text("\n".join(link_lines))


# ── main ───────────────────────────────────────────────────────────

def main():
    progress = load_progress()
    completed = progress.get("completed", {})
    total = len(NAMES)
    session = requests.Session()

    done_count = len(completed)
    last_event_count = done_count

    for i, name in enumerate(NAMES):
        if name in completed:
            continue

        print(f"[{i+1}/{total}] Querying: {name} …", flush=True)
        result = search_icr(name, session)
        completed[name] = result
        done_count += 1

        # Save progress after each query
        progress["completed"] = completed
        save_progress(progress)

        # Write outputs incrementally (in original order)
        ordered = [completed[n] for n in NAMES if n in completed]
        write_outputs(ordered)

        # Progress event every ~10 names
        if done_count - last_event_count >= 10:
            send_event(f"ICR match audit progress: {done_count}/{total}")
            last_event_count = done_count

        # Rate limit (skip after last name)
        if i < total - 1:
            time.sleep(RATE_LIMIT_SECONDS)

    # Final ordered output
    ordered_results = [completed[n] for n in NAMES if n in completed]
    write_outputs(ordered_results)
    save_progress(progress)

    send_event("ICR match audit complete")

    stats = Counter(r["status"] for r in ordered_results)
    print(f"\nDone. {total} names processed.")
    for s in ["exact", "fuzzy_match", "ambiguous", "no_match", "error"]:
        if stats.get(s):
            print(f"  {s}: {stats[s]}")


if __name__ == "__main__":
    main()
