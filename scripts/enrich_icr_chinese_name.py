#!/usr/bin/env python3
import argparse
import html
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests

ICR_HOST = "camellia.iflora.cn"
CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
TAG_RE = re.compile(r"<[^>]+>", re.S)
WS_RE = re.compile(r"\s+")


@dataclass
class UrlResult:
    chinese_name: List[str]
    status: str  # ok|missing|failed|skipped
    http_status: Optional[int]
    error: str
    fetched_at: str


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def dump_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.write("\n")


def normalize_text(s: str) -> str:
    s = html.unescape(s or "")
    s = TAG_RE.sub(" ", s)
    s = WS_RE.sub(" ", s).strip()
    return s


def split_candidate_names(value: str) -> List[str]:
    value = normalize_text(value)
    if not value:
        return []
    parts = re.split(r"[、，,；;|/]+", value)
    out: List[str] = []
    seen = set()
    for p in parts:
        name = p.strip(" \t\r\n:：()[]{}")
        if not name:
            continue
        if "Japanese Name" in name or "Meaning" in name or "Synonym" in name:
            continue
        if not CJK_RE.search(name):
            continue
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out


def extract_chinese_names(html_doc: str) -> List[str]:
    names: List[str] = []
    seen = set()

    # Pattern 1: <p><b>Chinese Name</b>：...</p>
    p_pat = re.compile(
        r"<p[^>]*>\s*(?:<[^>]+>\s*)*Chinese\s*Name\s*(?:</[^>]+>\s*)*[：:]\s*(.*?)</p>",
        re.I | re.S,
    )
    for m in p_pat.finditer(html_doc):
        for n in split_candidate_names(m.group(1)):
            if n not in seen:
                seen.add(n)
                names.append(n)

    # Pattern 2: table-row style ...Chinese Name...</td><td>...</td>
    td_pat = re.compile(
        r"<td[^>]*>\s*(?:<[^>]+>\s*)*Chinese\s*Name\s*(?:</[^>]+>\s*)*</td>\s*<td[^>]*>(.*?)</td>",
        re.I | re.S,
    )
    for m in td_pat.finditer(html_doc):
        for n in split_candidate_names(m.group(1)):
            if n not in seen:
                seen.add(n)
                names.append(n)

    # Pattern 3: text fallback constrained after label and before next known label.
    text = normalize_text(html_doc)
    fallback = re.finditer(
        r"Chinese\s*Name\s*[：:]\s*(.+?)(?=\b(?:Japanese\s*Name|Meaning|Synonym|Scientific\s*Name|Species/Combination|Id)\b|$)",
        text,
        flags=re.I,
    )
    for m in fallback:
        for n in split_candidate_names(m.group(1)):
            if n not in seen:
                seen.add(n)
                names.append(n)

    return names


def valid_icr_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in {"http", "https"} and p.netloc.lower().endswith(ICR_HOST)
    except Exception:
        return False


def ensure_chinese_name_field(records: List[dict]) -> bool:
    changed = False
    for r in records:
        cur = r.get("chinese_name")
        if not isinstance(cur, list):
            r["chinese_name"] = []
            changed = True
        else:
            cleaned = []
            seen = set()
            for x in cur:
                if isinstance(x, str):
                    x = normalize_text(x)
                    if x and x not in seen:
                        seen.add(x)
                        cleaned.append(x)
            if cleaned != cur:
                r["chinese_name"] = cleaned
                changed = True
    return changed


def collect_unique_icr_urls(records: List[dict]) -> List[str]:
    urls: List[str] = []
    seen = set()
    for r in records:
        u = r.get("icr_url")
        if isinstance(u, str):
            u = u.strip()
            if u and valid_icr_url(u) and u not in seen:
                seen.add(u)
                urls.append(u)
    return urls


def append_jsonl(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def load_checkpoint(path: Path, total_urls: int, reset: bool) -> dict:
    if (not reset) and path.exists():
        cp = load_json(path)
    else:
        cp = {
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "total_urls": total_urls,
            "next_index": 0,
            "last_request_ts": 0.0,
        }
    cp["total_urls"] = total_urls
    cp.setdefault("next_index", 0)
    cp.setdefault("last_request_ts", 0.0)
    return cp


def load_best_checkpoint(paths: List[Path], total_urls: int, reset: bool) -> dict:
    if reset:
        return load_checkpoint(paths[0], total_urls, reset=True)

    candidates = []
    for p in paths:
        if p.exists():
            try:
                cp = load_json(p)
                cp["total_urls"] = total_urls
                cp.setdefault("next_index", 0)
                cp.setdefault("last_request_ts", 0.0)
                candidates.append(cp)
            except Exception:
                continue

    if not candidates:
        return load_checkpoint(paths[0], total_urls, reset=True)

    def key(cp: dict):
        return (
            int(cp.get("next_index", 0)),
            str(cp.get("updated_at", "")),
        )

    return max(candidates, key=key)


def save_checkpoint(path: Path, cp: dict) -> None:
    cp["updated_at"] = now_iso()
    dump_json(path, cp)


def save_checkpoints(paths: List[Path], cp: dict) -> None:
    cp["updated_at"] = now_iso()
    for p in paths:
        dump_json(p, cp)


def load_url_cache(path: Path, reset: bool) -> Dict[str, UrlResult]:
    if reset or (not path.exists()):
        return {}
    raw = load_json(path)
    out: Dict[str, UrlResult] = {}
    for u, v in raw.items():
        raw_name = v.get("chinese_name", [])
        if isinstance(raw_name, list):
            names = []
            seen = set()
            for n in raw_name:
                if isinstance(n, str):
                    n = normalize_text(n)
                    if n and CJK_RE.search(n) and n not in seen:
                        seen.add(n)
                        names.append(n)
        elif isinstance(raw_name, str):
            names = split_candidate_names(raw_name)
        else:
            names = []

        out[u] = UrlResult(
            chinese_name=names,
            status=v.get("status", "failed"),
            http_status=v.get("http_status"),
            error=v.get("error", ""),
            fetched_at=v.get("fetched_at", ""),
        )
    return out


def save_url_cache(path: Path, cache: Dict[str, UrlResult]) -> None:
    dump_json(path, {u: asdict(r) for u, r in cache.items()})


def fetch_one(session: requests.Session, url: str, timeout: int) -> UrlResult:
    try:
        resp = session.get(url, timeout=timeout)
        if resp.status_code != 200:
            return UrlResult([], "failed", resp.status_code, f"HTTP {resp.status_code}", now_iso())
        names = extract_chinese_names(resp.text)
        if names:
            return UrlResult(names, "ok", resp.status_code, "", now_iso())
        return UrlResult([], "missing", resp.status_code, "", now_iso())
    except Exception as e:
        return UrlResult([], "failed", None, str(e), now_iso())


def apply_result_map(records: List[dict], result_map: Dict[str, UrlResult]) -> bool:
    changed = ensure_chinese_name_field(records)
    for r in records:
        u = r.get("icr_url")
        if isinstance(u, str):
            u = u.strip()
            if u in result_map:
                names = list(result_map[u].chinese_name)
                if r.get("chinese_name") != names:
                    r["chinese_name"] = names
                    changed = True
    return changed


def update_file(path: Path, result_map: Dict[str, UrlResult]) -> bool:
    obj = load_json(path)
    changed = False
    if isinstance(obj, list):
        changed = apply_result_map(obj, result_map)
    elif isinstance(obj, dict):
        for k in ("data", "camellias", "items", "records"):
            if isinstance(obj.get(k), list):
                if apply_result_map(obj[k], result_map):
                    changed = True
        # ensure index entries also carry array field if present
        if isinstance(obj.get("index"), list):
            if apply_result_map(obj["index"], result_map):
                changed = True
    if changed:
        dump_json(path, obj)
    return changed


def format_eta(seconds: float) -> str:
    if seconds <= 0:
        return "0s"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m}m {sec}s"
    if m > 0:
        return f"{m}m {sec}s"
    return f"{sec}s"


def candidate_json_paths(repo: Path) -> List[Path]:
    paths = [
        repo / "camellias.json",
        repo / "data.json",
        repo / "index.json",
        repo / "data" / "camellias.json",
        repo / "data" / "index.json",
        repo / "latest" / "index.json",
        repo / "latest" / "data" / "camellias.json",
        repo / "latest" / "data" / "index.json",
    ]
    for pat in ["data/*.json", "latest/data/*.json"]:
        paths.extend(sorted(repo.glob(pat)))

    uniq = []
    seen = set()
    for p in paths:
        if p.exists() and p not in seen:
            uniq.append(p)
            seen.add(p)
    return uniq


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich all datasets with chinese_name arrays from ICR pages")
    parser.add_argument("--repo", default=".")
    parser.add_argument("--delay", type=float, default=8.0, help="Minimum seconds between network requests")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--progress-interval", type=int, default=300)
    parser.add_argument("--max-urls", type=int, default=0, help="Optional limit for this run")
    parser.add_argument("--resume", action="store_true", help="Resume from cache/checkpoint if present")
    parser.add_argument("--checkpoint", default="logs/icr_chinese_name_checkpoint.json")
    parser.add_argument("--checkpoint-mirror", default="tmp/icr_checkpoint.json")
    parser.add_argument("--url-cache", default="logs/icr_chinese_name_url_cache.json")
    parser.add_argument("--processed-log", default="logs/icr_chinese_name_processed.jsonl")
    parser.add_argument("--failed-log", default="logs/icr_chinese_name_failed.jsonl")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    logs = repo / "logs"
    logs.mkdir(parents=True, exist_ok=True)

    base = load_json(repo / "camellias.json")
    ensure_chinese_name_field(base)
    urls = collect_unique_icr_urls(base)
    total = len(urls)

    cp_path = repo / args.checkpoint
    cp_mirror_path = repo / args.checkpoint_mirror
    cp_paths = [cp_path, cp_mirror_path]
    cache_path = repo / args.url_cache
    reset = not args.resume

    checkpoint = load_best_checkpoint(cp_paths, total, reset=reset)
    cache = load_url_cache(cache_path, reset=reset)

    # Ensure next_index starts from 0 for fresh runs.
    if reset:
        checkpoint["next_index"] = 0

    save_checkpoints(cp_paths, checkpoint)

    # Keep checkpoint coherent if cache already has leading contiguous URLs.
    if args.resume:
        i = int(checkpoint.get("next_index", 0))
        i = max(0, min(i, total))
        checkpoint["next_index"] = i

    session = requests.Session()
    session.headers.update({"User-Agent": "camellia-chinese-name-array-enricher/2.0"})

    i = int(checkpoint.get("next_index", 0))
    processed_this_run = 0
    limit = args.max_urls if args.max_urls > 0 else None
    last_progress = 0.0
    run_started = time.time()

    pct0 = (i / total * 100.0) if total else 100.0
    print(f"1 {i} of {total} ({pct0:.2f}%) has been processed.", flush=True)

    while i < total:
        if limit is not None and processed_this_run >= limit:
            break

        url = urls[i]
        if url in cache:
            result = cache[url]
        else:
            elapsed = time.time() - float(checkpoint.get("last_request_ts", 0.0))
            if elapsed < args.delay:
                time.sleep(args.delay - elapsed)
            result = fetch_one(session, url, args.timeout)
            checkpoint["last_request_ts"] = time.time()
            cache[url] = result

        payload = {"index": i, "total": total, "url": url, **asdict(result)}
        append_jsonl(repo / args.processed_log, payload)
        if result.status == "failed":
            append_jsonl(repo / args.failed_log, payload)

        i += 1
        processed_this_run += 1
        checkpoint["next_index"] = i
        save_checkpoints(cp_paths, checkpoint)
        save_url_cache(cache_path, cache)

        now = time.time()
        if last_progress == 0.0 or (now - last_progress >= args.progress_interval):
            pct = (i / total * 100.0) if total else 100.0
            print(f"1 {i} of {total} ({pct:.2f}%) has been processed.", flush=True)
            last_progress = now

    updated = []
    for p in candidate_json_paths(repo):
        try:
            if update_file(p, cache):
                updated.append(str(p.relative_to(repo)))
        except Exception:
            continue

    found = sum(1 for r in cache.values() if r.chinese_name)
    failed = sum(1 for r in cache.values() if r.status == "failed")
    missing = sum(1 for r in cache.values() if (r.status in {"ok", "missing"}) and not r.chinese_name)

    summary = {
        "timestamp": now_iso(),
        "total_unique_icr_urls": total,
        "processed_unique_icr_urls": len(cache),
        "found_urls_with_chinese_names": found,
        "missing_urls": missing,
        "failed_urls": failed,
        "next_index": i,
        "updated_files": updated,
    }
    dump_json(repo / "logs" / "icr_chinese_name_summary.json", summary)

    pct = (i / total * 100.0) if total else 100.0
    print(f"1 {i} of {total} ({pct:.2f}%) has been processed.")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
