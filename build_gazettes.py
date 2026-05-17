#!/usr/bin/env python3
"""Gazettes corpus builder — orchestrator.

v1 scope: Central Gazette of India (Weekly + Extraordinary), pulled
from archive.org's `gazetteofindia` collection. No egazette.gov.in
scraping. No own-OCR. Pulls archive.org's pre-OCR'd djvu.txt directly.

Per scheduled (or workflow_dispatch) run:
  1. Walk archive.org newest-first via the scrape API. For each item:
     - If identifier is already in our index → potentially STOP
       (we've reached previously-known territory).
     - Else: fetch metadata + djvu.txt. Write text/central/<file_id>.txt.
       Add the record dict to the merged reports list.
  2. Save reports as sharded reports-central-NN.json + reports-meta.json.
  3. (Derive phase) Build manifest + sharded text shards + sharded
     bundle + sharded index + meta + audit.

PDF mirror push to the per-date sharded mirror repos is a follow-up
(session 1.5 / 2). For v1, the index repo's metadata records carry
`pdf_url_ia` pointing at archive.org — that's where the app sends
"View PDF" clicks. The mirror is insurance / monetization asset for
later activation.

Output goes under docs/gazettes/, served at
gazettes.sansadsaar-data.naklitechie.com/ (planned).

Independence Principle: no imports from cag, lc, fc, drsc, bills,
debates, or questions scrapers. The HTTP layer + RateLimited +
jitter primitives are re-implemented under gazettes/common.py.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# parliamentwatch_text_shards is the shared helper for bundling
# per-record text files into size-targeted shard JSONs + the
# write_json_idempotent helper that skips no-op timestamp-only meta /
# audit writes. Vendored from parliamentwatch-data — copy maintained
# here under the Independence Principle. See
# parliamentwatch_text_shards.py.
from parliamentwatch_text_shards import write_text_shards, write_json_idempotent  # noqa: E402

from gazettes.common import RateLimited, http_get, DOWNLOAD_API
from gazettes.scrapers.archive_org import (
    GazetteRecord,
    enumerate_items,
    fetch_record,
    fetch_djvu_text,
    file_id,
    report_key,
    mirror_repo_for,
    mirror_path_for,
)

# ── Paths ───────────────────────────────────────────────────────────────────

ASSETS    = ROOT / "docs"
DOCS      = ASSETS / "gazettes"
TEXT_DIR  = DOCS / "text"            # text/central/<fid>.txt (transient before bundling)
DOCS.mkdir(parents=True, exist_ok=True)

REPORTS_META_JSON = DOCS / "reports-meta.json"      # shard manifest the app fetches first
MANIFEST_JSON     = DOCS / "manifest.json"
META_JSON         = DOCS / "meta.json"
AUDIT_JSON        = DOCS / "audit.json"

# Categories we cover. Reserved for future expansion (state gazettes
# would land here as additional categories like "andhra", "karnataka",
# or — more likely — as their own corpora).
CATEGORIES = ["central"]

# Per-category shard size. 2,500 records × ~1 KB metadata = ~2.5 MB
# per shard — comfortably under CF's 25 MiB per-file cap.
SHARD_SIZE = 2500

# ── Per-run budget ─────────────────────────────────────────────────────────
#
# Per recon (plan/gazettes-recon-001.md): archive.org is the upstream.
# Each new record costs ~3 HTTP requests (metadata + djvu.txt). With
# 200-400ms jitter, a burst of 50 takes ~30-60 sec sustained. Hourly
# cron at 50/run = 1200/day → ~142 days to clear ~170K archive items.

MAX_EXTRACTIONS_PER_RUN = int(os.environ.get("MAX_EXTRACTIONS_PER_RUN", "50"))
MAX_RUN_SECONDS         = int(os.environ.get("MAX_RUN_SECONDS", "900"))   # 15 min

# Per-run budget for the djvu retry pass. archive.org's djvu derivative
# isn't generated immediately when an item is uploaded (~24h delay), so
# the first scrape visit often finds an empty djvu_url and records the
# item as djvu_pending. retry_djvu_pending revisits those records each
# run, attempting to construct the djvu URL directly from the
# identifier and fetch the now-ready derivative. ~1 HTTP request per
# retry; 50/run = ~50 sec sustained burst on top of the new-record
# pass. See diagnosis in plan/gazettes-djvu-pending.md.
MAX_DJVU_RETRY_PER_RUN = int(os.environ.get("MAX_DJVU_RETRY_PER_RUN", "50"))

# Newest-first enumeration: when we see this many already-known records
# in a row, assume we've reached steady-state catch-up and stop.
# Configurable so dispatches can override (e.g. set high for one-shot
# deep backfill).
ENUM_STOP_AFTER_KNOWN = int(os.environ.get("ENUM_STOP_AFTER_KNOWN", "200"))

# Cooldown after a 429/503.
RATE_LIMIT_COOLDOWN_SECONDS = int(os.environ.get("RATE_LIMIT_COOLDOWN_SECONDS", str(6 * 3600)))

# In-flight checkpoint cadence.
CHECKPOINT_EVERY_N = int(os.environ.get("CHECKPOINT_EVERY_N", "25"))
CHECKPOINT_EVERY_S = int(os.environ.get("CHECKPOINT_EVERY_S", "300"))


def _git(*args) -> tuple[int, str, str]:
    p = subprocess.run(["git", *args], capture_output=True, text=True, check=False)
    return p.returncode, p.stdout, p.stderr


def checkpoint_commit(message: str, paths: list[str]) -> bool:
    """Same pattern as other corpora: add → commit → pull --rebase → push.
    Failures are best-effort; the next checkpoint retries.
    """
    rc, _, err = _git("add", "--", *paths)
    if rc != 0:
        print(f"  [checkpoint] git add failed: {err.strip() or 'unknown'}")
        return False
    rc, _, _ = _git("diff", "--cached", "--quiet")
    if rc == 0:
        return True
    rc, _, err = _git("commit", "-m", message)
    if rc != 0:
        print(f"  [checkpoint] git commit failed: {err.strip()}")
        return False
    rc, _, err = _git("pull", "--rebase", "origin", "main")
    if rc != 0:
        print(f"  [checkpoint] git pull --rebase failed: {err.strip()} — aborting")
        _git("rebase", "--abort")
        return False
    rc, _, err = _git("push")
    if rc != 0:
        print(f"  [checkpoint] git push failed: {err.strip()}")
        return False
    print(f"  [checkpoint] pushed: {message}")
    return True


# ── Phase 1: load + walk + merge ───────────────────────────────────────────


def load_existing_reports() -> dict[str, list[dict]]:
    """Load category → list-of-reports dict from the sharded format.
    Returns empty lists on first run.
    """
    if not REPORTS_META_JSON.exists():
        return {c: [] for c in CATEGORIES}
    try:
        with open(REPORTS_META_JSON, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"  ! reports-meta.json unreadable ({e}); starting fresh")
        return {c: [] for c in CATEGORIES}
    result: dict[str, list[dict]] = {c: [] for c in CATEGORIES}
    for cat, shard_entries in (meta.get("shards") or {}).items():
        for entry in shard_entries:
            path = DOCS / entry["file"]
            if not path.exists():
                print(f"  ! shard missing on disk: {entry['file']} — skipping")
                continue
            with open(path, "r", encoding="utf-8") as f:
                result.setdefault(cat, []).extend(
                    json.load(f).get("records", []))
    return result


def _shard_filename(category: str, idx: int) -> str:
    return f"reports-{category}-{idx:02d}.json"


def _write_sharded_reports(reports: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """Partition each category's records into SHARD_SIZE-sized shards.
    Cleans up orphans (excluding reports-meta.json which we overwrite
    in _write_reports_meta).
    """
    for path in DOCS.glob("reports-*.json"):
        if path.name == REPORTS_META_JSON.name:
            continue
        try:
            path.unlink()
        except OSError:
            pass

    shard_entries: dict[str, list[dict]] = {}
    for cat in reports:
        items = reports[cat]
        entries: list[dict] = []
        for i in range(0, len(items), SHARD_SIZE):
            chunk = items[i:i + SHARD_SIZE]
            idx = i // SHARD_SIZE
            fname = _shard_filename(cat, idx)
            path = DOCS / fname
            with open(path, "w", encoding="utf-8") as f:
                json.dump({
                    "records": chunk,
                    "count": len(chunk),
                    "category": cat,
                    "shard_index": idx,
                }, f, ensure_ascii=False, indent=2)
            entries.append({"file": fname, "count": len(chunk), "shard_index": idx})
        shard_entries[cat] = entries
    return shard_entries


def _write_reports_meta(reports: dict[str, list[dict]],
                       shard_entries: dict[str, list[dict]]) -> None:
    meta = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "shard_size": SHARD_SIZE,
        "totals": {c: len(reports.get(c, [])) for c in reports},
        "shards": shard_entries,
    }
    with open(REPORTS_META_JSON, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def save_reports(reports: dict[str, list[dict]]) -> None:
    """Sort each category's list (newest-first by issue_date, then by
    ugid for stability) and write as sharded files.
    """
    out: dict[str, list[dict]] = {}
    for cat in CATEGORIES:
        items = reports.get(cat, [])
        items.sort(key=lambda r: (
            str(r.get("issue_date") or ""),
            str(r.get("ugid") or ""),
        ), reverse=True)
        out[cat] = items
    for cat, items in reports.items():
        if cat not in CATEGORIES and cat not in out:
            out[cat] = items
    shard_entries = _write_sharded_reports(out)
    _write_reports_meta(out, shard_entries)


def _record_to_dict(r: GazetteRecord) -> dict:
    """Dehydrate dataclass → plain dict for JSON serialisation. Adds
    derived `pdf_url` (the archive.org-served URL the app links to),
    plus mirror routing fields for the eventual mirror-push pipeline.
    """
    return {
        "identifier":          r.identifier,
        "category":            r.category,
        "type_letter":         r.type_letter,
        "issue_date":          r.issue_date,
        "ugid":                r.ugid,
        "title":               r.title,
        "ministry":            r.ministry,
        "department":          r.department,
        "office":              r.office,
        "subject":             r.subject,
        "gazette_id":          r.gazette_id,
        "gazette_source_url":  r.gazette_source_url,
        "languages":           r.languages,
        # Serving URL — the archive.org-served PDF. App's "View PDF"
        # click goes here. If we flip to self-hosting later, this
        # field gets rewritten by the derive phase.
        "pdf_url":             r.pdf_url_ia,
        # Mirror routing — populated for future activation. App ignores
        # these today.
        "ia_identifier":       r.identifier,
        "mirror_repo":         mirror_repo_for(r),
        "mirror_path":         mirror_path_for(r),
        # Topic classification stub — populated by derive phase when
        # the classifier ships. Empty array for now.
        "topics":              [],
    }


def _records_index(reports: dict[str, list[dict]]) -> set[str]:
    """Set of identifiers already in our index. Used to skip during
    enumeration (idempotent re-runs).
    """
    out: set[str] = set()
    for cat in CATEGORIES:
        for r in reports.get(cat, []):
            ident = r.get("identifier")
            if ident:
                out.add(ident)
    return out


def _check_cooldown_and_skip() -> bool:
    if not META_JSON.exists():
        return False
    try:
        with open(META_JSON, "r", encoding="utf-8") as f:
            prev = json.load(f)
    except Exception:
        return False
    if not prev.get("rate_limited"):
        return False
    last_at = prev.get("rate_limited_at")
    if not last_at:
        return False
    try:
        last_dt = datetime.fromisoformat(last_at.replace("Z", "+00:00"))
    except Exception:
        return False
    elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds()
    if elapsed < RATE_LIMIT_COOLDOWN_SECONDS:
        print(f"  COOLDOWN: previous run rate-limited {elapsed:.0f}s ago; "
              f"skipping (limit {RATE_LIMIT_COOLDOWN_SECONDS}s)")
        return True
    return False


def walk_archive_and_extract(reports: dict[str, list[dict]], *,
                              deadline: float) -> dict:
    """Walk archive.org newest-first, skipping identifiers already in
    our index. For each new item, fetch metadata + djvu.txt, write text
    to text/central/, append record to the merged list. Bounded by
    MAX_EXTRACTIONS_PER_RUN.

    Returns a dict with run stats.
    """
    if _check_cooldown_and_skip():
        return {"extracted": [], "skipped_known": 0, "rate_limited": True,
                "budget_hit": False, "skipped_due_to_cooldown": True}

    known = _records_index(reports)
    print(f"  index: {len(known)} records already present")

    text_dir = TEXT_DIR / "central"
    text_dir.mkdir(parents=True, exist_ok=True)

    extracted: list[str] = []     # identifiers added this run
    failed: list[str] = []
    rate_limited = False
    budget_hit = False
    last_checkpoint_at = time.monotonic()
    extracted_since_checkpoint = 0
    consecutive_known = 0         # for ENUM_STOP_AFTER_KNOWN heuristic

    print(f"  walking archive.org (newest-first), budget={MAX_EXTRACTIONS_PER_RUN}, "
          f"stop_after_known={ENUM_STOP_AFTER_KNOWN}")

    try:
        # Walk newest-first by reversing sort.
        for item in _enumerate_newest_first():
            identifier = item.get("identifier")
            if not identifier:
                continue

            if identifier in known:
                consecutive_known += 1
                if consecutive_known >= ENUM_STOP_AFTER_KNOWN:
                    print(f"  saw {ENUM_STOP_AFTER_KNOWN} known in a row — "
                          f"stopping enumeration (steady-state catch-up)")
                    break
                continue
            # New record — reset the streak.
            consecutive_known = 0

            now = time.monotonic()
            if now > deadline:
                print(f"  [BUDGET] wall-clock budget hit after {len(extracted)} extractions")
                budget_hit = True
                break
            if len(extracted) >= MAX_EXTRACTIONS_PER_RUN:
                print(f"  [BUDGET] hit MAX_EXTRACTIONS_PER_RUN={MAX_EXTRACTIONS_PER_RUN}")
                budget_hit = True
                break

            # Fetch this record.
            try:
                rec = fetch_record(identifier)
            except RateLimited as rl:
                print(f"  [RATE-LIMITED] fetch_record {identifier}: {rl}")
                rate_limited = True
                break
            except Exception as e:
                print(f"    fetch_record failed for {identifier}: {e}")
                failed.append(identifier)
                continue
            if rec is None:
                # Identifier didn't match the central pattern. Skip silently.
                continue

            # Fetch djvu text (the body).
            try:
                djvu_text = fetch_djvu_text(rec)
            except RateLimited as rl:
                print(f"  [RATE-LIMITED] djvu fetch {identifier}: {rl}")
                rate_limited = True
                break
            except Exception as e:
                print(f"    djvu fetch failed for {identifier}: {e}")
                djvu_text = None

            # Persist text (if available) to text/central/<fid>.txt.
            fid = file_id(rec)
            if djvu_text:
                text_path = text_dir / f"{fid}.txt"
                with open(text_path, "w", encoding="utf-8") as f:
                    f.write(djvu_text)

            # Append to reports.
            reports.setdefault("central", []).append(_record_to_dict(rec))
            known.add(identifier)
            extracted.append(identifier)
            extracted_since_checkpoint += 1

            # Periodic checkpoint.
            if (extracted_since_checkpoint >= CHECKPOINT_EVERY_N or
                (extracted_since_checkpoint > 0 and
                 time.monotonic() - last_checkpoint_at >= CHECKPOINT_EVERY_S)):
                save_reports(reports)
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
                checkpoint_commit(
                    f"Auto-checkpoint gazettes primary data (extracted={len(extracted)} this run) [{ts}]",
                    ["docs/gazettes/"],
                )
                extracted_since_checkpoint = 0
                last_checkpoint_at = time.monotonic()
    except RateLimited as rl:
        print(f"  [RATE-LIMITED] enumerate: {rl}")
        rate_limited = True

    # Final save (always; even if no extractions, in case of marker changes).
    save_reports(reports)
    if extracted_since_checkpoint > 0:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
        checkpoint_commit(
            f"Auto-checkpoint gazettes primary data (final, extracted={len(extracted)} this run) [{ts}]",
            ["docs/gazettes/"],
        )

    return {
        "extracted": extracted,
        "failed": failed,
        "skipped_known": len(known) - len(reports.get("central", [])),
        "rate_limited": rate_limited,
        "budget_hit": budget_hit,
    }


def retry_djvu_pending(reports: dict[str, list[dict]], *,
                       deadline: float) -> dict:
    """Retry djvu fetch for records that were indexed before archive.org
    finished generating their djvu derivative.

    Diagnosis: archive.org generates djvu.txt as a derivative file ~24h
    after an item is uploaded. The first scraper visit (newest-first
    walk) often lands on items uploaded in the last 24h, finds no
    djvu file in the IA metadata response, records the metadata, and
    moves on — leaving the record permanently flagged djvu_pending.
    On 2026-05-15 this was 435 of 438 records (~99%).

    Fix: for each record that has no text file on disk, construct the
    djvu URL directly from the identifier (pattern: <id>/<seq>_djvu.txt
    where seq is the trailing numeric segment of the identifier) and
    attempt to fetch. On success, write text/<cat>/<fid>.txt and
    update the record's djvu_url field in memory; save_reports persists
    it. On 404 / empty / RateLimited, leave the record as-is for the
    next run.

    Ordering: oldest issue_date first. Older records have had more time
    for archive.org to derivative their djvu, so they have a higher
    success rate per HTTP request — better budget utilisation than
    revisiting recent records that probably aren't ready yet.
    """
    text_dir = TEXT_DIR / "central"
    text_dir.mkdir(parents=True, exist_ok=True)

    # Build the pending list — records with no text file on disk.
    pending: list[dict] = []
    for r in reports.get("central", []):
        fid = file_id_from_dict(r)
        if not (text_dir / f"{fid}.txt").exists():
            pending.append(r)

    if not pending:
        return {"attempted": 0, "succeeded": 0, "pending_total": 0,
                "rate_limited": False, "budget_hit": False}

    # Oldest issue_date first — see docstring for rationale.
    pending.sort(key=lambda r: r.get("issue_date", ""))

    attempted = 0
    succeeded = 0
    rate_limited = False
    budget_hit = False
    last_checkpoint_at = time.monotonic()
    extracted_since_checkpoint = 0

    print(f"  djvu retry pool: {len(pending)} pending; budget={MAX_DJVU_RETRY_PER_RUN}/run")

    for r in pending:
        if time.monotonic() > deadline:
            print(f"  [BUDGET] djvu retry wall-clock budget hit after {attempted} attempts")
            budget_hit = True
            break
        if attempted >= MAX_DJVU_RETRY_PER_RUN:
            print(f"  [BUDGET] hit MAX_DJVU_RETRY_PER_RUN={MAX_DJVU_RETRY_PER_RUN}")
            budget_hit = True
            break

        identifier = r.get("identifier", "")
        # Trailing numeric segment of the identifier is the seq prefix
        # archive.org uses on the djvu derivative filename. Pattern:
        # `in.gazette.central.<t>.<YYYY-MM-DD>.<seq>` → `<seq>_djvu.txt`.
        seq = identifier.rsplit(".", 1)[-1] if "." in identifier else ""
        if not seq.isdigit():
            attempted += 1
            continue
        djvu_url = f"{DOWNLOAD_API}/{identifier}/{seq}_djvu.txt"

        attempted += 1
        try:
            resp = http_get(djvu_url, timeout=120)
        except RateLimited as rl:
            print(f"  [RATE-LIMITED] djvu retry {identifier}: {rl}")
            rate_limited = True
            break
        except Exception:
            # 404 = derivative still not ready, or some other transient.
            # Don't log per-record — the pending pool can be large and
            # logging each miss would flood the run log.
            continue
        text = resp.text or ""
        if not text.strip():
            continue

        # Persist + update the record so subsequent runs don't re-fetch.
        fid = file_id_from_dict(r)
        text_path = text_dir / f"{fid}.txt"
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(text)
        r["djvu_url"] = djvu_url
        succeeded += 1
        extracted_since_checkpoint += 1

        # Same checkpoint cadence as the extract loop.
        now = time.monotonic()
        if (extracted_since_checkpoint >= CHECKPOINT_EVERY_N or
            (extracted_since_checkpoint > 0 and
             now - last_checkpoint_at >= CHECKPOINT_EVERY_S)):
            save_reports(reports)
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
            checkpoint_commit(
                f"Auto-checkpoint gazettes djvu retry (succeeded={succeeded} this run) [{ts}]",
                ["docs/gazettes/"],
            )
            extracted_since_checkpoint = 0
            last_checkpoint_at = now

    save_reports(reports)
    if extracted_since_checkpoint > 0:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
        checkpoint_commit(
            f"Auto-checkpoint gazettes djvu retry (final, succeeded={succeeded} this run) [{ts}]",
            ["docs/gazettes/"],
        )

    print(f"  djvu retry done: attempted={attempted} succeeded={succeeded} "
          f"pool_remaining={len(pending) - succeeded}")
    return {
        "attempted": attempted,
        "succeeded": succeeded,
        "pending_total": len(pending),
        "rate_limited": rate_limited,
        "budget_hit": budget_hit,
    }


def _enumerate_newest_first():
    """Wrap enumerate_items() with newest-first sort. The scraper module's
    default sort is `date asc`; we override here to walk descending so
    the steady-state catch-up exits quickly.
    """
    # Drop into the lower-level scrape API directly with sort override.
    from gazettes.common import http_get, SCRAPE_API
    from gazettes.scrapers.archive_org import QUERY_ALL, MIN_PAGE_SIZE
    params = {
        "q": QUERY_ALL,
        "fields": "identifier,date,title",
        "count": max(MIN_PAGE_SIZE, 500),
        "sorts": "date desc",
    }
    while True:
        resp = http_get(SCRAPE_API, params=params)
        try:
            data = resp.json()
        except Exception as e:
            print(f"  ! scrape API non-JSON: {e}")
            return
        for item in (data.get("items") or []):
            yield item
        cursor = data.get("cursor")
        if not cursor:
            return
        params["cursor"] = cursor


# ── Phase 3: derived files ─────────────────────────────────────────────────


def build_manifest() -> dict:
    """Texts index: which records have body text on disk (transient,
    pre-bundle). After write_text_shards, this dict is empty — the
    canonical record-to-shard map is in texts-meta.json.
    """
    out: dict[str, dict] = {}
    if not TEXT_DIR.exists():
        return {"texts": out}
    for cat in CATEGORIES:
        cdir = TEXT_DIR / cat
        if not cdir.exists():
            continue
        out[cat] = {}
        for text_file in sorted(cdir.glob("*.txt")):
            fid = text_file.stem
            out[cat][fid] = {
                "size": text_file.stat().st_size,
                "url":  f"text/{cat}/{text_file.name}",
            }
    return {"texts": out}


def compute_audit(reports: dict[str, list[dict]]) -> dict:
    """Per-record extraction status."""
    counts = {
        "central_records":      len(reports.get("central", [])),
        "djvu_ok":              0,    # text present in shards
        "djvu_pending":         0,    # newly indexed but djvu not yet derivatived
        "no_text":              0,    # neither bundled nor on-disk
        "topics_assigned":      0,    # topic classifier ran (v1.x)
    }
    bundled_ids: set = set()
    texts_meta_path = DOCS / "texts-meta.json"
    if texts_meta_path.exists():
        try:
            with open(texts_meta_path, "r", encoding="utf-8") as f:
                tm = json.load(f)
            bundled_ids = set((tm.get("record_to_shard") or {}).keys())
        except (OSError, json.JSONDecodeError):
            pass

    central_text_dir = TEXT_DIR / "central"
    for r in reports.get("central", []):
        fid_full = f"central|{file_id_from_dict(r)}"
        if fid_full in bundled_ids:
            counts["djvu_ok"] += 1
        elif central_text_dir.exists() and (central_text_dir / f"{file_id_from_dict(r)}.txt").exists():
            counts["djvu_ok"] += 1
        else:
            counts["djvu_pending"] += 1   # treat as transiently missing — retry-able
        if r.get("topics"):
            counts["topics_assigned"] += 1

    counts["no_text"] = counts["central_records"] - counts["djvu_ok"] - counts["djvu_pending"]
    counts["no_text"] = max(0, counts["no_text"])
    return {
        "audited_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totals":     counts,
    }


def file_id_from_dict(r: dict) -> str:
    """Reproduce file_id from a dehydrated record dict.
    `central_<e|w>_<YYYYMMDD>_<ugid>`.
    """
    date_compact = (r.get("issue_date") or "").replace("-", "")
    return f"central_{r.get('type_letter','?')}_{date_compact}_{r.get('ugid','')}"


# ── Search bundle + index (same sharding as other corpora) ─────────────────

DOCS_PER_SHARD = 2500

# Separate, smaller cap for the search-INDEX shards. The bundle shards
# carry only `{key, title, head}` with HEAD=5000 chars/record (~8 KB/rec),
# so 2,500/shard yields ~20 MB shards — fine. The index shards carry
# the full tokenized text (vocab + postings), which is ~5× denser per
# record. At 2,500/shard the index crossed Cloudflare Workers' 25 MiB
# per-asset hard cap once the corpus grew past ~6k records-with-text
# (CF deploy failed 2026-05-17 on search-index-03.json at 26.9 MiB,
# see plan).
#
# 500/shard is conservative — at the failure-time per-record density,
# that gives ~5-6 MB per shard. More HTTP requests at query time (the
# app fetches all shards on first search), but well under the size cap
# with headroom for further growth. Revisit at ~50k records by moving
# shards to R2 (5 TiB per object) rather than chasing smaller and
# smaller shard caps.
SEARCH_INDEX_SHARD_SIZE = 500


def _delete_legacy(path: Path) -> None:
    if path.exists():
        path.unlink()


def _load_bundled_texts() -> dict[str, dict[str, str]]:
    """{ category → {file_id → text} } from texts-NN.json shards."""
    out: dict[str, dict[str, str]] = {c: {} for c in CATEGORIES}
    texts_meta_path = DOCS / "texts-meta.json"
    if not texts_meta_path.exists():
        return out
    try:
        with open(texts_meta_path, "r", encoding="utf-8") as f:
            tm = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"  ! couldn't read texts-meta.json ({e})")
        return out
    for shard_entry in (tm.get("shards") or []):
        sp = DOCS / shard_entry["file"]
        if not sp.exists():
            continue
        try:
            with open(sp, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        for k, v in (payload.get("records") or {}).items():
            if not isinstance(v, str):
                continue
            cat, _, fid = k.partition("|")
            if cat in out and fid:
                out[cat][fid] = v
    return out


def build_search_bundle(reports: dict[str, list[dict]],
                        docs_per_shard: int = DOCS_PER_SHARD) -> dict | None:
    """Subject + first 5K chars per record. Sharded by sorted report_key.

    Source of truth is `_load_bundled_texts()` (texts-NN.json shards). The
    on-disk `text/central/<fid>.txt` files are only used as a fallback for
    records that haven't been bundled yet (the brief window between a fresh
    scrape commit and the next derive run). On a fresh runner checkout the
    transient `text/` dir typically doesn't exist at all — that's fine, we
    can build the bundle entirely from bundled texts. Historically this
    function early-returned `None` whenever TEXT_DIR was missing, which
    silently disabled re-sharding on derive-only dispatches (e.g. after a
    SHARD_SIZE change). Removed that early-return.
    """
    HEAD = 5000
    entries = []
    truncated = 0
    bundled = _load_bundled_texts()
    central_dir = TEXT_DIR / "central"

    for r in reports.get("central", []):
        fid = file_id_from_dict(r)
        text: Optional[str] = bundled.get("central", {}).get(fid)
        if text is None and central_dir.exists():
            text_path = central_dir / f"{fid}.txt"
            if text_path.exists():
                try:
                    text = text_path.read_text(encoding="utf-8", errors="replace")
                except Exception:
                    text = None
        if text is None:
            continue
        head = text[:HEAD]
        if len(text) > HEAD:
            truncated += 1
        k = f"gazettes|central|{r.get('type_letter')}|{r.get('issue_date')}|{r.get('ugid')}"
        entries.append({"k": k, "t": r.get("subject", ""), "head": head})

    entries.sort(key=lambda e: e["k"])
    if not entries:
        return None

    _delete_legacy(DOCS / "search-bundle.json")
    for path in DOCS.glob("search-bundle-*.json"):
        try: path.unlink()
        except OSError: pass

    shard_count = (len(entries) + docs_per_shard - 1) // docs_per_shard
    shard_sizes: dict[str, int] = {}
    max_shard = 0; total_bytes = 0
    for i in range(shard_count):
        chunk = entries[i*docs_per_shard:(i+1)*docs_per_shard]
        path = DOCS / f"search-bundle-{i:02d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"version": "1.0", "head_chars": HEAD,
                       "shard_index": i, "entries": chunk}, f, ensure_ascii=False)
        sz = path.stat().st_size
        shard_sizes[f"search-bundle-{i:02d}.json"] = sz
        max_shard = max(max_shard, sz); total_bytes += sz
    return {
        "shard_count":     shard_count,
        "shards":          list(shard_sizes.keys()),
        "shard_sizes":     shard_sizes,
        "total":           len(entries),
        "truncated":       truncated,
        "head_chars":      HEAD,
        "size_bytes":      total_bytes,
        "max_shard_bytes": max_shard,
    }


_TOKEN_RE = re.compile(r"[a-z0-9]{3,}")
# Devanagari range U+0900-U+097F for Hindi tokenisation. Tokens are
# 3+ chars to filter noise.
_TOKEN_HI_RE = re.compile(r"[ऀ-ॿ]{3,}")


def _tokenize(text: str) -> list[str]:
    """Tokenise English (a-z + 0-9) plus Devanagari (Hindi). Both
    lowercased — Devanagari has no case but we apply the same .lower()
    for consistency.
    """
    lower = text.lower()
    return _TOKEN_RE.findall(lower) + _TOKEN_HI_RE.findall(lower)


def build_search_index(reports: dict[str, list[dict]],
                       docs_per_shard: int = DOCS_PER_SHARD) -> dict | None:
    """Full-body inverted token index, sharded. Bilingual: indexes
    both English (a-z0-9) and Devanagari tokens.

    Source of truth is `_load_bundled_texts()` (texts-NN.json shards). See
    build_search_bundle for the rationale behind not bailing when TEXT_DIR
    is absent — same logic applies here, and the same historical bug (the
    early-return silently disabled re-sharding on derive-only dispatches)
    motivated the removal.
    """
    docs: list[tuple[str, str, str]] = []   # (fid, k, text)
    bundled = _load_bundled_texts()
    central_dir = TEXT_DIR / "central"

    for r in reports.get("central", []):
        fid = file_id_from_dict(r)
        k = f"gazettes|central|{r.get('type_letter')}|{r.get('issue_date')}|{r.get('ugid')}"
        text: Optional[str] = bundled.get("central", {}).get(fid)
        if text is None and central_dir.exists():
            text_path = central_dir / f"{fid}.txt"
            if text_path.exists():
                try:
                    text = text_path.read_text(encoding="utf-8", errors="replace")
                except Exception:
                    continue
        if text:
            docs.append((fid, k, text))

    docs.sort(key=lambda d: d[1])
    if not docs:
        return None

    _delete_legacy(DOCS / "search-index.json")
    for path in DOCS.glob("search-index-*.json"):
        try: path.unlink()
        except OSError: pass

    shard_count = (len(docs) + docs_per_shard - 1) // docs_per_shard
    shard_sizes: dict[str, int] = {}
    max_shard = 0; total_bytes = 0
    for i in range(shard_count):
        chunk = docs[i*docs_per_shard:(i+1)*docs_per_shard]
        index: dict[str, list[int]] = {}
        keys: list[str] = []
        for di, (fid, k, text) in enumerate(chunk):
            keys.append(k)
            seen: set[str] = set()
            for tok in _tokenize(text):
                if tok in seen: continue
                seen.add(tok)
                index.setdefault(tok, []).append(di)
        for tok in index:
            index[tok].sort()
        path = DOCS / f"search-index-{i:02d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "version":     "1.0",
                "shard_index": i,
                "keys":        keys,
                "index":       index,
            }, f, ensure_ascii=False)
        sz = path.stat().st_size
        shard_sizes[f"search-index-{i:02d}.json"] = sz
        max_shard = max(max_shard, sz); total_bytes += sz
    return {
        "shard_count":     shard_count,
        "shards":          list(shard_sizes.keys()),
        "shard_sizes":     shard_sizes,
        "total":           len(docs),
        "size_bytes":      total_bytes,
        "max_shard_bytes": max_shard,
    }


# ── Phase orchestration ───────────────────────────────────────────────────


def phase_extract() -> int:
    """Walk + extract. Writes reports shards + transient text/central/*.txt
    + thin meta.json with run stats.
    """
    t_start = time.monotonic()
    overall_deadline = t_start + MAX_RUN_SECONDS

    # Reserve a slice of the budget for the djvu retry pass at the end.
    # Without this, the extract walk consumes the full deadline and the
    # retry pass starts with 0 time remaining — it prints "djvu retry
    # pool: N pending; budget=M/run" then immediately exits with
    # `[BUDGET] djvu retry wall-clock budget hit after 0 attempts`.
    # Diagnosed 2026-05-15 after 5 deep-seed dispatches grew the index
    # by 4,659 records but produced zero djvu_ok progress.
    #
    # Sizing: retry caps at MAX_DJVU_RETRY_PER_RUN records, each ~1-3 s
    # (single archive.org GET with redirect-follow). Cap reserve at
    # 1/3 of total budget OR 600 s, whichever is smaller, so the
    # hourly cron (900 s budget) still gives extract 600 s and the
    # sprint dispatches (3600 s budget) give extract 3000 s.
    retry_reserve_seconds = min(600, MAX_RUN_SECONDS // 3)
    extract_deadline = t_start + (MAX_RUN_SECONDS - retry_reserve_seconds)

    print(f"[phase_extract] run start: "
          f"budget={MAX_EXTRACTIONS_PER_RUN}/{MAX_RUN_SECONDS}s "
          f"(extract<={MAX_RUN_SECONDS - retry_reserve_seconds}s, "
          f"retry<={retry_reserve_seconds}s), "
          f"stop_after_known={ENUM_STOP_AFTER_KNOWN}")

    existing = load_existing_reports()
    print(f"  existing: " + ", ".join(f"{c}={len(existing.get(c, []))}" for c in CATEGORIES))

    extract_result = walk_archive_and_extract(existing, deadline=extract_deadline)
    rate_limited = extract_result.get("rate_limited", False)

    # Retry pass for djvu_pending records — see retry_djvu_pending docstring.
    # Skip if we already hit a rate-limit during extract (budget exhausted
    # AND archive.org is throttling us; back off and let the next run try).
    if not rate_limited:
        retry_result = retry_djvu_pending(existing, deadline=overall_deadline)
        if retry_result.get("rate_limited"):
            rate_limited = True
    else:
        retry_result = {"attempted": 0, "succeeded": 0, "pending_total": 0,
                        "rate_limited": False, "budget_hit": False,
                        "skipped_reason": "rate_limited_during_extract"}

    meta = {
        "phase":        "extract",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "elapsed_s":    round(time.monotonic() - t_start, 1),
        "extracted":    len(extract_result.get("extracted", [])),
        "failed":       len(extract_result.get("failed", [])),
        "djvu_retry": {
            "attempted":      retry_result.get("attempted", 0),
            "succeeded":      retry_result.get("succeeded", 0),
            "pending_total":  retry_result.get("pending_total", 0),
            "budget":         MAX_DJVU_RETRY_PER_RUN,
        },
        "rate_limited": rate_limited,
        "budget": {
            "max_extractions": MAX_EXTRACTIONS_PER_RUN,
            "max_run_seconds": MAX_RUN_SECONDS,
            "stop_after_known": ENUM_STOP_AFTER_KNOWN,
        },
    }
    if rate_limited:
        meta["rate_limited_at"] = meta["generated_at"]
    # Idempotent write: skip the write entirely if only `generated_at`
    # would change. The next derive run's meta.json fully replaces this
    # one, so a no-op extract-meta write is pure CF-build noise.
    # `elapsed_s` is timing-variant so ignore it too.
    wrote_meta = write_json_idempotent(
        META_JSON, meta, ignore_keys=("generated_at", "elapsed_s"),
    )
    if not wrote_meta:
        print("  [skip] extract meta.json unchanged (besides timing) — no commit churn")

    print(f"[phase_extract] done in {meta['elapsed_s']}s: "
          f"extracted={meta['extracted']}, "
          f"djvu_retry_succeeded={retry_result.get('succeeded', 0)}, "
          f"rate_limited={rate_limited}")

    # Emit outputs for the workflow's chain step (deep-seed sprint).
    # See .github/workflows/gazettes.yml "Chain another deep-seed run".
    extracted_count = len(extract_result.get("extracted", []))
    retry_succeeded = retry_result.get("succeeded", 0)
    progress = extracted_count + retry_succeeded
    budget_hit = (
        extract_result.get("budget_hit", False) or
        retry_result.get("budget_hit", False)
    )
    gh_output_path = os.environ.get("GITHUB_OUTPUT")
    if gh_output_path:
        try:
            with open(gh_output_path, "a", encoding="utf-8") as f:
                f.write(f"progress={progress}\n")
                f.write(f"budget_hit={'true' if budget_hit else 'false'}\n")
                f.write(f"rate_limited={'true' if rate_limited else 'false'}\n")
            print(f"  workflow outputs: progress={progress} "
                  f"budget_hit={budget_hit} rate_limited={rate_limited}")
        except OSError as e:
            print(f"  (couldn't write GITHUB_OUTPUT: {e})")

    return 0 if not rate_limited else 1


def phase_derive() -> int:
    """Derived files: manifest, audit, bundled text shards, search
    bundle/index, meta.
    """
    t_start = time.monotonic()
    print("[phase_derive] start")
    reports = load_existing_reports()
    print(f"  reports loaded: " + ", ".join(f"{c}={len(reports.get(c, []))}" for c in CATEGORIES))

    # Idempotent writes for the three small derive outputs that always
    # carry a build-time timestamp. If the underlying data hasn't
    # actually changed, these no-op writes used to trigger a commit +
    # CF build every hour for no reason. See parliamentwatch_text_shards
    # write_json_idempotent for the full rationale.
    manifest = build_manifest()
    if not write_json_idempotent(MANIFEST_JSON, manifest):
        print("  [skip] manifest.json unchanged")

    audit = compute_audit(reports)
    if not write_json_idempotent(AUDIT_JSON, audit):
        print("  [skip] audit.json unchanged (besides timestamp)")

    # Bundle texts. Composite IDs: `central|<file_id>`.
    items: list[tuple[str, Path]] = []
    for cat in CATEGORIES:
        cdir = TEXT_DIR / cat
        if cdir.exists():
            for path in sorted(cdir.glob("*.txt")):
                items.append((f"{cat}|{path.stem}", path))
    text_meta = write_text_shards(DOCS, items)
    print(f"  bundled text shards: {text_meta['totals']}")

    bundle = build_search_bundle(reports)
    index  = build_search_index(reports, docs_per_shard=SEARCH_INDEX_SHARD_SIZE)

    meta = {
        "phase":         "derive",
        "generated_at":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "elapsed_s":     round(time.monotonic() - t_start, 1),
        "totals":        {c: len(reports.get(c, [])) for c in CATEGORIES},
        "search_bundle": bundle,
        "search_index":  index,
        "audit":         audit["totals"],
        "text_shards":   text_meta["totals"],
    }
    # Idempotent meta write — see manifest/audit above. `elapsed_s` is
    # timing-variant per run so ignore it; the rest of meta is genuinely
    # derived from disk state.
    wrote_meta = write_json_idempotent(
        META_JSON, meta, ignore_keys=("generated_at", "elapsed_s"),
    )
    if not wrote_meta:
        print("  [skip] derive meta.json unchanged (besides timestamp)")

    print(f"[phase_derive] done in {meta['elapsed_s']}s")
    return 0


def main() -> int:
    phase = os.environ.get("BUILD_PHASE", "extract").strip().lower()
    if phase == "extract":
        return phase_extract()
    elif phase == "derive":
        return phase_derive()
    elif phase == "all":
        rc = phase_extract()
        if rc != 0:
            print(f"  [main] extract failed (rc={rc}); skipping derive")
            return rc
        return phase_derive()
    else:
        print(f"unknown BUILD_PHASE={phase!r} (extract|derive|all)")
        return 2


if __name__ == "__main__":
    sys.exit(main())
