"""Shared primitives for the gazettes corpus.

Architectural approach (per plan/gazettes-recon-001.md):
- archive.org's `gazetteofindia` collection is the SOLE upstream for v1.
- Sushant354's iasync push has already deposited 170K+ Central Gazette
  items with full metadata + tesseract OCR (-l hin+eng).
- We never touch egazette.gov.in for the back-catalogue. The
  archive.org-first policy applies project-wide.

Scope: HTTP only. The scraper module owns parsing + dataclasses.
"""

from __future__ import annotations

import os
import random
import time
from typing import Optional

import requests


# archive.org endpoints
ARCHIVE_BASE  = "https://archive.org"
SCRAPE_API    = f"{ARCHIVE_BASE}/services/search/v1/scrape"   # bulk enum with cursor
METADATA_API  = f"{ARCHIVE_BASE}/metadata"                    # /<identifier>
DOWNLOAD_API  = f"{ARCHIVE_BASE}/download"                    # /<identifier>/<filename>

# Politeness budget. archive.org allows ~30 req/sec per IP but we go
# much slower — small project, no need to push limits.
_RETRY_AFTER_DEFAULT_SECONDS = 30
_JITTER_MIN_MS = int(os.environ.get("JITTER_MIN_MS", "200"))
_JITTER_MAX_MS = int(os.environ.get("JITTER_MAX_MS", "400"))


class RateLimited(Exception):
    """Raised on 429 / 503 from archive.org with parsed Retry-After.
    Corpus-local per the Independence Principle.
    """


def jitter() -> None:
    delay = random.randint(_JITTER_MIN_MS, _JITTER_MAX_MS) / 1000.0
    time.sleep(delay)


def http_get(url: str, *, headers: Optional[dict] = None, params: Optional[dict] = None,
             timeout: int = 60, retries: int = 1,
             stream: bool = False) -> requests.Response:
    """GET with one retry, polite jitter, and 429/503 handling.

    archive.org occasionally 503s when their search backend is backed up
    or during their daily index rebuilds. Treat both 429 and 503 as
    rate-limit signals.
    """
    headers = dict(headers or {})
    headers.setdefault("User-Agent", "sansadsaar-gazettes/1.0 (+https://sansadsaar.naklitechie.com)")
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            jitter()
            resp = requests.get(url, headers=headers, params=params,
                                timeout=timeout, stream=stream)
            if resp.status_code in (429, 503):
                ra = resp.headers.get("Retry-After")
                wait = _RETRY_AFTER_DEFAULT_SECONDS
                if ra:
                    try:
                        wait = int(ra)
                    except ValueError:
                        pass
                raise RateLimited(f"{resp.status_code} on {url} (retry-after {wait}s)")
            resp.raise_for_status()
            return resp
        except RateLimited:
            raise
        except Exception as e:
            last_exc = e
            if attempt < retries:
                jitter()
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError(f"unreachable http_get fallthrough for {url}")
