"""archive.org scraper for the Central Gazette of India.

Pulls from the `gazetteofindia` collection. Each item is a single
notification (Extraordinary) or weekly issue. Per-item we fetch:

  1. /metadata/<identifier>           → ministry, department, subject, etc.
  2. /download/<id>/<seq>_djvu.txt    → OCR'd body text (pre-existing
                                          via tesseract -l hin+eng)
  3. /download/<id>/<seq>.pdf         → the original PDF (for cold mirror)

No pypdf, no OCR runs, no postback dance. archive.org has already done
the work; we're a downstream consumer of sushant354's iasync pipeline.

Identifier pattern: `in.gazette.central.<t>.<YYYY-MM-DD>.<seq>` where
`<t>` is `e` (extraordinary) or `w` (weekly). The trailing `<seq>` is
the egazette.gov.in UGID — globally unique across both types.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Iterator, Optional
from urllib.parse import quote

from gazettes.common import (
    RateLimited, http_get,
    SCRAPE_API, METADATA_API, DOWNLOAD_API,
)


CENTRAL_PREFIX = "in.gazette.central"
QUERY_ALL = f"collection:gazetteofindia AND (identifier:{CENTRAL_PREFIX}.e.* OR identifier:{CENTRAL_PREFIX}.w.*)"

# Default page size for archive.org's scrape API. Min is 100 (enforced
# by the API — smaller values return 400), max is documented at 10,000
# but practical max is ~1000 before responses get sluggish.
DEFAULT_PAGE_SIZE = 500
MIN_PAGE_SIZE     = 100


# ── Records ────────────────────────────────────────────────────────────────


@dataclass
class GazetteRecord:
    """One Central Gazette notification (Extraordinary) or weekly issue.

    Composite primary key: (category, ugid). The same UGID is unique
    across categories so it's the natural primary key on its own — we
    carry category for filtering / display only.
    """
    identifier:     str                # e.g. in.gazette.central.e.2025-02-27.261350
    category:       str                # "extraordinary" | "weekly"
    type_letter:    str                # "e" | "w"
    issue_date:     str                # YYYY-MM-DD
    ugid:           str                # last segment, e.g. "261350"
    title:          str = ""
    ministry:       str = ""
    department:     str = ""
    office:         str = ""
    subject:        str = ""
    gazette_id:     str = ""           # e.g. "CG-DL-E-28022025-261350"
    gazette_source_url: str = ""       # the original egazette.gov.in URL
    languages:      list[str] = field(default_factory=list)
    # Resolution paths
    djvu_url:       str = ""
    pdf_url_ia:     str = ""           # archive.org PDF (we link to this)
    pdf_filename:   str = ""           # bare filename inside the IA item


def _parse_identifier(identifier: str) -> Optional[dict]:
    """`in.gazette.central.e.2025-02-27.261350` →
        {type: 'e', date: '2025-02-27', ugid: '261350'}.
    Returns None if pattern doesn't match.
    """
    m = re.match(rf"^{re.escape(CENTRAL_PREFIX)}\.([ew])\.(\d{{4}}-\d{{2}}-\d{{2}})\.(.+)$",
                 identifier)
    if not m:
        return None
    return {"type": m.group(1), "date": m.group(2), "ugid": m.group(3)}


# Parser for the archive.org `description` field. Sushant354's iasync
# populates this with a structured block like:
#   <p>Date: 2025-02-27<br />Ministry: ...<br />Department: ...<br />...
# We extract the key:value pairs into our record fields.
_DESCRIPTION_FIELD_RE = re.compile(
    r"(?P<key>[A-Z][A-Za-z ]+?):\s*(?P<value>[^<]*)",
    re.IGNORECASE | re.DOTALL,
)
# Hrefs are slightly different shape — "Gazette Source: <a href=...>URL</a>"
_HREF_RE = re.compile(r'href="([^"]+)"')


def _parse_description(html: str) -> dict[str, str]:
    """Pull key:value pairs out of sushant354's archive.org description.
    Handles the standard fields (Date, Ministry, Department, Subject,
    Office, Gazette ID) and the embedded Gazette Source <a href>.
    """
    fields: dict[str, str] = {}
    if not html:
        return fields
    # Normalise whitespace inside the HTML so the regex behaves.
    h = html.replace("\r", "").replace("\n", " ")
    for m in _DESCRIPTION_FIELD_RE.finditer(h):
        key = m.group("key").strip()
        val = m.group("value").strip()
        if val:
            fields[key] = val
    # Extract the embedded URL too.
    a = _HREF_RE.search(h)
    if a:
        fields["Gazette Source URL"] = a.group(1)
    return fields


# ── Enumerate items via scrape API ──────────────────────────────────────────


def enumerate_items(*, page_size: int = DEFAULT_PAGE_SIZE,
                    cursor: Optional[str] = None,
                    extra_query: str = "") -> Iterator[dict]:
    """Iterator over archive.org item summaries matching the Central
    Gazette pattern. Uses the scrape API with cursor pagination
    (handles deep pagination cleanly; advancedsearch.php caps at
    ~10K results).

    Yields dicts with at minimum: identifier, date.
    Caller can pass `extra_query` to narrow further (e.g.
    "AND date:2026").
    """
    q = QUERY_ALL + (f" AND {extra_query}" if extra_query else "")
    page_size = max(page_size, MIN_PAGE_SIZE)
    params = {
        "q": q,
        "fields": "identifier,date,title",
        "count": page_size,
        "sorts": "date asc",     # walk oldest-first
    }
    if cursor:
        params["cursor"] = cursor

    while True:
        resp = http_get(SCRAPE_API, params=params)
        try:
            data = resp.json()
        except Exception as e:
            print(f"  ! scrape API returned non-JSON: {e}")
            return
        for item in (data.get("items") or []):
            yield item
        cursor = data.get("cursor")
        if not cursor:
            return
        params["cursor"] = cursor


# ── Per-item metadata ───────────────────────────────────────────────────────


def fetch_record(identifier: str) -> Optional[GazetteRecord]:
    """Fetch metadata + derivative file info for one IA item.
    Returns a populated GazetteRecord (still without text) or None
    if the item shape doesn't match a Central Gazette pattern.
    """
    parsed = _parse_identifier(identifier)
    if parsed is None:
        return None

    resp = http_get(f"{METADATA_API}/{identifier}")
    data = resp.json()
    meta = data.get("metadata") or {}
    files = data.get("files") or []

    # Find the PDF + djvu derivative filenames inside this item.
    pdf_filename = ""
    djvu_filename = ""
    for f in files:
        n = f.get("name") or ""
        if n.endswith("_djvu.txt"):
            djvu_filename = n
        elif n.endswith(".pdf") and "_text" not in n and not n.endswith("_searchable.pdf"):
            # Plain PDF (skip _text.pdf/_searchable.pdf derivatives).
            pdf_filename = n

    description = meta.get("description") or ""
    desc_fields = _parse_description(description)

    type_letter = parsed["type"]
    category = "extraordinary" if type_letter == "e" else "weekly"

    languages_raw = meta.get("language") or ""
    if isinstance(languages_raw, str):
        languages = [s.strip() for s in re.split(r"[,;]", languages_raw) if s.strip()]
    elif isinstance(languages_raw, list):
        languages = [str(s).strip() for s in languages_raw if str(s).strip()]
    else:
        languages = []

    rec = GazetteRecord(
        identifier=     identifier,
        category=       category,
        type_letter=    type_letter,
        issue_date=     parsed["date"],
        ugid=           parsed["ugid"],
        title=          meta.get("title") or "",
        ministry=       desc_fields.get("Ministry", ""),
        department=     desc_fields.get("Department", ""),
        office=         desc_fields.get("Office", "") if desc_fields.get("Office", "") != "Not Applicable" else "",
        subject=        desc_fields.get("Subject", ""),
        gazette_id=     desc_fields.get("Gazette ID", ""),
        gazette_source_url= desc_fields.get("Gazette Source URL", ""),
        languages=      languages,
        djvu_url=       f"{DOWNLOAD_API}/{identifier}/{djvu_filename}" if djvu_filename else "",
        pdf_url_ia=     f"{DOWNLOAD_API}/{identifier}/{pdf_filename}" if pdf_filename else "",
        pdf_filename=   pdf_filename,
    )
    return rec


# ── Fetch djvu (OCR'd body text) ────────────────────────────────────────────


def fetch_djvu_text(record: GazetteRecord) -> Optional[str]:
    """Fetch archive.org's pre-OCR'd djvu.txt for this record. Returns
    plain text or None if the derivative isn't available yet (recent
    items take ~24h to derivative).
    """
    if not record.djvu_url:
        return None
    try:
        resp = http_get(record.djvu_url, timeout=120)
    except RateLimited:
        raise
    except Exception as e:
        # 404 here means the derivative isn't ready yet — not fatal.
        print(f"    djvu fetch failed for {record.ugid}: {e}")
        return None
    text = resp.text or ""
    if not text.strip():
        return None
    return text


# ── Fetch PDF (for cold mirror) ─────────────────────────────────────────────


def download_pdf(record: GazetteRecord, *, out_dir: str) -> Optional[str]:
    """Download the PDF for one record to `out_dir/<file_id>.pdf`.
    Returns the local path or None on failure.
    """
    if not record.pdf_url_ia:
        return None
    os.makedirs(out_dir, exist_ok=True)
    fid = file_id(record)
    pdf_path = os.path.join(out_dir, f"{fid}.pdf")
    if os.path.exists(pdf_path):
        return pdf_path
    try:
        resp = http_get(record.pdf_url_ia, timeout=300, stream=True)
        with open(pdf_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return pdf_path
    except RateLimited:
        raise
    except Exception as e:
        print(f"    PDF download failed for {record.ugid}: {e}")
        return None


# ── File ID + key conventions ───────────────────────────────────────────────


def file_id(record: GazetteRecord) -> str:
    """Stable id for text files + composite key suffix.
    `central_<e|w>_<YYYYMMDD>_<ugid>`.
    """
    date_compact = record.issue_date.replace("-", "")
    return f"central_{record.type_letter}_{date_compact}_{record.ugid}"


def report_key(record: GazetteRecord) -> str:
    """App-side primary key: `gazettes|central|<t>|<date>|<ugid>`."""
    return f"gazettes|central|{record.type_letter}|{record.issue_date}|{record.ugid}"


# ── Mirror shard routing ────────────────────────────────────────────────────

# Maps a (issue_date) to a mirror repo name. Bounds chosen during recon
# (plan/gazettes-recon-001.md) to keep each mirror under git's ~5 GB
# soft cap. Adjust upper-bound bracket as years pass.
MIRROR_REPO_FOR_DATE_RANGES = [
    # (start_year, end_year_inclusive, repo_name)
    (1982, 1995, "sansadsaar-gazettes-mirror-1982-1995"),
    (1996, 2010, "sansadsaar-gazettes-mirror-1996-2010"),
    (2011, 2018, "sansadsaar-gazettes-mirror-2011-2018"),
    (2019, 2022, "sansadsaar-gazettes-mirror-2019-2022"),
    (2023, 2025, "sansadsaar-gazettes-mirror-2023-2025"),
    (2026, 2099, "sansadsaar-gazettes-mirror-2026"),    # active; splits later
]


def mirror_repo_for(record: GazetteRecord) -> str:
    """Pick the mirror repo name for this record's date. Returns the
    repo name (relative path used by the orchestrator's cross-repo push).
    """
    try:
        year = int(record.issue_date[:4])
    except (ValueError, TypeError):
        # Defensive: an unparseable date defaults to the active mirror.
        return MIRROR_REPO_FOR_DATE_RANGES[-1][2]
    for (start, end, repo) in MIRROR_REPO_FOR_DATE_RANGES:
        if start <= year <= end:
            return repo
    return MIRROR_REPO_FOR_DATE_RANGES[-1][2]


def mirror_path_for(record: GazetteRecord) -> str:
    """Relative path inside the mirror repo where this record's PDF
    lives: `pdf/central/<e|w>/<YYYY-MM-DD>/<ugid>.pdf`.
    """
    return f"pdf/central/{record.type_letter}/{record.issue_date}/{record.ugid}.pdf"


# ── End-to-end fetch for one item ───────────────────────────────────────────


def fetch_all(identifier: str, *, text_dir: str, pdfs_dir: str,
              fetch_pdf: bool = True
              ) -> tuple[Optional[GazetteRecord], Optional[str], Optional[str]]:
    """End-to-end: fetch metadata → djvu text → PDF (optional).
    Returns (record, djvu_text, pdf_local_path). Any may be None on
    partial failure.

    `text_dir`: where djvu.txt gets written as `<file_id>.txt`.
    `pdfs_dir`: where PDF gets cached (caller manages lifecycle / push
    to mirror).
    """
    record = fetch_record(identifier)
    if record is None:
        return None, None, None

    djvu_text = fetch_djvu_text(record)
    if djvu_text:
        os.makedirs(text_dir, exist_ok=True)
        fid = file_id(record)
        text_path = os.path.join(text_dir, f"{fid}.txt")
        with open(text_path, "w", encoding="utf-8") as f:
            f.write(djvu_text)

    pdf_path = None
    if fetch_pdf:
        pdf_path = download_pdf(record, out_dir=pdfs_dir)

    return record, djvu_text, pdf_path
