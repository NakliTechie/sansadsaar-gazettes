# Pending: Gazettes PDF mirror

**Status:** deferred to next month.
**Filed:** 2026-05-15.

## Why deferred

archive.org has been stable as a long-term host for `gazetteofindia`
items. The current corpus stores `pdf_url_ia` (the archive.org URL) on
every record; the SansadSaar app links directly there. Users get the
PDF without the mirror needing to hold it.

Self-hosting the PDFs is a *durability hedge*, not a working
requirement. The hedge value is real (archive.org could rotate URLs,
restrict access, or rate-limit at scale) but the risk profile doesn't
justify the cost right now.

## Why not now

Two near-term constraints we'd run into before getting any real value:

1. **Cloudflare build quota.** As of 2026-05-15 we're already at ~50%
   of the monthly free-tier build budget across three data repos. A
   PDF-mirror repo would add another commit stream and likely push us
   into paid territory before this is the most-valuable spend.
2. **Storage shape.** ~170K Central Gazette items × even a modest
   avg PDF size (~500 KB) ≈ 85 GB. Won't fit in a single GitHub repo
   (1 GB recommended ceiling). Needs either:
   - sharding into many small repos (operationally annoying), or
   - R2 / external object storage (extra cost + auth), or
   - GitHub LFS (not free at this volume).

Neither path is wrong — both want a clear decision and a small
experiment first.

## When to revisit

- After the CF quota mitigation lands (this session) and we have a
  month or two of build-usage data showing how much headroom the
  skip-no-op-derive change actually buys.
- After the djvu-text retry pass (this session) catches up the
  pending pool — that's the more valuable text-side work to land
  first.
- Once we have a clearer signal on which other corpora are likely
  to consume CF builds (e.g. if Questions per-question granularity
  lands and starts pushing more frequently).
- Before any decision to expand scope to State Gazettes (separate
  upstream, different identifier patterns, will roughly double the
  corpus item count).

## What to revisit

When the time comes:

1. **Pick the storage tier.** Compare:
   - Multiple GitHub repos sharded by year (e.g.
     `sansadsaar-gazettes-pdfs-2026`, `…-2025`, etc.) — ~1-3 GB each,
     served via `raw.githubusercontent.com` (already CORS-enabled).
   - One R2 bucket — flat pricing, single auth boundary, but cost
     scales with stored bytes + egress (likely cheap at gazette
     traffic levels but not zero).
   - LFS — convenient but billed and the bandwidth caps are easy to
     blow through.
2. **Wire the workflow.** Pattern is the same as
   `parliamentwatch-data/lc.yml` → `sansadsaar-lc` archive: per-record
   `archive_pdf()` call as a side effect of `walk_archive_and_extract`,
   sha256+size short-circuit for idempotence, separate PAT-authenticated
   push.
3. **Slow cadence.** Whatever the storage tier, run this less
   frequently than the djvu-text pull. Maybe 25 PDFs every 6 h. The
   PDFs are the heavy bandwidth side; the text is the index-side
   value.

## What's NOT deferred

- **djvu OCR-text mirror** — actively prioritised this session. The
  text is what makes the corpus searchable; PDFs are just for
  user-facing "view original" clicks (and the archive.org link
  already covers that).
- **State Gazettes (next direction).** State-level identifier
  patterns (`in.gazette.s.*` instead of `in.gazette.central.*`) need
  parser work, but the upstream is the same archive.org collection.
  Worth scoping after djvu pending pool clears.
