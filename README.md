# sansadsaar-gazettes

Index repo for the Central Gazette of India corpus on SansadSaar.

## What this repo holds

- **`docs/gazettes/`** — corpus data, served by Cloudflare Pages.
  - `reports-meta.json` + `reports-central-NN.json` — record metadata
    (sharded).
  - `texts-NN.json` + `texts-meta.json` — bundled body text (sharded).
  - `search-bundle-NN.json` + `search-index-NN.json` — search artifacts.
  - `manifest.json`, `audit.json`, `meta.json` — derived overviews.
- **`gazettes/`** — Python scraper package (`scrapers/archive_org.py`).
- **`build_gazettes.py`** — orchestrator (extract + derive phases).
- **`.github/workflows/`** — cron-driven scrape + derive workflows.

## What this repo does NOT hold

- **Original PDFs.** Those live at archive.org (which serves them
  directly to the SansadSaar app) and, separately, in cold-mirror
  repos named `sansadsaar-gazettes-mirror-<years>` for continuity
  insurance.

## Upstream

[archive.org/details/gazetteofindia](https://archive.org/details/gazetteofindia)
— a continuously-maintained mirror of the Central Gazette + ~25 state
gazettes maintained by Sushant Sinha. We pull from here exclusively
per the project's archive.org-first policy.

## Approach

For each archive.org item matching `in.gazette.central.{e|w}.*`:

1. Fetch `/metadata/<identifier>` → ministry, department, subject,
   languages, gazette ID, source URL.
2. Fetch `<identifier>_djvu.txt` → archive.org's pre-OCR'd text
   (`tesseract 5.x -l hin+eng`, bilingual English + Devanagari Hindi).
3. Bundle text + metadata into our sharded index.

No own OCR, no pypdf, no `egazette.gov.in` scraping. archive.org has
already done the work; we're a downstream consumer.

## License

The Python code in this repo is original work by SansadSaar. The
gazette data flows from public-domain Government of India
publications and is consumed via archive.org's open APIs.
