# sansadsaar-gazettes

Index data for the **Central Gazette of India** corpus on [SansadSaar](https://github.com/NakliTechie/SansadSaar).

Holds metadata + bilingual (English + Hindi) extracted text + search artifacts for ~170K Central Gazette items (Weekly + Extraordinary, 1947→present). The actual PDFs live at archive.org and are served directly from there to the app.

## Why a separate repo?

Volume. The Central Gazette corpus alone is comparable in scale to the rest of [parliamentwatch-data](https://github.com/NakliTechie/parliamentwatch-data) combined. Splitting keeps each repo's Cloudflare Pages deploy comfortably under the per-project file count cap.

## Credits

PDFs + OCR text sourced from the [`gazetteofindia` collection](https://archive.org/details/gazetteofindia) on archive.org, continuously populated since 2014 by [Sushant Sinha](https://github.com/sushant354)'s [`egazette`](https://github.com/sushant354/egazette) project. Our scraper pattern is observed from that project but independently re-implemented per the project's Independence Principle.
