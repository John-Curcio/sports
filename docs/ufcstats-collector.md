# Local UFCstats collection

[DAN-7](https://linear.app/danaher/issue/DAN-7/fix-ufcstats-scraper-with-firefox)
uses ordinary headless Firefox to acquire UFCstats HTML. It keeps the existing
BeautifulSoup/pandas parsers and URL identities. A browser is created lazily and
reused throughout discovery and collection; imports and cached parsing need no
running browser. Other shared HTTP scrapers and the ESPN API retain their own
acquisition paths.

## Setup

Install Firefox locally. From the repository root, create a small scraping
environment independent of the legacy Stan environment:

```sh
python3 -m venv .venv-ufcstats
source .venv-ufcstats/bin/activate
python -m pip install -r requirements-ufcstats.txt
python -m pytest -q tests/test_ufcstats.py
```

Python 3.9+ is supported; validation used Python 3.14.6, Selenium 4.36.0,
pandas 2.3.3 and locally installed Firefox. Selenium Manager locates/downloads
geckodriver when necessary. For explicit local paths, pass `--firefox-binary`
and `--geckodriver`. These use the current Selenium Options/Service interfaces;
see [Firefox configuration](https://www.selenium.dev/documentation/webdriver/browsers/firefox/).
Temporary research dependency/driver paths are not application dependencies.

## Collection and resume

Start with a bounded historical selection and a separate output database:

```sh
python -m scrape.scrape_ufcstats historical \
  --event-url http://ufcstats.com/event-details/253d3f9e97ca149a \
  --max-fighters 2 --max-fights 2 \
  --checkpoint /tmp/ufcstats-sample-checkpoint.sqlite \
  --db /tmp/ufcstats-sample.sqlite
python -m scrape.scrape_ufcstats upcoming --max-events 2 \
  --db /tmp/ufcstats-sample.sqlite
```

Omitting `--db` collects and reports counts without publishing tables. Passing
`--db` explicitly replaces the selected UFCstats output tables in that database;
bounded selections produce a bounded dataset. Historical limits take a
deterministic sorted selection of discovered URLs, not a date range. Without
`--event-url`, discovery starts with `--letters` (default a-z), then traverses
fighter histories. `--max-fighters` limits the profiles traversed, while
`--max-events`/`--max-fights` limit the later collection; those latter limits do
not limit discovery itself. Use event URLs for small, predictable runs.

Historical HTML is committed to a separate SQLite checkpoint after each
successful acquisition. Rerun the same command/checkpoint to reuse acquired
pages and retry unfinished pages. Resume reparses saved HTML, so it survives
process termination without serializing pandas objects. Cache reads validate
page structure again. The default checkpoint is
`.cache/ufcstats/checkpoint.sqlite`. Use one process per checkpoint.

Historical pages remain cached indefinitely. `--refresh` fetches each selected
page again once per run, including corrections; failed refreshes stop the run
and do not substitute older cached HTML. A new checkpoint path starts a new
snapshot. Upcoming listings and cards always load fresh and are not cached.

Defaults are a 45-second navigation timeout, a 30-second content wait, two
retries, and at least one second between navigation starts. Configure these
with `--page-timeout`, `--wait-timeout`, `--retries`, and `--pace`. Explicit
content waits follow [Selenium's wait guidance](https://www.selenium.dev/documentation/webdriver/waits/).
Readiness differs for directories, biographies, events, fights, and upcoming
listings. A browser-check page is a failure, not an empty result. Exhausted
attempts report the URL and stop collection. Failed browser sessions are shut
down and recreated before a bounded retry; successful checkpoints remain.

## Outputs and ownership

Historical publication preserves `ufc_fight_description`, `ufc_totals`,
`ufc_strikes`, `ufc_round_totals`, `ufc_round_strikes`, `ufc_events`, and
`ufc_fighters`. Upcoming publication preserves `ufc_upcoming_fights`. Round
indices remain zero-based, matching the existing contract. HTTP URL identities
are retained even if links use HTTPS.

A completed fight with the site's explicit missing-round-statistics notice
keeps its description and omits unavailable statistics. `UfcDataCleaner(db)`
accepts an isolated database and permits descriptions without statistics; its
existing left joins preserve metadata and leave statistics missing. Genuine
empty upcoming listings/cards produce schemaful empty outputs.

Publication refuses unfinished historical/upcoming collections. It stages all
selected tables and swaps them in one explicit SQLite transaction. Acquisition,
parsing, staging, or swap failures preserve existing output tables. Successful
HTML checkpoints remain independently available for resume. A process killed
during staging can leave unused `_ufcstats_` staging tables; published tables
remain intact.

The existing `UfcUrlScraper`, `FullUfcScraper`, `UpcomingUfcScraper`, and `main()`
entry points remain. `main()` is the full legacy pipeline entry point and writes
the default `mma.db` only after historical and upcoming collection both succeed.
Do not use that entry point for bounded validation.

For Python callers, wrap `FirefoxClient(...)` in `with` and pass `client=` to
scrapers to share ownership across stages. Without an injected client, each
public collection call owns and closes its session, including on exceptions.
Individual page scrapers own a temporary session if no client is supplied.

## Validation

Run the bounded live check with only temporary output/checkpoint databases:

```sh
PYTHONPATH=. python tests/live_ufcstats_smoke.py
```

It acquires the four researched page types, a five-round fight, the current
upcoming listing and at most two upcoming cards. It verifies actual statistics,
event metadata, per-round alignment, isolated table publication/cleaning, and
historical resume with zero browser starts. Offline fixtures cover missing
statistics, challenge pages, real empty cards, interruptions, browser restart,
refresh failures, and transaction rollback. The missing-statistics and empty
fixtures are synthetic variants of real source HTML.

Bounded live validation passed on October 2, 2026: UFC 274 had 14 fights;
Oliveira–Gaethje had 30/21 significant strikes; Esparza–Namajunas yielded five
rounds per fighter. Two upcoming cards yielded 14 and 12 matchups. A sustained
full historical crawl and the complete legacy pipeline remain untested.
