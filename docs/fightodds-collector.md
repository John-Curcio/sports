# FightOdds collector

Collect moneylines, all available props, and timestamped per-book histories from the public, undocumented `https://api.fightodds.io/gql` endpoint. Python 3.9+ and `requests` are required; identity mapping uses existing `pandas`/`numpy` dependencies. No credentials or browser were needed in bounded validation.

## Usage

Run from the repository root with a separate SQLite database:

```sh
python -m scrape.scrape_fightodds --db /tmp/fightodds.sqlite collect \
  --promotion UFC --name 'UFC 285:' --fight-slug jon-jones-vs-cyril-gane-43970 \
  --pace 1 --report /tmp/fightodds-report.json
python -m scrape.scrape_fightodds --db /tmp/fightodds.sqlite snapshot \
  --event-id 'RXZlbnROb2RlOjQ0NTA=' --output /tmp/fightodds-midnight.json
python -m pytest -q tests/test_fightodds.py
```

`--start`/`--end` filter inclusive source dates; `--promotion` matches the promotion short name. Discovery defaults to one event (`--max-events`). Without `--fight-slug`, all fights on selected events are collected; repeat it to select several fights. Event-linked offers are queried alongside fight offers. See each subcommand's `--help` for options.

Successful pages are cached indefinitely: rerun to resume failed requests, or use `--refresh` to fetch changes, especially for current events. Refresh retains existing histories and archived corrections. Reports distinguish complete, empty/unavailable, and failed histories; failures exit nonzero. Defaults bound collection to 100 nodes/page, 100 pages/connection, and 1,000 network attempts, with 30-second timeouts and two retries. Increase `--max-pages` for large histories. Use one process and `--pace 1` or slower if rate limited. The CLI refuses output databases named `mma.db`.

## Historical prices

Default cutoff is midnight `America/New_York` on the advertised event date, including DST: Friday into Saturday for a Saturday date. Source date timezone semantics and historical `startTime` values are unverified, so this fallback exposes `timing_uncertain: true`. `--trust-start-time` derives the Eastern day from a separately verified start; `--cutoff` accepts an explicit offset-bearing instant. `--hour` changes the Eastern hour; nonexistent DST hours are rejected and repeated hours use their earliest occurrence. A cutoff at/after source start is flagged `possible_after_start`.

Each outcome selects its latest observation at or before the cutoff and retains its timestamp, quote age, and provenance. Later quotes cannot influence the result. Missing earlier observations, null prices, same-time conflicts, and cancelled/hypothetical records remain explicit. Incomplete or retained archive histories produce candidate states. `--stale-seconds` optionally flags old quotes; there is no default age limit.

Historical transaction availability and in-play status remain unknown. Current offer status/disabled flags cannot establish past availability. These snapshots are not closing odds; no regular sampling or availability through gaps is inferred.

## Storage and integration

Queries and the schema live in [scrape_fightodds.py](../scrape/scrape_fightodds.py). Collection paginates `allEvents`, `allFights`, `allOffers`, `fightPropOfferTable.propOffers`, and `allOdds`. Nested `Offer.outcomes` rejects pagination arguments despite advertising them; its lists were complete in validation, and truncated lists are reported as failures. Moneylines use `STRAIGHT`; every other offer is retained in prop coverage, including unknown types. Original labels, lines, source IDs, and duplicate book offers stay distinct.

SQLite tables use a `fightodds_` prefix:

- `records`: current event/fight/fighter/book/market/offer/outcome/group JSON keyed by `(kind, id)`. Joins follow fight → `event_id`, offer → `market_id`/source fight/book, and outcome → `offer_id`. Market lines use offer `value`, otherwise offer-type `value`; unknown types retain offer-specific identities.
- `prices`: source price ID, outcome ID, UTC timestamp, nullable American odds, and raw response ID. Distinct IDs at equal timestamps remain separate; corrections update the current record.
- `record_versions`, `price_versions`, `responses`: changed records and full successful responses, including queries, variables, and fetch times. Refresh never deletes observations solely because they disappeared from a response.
- `history_status`, `runs`, `identity_maps`: coverage checks, run reports, and explicit cross-source mappings.

[The identity adapter](../wrangle/fightodds_data.py) accepts caller-supplied canonical rows and reuses the existing matcher without opening the legacy database. Snapshots retain source IDs, book/market/outcome identity, labels/lines, cutoff basis, source dates, quote age, coverage state, and provenance; downstream joins must preserve them. Pipeline publication, consumer migration, prop settlement, and backtests remain separate work.

## Validation

Bounded live collection on October 2, 2026 completed every selected connection, with no remaining request failures after resume. Counts include retained source duplicates; each row represents one selected fight, not event-wide coverage.

| Sample | Moneyline outcomes / observations | Prop outcomes / observations | Display groups |
| --- | --- | --- | --- |
| UFC 285: Jones–Gane | 34 / 2,070 | 466 / 1,949 | 97 |
| UFC 284: Makhachev–Volkanovski | 36 / 12,324 | 427 / 2,025 | 67 |
| Currently listed Silva–Wang, event 9743 | 58 / 1,325 | 1,086 / 9,018 | 154 |

DraftKings Jones reproduced the researched 37-point series; midnight March 4 selected Jones −175 and Gane +150, with quotes about 5.8 hours old. All display groups had corresponding offer types. Event-linked offers were empty for all three samples. Rate-limited requests resumed successfully; two UFC 284 histories needed a larger page cap.

Offline [tests and fixtures](../tests/test_fightodds.py) cover pagination, errors, duplicate offers, resume/refresh, missing/null histories, corrections, cancellations, DST, future-price exclusion, and isolated ESPN/UFCstats mappings. Jones–Gane mapped two fighters, one fight, and one event per source without changing snapshots. Validation explicitly recorded `Cyril Gané` → `Ciryl Gane`, matching the source moneyline label and canonical names; legacy ESPN event targets use name plus date. Global coverage and sustained crawling reliability remain unverified. The full implementation ticket is [DAN-8](https://linear.app/danaher/issue/DAN-8/implement-a-fightodds-graphql-scraper).
