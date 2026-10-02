# FightOdds collector

The collector uses the public, undocumented `https://api.fightodds.io/gql` endpoint. No credentials or browser are needed for the bounded samples verified on October 2, 2026. This is a source collector with isolated storage and an identity adapter; publication into `mma.db`, consumer migration, prop settlement, and backtests belong to separate tickets.

## Run

Use Python 3.9+ with `requests`. Identity mapping additionally uses the repository's existing `pandas`/`numpy` dependencies; offline tests use `pytest`. Run from the repository root:

```sh
python -m scrape.scrape_fightodds --db /tmp/fightodds.sqlite collect \
  --promotion UFC --name 'UFC 285:' --max-events 1 \
  --fight-slug jon-jones-vs-cyril-gane-43970 \
  --page-size 50 --pace 1 --max-requests 500 \
  --report /tmp/fightodds-report.json
python -m scrape.scrape_fightodds --db /tmp/fightodds.sqlite snapshot \
  --event-id 'RXZlbnROb2RlOjQ0NTA=' --output /tmp/fightodds-midnight.json
python -m pytest -q tests/test_fightodds.py
```

`--start`/`--end` apply inclusive source-date filters. `--promotion` matches the source's promotion short name; `--name` is a substring search. Default `--max-events 1` bounds discovery. Without `--fight-slug`, every fight on the selected event is collected, including cancelled records; repeated `--fight-slug` restricts history collection to those fights while retaining the event's discovered identities. Event-level offers are queried even when selecting a fight. Do not infer event-wide price coverage from a run restricted to a single fight.

The default limits are 100 nodes per page, 100 pages per connection, 1,000 network attempts, 30-second request timeouts, two retries, and half a second between requests. History requests batch up to 20 outcomes, advancing each cursor independently. Rate-limit retries respect numeric `Retry-After` with a 30-second sleep cap, otherwise wait 5/10 seconds. A cap or request failure produces an incomplete report and a nonzero exit status, never a claim of completeness. For large movement series, increase the explicitly bounded page limit. The API enforces a 100-node maximum on discovery connections; page sizes are restricted to 1..100. Use one collector process at a time and `--pace 1` or slower when rate limited.

Successful response pages are durably cached by endpoint, exact query, and variables. Repeating the command resumes from those pages and retries failures. Cache entries have no automatic expiry: use `--refresh` to fetch updated discovery, offers, and histories, especially for current events. Refresh caches successful requests within that invocation, archives raw responses, and preserves earlier observations. Failed responses are never substituted with old cached successes during refresh. A run report distinguishes completed history, unavailable (successfully queried but empty) history, and failed requests. Zero available props is a coverage gap, not an API error. Collection failures do not prevent histories for offers already discovered from being validated.

The CLI refuses any output DB named `mma.db`; use a dedicated path. It does not import the legacy database singleton or orchestration scripts.

## Source queries and completeness

Dynamic values use GraphQL variables. The query constants and builders are in `scrape/scrape_fightodds.py`.

- `allEvents(first, after, promotion_ShortName, date_Gte, date_Lte, name_Icontains, orderBy: "date")` returns source IDs, slugs, date, startTime, cancellation and temporary-record flags.
- `allFights(event, first, after)` discovers fighters, original names, fighter URLs, orientation, fight type, and cancellations.
- `allOffers(fight, first, after)` collects every offer type rather than a prop allowlist. `allOffers(event, first, after)` collects event-linked offers. Each offer retains its ID, sportsbook ID, type metadata, exact source values/lines, timestamps, current disabled/status fields, and original outcomes. `STRAIGHT` fight offers are reported as moneyline; all others, including unclassified offers, appear in prop coverage.
- `fightPropOfferTable(slug).propOffers(first, after)` collects every displayed group and its exact `propName1`/`propName2` labels and type metadata. The report compares the discovered type IDs with collected offer types. These labels accompany the underlying offers; they are not a replacement for outcome names or offer identity. The UI table groups fewer markets than `allOffers` because some underlying props have no type metadata.
- `allOdds(outcome, first, after, orderBy: "timestamp")` returns source price IDs, nullable American odds, and timezone-aware timestamps. No observations are synthesized from current/open/best/worst summaries.

Every paginated connection checks `pageInfo`; repeated cursors, malformed edges, missing paths, and exhausted page limits are errors. Nested `Offer.outcomes` advertises pagination in introspection but the live resolver rejects `first` (unexpected keyword argument). Its unparameterized connection returned `hasNextPage: false` in all validated offers. The collector checks this for every offer and reports failure if it ever becomes truncated; it cannot promise to retrieve a truncated list from that broken resolver. Offer and group discovery use separate paginated root/table connections to avoid unverified nested display-offer pagination.

`eventOfferTable` exposes fight offers, with no dedicated event prop table in the inspected schema. The generic event-filtered offer query supports event-level collection; all three validated events returned zero event-linked offers. This establishes an observed gap, not that event-level props can never exist.

## Storage and downstream contract

SQLite tables use a `fightodds_` prefix. `fightodds_records` contains one current JSON record per `(kind, id)` for `event`, `fight`, `fighter`, `sportsbook`, `market`, `offer`, `outcome`, and `prop_group`. The source-specific IDs are never written into BFO fields. This deliberately keeps flexible source metadata alongside explicit normalized keys rather than imposing a prop settlement taxonomy.

| Record | Join keys and retained data |
| --- | --- |
| Event | Relay `id`, integer `pk`, slug, source date/startTime, promotion, cancellation/temp |
| Fight | Relay `id`, slug, `event_id`, original fighter1/fighter2 metadata |
| Market | Deterministic ID over context, source offer type ID and offer value; `offer_type_id`, `event_id`, nullable `fight_id`, `market_kind`, exact description/notDescription, line and line basis |
| Offer | Source ID, `event_id`, source fight link, `market_id`, sportsbook metadata, source value, current status/disabled/timestamps |
| Outcome | Source ID, `offer_id`, exact name, nullable explicit fighter ID, `isNot`, nullable last/open/extrema summaries |
| Prop group | Context and source-label digest ID, `fight_id`, exact two display labels, full offer type metadata |

When a type is missing, the market identity includes the original offer ID, labels remain on the outcomes, and type metadata is explicitly unknown. A line comes from offer `value` when supplied, otherwise offer-type `value`; neither is parsed from the label. Different offers/outcomes sharing a sportsbook name remain separate, including archived duplicates and one-sided markets.

`fightodds_prices` stores one current observation per source price ID with outcome ID, UTC microsecond timestamp, nullable original American odds, and raw response ID. Distinct IDs at the same timestamp remain distinct. A correction to an existing source ID updates its normalized record; previous versions remain in `fightodds_price_versions`. `fightodds_record_versions` similarly retains changed dates, identities, labels, and statuses. Observations absent from a refresh are retained as archive data; absence alone does not prove deletion or suspension. `fightodds_responses` preserves query, variables, full successful response, fetch time, and ingestion provenance. `fightodds_history_status` stores the last complete/unavailable/failed check per outcome; `fightodds_runs` stores per-run reports.

`wrangle.fightodds_data.identity_frame(store)` provides source fighter/opponent identities, names, calendar dates and explicit FightOdds event/fight IDs in the shape accepted by the existing matcher. `map_identities(store, canonical_dataframe, 'espn' | 'ufcstats', day_tol=0, name_overrides=None)` writes explicit fighter mappings and uniquely verified pair/date event/fight mappings into the isolated `fightodds_identity_maps` table. For the legacy ESPN shape, event targets retain the event name together with its source date because that table has no canonical event ID. Canonical rows are supplied by the caller; the adapter never reads `mma.db`. The existing matcher lives in `wrangle.identity_matching`, with compatibility imports in `join_datasets`; the algorithm is unchanged.

The bounded Jones–Gane identity fixture was copied from the existing ESPN/UFCstats tables using SQLite read-only mode. FightOdds fighter metadata spells Gane `Cyril Gané`, while its moneyline outcome says `Ciryl Gane`, matching both canonical sources. The validation supplies this specific name override explicitly and records it as mapping evidence. No global fuzzy rename or sportsbook aggregation is imposed. Unmapped and ambiguous identities remain for downstream resolution.

## Historical lookup

Default cutoff is `00:00 America/New_York` on the source event date, with DST. This is midnight Friday into Saturday for a Saturday source date. It is recorded as `source_date_assumed_eastern_day` with `timing_uncertain: true`: source date timezone semantics are undocumented. For UFC 285 it is March 4, 2023, 05:00 UTC.

The API exposes offset-bearing `startTime`, but historical UFC 284/285 both return `08:00 UTC` on their advertised dates, and the current sample returns midnight UTC. These values have not been verified as actual event start times. Consequently, the default does not treat them as verified. `--trust-start-time` is an explicit opt-in after verifying a start time: it derives the Eastern calendar day from that instant, including overseas day shifts. `--hour 0..23` selects a different Eastern hour (the earliest occurrence for a repeated fall-back hour; nonexistent spring-forward hours are rejected); `--cutoff` supplies an explicit offset-bearing instant. Missing event dates produce missing cutoffs. A supplied source start at/before a chosen cutoff is flagged as `possible_after_start`, including with the date fallback.

For each stored outcome/offer, the lookup selects the newest observation at or before the cutoff. Later points cannot change the result. A null newest observation stays null rather than resurrecting an older price; conflicting distinct observations at the same newest timestamp produce an explicit conflict. Cancelled events/fights and temporary events have no selected price. Histories that failed or were not checked produce `candidate_from_incomplete_history` rather than a confirmed selection. Retained archive points after an empty refresh are labelled `candidate_from_archived_history`. Old quotes remain selectable without an arbitrary age threshold; `--stale-seconds` optionally flags age.

Each snapshot row retains market, line, labels, offer/book/outcome identities, explicit fighter association, cutoff policy/basis, original source date/start, observation timestamp, quote age, missingness/selection state, history state, raw response ID, current offer status/disabled, and `availability_at_cutoff: unknown`. Current flags are not historical availability evidence. Downstream joins must retain these keys and uncertainty; snapshots must not be labelled closing odds or pooled across books/markets without a separate policy.

Timestamp documentation and historical suspension transitions are unavailable. Observations extend beyond source startTime, so excluding in-play prices cannot be established from the field alone. Treat timestamp as a source-recorded observation instant, with ingestion time retained separately. Price movement spacing is irregular; no forward-filled transaction availability or regularly sampled time series is claimed. Full raw history permits later cutoff policies and analyses.

## Validation

The accompanying `fightodds-validation.json` records bounded live coverage. Offline fixtures preserve the actual 37-point DraftKings Jones series, original labels/lines and identities, and a small sample of prop histories. Tests exercise multiple pages, HTTP/GraphQL failures, request/page budgets, resume, refresh, duplicate book/offer identity, null/missing odds, cancellations, reschedules, same-time conflicts, Eastern DST, overseas date conversion, future-price exclusion, prop failures with usable moneylines, and isolated canonical joins. Existing ESPN tests also verify the matcher extraction preserves its consumers.

Sustained crawling reliability, global historical coverage, reliable event start times, and historical quote availability remain unverified. No full historical crawl or pipeline publication was performed.
