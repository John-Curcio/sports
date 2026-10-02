"""ESPN MMA core API collection with durable checkpoints and atomic publication.

Run ``python -m scrape.scrape_espn --help`` for bounded and resumable runs.
No browser or production database is opened at import time.
"""
import argparse
from concurrent.futures import CancelledError, FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import logging
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
import uuid

import pandas as pd
import requests

BASE = 'https://sports.core.api.espn.com/v2/sports/mma'
LOG = logging.getLogger(__name__)
BIO_COLUMNS = ['FighterID', 'Name', 'Birthdate', 'HT/WT', 'Height', 'Weight',
               'Reach', 'WT Class', 'Stance', 'Team', 'Birthplace', 'Country', 'Nickname']
MATCH_COLUMNS = ['FighterID', 'Date', 'Opponent', 'OpponentID', 'Event', 'Res.',
                 'Decision', 'Rnd', 'Time', 'CompetitionID', 'FinishDescription']
STAT_MAP = dict(TSL='totalStrikesLanded', TSA='totalStrikesAttempted',
                SSL='sigStrikesLanded', SSA='sigStrikesAttempted', KD='knockDowns',
                TDL='takedownsLanded', TDA='takedownsAttempted', TDS='takedownsSlams',
                AD='advances', ADHG='advanceToHalfGuard', ADTS='advanceToSide',
                ADTM='advanceToMount', ADTB='advanceToBack', RV='reversals', SM='submissions')
for prefix, position in [('SD', 'Distance'), ('SC', 'Clinch'), ('SG', 'Ground')]:
    for letter, target in [('H', 'Head'), ('B', 'Body'), ('L', 'Leg')]:
        for suffix, action in [('L', 'Landed'), ('A', 'Attempted')]:
            STAT_MAP[prefix + letter + suffix] = 'sig' + position + target + 'Strikes' + action
FORMATTED = ['SDBL/A', 'SDHL/A', 'SDLL/A', 'TSL-TSA', '%BODY', '%HEAD', '%LEG', 'TK ACC', 'SR']
EXTRA_STATS = ['posBreakdownDistance', 'posBreakdownClinch', 'posBreakdownGround', 'timeInControl']
STAT_COLUMNS = ['FighterID', 'Date', 'Opponent', 'OpponentID', 'Event', 'Res.', 'CompetitionID'] + list(STAT_MAP) + FORMATTED + EXTRA_STATS + ['StatsAvailable']
FAILURE_COLUMNS = ['fighter_url', 'FighterID', 'stage', 'resource', 'reason']


class ApiError(RuntimeError):
    pass


class EspnAccessDenied(RuntimeError):
    """Stop the entire crawl rather than recording a site-wide block per fighter."""
    pass


def normalize_url(url, **params):
    if not isinstance(url, str):
        raise ApiError('Expected an ESPN URL string')
    parts = urlsplit(url)
    if parts.hostname != 'sports.core.api.espn.com' or not (parts.path == '/v2/sports/mma' or parts.path.startswith('/v2/sports/mma/')):
        raise ApiError('Unexpected ESPN reference: ' + url)
    query = {'lang': 'en', 'region': 'us'}
    query.update(parse_qsl(parts.query))
    query.update({k: str(v) for k, v in params.items()})
    return urlunsplit(('https', parts.netloc, parts.path, urlencode(sorted(query.items())), ''))


def require(data, *keys):
    if not isinstance(data, dict) or any(k not in data for k in keys):
        raise ApiError('Invalid response; required fields: ' + ', '.join(keys))
    return data


def validate_response(data, keys, missing_stats=False):
    require(data, *keys)
    for key in ('name', 'displayName', 'date'):
        if key in keys and (not isinstance(data[key], str) or not data[key]):
            raise ApiError('Invalid ' + key)
    if 'id' in keys and not str(data['id']).isdigit():
        raise ApiError('Invalid numeric identifier')
    if 'eventLog' in keys:
        require(data['eventLog'], '$ref')
    if 'type' in keys:
        require(data['type'], 'completed', 'name')
        if not isinstance(data['type']['completed'], bool) or not isinstance(data['type']['name'], str):
            raise ApiError('Invalid completion flag')
        if data['type']['completed'] and 'CANCEL' not in data['type']['name'].upper():
            require(require(data, 'result')['result'], 'name')
    if 'competitors' in keys:
        if not isinstance(data['competitors'], list) or len(data['competitors']) != 2:
            raise ApiError('Expected two competitors')
        require(data['status'], '$ref')
        for competitor in data['competitors']:
            require(competitor, 'id', 'athlete')
            require(competitor['athlete'], '$ref')
    if missing_stats:
        stats_row(data, {})
    return data


class EspnApiClient:
    def __init__(self, checkpoint_path='.cache/espn/checkpoint.sqlite', session=None,
                 timeout=30, retries=3, cache_ttl=math.inf, refresh=False,
                 session_factory=None, max_requests_per_second=0):
        if max_requests_per_second < 0:
            raise ValueError('max_requests_per_second must be nonnegative')
        Path(checkpoint_path).parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = checkpoint_path
        self.store = sqlite3.connect(str(checkpoint_path), timeout=60)
        self.store.execute('PRAGMA journal_mode=WAL')
        self.store.executescript('''
            CREATE TABLE IF NOT EXISTS responses (url TEXT PRIMARY KEY, body TEXT, fetched REAL);
            CREATE TABLE IF NOT EXISTS fighters (key TEXT PRIMARY KEY, body TEXT, fetched REAL);
        ''')
        if refresh:
            self.store.executescript('DELETE FROM responses; DELETE FROM fighters;')
        self.session_factory = session_factory or requests.Session
        self.session = session or self.session_factory()
        self.timeout, self.retries, self.cache_ttl = timeout, retries, cache_ttl
        self.stop = threading.Event()
        self.response_locks = [threading.Lock() for _ in range(256)]
        self.cooldown = {'until': 0.0, 'lock': threading.Lock()}
        self.max_requests_per_second = max_requests_per_second
        self.pacing = {'next': 0.0, 'lock': threading.Lock()}

    def fork(self, session=None):
        """A worker owns its SQLite connection and HTTP session, sharing coordination."""
        worker = EspnApiClient(self.checkpoint_path, timeout=self.timeout, retries=self.retries,
                               cache_ttl=self.cache_ttl, session=session,
                               session_factory=self.session_factory,
                               max_requests_per_second=self.max_requests_per_second)
        worker.stop, worker.response_locks, worker.cooldown = self.stop, self.response_locks, self.cooldown
        worker.pacing = self.pacing
        return worker

    def close(self):
        self.store.close()
        self.session.close()

    def get(self, url, keys=(), missing_stats=False):
        url = normalize_url(url)
        # Bounded striped locks prevent duplicate simultaneous fetches of shared fights.
        with self.response_locks[hash(url) % len(self.response_locks)]:
            if self.stop.is_set():
                raise CancelledError('Collection interrupted')
            return self._get(url, keys, missing_stats)

    def _get(self, url, keys, missing_stats):
        cached = self.store.execute('SELECT body, fetched FROM responses WHERE url=?', (url,)).fetchone()
        if cached and time.time() - cached[1] < self.cache_ttl:
            data = json.loads(cached[0])
            if data is None and missing_stats:
                return None
            return validate_response(data, keys, missing_stats)
        for attempt in range(self.retries + 1):
            try:
                if self.stop.is_set():
                    raise CancelledError('Collection interrupted')
                with self.cooldown['lock']:
                    delay = max(0, self.cooldown['until'] - time.monotonic())
                if delay:
                    time.sleep(delay)
                if self.max_requests_per_second:
                    with self.pacing['lock']:
                        now = time.monotonic()
                        request_time = max(now, self.pacing['next'])
                        self.pacing['next'] = request_time + 1 / self.max_requests_per_second
                    delay = max(0, request_time - time.monotonic())
                    if delay:
                        time.sleep(delay)
                # A peer can receive a rate limit while this request waits for its slot.
                with self.cooldown['lock']:
                    delay = max(0, self.cooldown['until'] - time.monotonic())
                if delay:
                    time.sleep(delay)
                if self.stop.is_set():
                    raise CancelledError('Collection interrupted')
                response = self.session.get(url, timeout=self.timeout)
                content_type = getattr(response, 'headers', {}).get('Content-Type', '').lower()
                if response.status_code == 403 and 'text/html' in content_type:
                    LOG.error('ESPN_ACCESS_DENIED: HTML HTTP 403; stopping all workers')
                    self.stop.set()
                    raise EspnAccessDenied(f'ESPN returned HTML HTTP 403; crawl stopped: {url}')
                if response.status_code == 404 and missing_stats:
                    try:
                        body = response.json()
                        message = body.get('message', '') if isinstance(body, dict) else ''
                    except ValueError:
                        message = ''
                    if isinstance(message, str) and 'no stats found' in message.lower():
                        with self.store:
                            self.store.execute('INSERT OR REPLACE INTO responses VALUES (?,?,?)',
                                               (url, 'null', time.time()))
                        return None
                if response.status_code == 429:
                    try:
                        pause = float(response.headers.get('Retry-After', 2 ** attempt))
                    except (ValueError, AttributeError):
                        pause = 2 ** attempt
                    with self.cooldown['lock']:
                        self.cooldown['until'] = max(self.cooldown['until'], time.monotonic() + min(max(pause, 1), 60))
                    LOG.warning('HTTP 429; pausing all workers before further requests')
                if response.status_code == 429 or response.status_code >= 500:
                    raise requests.RequestException('Transient HTTP ' + str(response.status_code))
                if response.status_code != 200:
                    raise ApiError(f'HTTP {response.status_code}: {url}')
                data = validate_response(response.json(), keys, missing_stats)
                if 'code' in data and 'message' in data:
                    raise ApiError(f'API error: {data["message"]}')
                with self.store:
                    self.store.execute('INSERT OR REPLACE INTO responses VALUES (?,?,?)',
                                       (url, json.dumps(data), time.time()))
                return data
            except (requests.RequestException, ValueError) as exc:
                if attempt == self.retries:
                    raise ApiError(f'{url}: {exc}') from exc
                time.sleep(min(2 ** attempt, 8))

    def pages(self, url, nested=None, limit=100, max_pages=math.inf):
        page = 1
        while True:
            data = self.get(normalize_url(url, limit=limit, page=page), keys=(nested,) if nested else ('items',))
            block = require(data[nested] if nested else data, 'items', 'pageIndex', 'pageCount', 'count')
            if (not isinstance(block['items'], list) or block['pageIndex'] != page
                    or not isinstance(block['pageCount'], int) or block['pageCount'] < 0
                    or not isinstance(block['count'], int) or block['count'] < 0
                    or (block['count'] > 0 and block['pageCount'] < page)
                    or (not block['items'] and block['count'] > 0)):
                raise ApiError('Invalid pagination: ' + url)
            yield from block['items']
            if page >= block['pageCount'] or page >= max_pages:
                break
            page += 1


def profile_row(profile):
    require(profile, 'id', 'displayName')
    height, weight = profile.get('displayHeight'), profile.get('displayWeight')
    if not height and profile.get('height') is not None:
        height = f'{int(profile["height"] // 12)}\' {profile["height"] % 12:g}"'
    if not weight and profile.get('weight') is not None:
        weight = f'{profile["weight"]:g} lbs'
    reach = profile.get('displayReach')
    if not reach and profile.get('reach') is not None:
        reach = f'{profile["reach"]:g}"'
    return dict(zip(BIO_COLUMNS, [str(profile['id']), profile['displayName'],
        (profile.get('dateOfBirth') or '')[:10], f'{height},{weight}' if height and weight else None,
        height, weight, reach, (profile.get('weightClass') or {}).get('text'),
        (profile.get('stance') or {}).get('text'), (profile.get('association') or {}).get('name'),
        (profile.get('birthPlace') or {}).get('city'), profile.get('citizenship'),
        profile.get('nickname')]))


def stats_row(data, match):
    row = {**match, **{k: None for k in STAT_MAP}, **{k: None for k in FORMATTED},
           **{k: None for k in EXTRA_STATS},
           'StatsAvailable': data is not None}
    if data is not None:
        categories = require(require(data, 'splits')['splits'], 'categories')['categories']
        if not isinstance(categories, list) or not categories:
            raise ApiError('Invalid statistics categories')
        values = {}
        for category in categories:
            stats = require(category, 'stats')['stats']
            if not isinstance(stats, list):
                raise ApiError('Invalid statistics list')
            for stat in stats:
                require(stat, 'name', 'value')
                if not isinstance(stat['value'], (int, float)) or not math.isfinite(stat['value']):
                    raise ApiError('Invalid numeric statistic')
                values[stat['name']] = stat['value']
        if not any(name in values for name in STAT_MAP.values()):
            raise ApiError('No recognized detailed statistics')
        row.update({col: values.get(name) for col, name in STAT_MAP.items()})
        row.update({name: values.get(name) for name in EXTRA_STATS})
        for col, name in [('%HEAD', 'targetBreakdownHead'), ('%BODY', 'targetBreakdownBody'),
                          ('%LEG', 'targetBreakdownLeg'), ('TK ACC', 'takedownAccuracy'), ('SR', 'slamRate')]:
            row[col] = values.get(name)
    for target in 'BHL':
        row[f'SD{target}L/A'] = '/'.join('-' if row[f'SD{target}{s}'] is None else str(row[f'SD{target}{s}']) for s in 'LA')
    row['TSL-TSA'] = '-'.join('-' if row[k] is None else str(row[k]) for k in ['TSL', 'TSA'])
    return row


def decision_text(outcome):
    """Keep the legacy decision parser's submission-method convention."""
    name = outcome['name'].lower()
    text = outcome.get('displayName', name)
    detail = outcome.get('displayDescription') or outcome.get('description')
    if 'submission' in text.lower() and detail:
        return f'{text} ({detail})'
    if name in ('disqualification', 'dq'):
        return 'DQ'
    return text


class Fighter:
    def __init__(self, url, client=None):
        match = re.search(r'(?:_/id/|/athletes/)(\d+)', str(url))
        self.fighter_id = match.group(1) if match else str(url)
        if not self.fighter_id.isdigit():
            raise ValueError('Expected ESPN athlete ID or URL')
        self.base_url = f'https://www.espn.com/mma/fighter/_/id/{self.fighter_id}'
        self.client = client or EspnApiClient()

    def collect(self, bio=True, stats=True, matches=True):
        result = {'bio': [], 'stats': [], 'matches': [], 'failures': []}
        def failure(stage, resource, exc):
            result['failures'].append(dict(zip(FAILURE_COLUMNS,
                [self.base_url, self.fighter_id, stage, resource, str(exc)])))
            LOG.warning('%s %s: %s', self.fighter_id, stage, exc)
        profile_url = f'{BASE}/athletes/{self.fighter_id}'
        try:
            profile = self.client.get(profile_url, keys=('id', 'displayName', 'eventLog'))
            if bio:
                result['bio'].append(profile_row(profile))
        except (ApiError, KeyError, TypeError, ValueError, AttributeError) as exc:
            failure('bio', profile_url, exc)
            return result
        if not (stats or matches):
            return result
        history_url = profile['eventLog']['$ref']
        try:
            for entry in self.client.pages(history_url, nested='events'):
                resource = history_url
                try:
                    require(entry, 'event', 'competition')
                    resource = require(entry['competition'], '$ref')['$ref']
                    require(entry['event'], '$ref')
                    competition = self.client.get(resource, keys=('id', 'competitors', 'status', 'date'))
                    status = self.client.get(competition['status']['$ref'], keys=('type',))
                    status_type = require(status['type'], 'completed', 'name')
                    if not status_type['completed'] or 'CANCEL' in status_type['name'].upper():
                        continue
                    competitors = competition['competitors']
                    if len(competitors) != 2:
                        raise ApiError('Expected two competitors')
                    own = next(c for c in competitors if str(c['id']) == self.fighter_id)
                    opponent = next(c for c in competitors if str(c['id']) != self.fighter_id)
                    other = self.client.get(opponent['athlete']['$ref'], keys=('id', 'displayName'))
                    event = self.client.get(entry['event']['$ref'], keys=('name',))
                    outcome = require(status, 'result')['result']
                    outcome_name = require(outcome, 'name')['name'].lower()
                    if outcome_name == 'no-contest':
                        res = 'NC'
                    elif 'draw' in outcome_name:
                        res = 'D'
                    else:
                        winners = [c for c in competitors if c.get('winner') is True]
                        if len(winners) != 1:
                            raise ApiError('Ambiguous completed fight result')
                        res = 'W' if own.get('winner') is True else 'L'
                    # ESPN HTML dates are US event dates; UTC midnight often falls on the following day.
                    date = pd.Timestamp(competition['date']).tz_convert('America/New_York').date().isoformat()
                    row = dict(zip(MATCH_COLUMNS, [self.fighter_id, date, other['displayName'],
                        f'https://www.espn.com/mma/fighter/_/id/{other["id"]}', event['name'], res,
                        decision_text(outcome), status.get('period'),
                        status.get('displayClock'), str(competition['id']),
                        outcome.get('displayDescription') or outcome.get('description')]))
                    if matches:
                        result['matches'].append(row)
                    if stats:
                        stats_url = None
                        try:
                            if own.get('statistics') is not None:
                                stats_url = require(own['statistics'], '$ref')['$ref']
                            data = self.client.get(stats_url, keys=('splits',), missing_stats=True) if stats_url else None
                            result['stats'].append(stats_row(data, row))
                        except (ApiError, KeyError, TypeError) as exc:
                            failure('stats', stats_url or resource, exc)
                except (ApiError, KeyError, TypeError, ValueError, AttributeError, StopIteration) as exc:
                    failure('history', resource, exc)
        except (ApiError, KeyError, TypeError) as exc:
            failure('history-pagination', history_url, exc)
        return result

    def scrape_bio(self):
        return pd.DataFrame(self.collect(stats=False, matches=False)['bio'], columns=BIO_COLUMNS)

    def scrape_stats(self):
        return pd.DataFrame(self.collect(bio=False, matches=False)['stats'], columns=STAT_COLUMNS)

    def scrape_matches(self):
        return pd.DataFrame(self.collect(bio=False, stats=False)['matches'], columns=MATCH_COLUMNS)


class FighterSearchScraper:
    def __init__(self, n_fighters=math.inf, n_pages=math.inf, start_letter='a', end_letter='z',
                 max_missed_fighter_retries=3, client=None, athlete_ids=None,
                 checkpoint_path='.cache/espn/checkpoint.sqlite', refresh=False, workers=1,
                 max_requests_per_second=0):
        self.n_fighters, self.n_pages = n_fighters, n_pages
        if n_fighters <= 0 or n_pages <= 0:
            raise ValueError('n_fighters and n_pages must be positive')
        if not (len(start_letter) == len(end_letter) == 1 and 'a' <= start_letter.lower() <= end_letter.lower() <= 'z'):
            raise ValueError('Expected an ordered letter range from a to z')
        self.start_letter, self.end_letter = start_letter.lower(), end_letter.lower()
        self.client = client or EspnApiClient(checkpoint_path, retries=max_missed_fighter_retries,
                                             refresh=refresh, max_requests_per_second=max_requests_per_second)
        self.athlete_ids = athlete_ids
        if not isinstance(workers, int) or not 1 <= workers <= 16:
            raise ValueError('workers must be an integer from 1 to 16')
        self.workers = workers
        self.failures = []
        self.completed_run = False
        self.bio_df = self.stats_df = self.matches_df = self.missed_fighters_df = None

    def scrape_fighters(self):
        fighters = []
        if self.athlete_ids is not None:
            refs = [f'{BASE}/athletes/{ident}' for ident in self.athlete_ids]
        else:
            def discover():
                # n_pages now limits numeric API index pages, rather than alphabet searches.
                for item in self.client.pages(BASE + '/athletes', limit=1000, max_pages=self.n_pages):
                    yield require(item, '$ref')['$ref']
            refs = discover()
        seen = set()
        for ref in refs:
            if len(fighters) >= self.n_fighters:
                break
            fighter = Fighter(normalize_url(ref), self.client)
            if fighter.fighter_id in seen:
                continue
            seen.add(fighter.fighter_id)
            if self.athlete_ids is None and (self.start_letter != 'a' or self.end_letter != 'z'):
                profile = self.client.get(f'{BASE}/athletes/{fighter.fighter_id}', keys=('displayName',))
                if not self.start_letter <= profile['displayName'][0].lower() <= self.end_letter:
                    continue
            fighters.append(fighter)
        return fighters

    def run_scraper(self, verbose=True, bio=True, stats=True, matches=True):
        self.completed_run = False
        self.failures = []
        rows = {'bio': [], 'stats': [], 'matches': []}
        try:
            self.fighters = self.scrape_fighters()
        except (ApiError, KeyError, TypeError, ValueError) as exc:
            self.fighters = []
            self.failures.append(dict(zip(FAILURE_COLUMNS, ['', '', 'discovery', BASE + '/athletes', str(exc)])))
        def collect(fighter, api):
            key = f'v3:{fighter.fighter_id}:{bio}:{stats}:{matches}'
            checkpoint = api.store.execute('SELECT body,fetched FROM fighters WHERE key=?', (key,)).fetchone()
            if (checkpoint and time.time() - checkpoint[1] < api.cache_ttl
                    and not json.loads(checkpoint[0])['failures']):
                result = json.loads(checkpoint[0])
            else:
                result = Fighter(fighter.fighter_id, api).collect(bio, stats, matches)
                # Failed fighters are retried on resume; successful resource responses remain cached.
                with api.store:
                    api.store.execute('INSERT OR REPLACE INTO fighters VALUES (?,?,?)',
                                              (key, json.dumps(result), time.time()))
            return fighter, result

        def results():
            if self.workers == 1:
                for fighter in self.fighters:
                    yield collect(fighter, self.client)
                return
            local = threading.local()
            sessions = []
            session_lock = threading.Lock()
            def worker(fighter):
                # Each task closes its connection on its owning thread. HTTP sessions are
                # reused by that thread across fighters, including connection pooling.
                if not hasattr(local, 'session'):
                    local.session = self.client.session_factory()
                    with session_lock:
                        sessions.append(local.session)
                api = self.client.fork(session=local.session)
                try:
                    return collect(fighter, api)
                finally:
                    api.store.close()
            executor = ThreadPoolExecutor(max_workers=self.workers)
            pending = set()
            fighters = iter(self.fighters)
            try:
                # Keep only two tasks per worker in flight, avoiding an unbounded future queue.
                for _ in range(self.workers * 2):
                    fighter = next(fighters, None)
                    if fighter is not None:
                        pending.add(executor.submit(worker, fighter))
                while pending:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        yield future.result()
                        fighter = next(fighters, None)
                        if fighter is not None:
                            pending.add(executor.submit(worker, fighter))
            finally:
                self.client.stop.set()
                for future in pending:
                    future.cancel()
                executor.shutdown(wait=True)
                for session in sessions:
                    session.close()
                self.client.stop.clear()

        for index, (fighter, result) in enumerate(results()):
            for kind in rows:
                rows[kind].extend(result[kind])
            self.failures.extend(result['failures'])
            if verbose:
                LOG.info('Collected %s (%s/%s)', fighter.fighter_id, index + 1, len(self.fighters))
        for kind, columns, attr, enabled in [('bio', BIO_COLUMNS, 'bio_df', bio),
                ('stats', STAT_COLUMNS, 'stats_df', stats), ('matches', MATCH_COLUMNS, 'matches_df', matches)]:
            df = pd.DataFrame(rows[kind], columns=columns)
            if not df.empty:
                df = df.drop_duplicates(['FighterID'] if kind == 'bio' else ['FighterID', 'CompetitionID'])
            setattr(self, attr, df if enabled else None)
        self.missed_fighters_df = pd.DataFrame(self.failures, columns=FAILURE_COLUMNS)
        self.completed_run = True
        return self

    def write_all_to_tables(self, db=None, allow_partial=False):
        if not self.completed_run:
            raise ApiError('Collection has not finished; resume before publishing')
        if self.failures and not allow_partial:
            raise ApiError(f'{len(self.failures)} collection failures; existing tables preserved. '
                           'Inspect missed_fighters_df; allow_partial=True explicitly saves partial results.')
        if db is None:
            from db import base_db_interface
            db = base_db_interface
        con = db if isinstance(db, sqlite3.Connection) else db._con
        tables = [('espn_bio', self.bio_df), ('espn_stats', self.stats_df),
                  ('espn_matches', self.matches_df), ('espn_missed_fighters', self.missed_fighters_df)]
        staged = []
        try:
            for name, df in tables:
                if df is not None:
                    stage = '_espn_stage_' + uuid.uuid4().hex
                    staged.append((name, stage))
                    df.to_sql(stage, con, index=False)
            # pandas commits to_sql separately; only staged tables are affected until this transaction.
            with con:
                con.execute('BEGIN')
                for name, stage in staged:
                    con.execute(f'DROP TABLE IF EXISTS "{name}"')
                    con.execute(f'ALTER TABLE "{stage}" RENAME TO "{name}"')
        finally:
            with con:
                for _, stage in staged:
                    con.execute(f'DROP TABLE IF EXISTS "{stage}"')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--athlete-id', action='append', help='Explicit athlete ID; repeat for multiple fighters')
    parser.add_argument('--n-fighters', type=int, default=math.inf)
    parser.add_argument('--n-pages', type=int, default=math.inf, help='Maximum athlete index pages')
    parser.add_argument('--start-letter', default='a')
    parser.add_argument('--end-letter', default='z')
    parser.add_argument('--checkpoint', default='.cache/espn/checkpoint.sqlite')
    parser.add_argument('--workers', type=int, default=1, help='Concurrent fighter workers (1–16)')
    parser.add_argument('--max-requests-per-second', type=float, default=0,
                        help='Global request cap across all workers (0 means uncapped)')
    parser.add_argument('--refresh', action='store_true', help='Discard checkpoints and cached responses')
    parser.add_argument('--allow-partial', action='store_true')
    parser.add_argument('--database', help='Output SQLite path; default is repository mma.db')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    scraper = FighterSearchScraper(n_fighters=args.n_fighters, n_pages=args.n_pages,
        start_letter=args.start_letter, end_letter=args.end_letter, athlete_ids=args.athlete_id,
        checkpoint_path=args.checkpoint, refresh=args.refresh, workers=args.workers,
        max_requests_per_second=args.max_requests_per_second)
    con = None
    try:
        scraper.run_scraper()
        if args.database:
            con = sqlite3.connect(args.database)
        scraper.write_all_to_tables(db=con, allow_partial=args.allow_partial)
    finally:
        if con is not None:
            con.close()
        scraper.client.close()


if __name__ == '__main__':
    main()
