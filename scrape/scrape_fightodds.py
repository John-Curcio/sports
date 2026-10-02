"""Bounded FightOdds collection. No imports or writes through the legacy db singleton."""
import argparse
from datetime import date, datetime, time as day_time, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from zoneinfo import ZoneInfo

import requests

ENDPOINT = 'https://api.fightodds.io/gql'
EASTERN = ZoneInfo('America/New_York')
PAGE = 'pageInfo { hasNextPage endCursor } edges { cursor node { %s } }'
FIGHTER = 'id slug firstName lastName fightmetricUrl'
EVENT = 'id pk name slug date startTime isCancelled temp promotion { id shortName slug }'
FIGHT = ('id pk slug isCancelled fightType fighter1 { ' + FIGHTER +
         ' } fighter2 { ' + FIGHTER + ' }')
OUTCOME = 'id name fighter { id slug } isNot odds oddsOpen oddsBest oddsWorst'
TYPE = 'id offerTypeId category subCategory description notDescription value'
OFFER = ('id value timestamp createdAt disabled status event { id } fight { id slug } '
         'sportsbook { id slug shortName } offerType { ' + TYPE +
         ' } outcomes { ' + PAGE % OUTCOME + ' }')


class ApiError(RuntimeError):
    pass


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def instant(value):
    """Reject invented local offsets; normalize instants for ordering/storage."""
    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError('Timestamp must include a timezone: ' + value)
    return dt.astimezone(timezone.utc)


def utc(value):
    return instant(value).isoformat(timespec='microseconds')


def event_cutoff(event, hour=0, trust_start_time=False):
    """Source date is an explicit Eastern-date fallback unless start is verified."""
    if not 0 <= hour <= 23:
        raise ValueError('Cutoff hour must be in 0..23')
    start = event.get('startTime')
    verified = False
    if trust_start_time and start:
        eastern_day = instant(start).astimezone(EASTERN).date()
        basis, verified = 'verified_start_time_eastern_day', True
    elif event.get('date'):
        eastern_day = date.fromisoformat(event['date'])
        basis = 'source_date_assumed_eastern_day'
    else:
        return {'cutoff': None, 'basis': 'missing_event_date', 'timing_uncertain': True}
    cutoff = datetime.combine(eastern_day, day_time(hour), tzinfo=EASTERN)
    if cutoff.astimezone(timezone.utc).astimezone(EASTERN) != cutoff:
        raise ValueError('Cutoff falls in a nonexistent Eastern hour; use an explicit instant')
    # Even unverified timestamps are a reason to flag a possible after-start cutoff.
    conflict = bool(start and instant(start) <= cutoff.astimezone(timezone.utc))
    return {'cutoff': cutoff.isoformat(), 'basis': basis,
            'timing_uncertain': not verified or conflict, 'possible_after_start': conflict}


SCHEMA = '''
CREATE TABLE IF NOT EXISTS fightodds_responses (
 response_id INTEGER PRIMARY KEY, request_key TEXT, query TEXT, variables TEXT,
 fetched_at TEXT, response TEXT);
CREATE INDEX IF NOT EXISTS fightodds_cache ON fightodds_responses(request_key, response_id);
CREATE TABLE IF NOT EXISTS fightodds_records (
 kind TEXT, id TEXT, data TEXT, response_id INTEGER, PRIMARY KEY(kind,id));
CREATE TABLE IF NOT EXISTS fightodds_record_versions (
 kind TEXT, id TEXT, digest TEXT, data TEXT, response_id INTEGER,
 PRIMARY KEY(kind,id,digest));
CREATE TABLE IF NOT EXISTS fightodds_prices (
 id TEXT PRIMARY KEY, outcome_id TEXT, timestamp TEXT, odds INTEGER, response_id INTEGER);
CREATE INDEX IF NOT EXISTS fightodds_price_lookup ON fightodds_prices(outcome_id,timestamp);
CREATE TABLE IF NOT EXISTS fightodds_price_versions (
 id TEXT, digest TEXT, data TEXT, response_id INTEGER, PRIMARY KEY(id,digest));
CREATE TABLE IF NOT EXISTS fightodds_history_status (
 outcome_id TEXT PRIMARY KEY, state TEXT, observations INTEGER, error TEXT, checked_at TEXT);
CREATE TABLE IF NOT EXISTS fightodds_runs (
 id INTEGER PRIMARY KEY, started_at TEXT, finished_at TEXT, report TEXT);
CREATE TABLE IF NOT EXISTS fightodds_identity_maps (
 source_kind TEXT, source_id TEXT, target_source TEXT, target_id TEXT, evidence TEXT,
 PRIMARY KEY(source_kind,source_id,target_source));
'''


class Store:
    def __init__(self, path):
        if Path(path).name == 'mma.db':
            raise ValueError('Use an isolated FightOdds database, not mma.db')
        self.con = sqlite3.connect(str(path))
        self.con.row_factory = sqlite3.Row
        self.con.executescript(SCHEMA)

    def close(self):
        self.con.close()

    def record(self, kind, node, response_id=None):
        data = encoded(node)
        digest = hashlib.sha256(data.encode()).hexdigest()
        with self.con:
            self.con.execute('INSERT OR IGNORE INTO fightodds_record_versions VALUES (?,?,?,?,?)',
                             (kind, node['id'], digest, data, response_id))
            self.con.execute('INSERT OR REPLACE INTO fightodds_records VALUES (?,?,?,?)',
                             (kind, node['id'], data, response_id))

    def records(self, kind):
        return [json.loads(r[0]) for r in self.con.execute(
            'SELECT data FROM fightodds_records WHERE kind=? ORDER BY id', (kind,))]

    def get(self, kind, identifier):
        row = self.con.execute('SELECT data FROM fightodds_records WHERE kind=? AND id=?',
                               (kind, identifier)).fetchone()
        return json.loads(row[0]) if row else None

    def price(self, outcome_id, node, response_id):
        timestamp = utc(node['timestamp'])
        if node['odds'] is not None and (isinstance(node['odds'], bool) or not isinstance(node['odds'], int)):
            raise ApiError('Non-integer American odds')
        data = encoded(node)
        with self.con:
            self.con.execute('INSERT OR IGNORE INTO fightodds_price_versions VALUES (?,?,?,?)',
                (node['id'], hashlib.sha256(data.encode()).hexdigest(), data, response_id))
            self.con.execute('INSERT OR REPLACE INTO fightodds_prices VALUES (?,?,?,?,?)',
                (node['id'], outcome_id, timestamp, node['odds'], response_id))

    def history_status(self, identifier, state, count, error=None):
        with self.con:
            self.con.execute('INSERT OR REPLACE INTO fightodds_history_status VALUES (?,?,?,?,?)',
                (identifier, state, count, error, datetime.now(timezone.utc).isoformat()))

    def snapshot(self, event_id, cutoff=None, hour=0, trust_start_time=False, stale_seconds=None):
        event = self.get('event', event_id)
        if not event:
            raise ValueError('Unknown event: ' + event_id)
        policy = event_cutoff(event, hour, trust_start_time)
        if cutoff is not None:
            policy = {'cutoff': utc(cutoff), 'basis': 'explicit_instant',
                      'timing_uncertain': False, 'possible_after_start': bool(
                          event.get('startTime') and instant(cutoff) >= instant(event['startTime']))}
        selected = utc(policy['cutoff']) if policy['cutoff'] else None
        rows = []
        prop_labels = {}
        for group in self.records('prop_group'):
            key = (group['fight_id'], (group.get('offerType') or {}).get('id'))
            labels = {'propName1':group.get('propName1'), 'propName2':group.get('propName2')}
            if labels not in prop_labels.setdefault(key, []):
                prop_labels[key].append(labels)
        for outcome in self.records('outcome'):
            offer = self.get('offer', outcome['offer_id'])
            if offer['event_id'] != event_id:
                continue
            fight = self.get('fight', offer['fight']['id']) if offer.get('fight') else None
            row = dict(policy, event_id=event_id, source_date=event.get('date'),
                source_start_time=event.get('startTime'), fight_id=fight['id'] if fight else None,
                offer_id=offer['id'], market_id=offer['market_id'], market_kind=offer['market_kind'],
                market=self.get('market', offer['market_id']), sportsbook=offer['sportsbook'],
                prop_labels=prop_labels.get(((offer.get('fight') or {}).get('id'),
                    (offer.get('offerType') or {}).get('id')), []),
                outcome_id=outcome['id'], outcome_name=outcome['name'],
                current_offer_status=offer.get('status'), current_offer_disabled=offer.get('disabled'),
                fighter_id=(outcome.get('fighter') or {}).get('id'), is_not=outcome.get('isNot'),
                odds=None, observation_timestamp=None, quote_age_seconds=None, stale=None,
                availability_at_cutoff='unknown', response_id=None)
            status = self.con.execute('SELECT * FROM fightodds_history_status WHERE outcome_id=?',
                                      (outcome['id'],)).fetchone()
            row['history_state'] = status['state'] if status else 'not_collected'
            if event.get('isCancelled') or event.get('temp') or (fight and fight.get('isCancelled')):
                row['selection_state'] = 'cancelled_or_hypothetical'
            elif not selected:
                row['selection_state'] = 'missing_cutoff'
            else:
                points = self.con.execute('SELECT * FROM fightodds_prices WHERE outcome_id=? '
                    'AND timestamp<=? ORDER BY timestamp DESC,id', (outcome['id'], selected)).fetchall()
                if not points:
                    row['selection_state'] = 'no_earlier_observation'
                else:
                    newest = [p for p in points if p['timestamp'] == points[0]['timestamp']]
                    row['observation_timestamp'] = newest[0]['timestamp']
                    row['quote_age_seconds'] = (instant(selected) - instant(newest[0]['timestamp'])).total_seconds()
                    row['stale'] = (row['quote_age_seconds'] > stale_seconds) if stale_seconds is not None else None
                    # Equal timestamps with conflicting prices have no defensible ordering.
                    if len({p['odds'] for p in newest}) > 1:
                        row['selection_state'] = 'conflicting_same_timestamp'
                    else:
                        row.update(odds=newest[0]['odds'], response_id=newest[0]['response_id'],
                                   selection_state='selected' if newest[0]['odds'] is not None else 'null_observation')
            if row['selection_state'] == 'selected' and row['history_state'] == 'unavailable':
                row['selection_state'] = 'candidate_from_archived_history'
            if row['selection_state'] == 'selected' and row['history_state'] != 'complete':
                row['selection_state'] = 'candidate_from_incomplete_history'
            rows.append(row)
        return rows


class Client:
    def __init__(self, store, session=None, timeout=30, retries=2, pace=.5,
                 refresh=False, max_requests=1000):
        if timeout <= 0 or retries < 0 or pace < 0 or max_requests < 1:
            raise ValueError('Invalid client limits')
        self.store, self.session = store, session or requests.Session()
        self.timeout, self.retries, self.pace = timeout, retries, pace
        self.refresh, self.max_requests = refresh, max_requests
        self.requests = 0
        self.last_response_id = None
        self.last_call = 0.
        self.seen = {}

    def query(self, query, variables):
        key = hashlib.sha256(encoded([ENDPOINT, query, variables]).encode()).hexdigest()
        cached = self.store.con.execute('SELECT * FROM fightodds_responses WHERE request_key=? '
            'ORDER BY response_id DESC LIMIT 1', (key,)).fetchone()
        if key in self.seen:
            self.last_response_id, data = self.seen[key]
            return data
        if cached and not self.refresh:
            self.last_response_id = cached['response_id']
            return json.loads(cached['response'])['data']
        for attempt in range(self.retries + 1):
            if self.requests >= self.max_requests:
                raise ApiError('Request budget exhausted; resume from cached pages')
            time.sleep(max(0, self.pace - (time.monotonic() - self.last_call)))
            self.requests += 1
            self.last_call = time.monotonic()
            try:
                response = self.session.post(ENDPOINT, json={'query': query, 'variables': variables},
                                             timeout=self.timeout)
                if response.status_code == 429 or response.status_code >= 500:
                    error = requests.RequestException('HTTP ' + str(response.status_code))
                    error.response = response
                    raise error
                if response.status_code != 200:
                    raise ApiError('HTTP ' + str(response.status_code))
                body = response.json()
                if not isinstance(body, dict) or body.get('errors') or not isinstance(body.get('data'), dict):
                    raise ApiError('Invalid GraphQL response: ' + encoded(body)[:1500])
                with self.store.con:
                    cur = self.store.con.execute('INSERT INTO fightodds_responses '
                        '(request_key,query,variables,fetched_at,response) VALUES (?,?,?,?,?)',
                        (key, query, encoded(variables), datetime.now(timezone.utc).isoformat(), encoded(body)))
                self.last_response_id = cur.lastrowid
                self.seen[key] = (cur.lastrowid, body['data'])
                return body['data']
            except (requests.RequestException, ValueError) as exc:
                if attempt == self.retries:
                    raise ApiError('Request failed after bounded retries: ' + str(exc)) from exc
                retry_response = getattr(exc, 'response', None)
                headers = getattr(retry_response, 'headers', {}) or {}
                try:
                    retry_after = float(headers.get('Retry-After', 0))
                except (ValueError, TypeError):
                    retry_after = 0
                delay = max(retry_after, 5 * (attempt+1)) if getattr(retry_response, 'status_code', None)==429 else 2 ** attempt
                time.sleep(min(max(delay, 0), 30))
        raise AssertionError('Unreachable')

    def close(self):
        self.session.close()


def connection(value):
    if not isinstance(value, dict) or not isinstance(value.get('edges'), list):
        raise ApiError('Missing connection/edges')
    info = value.get('pageInfo')
    if not isinstance(info, dict) or not isinstance(info.get('hasNextPage'), bool):
        raise ApiError('Missing pagination metadata')
    if any(not isinstance(e, dict) or not isinstance(e.get('node'), dict) for e in value['edges']):
        raise ApiError('Invalid connection edge')
    if info['hasNextPage'] and (not info.get('endCursor') or not value['edges']):
        raise ApiError('Non-progressing pagination')
    return value['edges'], info


class Collector:
    def __init__(self, client, page_size=100, max_pages=100):
        if not 1 <= page_size <= 100 or max_pages < 1:
            raise ValueError('Page size must be 1..100; max_pages must be positive')
        self.client, self.store = client, client.store
        self.page_size, self.max_pages = page_size, max_pages
        self.failures = []
        self.coverage = []

    def pages(self, query, variables, path, limit=None):
        cursor, seen, count = None, set(), 0
        for _ in range(self.max_pages):
            args = dict(variables, first=min(self.page_size, limit-count) if limit else self.page_size,
                        after=cursor)
            data = self.client.query(query, args)
            for key in path:
                if not isinstance(data, dict) or data.get(key) is None:
                    raise ApiError('Missing query path: ' + '.'.join(path))
                data = data[key]
            edges, info = connection(data)
            response_id = self.client.last_response_id
            for edge in edges:
                yield edge, response_id
                count += 1
                if limit and count >= limit:
                    return
            if not info['hasNextPage']:
                return
            cursor = info['endCursor']
            if cursor in seen:
                raise ApiError('Repeated pagination cursor')
            seen.add(cursor)
        raise ApiError('Page limit exceeded; connection incomplete')

    def discover(self, promotion='UFC', start=None, end=None, name=None, max_events=1):
        if max_events < 1:
            raise ValueError('max_events must be positive')
        query = ('query Events($first:Int!,$after:String,$promotion:String,$start:Date,$end:Date,$name:String) '
            '{ allEvents(first:$first,after:$after,promotion_ShortName:$promotion,date_Gte:$start,'
            'date_Lte:$end,name_Icontains:$name,orderBy:"date") { ' + PAGE % EVENT + ' } }')
        result = []
        for edge, rid in self.pages(query, dict(promotion=promotion,start=start,end=end,name=name),
                                    ['allEvents'], limit=max_events):
            self.store.record('event', edge['node'], rid)
            result.append(edge['node'])
        return result

    def fights(self, event):
        self.store.record('event', event)
        query = ('query Fights($event:ID!,$first:Int!,$after:String) '
                 '{ allFights(event:$event,first:$first,after:$after) { ' + PAGE % FIGHT + ' } }')
        result = []
        for edge, rid in self.pages(query, {'event':event['id']}, ['allFights']):
            fight = dict(edge['node'], event_id=event['id'])
            self.store.record('fight', fight, rid)
            for side in ['fighter1','fighter2']:
                if fight.get(side):
                    self.store.record('fighter', fight[side], rid)
            result.append(fight)
        return result

    def offers(self, event, fight=None):
        query = ('query Offers($event:ID,$fight:ID,$first:Int!,$after:String) '
            '{ allOffers(event:$event,fight:$fight,first:$first,after:$after) { ' + PAGE % OFFER + ' } }')
        identifiers = []
        self.offers_complete = True
        scope = fight['slug'] if fight else 'event:'+event['slug']
        try:
            for edge, rid in self.pages(query, {'event':None if fight else event['id'],
                                              'fight':fight['id'] if fight else None}, ['allOffers']):
                try:
                    identifiers.extend(self.store_offer(edge['node'], event, fight, rid))
                except (ApiError, KeyError, ValueError) as exc:
                    self.offers_complete = False
                    self.failures.append({'stage':'offer','scope':scope,
                                          'offer_id':edge['node'].get('id'),'error':str(exc)})
        except ApiError as exc:
            self.offers_complete = False
            self.failures.append({'stage':'offer_pages','scope':scope,'error':str(exc)})
        return list(dict.fromkeys(identifiers))

    def store_offer(self, offer, event, fight, rid):
        result = []
        if fight and (offer.get('fight') or {}).get('id') != fight['id']:
            raise ApiError('Offer fight filter mismatch')
        if not fight and offer.get('fight'):
            return []
        typ = offer.get('offerType') or {'id':'unknown:'+offer['id'], 'offerTypeId':None,
            'description':None, 'value':None}
        if not offer.get('sportsbook'):
            raise ApiError('Offer lacks sportsbook identity')
        context = fight['id'] if fight else event['id']
        market_id = hashlib.sha256(encoded([context,typ['id'],offer.get('value')]).encode()).hexdigest()
        kind = 'moneyline' if typ['offerTypeId'] == 'STRAIGHT' and fight else 'prop'
        self.store.record('market', dict(typ, id=market_id, offer_type_id=typ['id'],
            event_id=event['id'], fight_id=fight['id'] if fight else None,
            line=offer.get('value') if offer.get('value') is not None else typ.get('value'),
            line_basis='offer_value' if offer.get('value') is not None else 'offer_type_value', market_kind=kind), rid)
        outcomes, info = connection(offer['outcomes'])
        # Live resolver rejects first/after despite advertising them in the schema.
        # Never call a truncated nested list complete.
        if info['hasNextPage']:
            raise ApiError('Offer outcomes truncated; API resolver does not support pagination arguments')
        self.store.record('sportsbook', offer['sportsbook'], rid)
        offer = dict(offer, offerType=typ, event_id=event['id'], market_id=market_id, market_kind=kind)
        self.store.record('offer', offer, rid)
        for out in outcomes:
            self.store.record('outcome', dict(out['node'],offer_id=offer['id']), rid)
            result.append((out['node']['id'], kind))
        return result

    def prop_groups(self, fight):
        query = ('query Props($slug:String!,$first:Int!,$after:String) '
            '{ fightPropOfferTable(slug:$slug) { propOffers(first:$first,after:$after) { '+
            PAGE % ('propName1 propName2 offerType { '+TYPE+' }') + ' } } }')
        count = 0
        self.last_prop_groups = []
        for edge, rid in self.pages(query, {'slug':fight['slug']}, ['fightPropOfferTable','propOffers']):
            node = edge['node']
            identifier = hashlib.sha256(encoded([fight['id'],(node.get('offerType') or {}).get('id'),
                node.get('propName1'),node.get('propName2')]).encode()).hexdigest()
            self.last_prop_groups.append(dict(node,id=identifier))
            self.store.record('prop_group', dict(node,id=identifier,fight_id=fight['id']), rid)
            count += 1
        return count

    def histories(self, identifiers, batch_size=20):
        """Batch first pages, independently advance every connection, cache every page."""
        for offset in range(0, len(identifiers), batch_size):
            batch = identifiers[offset:offset+batch_size]
            pending = {oid:None for oid, _ in batch}
            seen = {oid:set() for oid in pending}
            counts = {oid:0 for oid in pending}
            price_ids = {oid:set() for oid in pending}
            for _ in range(self.max_pages):
                if not pending:
                    break
                variables, decl, fields, aliases = {'first':self.page_size}, ['$first:Int!'], [], {}
                for i,(oid,cursor) in enumerate(pending.items()):
                    alias = 'h'+str(i)
                    aliases[alias] = oid
                    variables['o'+str(i)], variables['a'+str(i)] = oid,cursor
                    decl.extend(['$o'+str(i)+':ID!','$a'+str(i)+':String'])
                    fields.append(alias+':allOdds(outcome:$o'+str(i)+',after:$a'+str(i)+
                        ',first:$first,orderBy:"timestamp") { '+PAGE % 'id odds timestamp'+' }')
                query = 'query Histories('+','.join(decl)+') { '+' '.join(fields)+' }'
                try:
                    data = self.client.query(query,variables)
                    rid = self.client.last_response_id
                    for alias, oid in aliases.items():
                        edges, info = connection(data.get(alias))
                        for edge in edges:
                            self.store.price(oid, edge['node'], rid)
                            price_ids[oid].add(edge['node']['id'])
                            counts[oid] = len(price_ids[oid])
                        if info['hasNextPage']:
                            cursor = info['endCursor']
                            if cursor in seen[oid]:
                                raise ApiError('Repeated history cursor: '+oid)
                            seen[oid].add(cursor)
                            pending[oid] = cursor
                        else:
                            del pending[oid]
                            self.store.history_status(oid,'complete' if counts[oid] else 'unavailable',counts[oid])
                except (ApiError, KeyError, ValueError) as exc:
                    for oid in pending:
                        self.store.history_status(oid,'failed',counts[oid],str(exc))
                    self.failures.append({'stage':'histories','outcomes':list(pending),'error':str(exc)})
                    pending = {}
                    break
            if pending:
                for oid in pending:
                    self.store.history_status(oid,'failed',counts[oid],'History page limit exceeded')
                self.failures.append({'stage':'histories','outcomes':list(pending),'error':'History page limit exceeded'})

    def collect(self, events, fight_slugs=None):
        self.failures, self.coverage = [], []
        with self.store.con:
            run = self.store.con.execute('INSERT INTO fightodds_runs(started_at) VALUES (?)',
                                         (datetime.now(timezone.utc).isoformat(),)).lastrowid
        for event in events:
            self.store.record('event',event)
            try:
                fights = self.fights(event)
                if fight_slugs:
                    fights = [f for f in fights if f['slug'] in fight_slugs]
                    missing = set(fight_slugs) - {f['slug'] for f in fights}
                    if missing:
                        raise ApiError('Selected fight slugs absent from event: '+str(sorted(missing)))
            except ApiError as exc:
                self.failures.append({'stage':'fights','event':event['id'],'error':str(exc)})
                continue
            for fight in [None] + fights:
                scope = fight['slug'] if fight else 'event:'+event['slug']
                entry = {'scope':scope,'event_id':event['id'],'offers_complete':False,
                         'prop_groups_complete':None if not fight else False}
                self.coverage.append(entry)
                try:
                    ids = self.offers(event,fight)
                    entry['offers_complete'] = self.offers_complete
                    for kind in ['moneyline','prop']:
                        selected = [(oid,k) for oid,k in ids if k==kind]
                        self.histories(selected)
                        states = {'complete':0,'unavailable':0,'failed':0}
                        observations = 0
                        for oid,_ in selected:
                            row = self.store.con.execute('SELECT * FROM fightodds_history_status WHERE outcome_id=?',(oid,)).fetchone()
                            states[row['state']] += 1
                            observations += row['observations']
                        entry[kind] = dict(states,outcomes=len(selected),observations=observations)
                    if fight:
                        entry['prop_groups'] = self.prop_groups(fight)
                        entry['prop_groups_complete'] = True
                        offer_type_ids = {self.get_offer_type(oid) for oid,_ in ids}
                        groups = self.last_prop_groups
                        unmatched = [g['id'] for g in groups if (g.get('offerType') or {}).get('id') not in offer_type_ids]
                        entry['groups_without_offers'] = unmatched
                except (ApiError, KeyError, ValueError) as exc:
                    self.failures.append({'stage':'offers_or_props','scope':scope,'error':str(exc)})
        report = {'run_id':run,'endpoint':ENDPOINT,'requests':self.client.requests,
                  'coverage':self.coverage,'failures':self.failures}
        with self.store.con:
            self.store.con.execute('UPDATE fightodds_runs SET finished_at=?,report=? WHERE id=?',
                (datetime.now(timezone.utc).isoformat(),encoded(report),run))
        return report

    def get_offer_type(self, outcome_id):
        outcome = self.store.get('outcome',outcome_id)
        return self.store.get('offer',outcome['offer_id'])['offerType']['id']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True)
    sub = parser.add_subparsers(dest='command',required=True)
    collect = sub.add_parser('collect')
    collect.add_argument('--promotion',default='UFC')
    collect.add_argument('--start'); collect.add_argument('--end'); collect.add_argument('--name')
    collect.add_argument('--max-events',type=int,default=1)
    collect.add_argument('--fight-slug',action='append')
    collect.add_argument('--page-size',type=int,default=100)
    collect.add_argument('--max-pages',type=int,default=100)
    collect.add_argument('--max-requests',type=int,default=1000)
    collect.add_argument('--pace',type=float,default=.5)
    collect.add_argument('--refresh',action='store_true')
    collect.add_argument('--report',type=Path)
    snap = sub.add_parser('snapshot')
    snap.add_argument('--event-id',required=True)
    snap.add_argument('--cutoff'); snap.add_argument('--hour',type=int,default=0)
    snap.add_argument('--trust-start-time',action='store_true')
    snap.add_argument('--stale-seconds',type=float)
    snap.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    store = Store(args.db)
    try:
        if args.command=='snapshot':
            args.output.write_text(json.dumps(store.snapshot(args.event_id,args.cutoff,args.hour,
                args.trust_start_time,args.stale_seconds),indent=2)+'\n')
            return
        client = Client(store,pace=args.pace,refresh=args.refresh,max_requests=args.max_requests)
        try:
            collector = Collector(client,args.page_size,args.max_pages)
            events = collector.discover(args.promotion,args.start,args.end,args.name,args.max_events)
            if not events:
                raise ApiError('No matching events')
            report = collector.collect(events,args.fight_slug)
            if args.report:
                args.report.write_text(json.dumps(report,indent=2)+'\n')
            print(json.dumps(report,indent=2))
            if report['failures']:
                raise SystemExit(1)
        finally:
            client.close()
    finally:
        store.close()


if __name__ == '__main__':
    main()
