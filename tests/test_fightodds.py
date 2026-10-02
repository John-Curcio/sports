"""Offline tests use an isolated DB and real, bounded source fixtures."""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import re

import pytest
import requests

from scrape.scrape_fightodds import (ApiError, Client, Collector, Store,
    connection, event_cutoff, instant)

FIXTURE = json.loads((Path(__file__).parent/'fixtures/fightodds/jones-gane.json').read_text())


class Response:
    def __init__(self, data, status=200):
        self.data, self.status_code = data,status
    def json(self):
        if isinstance(self.data, Exception):
            raise self.data
        return copy.deepcopy(self.data)


class Session:
    def __init__(self, handler):
        self.handler,self.calls = handler,[]
    def post(self,url,json,timeout):
        assert url=='https://api.fightodds.io/gql' and timeout>0
        self.calls.append(json)
        return self.handler(json['query'],json['variables'])
    def close(self):
        pass


def conn(nodes, first=100, after=None):
    start = int(after or 0)
    items = nodes[start:start+first]
    return {'pageInfo':{'hasNextPage':start+len(items)<len(nodes),
                        'endCursor':str(start+len(items))},
            'edges':[{'cursor':str(start+i+1),'node':n} for i,n in enumerate(items)]}


def world(query, v):
    fixture = copy.deepcopy(FIXTURE)
    if 'query Events' in query:
        data = {'allEvents':conn([fixture['event']],v['first'],v['after'])}
    elif 'query Fights' in query:
        data = {'allFights':conn([fixture['fight']],v['first'],v['after'])}
    elif 'query Offers' in query:
        data = {'allOffers':conn(fixture['offers'] if v['fight'] else [],v['first'],v['after'])}
    elif 'query Props' in query:
        groups = [{'offerType':o['offerType'],'propName1':'Exact supplied label '+str(i),
                   'propName2':'No'} for i,o in enumerate(fixture['offers']) if o['offerType'] and o['offerType']['offerTypeId']]
        data = {'fightPropOfferTable':{'propOffers':conn(groups,v['first'],v['after'])}}
    elif 'query Histories' in query:
        data = {}
        for alias, idx in re.findall(r'(h\d+):allOdds\(outcome:\$o(\d+)',query):
            data[alias] = conn(fixture['histories'][v['o'+idx]],v['first'],v['a'+idx])
    else:
        raise AssertionError(query)
    return Response({'data':data})


def collector(tmp_path,handler=world,**kwargs):
    store = Store(tmp_path/'isolated.sqlite')
    client = Client(store,session=Session(handler),pace=0,retries=0,**kwargs)
    return Collector(client,page_size=2)


def test_collection_all_markets_pagination_duplicate_book_resume(tmp_path):
    c = collector(tmp_path)
    events = c.discover(name='UFC 285',start='2023-03-04',end='2023-03-04')
    report = c.collect(events)
    assert not report['failures']
    assert len(c.store.records('offer'))==4
    assert len(c.store.records('prop_group'))==3
    assert report['coverage'][1]['moneyline']['observations']==sum(
        len(FIXTURE['histories'][e['node']['id']]) for e in FIXTURE['offers'][0]['outcomes']['edges'])
    assert report['coverage'][1]['prop']['outcomes']>0
    assert report['coverage'][1]['prop_groups_complete']
    assert len({o['sportsbook']['id'] for o in c.store.records('offer')})<4
    # Actual outcome fighter metadata survives reversed outcome ordering.
    for out in c.store.records('outcome'):
        original=next(e['node'] for o in FIXTURE['offers'] for e in o['outcomes']['edges'] if e['node']['id']==out['id'])
        assert out['fighter']==original['fighter']
    before=c.store.snapshot(events[0]['id'])
    count=c.store.con.execute('select count(*) from fightodds_prices').fetchone()[0]
    c.client.close();c.store.close()
    def offline(q,v):
        raise AssertionError('Resume must use cached pages')
    resumed=collector(tmp_path,offline)
    assert not resumed.collect(resumed.discover(name='UFC 285',start='2023-03-04',end='2023-03-04'))['failures']
    assert resumed.store.con.execute('select count(*) from fightodds_prices').fetchone()[0]==count
    assert resumed.store.snapshot(events[0]['id'])==before


@pytest.mark.parametrize('body,status',[
    ({'errors':[{'message':'schema failure'}],'data':{}},200),
    ({'data':None},200), ({},200), ([],200),
    (ValueError('not JSON'),200), ({},403), ({},503)])
def test_errors_are_not_cached(tmp_path,body,status):
    c=collector(tmp_path,lambda q,v:Response(body,status))
    with pytest.raises(ApiError): c.discover()
    assert c.store.con.execute('select count(*) from fightodds_responses').fetchone()[0]==0


def test_retry_is_bounded_and_budget_is_enforced(tmp_path,monkeypatch):
    monkeypatch.setattr('scrape.scrape_fightodds.time.sleep',lambda _:None)
    n=[]
    def bad(q,v):
        n.append(1);raise requests.Timeout('slow')
    s=Store(tmp_path/'retry.sqlite')
    client=Client(s,session=Session(bad),retries=2,pace=0,max_requests=2)
    with pytest.raises(ApiError,match='budget'):client.query('query {}',{})
    assert len(n)==2
    with pytest.raises(ApiError,match='budget'):client.query('query {}',{})


def test_repeated_cursor_and_page_limit_fail(tmp_path):
    def repeat(q,v):
        response=world(q,v)
        if 'query Offers' in q and v['fight']:
            response.data['data']['allOffers']=conn(FIXTURE['offers'],1,None)
        return response
    c=collector(tmp_path,repeat)
    report=c.collect([FIXTURE['event']])
    assert any('Repeated pagination' in f['error'] for f in report['failures'])
    c.max_pages=1
    assert c.collect([FIXTURE['event']])['failures']


def test_unsupported_nested_pagination_is_explicit(tmp_path):
    def truncated(q,v):
        response=world(q,v)
        if 'query Offers' in q and v['fight']:
            response.data['data']['allOffers']['edges'][0]['node']['outcomes']['pageInfo']['hasNextPage']=True
        return response
    c=collector(tmp_path,truncated)
    assert 'truncated' in c.collect([FIXTURE['event']])['failures'][0]['error']


@pytest.mark.parametrize('value',[None,{}, {'edges':[]},
    {'edges':[],'pageInfo':{'hasNextPage':True,'endCursor':'x'}},
    {'edges':[{'node':None}],'pageInfo':{'hasNextPage':False}}])
def test_malformed_connection(value):
    with pytest.raises(ApiError):connection(value)


def test_unavailable_history_differs_from_failed_and_null(tmp_path):
    missing=list(FIXTURE['histories'])[0]
    def gaps(q,v):
        response=world(q,v)
        if 'query Histories' in q:
            for alias,idx in re.findall(r'(h\d+):allOdds\(outcome:\$o(\d+)',q):
                if v['o'+idx]==missing:response.data['data'][alias]=conn([])
        return response
    c=collector(tmp_path,gaps)
    report=c.collect([FIXTURE['event']])
    assert not report['failures']
    assert c.store.con.execute('select state from fightodds_history_status where outcome_id=?',(missing,)).fetchone()[0]=='unavailable'
    def fails(q,v):
        return Response({'errors':[{'message':'history unavailable upstream'}]}) if 'query Histories' in q else world(q,v)
    # Refresh must not substitute old cached successes for failed requests.
    c=collector(tmp_path,fails,refresh=True)
    assert c.collect([FIXTURE['event']])['failures']
    assert c.store.con.execute("select count(*) from fightodds_history_status where state='failed'").fetchone()[0]>0


def seeded(tmp_path):
    s=Store(tmp_path/'snap.sqlite')
    e={'id':'e','date':'2023-03-04','startTime':None,'isCancelled':False,'temp':False}
    s.record('event',e)
    s.record('market',{'id':'m','line':'2.5','description':'Rounds'})
    s.record('offer',{'id':'o','market_id':'m','market_kind':'prop','event_id':'e','fight':None,'sportsbook':{'id':'book'}})
    s.record('outcome',{'id':'x','offer_id':'o','name':'Under 2.5','isNot':False})
    return s,e


def test_asof_before_at_after_missing_future_null_age_conflict(tmp_path):
    s,e=seeded(tmp_path)
    point=lambda id,t,odds:s.price('x',dict(id=id,timestamp=t,odds=odds),1)
    assert s.snapshot('e')[0]['selection_state']=='no_earlier_observation'
    point('before','2023-03-04T04:00:00Z',120)
    assert s.snapshot('e',stale_seconds=3599)[0]['stale']
    point('at','2023-03-04T05:00:00Z',110)
    selected=s.snapshot('e')[0]
    assert selected['odds']==110 and selected['quote_age_seconds']==0
    assert selected['availability_at_cutoff']=='unknown'
    point('after','2023-03-04T05:00:00.000001Z',-200)
    assert s.snapshot('e')[0]==selected
    point('at','2023-03-04T05:00:00Z',None)
    assert s.snapshot('e')[0]['selection_state']=='null_observation'
    point('duplicate','2023-03-04T05:00:00Z',130)
    assert s.snapshot('e')[0]['selection_state']=='conflicting_same_timestamp'
    assert s.con.execute('select count(*) from fightodds_price_versions where id="at"').fetchone()[0]==2


@pytest.mark.parametrize('date_,utc_hour', [('2023-03-11',5),('2023-03-13',4),('2023-11-04',4),('2023-11-06',5)])
def test_eastern_dst(date_,utc_hour):
    result=event_cutoff({'date':date_})
    assert instant(result['cutoff']).hour==utc_hour
    assert result['timing_uncertain']


def test_overseas_ambiguous_missing_date_and_start_conflict():
    e={'date':'2023-03-05','startTime':'2023-03-05T01:00:00+09:00'}
    verified=event_cutoff(e,trust_start_time=True)
    assert verified['basis']=='verified_start_time_eastern_day'
    assert instant(verified['cutoff'])==datetime(2023,3,4,5,tzinfo=timezone.utc)
    fallback=event_cutoff(e)
    assert fallback['possible_after_start'] and fallback['timing_uncertain']
    assert event_cutoff({})['cutoff'] is None
    with pytest.raises(ValueError):event_cutoff({'startTime':'2023-03-04T20:00:00'},trust_start_time=True)
    with pytest.raises(ValueError):instant('2023-03-04T20:00:00')


@pytest.mark.parametrize('field',['isCancelled','temp'])
def test_canceled_hypothetical_and_rescheduled_versions(tmp_path,field):
    s,e=seeded(tmp_path)
    s.price('x',{'id':'p','timestamp':'2023-03-04T01:00:00Z','odds':120},1)
    e[field]=True;s.record('event',e)
    assert s.snapshot('e')[0]['selection_state']=='cancelled_or_hypothetical'
    e['date']='2023-03-11';s.record('event',e)
    assert s.con.execute("select count(*) from fightodds_record_versions where kind='event'").fetchone()[0]==3


def test_guard_repository_database(tmp_path):
    with pytest.raises(ValueError):Store(tmp_path/'mma.db')


def test_existing_identity_join_in_isolated_database_preserves_snapshot(tmp_path):
    import pandas as pd
    from wrangle.fightodds_data import identity_frame, map_identities
    c=collector(tmp_path)
    assert not c.collect([FIXTURE['event']])['failures']
    before=c.store.snapshot(FIXTURE['event']['id'])
    canonical=json.loads((Path(__file__).parent/'fixtures/fightodds/legacy-identities.json').read_text())
    for source in ['espn','ufcstats']:
        frame=pd.DataFrame(canonical[source])
        # Copy source rows to the isolated DB before running the existing matcher.
        frame.to_sql('canonical_'+source,c.store.con,index=False,if_exists='replace')
        frame=pd.read_sql_query('select * from canonical_'+source,c.store.con)
        mapping=map_identities(c.store,frame,source, name_overrides={'Cyril Gané':'Ciryl Gane'})
        fight=FIXTURE['fight']
        assert mapping[fight['fighter1']['id']]==canonical[source][0]['FighterID']
        assert mapping[fight['fighter2']['id']]==canonical[source][0]['OpponentID']
        assert c.store.con.execute("select count(*) from fightodds_identity_maps where target_source=?",(source,)).fetchone()[0]==4
    assert c.store.snapshot(FIXTURE['event']['id'])==before
    assert identity_frame(c.store).iloc[0]['FighterID'].startswith('RmlnaHR')
    assert before[0]['basis']=='source_date_assumed_eastern_day'


def test_prop_failure_does_not_prevent_moneyline_validation(tmp_path):
    def malformed(q,v):
        response=world(q,v)
        if 'query Offers' in q and v['fight']:
            for edge in response.data['data']['allOffers']['edges']:
                if edge['node'].get('offerType') and edge['node']['offerType']['offerTypeId']!='STRAIGHT':
                    edge['node']['sportsbook']=None
        return response
    c=collector(tmp_path,malformed)
    report=c.collect([FIXTURE['event']])
    assert report['failures']
    assert not report['coverage'][1]['offers_complete']
    assert report['coverage'][1]['moneyline']['complete']==2


def test_line_labels_and_null_summary_are_preserved(tmp_path):
    c=collector(tmp_path)
    assert not c.collect([FIXTURE['event']])['failures']
    typed=next(o for o in FIXTURE['offers'] if o.get('offerType') and o['offerType'].get('value') is not None)
    market=next(m for m in c.store.records('market') if m['offer_type_id']==typed['offerType']['id'])
    assert market['line']==typed['offerType']['value']
    assert market['description']==typed['offerType']['description']
    snapshots=c.store.snapshot(FIXTURE['event']['id'])
    labelled=next(r for r in snapshots if r['offer_id']==typed['id'])
    assert labelled['prop_labels']
    unknown=next(o for o in FIXTURE['offers'] if o.get('offerType') is None)
    assert c.store.get('offer',unknown['id'])['offerType']['offerTypeId'] is None
    out=copy.deepcopy(typed['outcomes']['edges'][0]['node']);out['odds']=None
    c.store.record('outcome',dict(out,offer_id=typed['id']))
    assert c.store.get('outcome',out['id'])['odds'] is None
    assert c.store.con.execute('select count(*) from fightodds_prices where outcome_id=?',(out['id'],)).fetchone()[0]>0


def test_duplicate_offers_same_book_market_and_price_ids(tmp_path,monkeypatch):
    fixture=copy.deepcopy(FIXTURE)
    duplicate=copy.deepcopy(fixture['offers'][0])
    duplicate['id']='distinct-offer-same-book-market'
    for edge in duplicate['outcomes']['edges']:
        oid=edge['node']['id'];edge['node']['id']=oid+'-duplicate'
        fixture['histories'][oid+'-duplicate']=[dict(p,id=p['id']+'-duplicate') for p in fixture['histories'][oid]]
    fixture['offers'].append(duplicate)
    # Also repeat an identical offer identity; normalized inserts must be idempotent.
    fixture['offers'].append(copy.deepcopy(duplicate))
    monkeypatch.setattr(__name__+'.FIXTURE',fixture)
    c=collector(tmp_path)
    report=c.collect([fixture['event']])
    assert not report['failures']
    original=c.store.get('offer',fixture['offers'][0]['id'])
    copied=c.store.get('offer',duplicate['id'])
    assert original['market_id']==copied['market_id']
    assert original['sportsbook']==copied['sportsbook']
    assert len(c.store.records('offer'))==5
    assert report['coverage'][1]['moneyline']['outcomes']==4


def test_client_refresh_corrections_archive_raw_responses_and_keep_history(tmp_path):
    c=collector(tmp_path)
    c.collect([FIXTURE['event']])
    raw_count=c.store.con.execute('select count(*) from fightodds_responses').fetchone()[0]
    fixture=copy.deepcopy(FIXTURE)
    oid=next(iter(fixture['histories']))
    old=fixture['histories'][oid][0]
    def correction(q,v):
        response=world(q,v)
        if 'query Histories' in q:
            for connection_ in response.data['data'].values():
                for edge in connection_['edges']:
                    if edge['node']['id']==old['id']:edge['node']['odds']=999
        return response
    refreshed=collector(tmp_path,correction,refresh=True)
    assert not refreshed.collect([FIXTURE['event']])['failures']
    assert refreshed.store.con.execute('select odds from fightodds_prices where id=?',(old['id'],)).fetchone()[0]==999
    assert refreshed.store.con.execute('select count(*) from fightodds_price_versions where id=?',(old['id'],)).fetchone()[0]==2
    assert refreshed.store.con.execute('select count(*) from fightodds_responses').fetchone()[0]>raw_count


def test_nonexistent_dst_hour_rejected_and_ambiguous_hour_is_earliest():
    with pytest.raises(ValueError,match='nonexistent'):
        event_cutoff({'date':'2023-03-12'},hour=2)
    result=event_cutoff({'date':'2023-11-05'},hour=1)
    assert instant(result['cutoff'])==datetime(2023,11,5,5,tzinfo=timezone.utc)


def test_fight_cancellation_missing_date_and_incomplete_snapshot(tmp_path):
    s,e=seeded(tmp_path)
    s.record('fight',{'id':'fight','isCancelled':True})
    offer=s.get('offer','o');offer['fight']={'id':'fight'};s.record('offer',offer)
    s.price('x',{'id':'p','timestamp':'2023-03-04T01:00:00Z','odds':120},1)
    assert s.snapshot('e')[0]['selection_state']=='cancelled_or_hypothetical'
    offer['fight']=None;s.record('offer',offer)
    assert s.snapshot('e')[0]['selection_state']=='candidate_from_incomplete_history'
    s.history_status('x','complete',1)
    assert s.snapshot('e')[0]['selection_state']=='selected'
    s.history_status('x','unavailable',0)
    assert s.snapshot('e')[0]['selection_state']=='candidate_from_archived_history'
    e['date']=None;s.record('event',e)
    assert s.snapshot('e')[0]['selection_state']=='missing_cutoff'
