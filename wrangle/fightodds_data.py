"""Source-specific identity adapter; no aggregation or active pipeline cutover."""
import json

import pandas as pd


def identity_frame(store):
    """The existing graph matcher expects names and a source calendar date."""
    rows = []
    for fight in store.records('fight'):
        event = store.get('event', fight['event_id'])
        first, second = fight.get('fighter1'), fight.get('fighter2')
        if not event or not first or not second or not event.get('date') or fight.get('isCancelled') or event.get('isCancelled') or event.get('temp'):
            continue
        rows.append({'FighterID':first['id'], 'OpponentID':second['id'],
            'FighterName':(first['firstName']+' '+first['lastName']).strip(),
            'OpponentName':(second['firstName']+' '+second['lastName']).strip(),
            'Date':event['date'], 'fightodds_fight_id':fight['id'],
            'fightodds_event_id':event['id']})
    return pd.DataFrame(rows, columns=['FighterID','OpponentID','FighterName','OpponentName',
        'Date','fightodds_fight_id','fightodds_event_id'])


def map_identities(store, canonical, target_source, day_tol=0, name_overrides=None):
    """Persist explicit mappings using the repository's existing identity matcher.

    canonical is caller-supplied data. This function never reads repository tables.
    Cancelled/hypothetical fights are retained by the collector but excluded here.
    """
    from wrangle.identity_matching import IsomorphismFinder
    if target_source not in {'espn','ufcstats'}:
        raise ValueError('target_source must be espn or ufcstats')
    auxiliary = identity_frame(store)
    if name_overrides:
        for column in ['FighterName','OpponentName']:
            auxiliary[column] = auxiliary[column].replace(name_overrides)
    canonical = canonical.copy()
    canonical['Date'] = pd.to_datetime(canonical['Date'])
    auxiliary['Date'] = pd.to_datetime(auxiliary['Date'])
    matcher = IsomorphismFinder(canonical.copy(), auxiliary.copy(), day_tol=day_tol)
    matcher.find_isomorphism()
    mapping = matcher.fighter_id_map.to_dict()
    with store.con:
        for source_id,target_id in mapping.items():
            store.con.execute('INSERT OR REPLACE INTO fightodds_identity_maps VALUES (?,?,?,?,?)',
                ('fighter',source_id,target_source,str(target_id),json.dumps(
                    {'method':'IsomorphismFinder','day_tolerance':day_tol,'name_overrides':name_overrides or {}})))
    # Require both fighters and a uniquely matched calendar date before mapping a fight/event.
    for fight in store.records('fight'):
        f1,f2 = fight.get('fighter1') or {},fight.get('fighter2') or {}
        if f1.get('id') not in mapping or f2.get('id') not in mapping or fight.get('isCancelled'):
            continue
        event = store.get('event',fight['event_id'])
        if not event or event.get('isCancelled') or event.get('temp') or not event.get('date'):
            continue
        a,b = mapping[f1['id']],mapping[f2['id']]
        candidates = canonical[((canonical.FighterID==a)&(canonical.OpponentID==b)) |
                               ((canonical.FighterID==b)&(canonical.OpponentID==a))].copy()
        days = (pd.to_datetime(candidates.Date).dt.normalize()-pd.Timestamp(event['date'])).dt.days.abs()
        candidates = candidates[days<=day_tol]
        for kind,column,source_id in [('fight','FightID',fight['id']),
            ('event','EventUrl',event['id'])] if target_source=='ufcstats' else [
            ('fight','fight_id',fight['id']),('event','Event',event['id'])]:
            if column not in candidates:
                continue
            ids = candidates[column].dropna().astype(str).unique()
            if len(ids)==1:
                with store.con:
                    store.con.execute('INSERT OR REPLACE INTO fightodds_identity_maps VALUES (?,?,?,?,?)',
                        (kind,source_id,target_source,
                            json.dumps([event['date'],ids[0]]) if target_source=='espn' and kind=='event' else ids[0],json.dumps(
                            {'method':'mapped_pair_and_date','day_tolerance':day_tol,'name_overrides':name_overrides or {}})))
    return mapping
