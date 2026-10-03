"""Bounded live Firefox check; all databases/checkpoints live in a temporary directory.

Run from the repository root: PYTHONPATH=. python tests/live_ufcstats_smoke.py
"""
import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile

import pandas as pd

from scrape.scrape_ufcstats import (CharFightersScraper, FullUfcScraper,
                                    UpcomingUfcScraper, publish_tables)
from scrape.ufcstats_browser import FirefoxClient
from wrangle.clean_ufc_stats_data import UfcDataCleaner

EVENT = 'http://ufcstats.com/event-details/253d3f9e97ca149a'
FIGHT = 'http://ufcstats.com/fight-details/7d4e49d8a6678157'
FIVE_ROUND = 'http://ufcstats.com/fight-details/9d61012a6020516e'
FIGHTER = 'http://ufcstats.com/fighter-details/07225ba28ae309b6'
DIRECTORY = 'http://ufcstats.com/statistics/fighters?char=a&page=all'


class Reader:
    def __init__(self, con):
        self.con = con

    def read(self, table):
        return pd.read_sql(f'SELECT * FROM "{table}"', self.con)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--firefox-binary')
    parser.add_argument('--geckodriver')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='ufcstats-smoke-') as directory:
        checkpoint = Path(directory) / 'checkpoint.sqlite'
        with FirefoxClient(checkpoint, firefox_binary=args.firefox_binary,
                           geckodriver=args.geckodriver) as client:
            links = CharFightersScraper(DIRECTORY, client=client).get_page_urls()
            assert len(links) > 100 and all('/fighter-details/' in url for url in links)
            full = FullUfcScraper([FIGHTER], [EVENT], [FIGHT, FIVE_ROUND], client=client)
            full.scrape_all()
            assert len(full.event_data) == 14 and len(full.fighter_data) == 1
            assert len(full.totals_df) == len(full.strikes_df) == 4
            assert len(full.round_totals_df) == len(full.round_strikes_df) == 12
            upcoming = UpcomingUfcScraper(client=client, max_events=2)
            upcoming.scrape_all()
            with closing(sqlite3.connect(Path(directory) / 'output.sqlite')) as con:
                publish_tables({**full.tables(), **upcoming.tables()}, con)
                clean = UfcDataCleaner(Reader(con)).parse_all()
                fight = clean.loc[clean['FightID'] == FIGHT]
                assert fight['SSL'].tolist() == [30, 21]
                assert fight['SHL'].tolist() == [18, 13]
                assert fight['ctrl_seconds'].tolist() == [39, 13]
                assert fight['time_dur'].tolist() == [202, 202]
                assert fight['location'].tolist() == ['Phoenix, Arizona, USA'] * 2
                assert con.execute('SELECT COUNT(*) FROM ufc_round_totals').fetchone()[0] == 12
                rounds = pd.read_sql('SELECT * FROM ufc_round_strikes', con)
                assert rounds.loc[rounds.FightID == FIVE_ROUND, 'Round'].tolist() == list(range(5)) * 2
                assert len(clean) == 28 + 2 * len(upcoming.upcoming_fights_df)
            print(json.dumps(dict(directory_fighters=len(links),
                 historical={name: len(frame) for name, frame in full.tables().items()},
                 upcoming_events=len(upcoming.upcoming_event_urls),
                 upcoming_fights=len(upcoming.upcoming_fights_df), clean_rows=len(clean),
                 navigations=client.requests, browser_starts=client.browser_starts,
                 cache_hits=client.cache_hits), indent=2))
        # A fresh client resumes the historical pages without starting Firefox.
        with FirefoxClient(checkpoint) as resumed:
            full = FullUfcScraper([FIGHTER], [EVENT], [FIGHT, FIVE_ROUND], client=resumed)
            full.scrape_all()
            assert resumed.requests == resumed.browser_starts == 0
            assert resumed.cache_hits == 4
            print('Historical resume passed without Firefox.')


if __name__ == '__main__':
    main()
