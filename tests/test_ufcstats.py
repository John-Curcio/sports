"""Real HTML parser fixtures, browser lifecycle, durable resume and isolated writes."""
from pathlib import Path
import sqlite3
import subprocess
import sys

from bs4 import BeautifulSoup
import pandas as pd
import pytest
from selenium.common.exceptions import WebDriverException

from scrape.scrape_ufcstats import (CharFightersScraper, UfcFighterScraper, UfcEventScraper,
    UfcFightDetails, FullUfcScraper, UpcomingUfcScraper, UpcomingUfcEventScraper,
    UfcUrlScraper, EVENT_COLUMNS, publish_tables)
from scrape.ufcstats_browser import CollectionError, FirefoxClient, MISSING_STATS, page_ready
from wrangle.clean_ufc_stats_data import UfcDataCleaner

FIXTURES = Path(__file__).parent / 'fixtures' / 'ufcstats'
EVENT = 'http://ufcstats.com/event-details/253d3f9e97ca149a'
FIGHT = 'http://ufcstats.com/fight-details/7d4e49d8a6678157'
FIGHTER = 'http://ufcstats.com/fighter-details/07225ba28ae309b6'
DIRECTORY = 'http://ufcstats.com/statistics/fighters?char=a&page=all'


def fixture(name):
    return (FIXTURES / (name + '.html')).read_text()


def page(cls, url, name):
    scraper = cls(url)
    scraper.raw_html = fixture(name)
    return scraper


def missing_html():
    # Synthetic missing-stat variant of a real completed fight. The metadata
    # and identities stay intact; the explicit site notice replaces all tables.
    soup = BeautifulSoup(fixture('fight'), 'lxml')
    for table in soup.select('table'):
        table.decompose()
    notice = soup.new_tag('p')
    notice.string = MISSING_STATS
    soup.body.append(notice)
    return str(soup)


class Driver:
    def __init__(self, handler):
        self.handler, self.calls, self.quit_count = handler, [], 0
        self.page_source = ''

    def set_page_load_timeout(self, timeout):
        assert timeout > 0

    def get(self, url):
        self.calls.append(url)
        self.current_url = url
        self.page_source = self.handler(url)

    def quit(self):
        self.quit_count += 1


def world(url):
    return {EVENT: fixture('event'), FIGHT: fixture('fight'), FIGHTER: fixture('fighter'),
            DIRECTORY: fixture('directory')}[url]


def test_directory_fighter_event_and_four_stat_tables():
    directory = page(CharFightersScraper, DIRECTORY, 'directory')
    assert len(directory.get_page_urls()) == 247
    assert all('/fighter-details/' in url for url in directory.get_page_urls())
    fighter = page(UfcFighterScraper, FIGHTER, 'fighter')
    assert fighter.get_page_data()['Reach:'] == '74"'
    assert fighter.get_page_data()['DOB:'] == 'Oct 17, 1989'
    assert FIGHT in fighter.get_fights() and EVENT in fighter.get_events()
    event = page(UfcEventScraper, EVENT, 'event')
    rows = event.get_page_data()
    assert len(rows) == 14
    assert rows.iloc[0]['FighterName'].strip() == 'Charles Oliveira'
    assert rows.iloc[0]['OpponentName'].strip() == 'Justin Gaethje'
    assert rows.iloc[0]['Date'] == 'May 07, 2022'
    assert rows.iloc[0]['Location'] == 'Phoenix, Arizona, USA'
    assert rows.iloc[0]['FightID'] == FIGHT
    fight = page(UfcFightDetails, FIGHT, 'fight')
    fight.get_page_data()
    assert fight.fight_description['Method'] == 'Submission'
    assert fight.totals['Sig. str.'].str.strip().tolist() == ['30 of 47', '21 of 33']
    assert fight.strikes['Head'].str.strip().tolist() == ['18 of 32', '13 of 22']
    assert fight.round_totals['Ctrl'].str.strip().tolist() == ['0:39', '0:13']
    assert fight.round_strikes['Ground'].str.strip().tolist() == ['2 of 3', '0 of 0']
    assert fight.round_totals['Round'].tolist() == [0, 0]  # Preserve the existing zero-based contract.
    assert fight.round_totals['FighterID'].tolist() == fight.totals['FighterID'].tolist()


def test_five_round_alignment_and_significant_strikes():
    fight = page(UfcFightDetails, 'http://ufcstats.com/fight-details/9d61012a6020516e', 'multi-round')
    fight.get_page_data()
    assert len(fight.round_totals) == len(fight.round_strikes) == 10
    assert fight.round_totals['Round'].tolist() == list(range(5)) * 2
    assert fight.round_totals['FighterID'].tolist() == [fight.totals.iloc[0]['FighterID']] * 5 + [fight.totals.iloc[1]['FighterID']] * 5
    assert fight.round_totals['Sig. str.'].str.strip().tolist() == [
        '4 of 19', '3 of 14', '11 of 36', '6 of 28', '13 of 39',
        '4 of 19', '4 of 22', '9 of 32', '8 of 29', '5 of 31']


def test_historical_discovery_excludes_scheduled_matchup():
    fighter = page(UfcFighterScraper, 'http://ufcstats.com/fighter-details/262d32ebda89efc4',
                   'scheduled-fighter')
    assert 'Matchup Preview' in fighter.get_soup().get_text(' ', strip=True)
    assert len(fighter.get_events()) == len(fighter.get_fights()) == 8
    assert 'http://ufcstats.com/event-details/ad3fdba28a7540cf' not in fighter.get_events()
    assert 'http://ufcstats.com/fight-details/3f804eec9183e597' not in fighter.get_fights()


def test_upcoming_reloads_mutable_cards_and_handles_genuine_empty_cards(tmp_path):
    def handler(url):
        return fixture('upcoming') if url.endswith('/upcoming') else fixture('upcoming-event')
    driver = Driver(handler)
    with FirefoxClient(tmp_path/'upcoming.sqlite', driver_factory=lambda: driver, pace=0) as client:
        upcoming = UpcomingUfcScraper(client=client, max_events=1)
        upcoming.scrape_all()
        assert len(upcoming.upcoming_fights_df) == 14
        assert upcoming.upcoming_fights_df.iloc[0]['FighterName'] == 'Natalia Silva'
        assert upcoming.upcoming_fights_df.iloc[0]['OpponentName'] == 'Wang Cong'
        upcoming.scrape_all()
        assert len(driver.calls) == 4 and client.browser_starts == 1
        soup = BeautifulSoup(fixture('upcoming-event'), 'lxml')
        soup.select_one('tbody').clear()
        driver.handler = lambda u: fixture('upcoming') if u.endswith('/upcoming') else str(soup)
        upcoming.scrape_all()
        assert upcoming.upcoming_fights_df.empty
        with sqlite3.connect(':memory:') as con:
            upcoming.write_all_to_tables(con)
            assert con.execute('SELECT COUNT(*) FROM ufc_upcoming_fights').fetchone()[0] == 0
        # A genuinely empty listing is also successful, unlike a challenge page.
        empty_listing = BeautifulSoup(fixture('upcoming'), 'lxml')
        empty_listing.select_one('tbody').clear()
        driver.handler = lambda u: str(empty_listing)
        upcoming.scrape_all()
        assert upcoming.upcoming_event_urls == [] and upcoming.upcoming_fights_df.empty
        driver.handler = lambda u: '<html>Checking your browser…</html>'
        client.wait_timeout = .01
        client.retries = 0
        with pytest.raises(CollectionError):
            upcoming.scrape_all()
        with pytest.raises(CollectionError, match='not finished'):
            upcoming.tables()
    assert driver.quit_count == 1


def test_reuse_durable_resume_and_discovery(tmp_path):
    driver = Driver(world)
    with FirefoxClient(tmp_path/'checkpoint.sqlite', driver_factory=lambda: driver, pace=0) as client:
        urls = UfcUrlScraper(client=client, letters='a', max_fighters=1)
        assert len(urls.get_all_fighter_urls()) == 1
        client.fetch(FIGHTER, 'fighter')
        client.fetch(FIGHT, 'fight')
        assert client.fetch(FIGHT, 'fight') == fixture('fight')
        urls.fighter_urls = {FIGHTER}
        events, fights = urls.get_all_event_and_fight_urls()
        assert EVENT in events and FIGHT in fights
        assert client.browser_starts == 1 and client.requests == 3
    assert driver.quit_count == 1
    def no_browser():
        raise AssertionError('Resume should not need a browser')
    with FirefoxClient(tmp_path/'checkpoint.sqlite', driver_factory=no_browser) as client:
        assert client.fetch(FIGHT, 'fight') == fixture('fight')
        assert client.fetch(FIGHTER, 'fighter') == fixture('fighter')
        assert client.cache_hits == 2 and client.requests == 0


def test_interrupted_directory_discovery_retries_unfinished_letters(tmp_path):
    broken = {'enabled': True}
    def handler(url):
        if 'char=b' in url and broken['enabled']:
            raise WebDriverException('directory interrupted')
        return fixture('directory')
    with FirefoxClient(tmp_path/'directories.sqlite', driver_factory=lambda: Driver(handler),
                       pace=0, retries=0) as client:
        urls = UfcUrlScraper(client=client, letters='ab', max_fighters=1)
        with pytest.raises(CollectionError):
            urls.get_all_event_and_fight_urls()
        assert urls.fighter_urls is urls.event_urls is urls.fight_urls is None
        broken['enabled'] = False
        assert len(urls.get_all_fighter_urls()) == 1
        assert client.cache_hits == 1 and client.requests == 3


def test_broken_browser_restarts_and_exhaustion_is_identifiable(tmp_path):
    def broken(url):
        raise WebDriverException('browser died')
    drivers = [Driver(broken), Driver(world)]
    with FirefoxClient(tmp_path/'retry.sqlite', driver_factory=lambda: drivers.pop(0), pace=0,
                       retries=1) as client:
        first = drivers[0]
        assert client.fetch(FIGHT, 'fight') == fixture('fight')
        assert client.browser_starts == 2 and first.quit_count == 1
        last = client.driver
    assert last.quit_count == 1
    drivers = []
    def factory():
        driver = Driver(broken)
        drivers.append(driver)
        return driver
    with FirefoxClient(tmp_path/'bad.sqlite', driver_factory=factory, pace=0, retries=1) as client:
        with pytest.raises(CollectionError, match=FIGHT + ': acquisition failed after 2'):
            client.fetch(FIGHT, 'fight')
        assert client.con.execute('SELECT COUNT(*) FROM ufcstats_pages').fetchone()[0] == 0
    assert len(drivers) == 2 and all(d.quit_count == 1 for d in drivers)


def test_interruption_preserves_progress_and_closes_browser(tmp_path):
    def interrupted(url):
        if url == FIGHT:
            raise KeyboardInterrupt()
        return world(url)
    driver = Driver(interrupted)
    with pytest.raises(KeyboardInterrupt):
        with FirefoxClient(tmp_path/'interrupt.sqlite', driver_factory=lambda: driver, pace=0) as client:
            client.fetch(EVENT, 'event')
            client.fetch(FIGHT, 'fight')
    assert driver.quit_count == 1
    resumed = Driver(world)
    with FirefoxClient(tmp_path/'interrupt.sqlite', driver_factory=lambda: resumed, pace=0) as client:
        client.fetch(EVENT, 'event')
        client.fetch(FIGHT, 'fight')
    assert resumed.calls == [FIGHT]


def test_challenge_partial_tables_and_error_pages_are_failures(tmp_path):
    challenge = '<html><title>Loading…</title><body>Checking your browser…</body></html>'
    assert not page_ready(challenge, 'fight')
    scraper = UfcFightDetails(FIGHT)
    scraper.raw_html = challenge
    with pytest.raises(CollectionError, match='blocked'):
        scraper.get_page_data()
    partial = BeautifulSoup(fixture('fight'), 'lxml')
    partial.select('table')[-1].decompose()
    assert not page_ready(str(partial), 'fight')
    driver = Driver(lambda _: challenge)
    with FirefoxClient(tmp_path/'challenge.sqlite', driver_factory=lambda: driver,
                       pace=0, retries=0, wait_timeout=.01) as client:
        with pytest.raises(CollectionError, match='acquisition failed'):
            client.fetch(FIGHT, 'fight')
        assert client.con.execute('SELECT COUNT(*) FROM ufcstats_pages').fetchone()[0] == 0
    assert driver.quit_count == 1


def test_refresh_failure_cannot_fall_back_to_old_cache(tmp_path):
    driver = Driver(world)
    with FirefoxClient(tmp_path/'refresh.sqlite', driver_factory=lambda: driver, pace=0) as client:
        client.fetch(FIGHT, 'fight')
    def broken(url):
        raise WebDriverException('refresh failed')
    with FirefoxClient(tmp_path/'refresh.sqlite', driver_factory=lambda: Driver(broken),
                       pace=0, retries=0, refresh=True) as client:
        with pytest.raises(CollectionError, match='refresh failed'):
            client.fetch(FIGHT, 'fight')
    corrected = Driver(lambda _: fixture('fight').replace('Rear Naked Choke', 'Corrected detail'))
    with FirefoxClient(tmp_path/'refresh.sqlite', driver_factory=lambda: corrected,
                       pace=0, refresh=True) as client:
        assert 'Corrected detail' in client.fetch(FIGHT, 'fight')
        client.fetch(FIGHT, 'fight')
        assert len(corrected.calls) == 1


class IsolatedDb:
    def __init__(self, con):
        self._con = con

    def read(self, name):
        return pd.read_sql(f'SELECT * FROM "{name}"', self._con)


@pytest.mark.parametrize('missing', [False, True])
def test_collection_cleaner_and_missing_stat_metadata(tmp_path, missing):
    driver = Driver(lambda u: missing_html() if missing and u == FIGHT else world(u))
    with FirefoxClient(tmp_path/'full.sqlite', driver_factory=lambda: driver, pace=0) as client:
        full = FullUfcScraper([FIGHTER], [EVENT], [FIGHT], client=client)
        full.scrape_all()
        with sqlite3.connect(tmp_path/'output.sqlite') as con:
            full.write_all_to_tables(con)
            publish_tables({'ufc_upcoming_fights': pd.DataFrame(columns=EVENT_COLUMNS)}, con)
            clean = UfcDataCleaner(IsolatedDb(con)).parse_all()
            rows = clean.loc[clean['FightID'] == FIGHT]
            assert len(rows) == 2
            assert rows['method_description'].tolist() == ['Submission'] * 2
            assert rows['time_dur'].tolist() == [202, 202]
            assert con.execute('SELECT COUNT(*) FROM ufc_round_totals').fetchone()[0] == (0 if missing else 2)
            if missing:
                assert rows['SSL'].isna().all()
            else:
                assert rows['SSL'].tolist() == [30, 21]
                assert rows['SHL'].tolist() == [18, 13]
                assert rows['ctrl_seconds'].tolist() == [39, 13]
    assert driver.quit_count == 1


def test_failed_or_interrupted_collection_cannot_publish(tmp_path):
    def broken(url):
        if url == FIGHT:
            raise WebDriverException('down')
        return world(url)
    with FirefoxClient(tmp_path/'failed.sqlite', driver_factory=lambda: Driver(broken),
                       retries=0, pace=0) as client:
        full = FullUfcScraper([FIGHTER], [EVENT], [FIGHT], client=client)
        with pytest.raises(CollectionError):
            full.scrape_all()
        with sqlite3.connect(':memory:') as con:
            con.execute('CREATE TABLE ufc_events (sentinel TEXT)')
            con.execute("INSERT INTO ufc_events VALUES ('preserved')")
            con.commit()
            with pytest.raises(CollectionError, match='not finished'):
                full.write_all_to_tables(con)
            assert con.execute('SELECT * FROM ufc_events').fetchall() == [('preserved',)]


def test_atomic_publication_survives_stage_and_swap_failures():
    class SwapFailure(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            if sql.startswith('ALTER TABLE') and sql.endswith('"ufc_totals"'):
                raise RuntimeError('injected swap failure')
            return super().execute(sql, *args, **kwargs)
    with sqlite3.connect(':memory:', factory=SwapFailure) as con:
        for name in ['ufc_events', 'ufc_totals']:
            con.execute(f'CREATE TABLE {name} (sentinel TEXT)')
            con.execute(f"INSERT INTO {name} VALUES ('preserved')")
        con.commit()
        frame = pd.DataFrame({'value': [1]})
        with pytest.raises(RuntimeError, match='swap failure'):
            publish_tables({'ufc_events': frame, 'ufc_totals': frame}, con)
        for name in ['ufc_events', 'ufc_totals']:
            assert con.execute('SELECT * FROM ' + name).fetchall() == [('preserved',)]
        with pytest.raises(sqlite3.OperationalError):
            publish_tables({'ufc_events': frame, 'ufc_totals': pd.DataFrame([[1,2]], columns=['x','x'])}, con)
        assert con.execute('SELECT * FROM ufc_events').fetchall() == [('preserved',)]
        assert not con.execute("SELECT name FROM sqlite_master WHERE name LIKE '_ufcstats_%'").fetchall()


def test_imports_do_not_start_firefox_or_open_database():
    code = '''
import builtins
original = builtins.__import__
def checked(name, *args, **kwargs):
    if name == 'db' or name.startswith('selenium'):
        raise AssertionError('Eager import: ' + name)
    return original(name, *args, **kwargs)
builtins.__import__ = checked
import scrape.base_scrape
import scrape.scrape_ufcstats
import scrape.scrape_espn
import wrangle.clean_ufc_stats_data
'''
    subprocess.run([sys.executable, '-c', code], check=True)
