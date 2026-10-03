"""
FullUfcScraper.scrape_all() and FullUfcScraper.write_all_to_tables() are 
probably the most useful bits of code in this module
"""

from scrape.base_scrape import BasePageScraper
import argparse
from contextlib import closing, contextmanager
from io import StringIO
from pathlib import Path
import sqlite3
import uuid

from scrape.ufcstats_browser import (CollectionError, FirefoxClient, MISSING_STATS,
                                     canonical_url, page_ready)
import pandas as pd
import string
from tqdm import tqdm
import re


class UfcPageScraper(BasePageScraper):
    page_kind = None

    def __init__(self, url, max_tries=3, sleep_time=1, client=None):
        super().__init__(canonical_url(url), max_tries, sleep_time)
        self.client = client

    def get_html(self):
        if self.client is None:
            with FirefoxClient(retries=self.max_tries - 1, pace=self.sleep_time) as client:
                self.raw_html = client.fetch(self.url, self.page_kind)
        else:
            self.raw_html = self.client.fetch(self.url, self.page_kind)
        return self.raw_html

    def get_request(self):
        return self.get_html()

    def get_soup(self):
        soup = super().get_soup()
        if not page_ready(self.raw_html, self.page_kind):
            raise CollectionError(f'{self.url}: incomplete or blocked {self.page_kind} page')
        return soup

    def get_page_data(self):
        try:
            return self._parse_page_data()
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
            raise CollectionError(f'{self.url}: parsing failed: {exc}') from exc


class UfcEventScraper(UfcPageScraper):
    """
    basically just use this to get weight classes
    http://ufcstats.com/event-details/253d3f9e97ca149a
    """
    page_kind = 'event'

    def _fight_rows(self, soup):
        return soup.select('tbody tr.b-fight-details__table-row[data-link]')

    def _selected_rows(self, rows):
        selected = []
        for rank, row in enumerate(rows):
            result = row.find('td').get_text(' ', strip=True)
            pending = any(text in row.get_text(' ', strip=True).lower()
                          for text in ('view matchup', 'matchup preview'))
            if not result and not pending:
                raise CollectionError(f'{self.url}: bout has neither a result nor a matchup preview')
            completed = bool(result)
            if completed == (self.page_kind == 'event'):
                selected.append((rank, row))
        return selected

    def get_fights(self):
        # Get urls for completed fights (not upcoming fights)
        rows = self._selected_rows(self._fight_rows(self.get_soup()))
        return [canonical_url(row['data-link']) for _, row in rows]
    
    def get_page_urls(self) -> set:
        return set(self.get_fights())
    
    def get_fighter_urls(self):
        fighter_links = []
        for _, row in self._selected_rows(self._fight_rows(self.get_soup())):
            fighter_links.extend(canonical_url(link['href'])
                                 for link in row.select('a[href*="/fighter-details/"]'))
        return fighter_links
    
    def _get_date_location(self):
        soup = self.get_soup()
        date, loc = soup.find_all("li", {"class": "b-list__box-list-item"})
        date = ' '.join(date.text.split())
        loc = ' '.join(loc.text.split())
        date = date[len("Date: "):]
        loc = loc[len("Location: "):]
        return date, loc
    
    def _get_img_pngs(self):
        fights = [row for _, row in self._selected_rows(self._fight_rows(self.get_soup()))]
        img_tags = [fight.find("img") for fight in fights]
        img_pngs = [img.get("src") if img else None for img in img_tags]
        return pd.Series(img_pngs).fillna("")

    def _parse_page_data(self) -> pd.DataFrame:
        soup = self.get_soup()
        self.data = pd.read_html(StringIO(str(soup)))[0]
        if self.data.empty:
            if self.page_kind == 'upcoming_event':
                return pd.DataFrame(columns=EVENT_COLUMNS)
            raise CollectionError(f'{self.url}: completed event has no fights')
        rows = self._fight_rows(soup)
        if len(rows) != len(self.data):
            raise CollectionError(f'{self.url}: event rows and metadata are misaligned')
        selected = self._selected_rows(rows)
        self.data = self.data.iloc[[rank for rank, _ in selected]].copy()
        self.data['fight_rank_on_card'] = self.data.index
        self.data.reset_index(drop=True, inplace=True)
        if self.data.empty:
            return pd.DataFrame(columns=EVENT_COLUMNS)
        if self.page_kind == 'event' and self.data[['Method', 'Round', 'Time']].isna().any().any():
            raise CollectionError(f'{self.url}: completed bout has missing result fields')
        date, loc = self._get_date_location()
        self.data["Date"] = date
        self.data["Location"] = loc
        names = self.data["Fighter"].str.split("  ")
        fighters, opponents = names.str[0], names.str[-1]
        self.data["FighterName"] = fighters.str.strip()
        self.data["OpponentName"] = opponents.str.strip()
        fighter_links = self.get_fighter_urls()
        self.data["FighterUrl"] = fighter_links[::2]
        self.data["OpponentUrl"] = fighter_links[1::2]
        
        # img pngs (performance of night, fight of night, sub of night, ko of the night)
        self.data["img_png_url"] = self._get_img_pngs()
        self.data["is_title_fight"] = self.data["img_png_url"].str.endswith("belt.png")
        
        self.data["FightID"] = self.get_fights()
        return self.data


class MissingStatsException(CollectionError):
    pass


class UfcFightDetails(UfcPageScraper):
    """
    Given the url to a fight-details page, grabs the following:
    * fight_description
    * totals
    * strikes
    * round_totals
    * round_strikes
    """
    
    page_kind = 'fight'

    def get_page_urls(self) -> set:
        soup = self.get_soup()
        class_str = "b-link b-link_style_black"
        possible_event = soup.find_all("a", {"class": class_str})
        links = [link.get('href') for link in possible_event]
        return links
    
    def get_fighter_urls(self):
        soup = self.get_soup()
        tags = soup.find_all("a", {"class": "b-link b-fight-details__person-link"})
        return [canonical_url(tag['href']) for tag in tags]
    
    @staticmethod
    def parse_sum_table(table, fighter_ids):
        # for parsing stats about the total number of TDs, SDLLs, etc
        # rather than round-level stats
        result = pd.DataFrame({
            col: table[col].str.split("_")[0][:-1]
            for col in table.columns
        })
        result["FighterID"] = fighter_ids
        return result
    
    @staticmethod
    def _parse_round_stats_table(table, fighter_ids):
        # helper function for parsing round-by-round stats
        round_totals_a = pd.DataFrame({
            col: table[col].str.split("_").str[0]
            for col in table.columns
        })
        round_totals_a["FighterID"] = fighter_ids[0]
        round_totals_a["Round"] = range(len(round_totals_a))
        round_totals_b = pd.DataFrame({
            col: table[col].str.split("_").str[1]
            for col in table.columns
        })
        round_totals_b["FighterID"] = fighter_ids[1]
        round_totals_b["Round"] = range(len(round_totals_b))
        return pd.concat([round_totals_a, round_totals_b]).reset_index(drop=True)
    
    @staticmethod
    def parse_round_totals_table(table, fighter_ids):
        # for parsing round-by-round stats (other than significant strikes)
        # eg TDs, sub attempts, control time
        table = table.copy()
        table.columns = [
            'Fighter', 'KD', 'Sig. str.', 'Sig. str. %', 'Total str.', 'Td',
            'Td %', 'Sub. att', 'Rev.', 'Ctrl'
        ]
        return UfcFightDetails._parse_round_stats_table(table, fighter_ids)
    
    @staticmethod
    def parse_round_strikes_table(table, fighter_ids):
        # for parsing round-by-round significant strikes
        table = table.copy()
        table.columns = [
            "Fighter", "Sig. str", "Sig. str. %", "Head", "Body", 
            "Leg", "Distance", "Clinch", "Ground", "dropme",
        ]
        table = table.drop(columns=["dropme"])
        return UfcFightDetails._parse_round_stats_table(table, fighter_ids)

    def get_fight_description(self):
        soup = self.get_soup()
        fight = soup.select_one('.b-fight-details__fight')
        normalize = lambda node: ' '.join(node.get_text(' ', strip=True).split())
        description = {'Weight': normalize(fight.select_one('.b-fight-details__fight-title'))}
        fields = {'Method:': 'Method', 'Round:': 'Round', 'Time:': 'Time',
                  'Time format:': 'Time Format', 'Referee:': 'Referee', 'Details:': 'Details'}
        for label in fight.select('.b-fight-details__label'):
            name = normalize(label)
            if name in fields:
                # Details extends past its label's enclosing <i> into the paragraph.
                container = label.find_parent('p') if name == 'Details:' else label.parent
                description[fields[name]] = normalize(container).removeprefix(name).strip()
        if set(description) != {'Weight', *fields.values()}:
            raise CollectionError(f'{self.url}: missing labeled fight description fields')
        if any(not description[field] for field in ('Weight', 'Method', 'Round', 'Time', 'Time Format')):
            raise CollectionError(f'{self.url}: empty required fight description fields')
        return pd.Series(description)

    def _parse_page_data(self):
        self.fight_description = self.get_fight_description()
        soup = self.get_soup()
        if MISSING_STATS in soup.text:
            self.totals = None 
            self.strikes = None 
            self.round_strikes = None 
            self.round_totals = None
            return None
        self.data = pd.read_html(StringIO(str(soup).replace('</p>','_</p>')))
        if len(self.data) != 4:
            raise MissingStatsException(f'{self.url}: found {len(self.data)} tables instead of 4')
        totals, round_totals, strikes, round_strikes = self.data
        fighter_ids = self.get_fighter_urls()
        self.totals = self.parse_sum_table(totals, fighter_ids)
        self.strikes = self.parse_sum_table(strikes, fighter_ids)
        self.round_strikes = self.parse_round_strikes_table(round_strikes, fighter_ids)
        self.round_totals = self.parse_round_totals_table(round_totals, fighter_ids)
        return self.totals
        #return self.totals.merge(self.strikes, on=["FighterID"], suffixes=("", "_y"))


class UfcFighterScraper(UfcPageScraper):
    page_kind = 'fighter'
    
    def get_page_urls(self):
        soup = self.get_soup()
        # get all the events this guy fought in
        class_str = "b-link b-link_style_black"
        possible_event = soup.find_all("a", {"class": class_str})
        links = [link.get('href') for link in possible_event]
        return [canonical_url(link) for link in links if link and
                ('/event-details/' in link or '/fight-details/' in link)]
    
    def _parse_page_data(self):
        soup = self.get_soup()
        tag = soup.find("ul", {"class": "b-list__box-list"})
        desc = tag.text.strip().replace("  ", "_").replace("\n", "")
        d = re.sub("_+", "_", desc).split("_")
        result = dict()
        prefixes = ["Height:", "Weight:", "Reach:", "STANCE:", "DOB:"]
        for i, s in enumerate(d[:-1]):
            if s in prefixes and d[i+1] not in prefixes:
                    result[s] = d[i+1]
        result["FighterID"] = self.url
        return pd.Series(result)

    def get_fights(self):
        soup = self.get_soup()
        class_str = "b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click"
        tags = soup.find_all("tr", {"class": class_str})
        return [canonical_url(tag['data-link']) for tag in tags]
    
    def get_events(self):
        urls = pd.Series(self.get_page_urls(), dtype="object")
        return urls.loc[urls.str.startswith("http://ufcstats.com/event-details/")].values
    

class CharFightersScraper(UfcPageScraper):
    """
    This class only gets used in UfcUrlScraper.get_all_fighter_urls, so 
    it's not really useful on its own.

    ufcstats.com has this thing where you can get all the fighters on 
    the website whose last names start with a given letter. So for example,
    http://ufcstats.com/statistics/fighters?char=a&page=all gets you all
    the fighters whose last names start with "a". This class is useful for 
    scraping all the links to the individual fighter pages that are 
    included in this "char=a" page.
    """

    page_kind = 'directory'

    def get_page_urls(self):
        # get set of urls mapping to other pages to scrape
        soup = self.get_soup()
        # TODO I should confirm that urls contain fighter-details
        return {canonical_url(tag['href']) for tag in soup.select('tbody a[href]')
                if '/fighter-details/' in tag['href']}
        
    def get_page_data(self):
        return None


# Empty outputs keep the same schema so legitimate missing stats/empty cards
# can still pass through the downstream cleaner.
TOTAL_COLUMNS = ['Fighter', 'KD', 'Sig. str.', 'Sig. str. %', 'Total str.', 'Td',
                 'Td %', 'Sub. att', 'Rev.', 'Ctrl', 'FighterID', 'FightID']
STRIKE_COLUMNS = ['Fighter', 'Sig. str', 'Sig. str. %', 'Head', 'Body', 'Leg',
                  'Distance', 'Clinch', 'Ground', 'FighterID', 'FightID']
DESCRIPTION_COLUMNS = ['Weight', 'Method', 'Round', 'Time', 'Time Format', 'Referee',
                       'Details', 'FightID']
EVENT_COLUMNS = ['W/L', 'Fighter', 'Kd', 'Str', 'Td', 'Sub', 'Weight class',
                 'Method', 'Round', 'Time', 'Date', 'Location', 'FighterName',
                 'OpponentName', 'FighterUrl', 'OpponentUrl', 'img_png_url',
                 'is_title_fight', 'FightID', 'fight_rank_on_card', 'EventUrl']
FIGHTER_COLUMNS = ['Height:', 'Weight:', 'Reach:', 'STANCE:', 'DOB:', 'FighterID']


def concatenate(frames, columns):
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)


def prefetch_pages(client, urls, kind):
    if hasattr(client, 'prefetch'):
        client.prefetch(urls, kind)


@contextmanager
def collection_session(scraper):
    # Public entry points own a browser only when the caller has not supplied one.
    if scraper.client is not None:
        yield scraper.client
    else:
        with FirefoxClient() as client:
            scraper.client = client
            try:
                yield client
            finally:
                scraper.client = None


def publish_tables(tables, db=None):
    """Stage frames before replacing all selected tables in one transaction.

    pandas.to_sql commits on sqlite connections, so write staging tables first.
    A failed stage or swap cannot leave a partially replaced collection.
    """
    if db is None:
        from db import base_db_interface
        db = base_db_interface
    con = db if isinstance(db, sqlite3.Connection) else db._con
    if con.in_transaction:
        raise CollectionError('Commit the caller transaction before publishing UFCstats')
    staged = []
    try:
        for name, frame in tables.items():
            staging = '_ufcstats_' + uuid.uuid4().hex
            staged.append((name, staging))
            frame.to_sql(staging, con, index=False, if_exists='fail')
        con.execute('BEGIN IMMEDIATE')
        with con:
            for name, staging in staged:
                con.execute(f'DROP TABLE IF EXISTS "{name}"')
                con.execute(f'ALTER TABLE "{staging}" RENAME TO "{name}"')
    finally:
        with con:
            for _, staging in staged:
                con.execute(f'DROP TABLE IF EXISTS "{staging}"')


class UfcUrlScraper:
    def __init__(self, client=None, letters=string.ascii_lowercase, max_fighters=None):
        self.client, self.letters, self.max_fighters = client, letters, max_fighters
        self.fighter_urls = self.event_urls = self.fight_urls = None

    def get_all_fighter_urls(self):
        self.fighter_urls = None
        fighter_urls = set()
        with collection_session(self) as client:
            for letter in tqdm(self.letters):
                url = f'http://ufcstats.com/statistics/fighters?char={letter}&page=all'
                fighter_urls |= CharFightersScraper(url, client=client).get_page_urls()
        if self.max_fighters is not None:
            fighter_urls = set(sorted(fighter_urls)[:self.max_fighters])
        self.fighter_urls = fighter_urls
        return self.fighter_urls

    def get_all_event_and_fight_urls(self):
        self.event_urls = self.fight_urls = None
        event_urls, fight_urls = set(), set()
        with collection_session(self) as client:
            if self.fighter_urls is None:
                self.get_all_fighter_urls()
            prefetch_pages(client, self.fighter_urls, 'fighter')
            for url in tqdm(sorted(self.fighter_urls)):
                fighter = UfcFighterScraper(url, client=client)
                event_urls.update(fighter.get_events())
                fight_urls.update(fighter.get_fights())
        self.event_urls, self.fight_urls = event_urls, fight_urls
        return self.event_urls, self.fight_urls


class FullUfcScraper:
    def __init__(self, fighter_urls, event_urls, fight_urls, client=None):
        self.client = client
        self.fighter_urls = sorted({canonical_url(u) for u in fighter_urls})
        self.event_urls = sorted({canonical_url(u) for u in event_urls})
        self.fight_urls = sorted({canonical_url(u) for u in fight_urls})
        self.totals_df = self.strikes_df = None
        self.round_totals_df = self.round_strikes_df = self.fight_description_df = None
        self.event_data = self.fighter_data = None
        self.completed = set()

    def scrape_fights(self):
        self.completed.discard('fights')
        totals, strikes, round_totals, round_strikes, descriptions = [], [], [], [], []
        with collection_session(self) as client:
            prefetch_pages(client, self.fight_urls, 'fight')
            for url in tqdm(self.fight_urls):
                fight = UfcFightDetails(url, client=client)
                fight.get_page_data()
                description = fight.fight_description.copy()
                description['FightID'] = url
                descriptions.append(description)
                for frames, frame in [(totals, fight.totals), (strikes, fight.strikes),
                                      (round_totals, fight.round_totals),
                                      (round_strikes, fight.round_strikes)]:
                    if frame is not None:
                        frames.append(frame.assign(FightID=url))
        self.totals_df = concatenate(totals, TOTAL_COLUMNS)
        self.strikes_df = concatenate(strikes, STRIKE_COLUMNS)
        self.round_totals_df = concatenate(round_totals, TOTAL_COLUMNS + ['Round'])
        self.round_strikes_df = concatenate(round_strikes, STRIKE_COLUMNS + ['Round'])
        self.fight_description_df = pd.DataFrame(descriptions, columns=DESCRIPTION_COLUMNS)
        self.completed.add('fights')
        return self.round_strikes_df

    def scrape_events(self):
        self.completed.discard('events')
        frames = []
        with collection_session(self) as client:
            prefetch_pages(client, self.event_urls, 'event')
            for url in tqdm(self.event_urls):
                frames.append(UfcEventScraper(url, client=client).get_page_data().assign(EventUrl=url))
        self.event_data = concatenate(frames, EVENT_COLUMNS)
        self.completed.add('events')
        return self.event_data

    def scrape_fighters(self):
        self.completed.discard('fighters')
        with collection_session(self) as client:
            prefetch_pages(client, self.fighter_urls, 'fighter')
            rows = [UfcFighterScraper(url, client=client).get_page_data()
                    for url in tqdm(self.fighter_urls)]
        self.fighter_data = pd.DataFrame(rows, columns=FIGHTER_COLUMNS)
        self.completed.add('fighters')
        return self.fighter_data

    def scrape_all(self):
        self.completed.clear()
        with collection_session(self):
            self.scrape_events()
            self.scrape_fighters()
            self.scrape_fights()

    def tables(self):
        if self.completed != {'events', 'fighters', 'fights'}:
            raise CollectionError('UFCstats collection not finished; existing tables preserved')
        if not self.fight_urls or not self.event_urls or not self.fighter_urls:
            raise CollectionError('Empty historical selection; existing tables preserved')
        return dict(ufc_fight_description=self.fight_description_df,
                    ufc_totals=self.totals_df, ufc_strikes=self.strikes_df,
                    ufc_round_totals=self.round_totals_df, ufc_round_strikes=self.round_strikes_df,
                    ufc_events=self.event_data, ufc_fighters=self.fighter_data)

    def write_all_to_tables(self, db=None):
        publish_tables(self.tables(), db)


class UpcomingUfcEventScraper(UfcEventScraper):
    page_kind = 'upcoming_event'


class UpcomingUfcScraper(UfcPageScraper):
    page_kind = 'upcoming'

    def __init__(self, client=None, max_events=None):
        super().__init__('http://ufcstats.com/statistics/events/upcoming', client=client)
        self.max_events = max_events
        self.upcoming_event_urls = self.upcoming_fights_df = None
        self.finished = False

    def get_page_urls(self):
        return list(dict.fromkeys(canonical_url(a['href']) for a in
                    self.get_soup().select('tbody a[href]') if '/event-details/' in a['href']))

    def scrape_upcoming_event_urls(self):
        self.finished = False
        # Mutable cards are deliberately refreshed, including repeated runs on this object.
        self.raw_html = None
        self.upcoming_event_urls = self.get_page_urls()
        if self.max_events is not None:
            self.upcoming_event_urls = self.upcoming_event_urls[:self.max_events]
        return self.upcoming_event_urls

    def scrape_upcoming_fights(self):
        self.finished = False
        if self.upcoming_event_urls is None:
            self.scrape_upcoming_event_urls()
        frames = []
        with collection_session(self) as client:
            for url in tqdm(self.upcoming_event_urls):
                frame = UpcomingUfcEventScraper(url, client=client).get_page_data()
                if not frame.empty:
                    frames.append(frame.assign(EventUrl=url))
        self.upcoming_fights_df = concatenate(frames, EVENT_COLUMNS)
        self.finished = True
        return self.upcoming_fights_df

    def scrape_all(self):
        self.finished = False
        with collection_session(self):
            self.scrape_upcoming_event_urls()
            self.scrape_upcoming_fights()

    def tables(self):
        if not self.finished:
            raise CollectionError('Upcoming UFCstats collection not finished; existing tables preserved')
        return {'ufc_upcoming_fights': self.upcoming_fights_df}

    def write_all_to_tables(self, db=None):
        publish_tables(self.tables(), db)


def main():
    # Preserve the legacy historical-pipeline entry point and publish only once
    # both historical and upcoming acquisition have succeeded.
    with FirefoxClient() as client:
        urls = UfcUrlScraper(client=client)
        urls.get_all_event_and_fight_urls()
        full = FullUfcScraper(urls.fighter_urls, urls.event_urls, urls.fight_urls, client=client)
        full.scrape_all()
        upcoming = UpcomingUfcScraper(client=client)
        upcoming.scrape_all()
        publish_tables({**full.tables(), **upcoming.tables()})


def cli():
    parser = argparse.ArgumentParser(description='Local Firefox UFCstats collection with durable resume')
    parser.add_argument('mode', choices=['historical', 'upcoming'])
    parser.add_argument('--checkpoint', default='.cache/ufcstats/checkpoint.sqlite')
    parser.add_argument('--db', help='Explicit output SQLite path; omitted means collect only')
    parser.add_argument('--event-url', action='append', help='Select historical events instead of full discovery')
    parser.add_argument('--letters', default=string.ascii_lowercase)
    parser.add_argument('--max-fighters', type=int)
    parser.add_argument('--max-events', type=int)
    parser.add_argument('--max-fights', type=int)
    parser.add_argument('--page-timeout', type=float, default=45)
    parser.add_argument('--wait-timeout', type=float, default=30)
    parser.add_argument('--retries', type=int, default=2)
    parser.add_argument('--pace', type=float, default=1)
    parser.add_argument('--workers', type=int, default=1, help='Independent Firefox workers for historical pages')
    parser.add_argument('--firefox-binary')
    parser.add_argument('--geckodriver')
    parser.add_argument('--refresh', action='store_true')
    args = parser.parse_args()
    if args.workers < 1:
        parser.error('--workers must be positive')
    if any(value is not None and value <= 0 for value in
           (args.max_fighters, args.max_events, args.max_fights)):
        parser.error('Sample limits must be positive')
    if not args.letters or any(c not in string.ascii_lowercase for c in args.letters):
        parser.error('--letters must contain lowercase a-z')
    if args.db and Path(args.db).resolve() == Path(args.checkpoint).resolve():
        parser.error('Output database and checkpoint must be separate files')
    try:
        client_type, worker_options = FirefoxClient, {}
        if args.workers > 1:
            from scrape.ufcstats_parallel import ParallelFirefoxClient
            client_type, worker_options = ParallelFirefoxClient, {'workers': args.workers}
        with client_type(args.checkpoint, page_timeout=args.page_timeout,
                           wait_timeout=args.wait_timeout, retries=args.retries, pace=args.pace,
                           firefox_binary=args.firefox_binary, geckodriver=args.geckodriver,
                           refresh=args.refresh, **worker_options) as client:
            if args.mode == 'upcoming':
                scraper = UpcomingUfcScraper(client=client, max_events=args.max_events)
            else:
                if args.event_url:
                    events = list(dict.fromkeys(canonical_url(u) for u in args.event_url))[:args.max_events]
                    fighters, fights = set(), set()
                    for url in events:
                        event = UfcEventScraper(url, client=client)
                        fighters.update(event.get_fighter_urls())
                        fights.update(event.get_fights())
                else:
                    urls = UfcUrlScraper(client=client, letters=args.letters,
                                         max_fighters=args.max_fighters)
                    urls.get_all_event_and_fight_urls()
                    fighters, events, fights = urls.fighter_urls, urls.event_urls, urls.fight_urls
                scraper = FullUfcScraper(sorted(fighters)[:args.max_fighters],
                                         sorted(events)[:args.max_events],
                                         sorted(fights)[:args.max_fights], client=client)
            scraper.scrape_all()
            tables = scraper.tables()
            print({name: len(frame) for name, frame in tables.items()})
            print(dict(navigations=client.requests, cache_hits=client.cache_hits,
                       browser_starts=client.browser_starts))
            if args.db:
                with closing(sqlite3.connect(args.db)) as con:
                    publish_tables(tables, con)
    except (CollectionError, ValueError) as exc:
        parser.exit(1, str(exc) + '\n')


if __name__ == '__main__':
    cli()
