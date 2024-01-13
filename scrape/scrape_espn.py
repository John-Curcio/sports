# aws-sam-cli-managed-default-samclisourcebucket-135j2mihh9lxf
import re
from bs4 import BeautifulSoup

from tqdm import tqdm
import pandas as pd
import numpy as np
import os

import requests
from io import StringIO 
from lxml import html

import boto3
from scrape.base_scrape import BasePageScraper
from db import base_db_interface

from scrape.base_scrape import driver, headers

from concurrent.futures import ProcessPoolExecutor



class Fighter(object):
    
    def __init__(self, url):
        self.base_url = url 
        self.fighter_id = url.split("_/id/")[1]
        # initializing empty variables
        self.name = None
        self.birthdate = None 
        self.height = None 
        self.weight = None 
        self.weight_class = None 
        self.birthplace = None
        self.association = None
        self.stats_soup = None
        self.fight_history_soup = None
        
    def get_soup(self, soup_type):
        assert soup_type in ["stats", "history", "bio"], soup_type
        soup_url = "https://www.espn.com/mma/fighter/{}/_/id/{}".format(soup_type, self.fighter_id)

        # driver.get(soup_url)
        # html = driver.page_source
        # return BeautifulSoup(html, "lxml")
        
        r = requests.get(soup_url, headers=headers)
        html = r.text
        return BeautifulSoup(html, "lxml")

    @staticmethod
    def _get_opponent_href_if_exists(row_tag):
        # fighters with pages like https://www.espn.com/mma/fighter/history/_/id/4275487/yan-xiaonan
        # pose a challenge. note the "TBA" at the bottom
        a = row_tag.find("a", href=True)
        if a is None:
            return None
        return a["href"]

    def scrape_stats(self):
        # get match statistics, like takedown accuracy or advance to mount or whatever
        stats_soup = self.get_soup("stats")
        tables = stats_soup.find_all("table", {"class": "Table"})
        if len(tables) == 0:
            # no available information
            self.stats_df = pd.DataFrame()
            return self.stats_df
        assert len(tables) == 3, len(tables)
        stat_df_list = []
        for table_tag in tables:
            curr_df = self._scrape_table(table_tag)
            # TODO TODO TODO get opponent IDs here, join with stats_df
            row_tags = table_tag.find_all("tr", {"class": "Table__TR Table__TR--sm Table__even"})
            # row_tags = table_tag.find_all("a", {"class": "AnchorLink tl"})
            opponent_ids = [self._get_opponent_href_if_exists(row_tag) for row_tag in row_tags]
            # opponent_ids = [row_tag.find("a", href=True)["href"] for row_tag in row_tags]
            curr_df["OpponentID"] = opponent_ids
            stat_df_list.append(curr_df)
        merge_on = ['Date', 'Opponent', 'Event', 'Res.', 'OpponentID']
        x, y, z = stat_df_list
        self.stats_df = x.merge(y, on=merge_on).merge(z, on=merge_on)
        return self.stats_df
    
    def _scrape_table(self, table_tag):
        columns = table_tag.find_all("th", {"class": "Table__TH"})
        columns = [col.text for col in columns]
        row_tags = table_tag.find_all("tr", {"class": "Table__TR Table__TR--sm Table__even"})
        # rows = [row_tag.getText(separator="\n").split("\n") for row_tag in row_tags]
        rows = [[td.text for td in row_tag.find_all("td")] for row_tag in row_tags]
        return pd.DataFrame(rows, columns=columns)

    def scrape_matches(self):
        # get record of all matches that this fighter has been in
        match_soup = self.get_soup("history")
        self.match_df = self._scrape_table(match_soup)
        # TODO get opponent IDs here, join with match_df
        # row_tags = match_soup.find_all("a", {"class": "AnchorLink tl"})
        row_tags = match_soup.find_all("tr", {"class": "Table__TR Table__TR--sm Table__even"})
        # opponent_ids = [row_tag.find("a", href=True)["href"] for row_tag in row_tags]
        opponent_ids = [self._get_opponent_href_if_exists(row_tag) for row_tag in row_tags]
        self.match_df["OpponentID"] = opponent_ids
        # for link in table_body.find_all("a", href=True):
        #     fighter_urls.append("https://espn.com" + link["href"])
        return self.match_df
    
    def scrape_bio(self):
        # get birthdate, height, weight, reach, etc
        bio_soup = self.get_soup("bio") # Bio__Item n8 mb4
        tags = bio_soup.find_all("div", {"class": "Bio__Item n8 mb4"})
        bio_dict = dict()
        for bio_tag in tags:
            col, val = bio_tag.getText(separator="\n").split("\n")
            bio_dict[col] = [val]
        # can't forget to grab the fighter's name!
        name_tag = bio_soup.find("h1", {"class": re.compile("^(PlayerHeader__Name)")})
        self.name = name_tag.getText(separator=u" ")
        bio_dict["Name"] = [self.name]
        self.bio_df = pd.DataFrame(bio_dict)
        return self.bio_df


def _scrape_fighter(fighter):
    # helper fn in order to use ProcessPoolExecutor
    # if verbose:
    # print(fighter.base_url)
    # update bio_df, stats_df, and matches_df
    bio_data, stats_data, matches_data = None, None, None
    try:
        bio_data = fighter.scrape_bio()
        bio_data["FighterID"] = fighter.fighter_id
        stats_data = fighter.scrape_stats()
        stats_data["FighterID"] = fighter.fighter_id
        matches_data = fighter.scrape_matches()
        matches_data["FighterID"] = fighter.fighter_id
    except:
        pass
    return bio_data, stats_data, matches_data

class FighterSearchScraper(object):

    def __init__(self, n_fighters=np.inf, n_pages=np.inf, start_letter="a", end_letter="z",
                max_missed_fighter_retries=10):
        self.n_fighters = n_fighters
        self.n_pages = n_pages
        self.fighters = None
        self.stats_df = None
        self.bio_df = None
        self.matches_df = None
        self.missed_fighters_df = None
        self.max_missed_fighter_retries = max_missed_fighter_retries
        self.start_letter = start_letter.lower()
        self.end_letter = end_letter.lower()
        
    def get_fighter_urls(self, search_url):
        r = requests.get(search_url, headers=headers)
        html = r.text
        # driver.get(search_url)
        # html = driver.page_source
        curr_soup = BeautifulSoup(html, "lxml")
        # table_body = curr_soup.find("tbody") # no idea why this works with selenium and not requests!!
        table_body = curr_soup.find("table", {"class": "tablehead"})
        fighter_urls = []
        for link in table_body.find_all("a", href=True):
            fighter_urls.append("https://espn.com" + link["href"])
        return fighter_urls
    
    def scrape_fighters(self):
        curr_n_pages = 0
        fighter_urls = []
        fighters = []
        base_url = "http://www.espn.com/mma/fighters?search="
        for letter in [chr(x) for x in range(ord(self.start_letter), ord(self.end_letter)+1)]:
            curr_url = base_url + letter
            fighter_urls.extend(self.get_fighter_urls(curr_url))
            curr_n_pages += 1
            if curr_n_pages >= self.n_pages or len(fighter_urls) >= self.n_fighters:
                break
        fighters = []
        for i in range(min(self.n_fighters, len(fighter_urls))):
            fighter_url = fighter_urls[i]
            fighters.append(Fighter(fighter_url))
        return fighters
    
    def run_scraper(self, verbose=True, bio=True, stats=True, matches=True):
        fighters = self.scrape_fighters()
        self.fighters = fighters
        stats_list = []
        bio_list = []
        matches_list = []

        with ProcessPoolExecutor() as executor:
            results = list(tqdm(executor.map(_scrape_fighter, fighters), total=len(fighters)))

        missed_fighters = [fighter for result, fighter in zip(results, fighters) if all(item is None for item in result)]
        print("failed to scrape the following fighters:")
        for fighter in missed_fighters:
            print(fighter.base_url)

        for result in results:
            bio_data, stats_data, matches_data = result
            if bio_data is not None:
                bio_list.append(bio_data)
            if stats_data is not None:
                stats_list.append(stats_data)
            if matches_data is not None:
                matches_list.append(matches_data)

        if stats:
            self.stats_df = pd.concat(stats_list)
        if bio:
            self.bio_df = pd.concat(bio_list)
        if matches:
            self.matches_df = pd.concat(matches_list)

        self.missed_fighters_df = pd.DataFrame({
            "fighter_url": list(set([f.base_url for f in missed_fighters]))
        })
        print("left with ", len(self.missed_fighters_df), " missed fighters")

        
    # def run_scraper(self, verbose=True, bio=True, stats=True, matches=True):
    #     fighters = self.scrape_fighters()
    #     self.fighters = fighters
    #     stats_list = []
    #     bio_list = []
    #     matches_list = []
    #     if not verbose:
    #         fighters = tqdm(fighters)
    #     def _scrape_fighter_success(fighter, print_missing_fighter=False):
    #         if verbose:
    #             print(fighter.base_url)
    #         # update bio_df, stats_df, and matches_df
    #         try:
    #             if bio:
    #                 curr_bio_df = fighter.scrape_bio()
    #                 if print_missing_fighter:
    #                     print("scraped bio!")
    #                 curr_bio_df["FighterID"] = fighter.fighter_id
    #                 bio_list.append(curr_bio_df)
    #             if stats:
    #                 curr_stats_df = fighter.scrape_stats()
    #                 if print_missing_fighter:
    #                     print("scraped stats!")
    #                     print(curr_stats_df)
    #                 curr_stats_df["FighterID"] = fighter.fighter_id
    #                 stats_list.append(curr_stats_df)
    #             if matches:
    #                 curr_matches_df = fighter.scrape_matches()
    #                 if print_missing_fighter:
    #                     print("scraped matches!")
    #                 curr_matches_df["FighterID"] = fighter.fighter_id
    #                 matches_list.append(curr_matches_df)
    #             return True
    #         except:
    #             return False
        
    #     missed_fighters = []
    #     for fighter in fighters:
    #         if not _scrape_fighter_success(fighter):
    #             missed_fighters.append(fighter)
    #     print("failed to scrape the following fighters:")
    #     for fighter in missed_fighters:
    #         print(fighter.base_url)
    #     # self._missed_fighters = foo
    #     # for _ in range(self.max_missed_fighter_retries):
    #     #     if len(foo) == 0:
    #     #         break
    #     #     for fighter in foo:
    #     #         if _scrape_fighter_success(fighter, print_missing_fighter=True):
    #     #             foo.remove(fighter)
    #     # the _scrape_fighter function appends fighters that couldn't be 
    #     # scraped to self._missed_fighters. Here, we try to scrape those
    #     # fighters again. And if they don't want to be scraped, well, we
    #     # scrape em again and again and again until they give in (or we give up)
    #     # for _ in range(self.max_missed_fighter_retries):
    #     #     if len(self._missed_fighters) == 0:
    #     #         break
    #     #     print("scraping ", len(self._missed_fighters), "missed fighters")
    #     #     _missed_fighters_copy = self._missed_fighters
    #     #     self._missed_fighters = []
    #     #     for fighter in _missed_fighters_copy:
    #     #         _scrape_fighter(fighter, print_missing_fighter=True)
    #     if stats:
    #         self.stats_df = pd.concat(stats_list)
    #     if bio:
    #         self.bio_df = pd.concat(bio_list) 
    #     if matches:
    #         self.matches_df = pd.concat(matches_list)
    #     self.missed_fighters_df = pd.DataFrame({
    #         "fighter_url": list(set([f.base_url for f in missed_fighters]))
    #     })
    #     print("left with ", len(self.missed_fighters_df), " missed fighters")

    def write_all_to_tables(self):
        for table_name, df in [
            ("espn_bio", self.bio_df),
            ("espn_stats", self.stats_df),
            ("espn_matches", self.matches_df),
            ("espn_missed_fighters", self.missed_fighters_df)
        ]:
            if df is not None:
                # print(df.columns)
                # print(df.head())
                # foo = base_db_interface.read(table_name=table_name)
                # print(foo.columns)
                # print(foo.head())
                print(f"writing {len(df)} rows to {table_name}")
                # base_db_interface.write_update(
                base_db_interface.write_replace(
                    table_name=table_name, 
                    df=df
                )
        return None   

class UpcomingEspnScraper(BasePageScraper):

    def __init__(self):
        pass 

    def get_page_urls(self):
        pass 

    def scrape_upcoming_event_urls(self):
        pass

    def scrape_upcoming_events(self):
        pass

    def scrape_all(self):
        # get all league URLs
        pass

    def write_all_to_tables(self):
        pass

def main():
    TEST_HISTORICAL = True
    TEST_UPCOMING = True
    if TEST_HISTORICAL:
        start_letter, end_letter = "a", "z"
        # start_letter, end_letter = "s", "z"
        foo = FighterSearchScraper(start_letter=start_letter, end_letter=end_letter)
        foo.run_scraper(bio=True, matches=True, stats=True)
        foo.write_all_to_tables()
        print("done with fighters in letter range {}-{}".format(start_letter, end_letter))
    if TEST_UPCOMING:
        pass 


    # url = "https://www.espn.com/mma/fighter/stats/_/id/2560713/derrick-lewis"
    # fighter = Fighter(url)
    # stats_df = fighter.scrape_stats()
    
