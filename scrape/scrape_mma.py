from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import re
from bs4 import BeautifulSoup

from tqdm import tqdm
import pandas as pd
import numpy as np
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait # available since 2.4.0
from selenium.webdriver.support import expected_conditions as EC # available since 2.26.0
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import threading, queue

class BaseScraper(object):
    
#     options = Options()
#     options.headless = True
#     driver = webdriver.Firefox(options=options)
    driver = webdriver.Firefox()


class Fighter(BaseScraper):
    
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
        self.driver.get(soup_url)
        html = self.driver.page_source
#         self.driver.close()
        return BeautifulSoup(html, features="lxml")

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
            stat_df_list.append(curr_df)
        merge_on = ['Date', 'Opponent', 'Event', 'Res.']
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

    def to_dict(self):
        return {
            "url": self.base_url, 
            "name": self.name, 
            "birthdate": self.birthdate,
            "height": self.height,
            "weight": self.weight,
            "weight_class": self.weight_class,
            "birthplace": self.birthplace,
            "association": self.association,
        } 

    ### useful for checking whether we've already scraped data for this fighter
    def __key__(self):
        return self.base_url

    def __hash__(self):
        return hash(self.__key__())

    def __eq__(self, other):
        if isinstance(other, Fighter):
            return self.__key__() == other.__key__()
        return NotImplemented    
    
    def __repr__(self):
        return self.base_url


class FighterSearchScraper(BaseScraper):

    def __init__(self, n_fighters=np.inf, n_pages=np.inf):
        self.n_fighters = n_fighters
        self.n_pages = n_pages
        self.fighters = None
        self.stats_df = None
        self.bio_df = None
        self.matches_df = None
        self.urls = None
        
    def get_fighter_urls(self, search_url):
        self.driver.get(search_url)
        html = self.driver.page_source
        curr_soup = BeautifulSoup(html, features="lxml")
        table_body = curr_soup.find("tbody")
        fighter_urls = []
        for link in table_body.find_all("a", href=True):
            fighter_urls.append("https://espn.com" + link["href"])
        return fighter_urls

    def get_urls(self):
        if self.urls is not None:
            return self.urls
        fighter_urls = []
        base_url = "http://www.espn.com/mma/fighters?search="
        for letter in [chr(x) for x in range(ord("a"), ord("z")+1)]:
            curr_url = base_url + letter
            fighter_urls.extend(self.get_fighter_urls(curr_url))
            curr_n_pages += 1
            if curr_n_pages >= self.n_pages or len(fighter_urls) >= self.n_fighters:
                break
        return fighter_urls

    
    def scrape_fighters(self):
        curr_n_pages = 0
        fighter_urls = self.get_urls()
        fighters = []
        for i in range(min(self.n_fighters, len(fighter_urls))):
            fighter_url = fighter_urls[i]
            fighters.append(Fighter(fighter_url))
        return fighters
    
    def run_scraper(self, verbose=True):
        fighters = self.scrape_fighters()
        self.fighters = fighters
        stats_list = []
        bio_list = []
        matches_list = []
        if not verbose:
            fighters = tqdm(fighters)
        for fighter in fighters:
            if verbose:
                print(fighter.base_url)
            # update bio_df, stats_df, and matches_df
            curr_bio_df = fighter.scrape_bio()
            curr_bio_df["FighterID"] = fighter.fighter_id
            bio_list.append(curr_bio_df)
            
            curr_stats_df = fighter.scrape_stats()
            curr_stats_df["FighterID"] = fighter.fighter_id
            stats_list.append(curr_stats_df)
            
            curr_matches_df = fighter.scrape_matches()
            curr_matches_df["FighterID"] = fighter.fighter_id
            matches_list.append(curr_matches_df)
            
        self.stats_df = pd.concat(stats_list)
        self.bio_df = pd.concat(bio_list) 
        self.matches_df = pd.concat(matches_list)


if __name__ == "__main__":            
    foo = FighterSearchScraper(n_fighters=25)
    foo.run_scraper(True)
    print(foo.bio_df.head())
    print(foo.stats_df.head())
    print(foo.matches_df.head())