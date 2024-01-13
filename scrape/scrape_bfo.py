# scrapes web pages on bestfightodds.com
# eg https://www.bestfightodds.com/fighters/Jon-Jones-819 

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
import time
from scrape.base_scrape import BaseBfs
from db import base_db_interface

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import FirefoxOptions

opts = FirefoxOptions()
opts.add_argument("--headless")
driver = webdriver.Firefox(options=opts)



dt_now = str(pd.to_datetime('today').date())

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

class EmptyResponse(Exception):
    pass

class BfoRequest(object):
    
    def __init__(self, url, max_tries=20, sleep_time=5):
        self.url = url
        self.max_tries = max_tries
        self.sleep_time = sleep_time
        # self.session = requests.Session()
        self.raw_html = None
        
    def get_request(self):
        driver.get(self.url)
        return driver.page_source

class BaseBfoPageScraper(BfoRequest):

    def __init__(self, url, **request_kwargs):
        super().__init__(url, **request_kwargs)
        self.raw_html = None
        self.data = None

    def get_html(self):
        self.raw_html = self.get_request()
        self.tables = pd.read_html(self.raw_html)
        for i, table in enumerate(self.tables):
            table["table_id"] = i
        self.data = pd.concat(self.tables)
        return self.data

class FighterScraper(BaseBfoPageScraper):
    
    def __init__(self, url, **request_kwargs):
        super().__init__(url, **request_kwargs)
        self.fighter_urls = None
        self.event_urls = None
        self.odds_df = None
    
    def get_fighter_urls(self):
        if self.data is None:
            self.get_html()
        # self.data["url"] = self.urlipython
        soup = BeautifulSoup(self.raw_html, features="lxml")
        tbody = soup.find("tbody")
        urls = [link.get("href") for link in tbody.find_all("a")]
        fighter_urls = [("https://www.bestfightodds.com"+u) 
                             for u in urls if u.startswith("/fighters/")]
        self.fighter_urls = set(fighter_urls) - {self.url}
        return self.fighter_urls

    def get_event_urls(self):
        if self.data is None:
            self.get_html()
        soup = BeautifulSoup(self.raw_html, "lxml")
        event_cells = soup.find_all("tr", {"class":"event-header item-mobile-only-row"})
        event_hrefs = [u.find("a").get("href") for u in event_cells]
        self.event_urls = {("https://www.bestfightodds.com"+u) for u in event_hrefs}
        return self.event_urls
    
    def get_odds(self):
        if self.data is None:
            self.get_html()
        odds_df_rows = []
        soup = BeautifulSoup(self.raw_html, "lxml")
        opp_cells = soup.find_all("th", {"class": "oppcell"})
        opp_cells = [u.find("a").get("href") for u in opp_cells]
        opp_cell_pairs = list(zip(opp_cells[::2], opp_cells[1::2]))
        event_cells = soup.find_all("tr", {"class":"event-header item-mobile-only-row"})
        event_hrefs = [u.find("a").get("href") for u in event_cells]
        self.data["match_id"] = self.data.index//3
        for i, grp in self.data.groupby("match_id"):
            fighter, opp = grp["Matchup"].iloc[1:3]
            fighter_open, opp_open = grp["Open"].iloc[1:3]
            fighter_close_left, opp_close_left = grp["Closing range"].iloc[1:3]
            fighter_close_right, opp_close_right = grp["Closing range.2"].iloc[1:3]
            event, date = grp["Event"].iloc[1:3]
            fighter_href, opp_href = opp_cell_pairs[i]
            odds_df_rows.append({
                "Event": event,
                "EventHref": event_hrefs[i],
                "Date": date,
                "FighterHref": fighter_href,
                "OpponentHref": opp_href,
                "FighterName": fighter,
                "OpponentName": opp,
                "FighterOpen": fighter_open,
                "OpponentOpen": opp_open,
                "FighterCloseLeft": fighter_close_left,
                "FighterCloseRight": fighter_close_right,
                "OpponentCloseLeft": opp_close_left,
                "OpponentCloseRight": opp_close_right,
            })

        self.odds_df = pd.DataFrame(odds_df_rows)
        return self.odds_df

class EventScraper(BaseBfoPageScraper):
    
    def __init__(self, url, **request_kwargs):
        super().__init__(url, **request_kwargs)
        self.fighter_urls = None
        self.fight_odds_df = None
        self.prop_odds_df = None
        self.prop_html = None
    
    def get_fighter_urls(self):
        if self.data is None:
            self.get_html()
        # self.data["url"] = self.url
        soup = BeautifulSoup(self.raw_html, "lxml")
        tbody = soup.find("tbody")
        urls = [link.get("href") for link in tbody.find_all("a")]
        fighter_urls = [("https://www.bestfightodds.com"+u) 
                             for u in urls if u.startswith("/fighters/")]
        self.fighter_urls = fighter_urls
        return self.fighter_urls
    
    def get_odds(self):
        if self.data is None:
            self.get_html()
        fight_df_rows = []
        prop_df_rows = []

        odds_df = self.tables[1]
        odds_df = odds_df.rename(columns={"Unnamed: 0": "FighterName"})
        # odds_df currently contains the odds for all fights on the card,
        # including the props. We need to split this into two dataframes:
        # one for the fights, and one for the props.
        # we do this by sliding window: if the next two rows have nulls in the
        # "Props.1" column, then we know these rows correspond to a fight, and 
        # the following rows correspond to props. The contents of the "Props.1"
        # column tell us how many props there are.
        i = 0
        while i < odds_df.shape[0]:
            nprops0 = odds_df["Props.1"].iloc[i]
            nprops1 = odds_df["Props.1"].iloc[i+1]
            if odds_df["Props.1"].iloc[i:(i+2)].isnull().all():
                # this is a fight without props
                fight_df_rows.append(odds_df.iloc[i:(i+2)])
                i += 2  
            elif odds_df["Props.1"].iloc[i:(i+2)].notnull().all():
                # this is a fight with props
                fight_df_rows.append(odds_df.iloc[i:(i+2)])
                prop_df_rows.append(
                    odds_df.iloc[(i+2):(i+2+2*int(nprops0))].rename(columns={
                        "FighterName": "PropName",
                    }).assign(
                        FighterName=odds_df["FighterName"].iloc[i],
                        OpponentName=odds_df["FighterName"].iloc[i+1],
                    )
                )
                i += 2 + 2*int(nprops0)
            elif odds_df["FighterName"].iloc[i] == "Event props":
                # The remaining rows are for the entire event
                prop_df_rows.append(
                    odds_df.iloc[(i+1):].rename(columns={
                        "FighterName": "PropName",
                    }).assign(
                        FighterName="Event",
                        OpponentName="Event",
                    )
                )
                i = odds_df.shape[0]
            else:
                raise ValueError("nprops0 != nprops1", i, nprops0, nprops1)

        fight_df = pd.concat(fight_df_rows).reset_index(drop=True)
        prop_df = pd.DataFrame()
        if len(prop_df_rows) > 0:
            prop_df = pd.concat(prop_df_rows).reset_index(drop=True)
            prop_df["EventHref"] = self.url[len("https://www.bestfightodds.com/events/"):]
        fight_df["match_id"] = fight_df.index//2
        fight_df["EventHref"] = self.url[len("https://www.bestfightodds.com/events/"):]
        fighter_urls = self.get_fighter_urls()
        fight_df["FighterHref"] = pd.Series(fighter_urls).str[len("https://www.bestfightodds.com/fighters/"):]
        self.fight_odds_df = fight_df
        self.prop_odds_df = prop_df
        return fight_df, prop_df
    
    def get_prop_html(self):
        """
        Get the html for the event, including the props
        This will be dirty, meant to be parsed elsewhere
        """
        # Load the webpage
        driver.get(self.url)
        # Find the buttons to reveal the props, and click them
        buttons = driver.find_elements_by_class_name("prop-cell prop-cell-exp")
        # click every other button - adjacent ones are redundant, 
        # and clicking the second one will only undo the first
        for button in buttons[::1]:
            button.click()

        # Wait for the page to load
        time.sleep(1)

        # Get the html
        html = driver.page_source
        self.prop_html = html
        return html
    
    def get_prop_table(self):
        """
        Get the table of props
        """
        if self.prop_html is None:
            self.get_prop_html()
        prop_table = pd.read_html(self.prop_html)[1]\
            .assign(url=self.url)\
            .rename(columns={"Unnamed: 0": "title"})
        prop_soup = BeautifulSoup(self.prop_html, "lxml")
        # add FighterHref if it's a fighter row
        fighter_hrefs = prop_soup.find("tbody").find_all("a", href=True)
        href_dict = {a.text: a.get("href") for a in fighter_hrefs}
        prop_table["FighterHref"] = prop_table["title"].map(href_dict)
        return prop_table


                                 
        
# class FighterBFS(BaseBfs):
    
#     def __init__(self, root_urls=None, max_depth=3, verbose=True):
#         if root_urls is None:
#             root_urls = [
#                 "https://www.bestfightodds.com/fighters/Aljamain-Sterling-4688",
#                 "https://www.bestfightodds.com/fighters/Deiveson-Figueiredo-7514",
#                 "https://www.bestfightodds.com/fighters/Alexander-Volkanovski-9523",
#                 "https://www.bestfightodds.com/fighters/Charles-Oliveira-1893",
#                 "https://www.bestfightodds.com/fighters/Kamaru-Usman-4664",
#                 "https://www.bestfightodds.com/fighters/Israel-Adesanya-7845",
#                 "https://www.bestfightodds.com/fighters/Glover-Teixeira-1477",
#                 "https://www.bestfightodds.com/fighters/Francis-Ngannou-5847",
#                 "https://www.bestfightodds.com/fighters/Rose-Namajunas-3803",
#                 "https://www.bestfightodds.com/fighters/Valentina-Shevchenko-5475",
#                 "https://www.bestfightodds.com/fighters/Amanda-Nunes-2225",
#                 "https://www.bestfightodds.com/fighters/Julianna-Pena-1816",
#             ]
#         self.root_urls = root_urls
#         self.max_depth = max_depth
#         self.verbose = verbose
#         self.urls_seen = set()
#         self.failed_urls = set()
#         self.event_urls = set()
        
#     def get_neighbor_urls(self, url:str) -> set:
#         curr_scraper = FighterScraper(url)
#         return curr_scraper.get_fighter_urls()
        
    
class FighterBFS(object):
    
    def __init__(self, root_url, max_iters=3):
        self.root_url = root_url
        self.max_iters = max_iters
        self.fighter_urls_seen = set()
        self.failed_fighter_urls = set()
        self.event_urls_seen = set()
        self.fighter_data = None
                
    def crawl(self):
        # just get the URLs of all fighters. Figure out what to do with them later! 
        curr_iter = 0
        frontier = {self.root_url} # bfs involves a queue but frankly idc
        while ((curr_iter < self.max_iters) and len(frontier) > 0):
            url = frontier.pop()
            self.fighter_urls_seen.add(url)
            print("iter={}, |frontier|={}, crawling page {}".format(curr_iter, len(frontier), url))
            try:
                # TODO I should probably have some kind of timeout/hang handler
                curr_scraper = FighterScraper(url)
                fighter_urls = curr_scraper.get_fighter_urls()
                frontier |= (fighter_urls - self.fighter_urls_seen)
                self.fighter_urls_seen |= fighter_urls
                self.event_urls_seen |= curr_scraper.get_event_urls()
            except EmptyResponse:
                print("oy, emptyresponse from {}".format(url))
                self.failed_fighter_urls.add(url)
            except ValueError:
                print("oy, valueerror from {}".format(url))
                self.failed_fighter_urls.add(url)
            curr_iter += 1
        return self.fighter_urls_seen

class BfoOddsScraper(object):

    def __init__(self, max_iters=np.inf):
        self.max_iters = max_iters
        self.fighter_urls_seen = set()
        self.failed_fighter_urls = set()
        self.event_urls_seen = set()
        self.failed_event_urls = set()

    def get_root_urls(self):
        # UFC champions as of 2022-05-08
        # just need this to ensure max coverage. all fighters should be connected
        # to one of these root urls
        return [
            "https://www.bestfightodds.com/fighters/Aljamain-Sterling-4688",
            "https://www.bestfightodds.com/fighters/Deiveson-Figueiredo-7514",
            "https://www.bestfightodds.com/fighters/Alexander-Volkanovski-9523",
            "https://www.bestfightodds.com/fighters/Charles-Oliveira-1893",
            "https://www.bestfightodds.com/fighters/Kamaru-Usman-4664",
            "https://www.bestfightodds.com/fighters/Israel-Adesanya-7845",
            "https://www.bestfightodds.com/fighters/Glover-Teixeira-1477",
            "https://www.bestfightodds.com/fighters/Francis-Ngannou-5847",
            "https://www.bestfightodds.com/fighters/Rose-Namajunas-3803",
            "https://www.bestfightodds.com/fighters/Valentina-Shevchenko-5475",
            "https://www.bestfightodds.com/fighters/Amanda-Nunes-2225",
            "https://www.bestfightodds.com/fighters/Julianna-Pena-1816",
        ]

    def scrape_and_write_all_urls(self):
        root_urls = self.get_root_urls()
        # TODO add multiprocessing
        for root_url in root_urls:
            bfs = FighterBFS(root_url, max_iters=self.max_iters)
            bfs.fighter_urls_seen = self.fighter_urls_seen
            self.fighter_urls_seen |= bfs.crawl()
            self.event_urls_seen |= bfs.event_urls_seen
            self.failed_fighter_urls |= bfs.failed_fighter_urls
        url_df = pd.DataFrame(self.fighter_urls_seen, columns=["url"])

        base_db_interface.write_replace(
            table_name="bfo_fighter_urls",
            df=url_df,
        )
        base_db_interface.write_replace(
            table_name="bfo_event_urls",
            df=pd.DataFrame(self.event_urls_seen, columns=["url"]),
        )
        base_db_interface.write_replace(
            table_name="bfo_failed_fighter_urls",
            df=pd.DataFrame(self.failed_fighter_urls, columns=["url"]),
        )
        base_db_interface.write_replace(
            table_name="bfo_failed_event_urls",
            df=pd.DataFrame(self.failed_event_urls, columns=["url"]),
        )
        return None
    
    def load_urls(self):
        self.fighter_urls_seen = set(
            base_db_interface.read("bfo_fighter_urls")["url"]
        )
        self.event_urls_seen = set(
            base_db_interface.read("bfo_event_urls")["url"]
        )
        self.failed_fighter_urls = set(
            base_db_interface.read("bfo_failed_fighter_urls")["url"]
        )
        self.failed_event_urls = set(
            base_db_interface.read("bfo_failed_event_urls")["url"]
        )
        return None

    # def scrape_all_opening_odds(self, url_df=None):
    #     if url_df is None:
    #         url_df = self.scrape_all_fighter_urls()
    #     start_letter_ind = len("https://www.bestfightodds.com/fighters/")
    #     url_df["start_letter"] = url_df["url"].str[start_letter_ind]
    #     for start_letter, grp in url_df.groupby("start_letter"):
    #         print("{} fighters with name beginning with {}".format(len(grp), start_letter))
    #         match_df = self.get_fighter_odds(grp["url"])
    #         path = "scraped_data/mma/bfo/{}_fighter_odds_{}.csv".format(start_letter, dt_now)
    #         match_df.to_csv(path, index=False)
    #     return None

    def get_event_odds(self, urls):
        fight_odds_df_list = []
        prop_odds_df_list = []
        for url in urls:
            print("scraping closing odds from {}".format(url))
            try:
                scraper = EventScraper(url)
                fight_odds_df, prop_odds_df = scraper.get_odds()
                fight_odds_df_list.append(fight_odds_df)
                prop_odds_df_list.append(prop_odds_df)
            except:
                print("!!! couldn't scrape odds from {}".format(url))
                continue
        return pd.concat(fight_odds_df_list), pd.concat(prop_odds_df_list)
    
    def get_event_prop_html(self, urls):
        """
        returns a df with columns url and html
        """
        html_df_list = []
        n = len(urls)
        for i, url in enumerate(urls):
            print("scraping prop html from {}, url {}/{}".format(url, i, n))
            try:
                scraper = EventScraper(url)
                html_df_list.append(scraper.get_prop_table())
            except:
                print("!!! couldn't scrape prop html from {}".format(url))
                continue
        return pd.concat(html_df_list)

    def get_fighter_odds(self, urls):
        # concat fighter odds dfs
        odds_df_list = []
        for url in urls:
            print("scraping odds from {}".format(url))
            try:
                fs = FighterScraper(url)
                odds_df_list.append(fs.get_odds())
            except:
                print("!!! Couldn't scrape odds for {}".format(url))
                continue
        return pd.concat(odds_df_list)

    def scrape_and_write_opening_odds(self, url_df=None):
        if url_df is None:
            url_df = base_db_interface.read("bfo_fighter_urls")
        # batching fighters by first letter of last name (as encoded in url)
        start_letter_ind = len("https://www.bestfightodds.com/fighters/")
        url_df["start_letter"] = url_df["url"].str[start_letter_ind]
        url_df = url_df.sort_values("url")
        match_df_list = []
        for start_letter, grp in url_df.groupby("start_letter"):
            print("{} fighters with name beginning with {}".format(len(grp), start_letter))
            match_df = self.get_fighter_odds(grp["url"])
            match_df_list.append(match_df)
        match_df = pd.concat(match_df_list).reset_index(drop=True)
        print("writing {} rows to db".format(len(match_df)))
        base_db_interface.write_replace(
            table_name="bfo_fighter_odds",
            df=match_df,
        )
        return match_df
    
    def scrape_and_write_closing_odds(self, url_df=None):
        if url_df is None:
            url_df = pd.DataFrame(self.event_urls_seen, columns=["url"])
        fight_odds_df, prop_odds_df = self.get_event_odds(url_df["url"])
        print("writing {} rows to db".format(len(fight_odds_df)))
        base_db_interface.write_replace(
            table_name="bfo_event_odds",
            df=fight_odds_df,
        )
        print("writing {} rows to db".format(len(prop_odds_df)))
        base_db_interface.write_replace(
            table_name="bfo_prop_odds",
            df=prop_odds_df,
        )

    def scrape_and_write_prop_html(self, url_df=None):
        if url_df is None:
            url_df = pd.DataFrame(self.event_urls_seen, columns=["url"])
        html_df = self.get_event_prop_html(url_df["url"])
        print("writing {} rows to db".format(len(html_df)))
        base_db_interface.write_replace(
            table_name="bfo_event_prop_html",
            df=html_df,
        )



def main():
    # max_iters = 1
    max_iters = np.inf
    bfo = BfoOddsScraper(max_iters=max_iters)
    bfo.scrape_and_write_all_urls()
    bfo.load_urls()
    bfo.scrape_and_write_opening_odds()
    bfo.scrape_and_write_prop_html()
    bfo.scrape_and_write_closing_odds()
    print("done!")
    
def just_scrape_props():
    # get urls of all events
    url_df = base_db_interface.read("bfo_event_urls")
    print("{} events".format(len(url_df)))
    # get html of all prop tables
    bfo = BfoOddsScraper()
    bfo.event_urls_seen = url_df["url"].tolist()
    bfo.scrape_and_write_prop_html()