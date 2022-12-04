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
from base_scrape import BaseBfs

dt_now = str(pd.to_datetime('today').date())

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

class EmptyResponse(Exception):
    pass

class BfoRequest(object):
    
    def __init__(self, url, max_tries=10, sleep_time=5):
        self.url = url
        self.max_tries = max_tries
        self.sleep_time = sleep_time
        self.raw_html = None
        
    def get_request(self):
        for i in range(self.max_tries):
            r = requests.get(self.url, headers=headers)
            r.close()
            if not r.text.startswith("Error "):
                # great, we got a legit response
                break
            # sleep for sleep_time
            print("{} gave us an Error 0, retrying in {} seconds".format(self.url, self.sleep_time))
            time.sleep(self.sleep_time)
        if r.text.startswith("Error "):
            raise EmptyResponse(self.url)
        return r

class BaseBfoPageScraper(BfoRequest):

    def __init__(self, url, **request_kwargs):
        super().__init__(url, **request_kwargs)
        self.raw_html = None
        self.data = None

    def get_html(self):
        r = self.get_request()
        self.raw_html = r.text
        tables = pd.read_html(str(r.text))
        for i, table in enumerate(tables):
            table["table_id"] = i
        self.data = pd.concat(tables)
        return self.data

class FighterScraper(BaseBfoPageScraper):
    
    def __init__(self, url, **request_kwargs):
        super().__init__(url, **request_kwargs)
        self.fighter_urls = None
        self.event_urls = None
        self.odds_df = None
        
    def get_html(self):
        r = self.get_request()
        self.raw_html = r.text
        tables = pd.read_html(str(r.text))
        for i, table in enumerate(tables):
            table["table_id"] = i
        self.data = pd.concat(tables)
        return self.data
    
    def get_fighter_urls(self):
        if self.data is None:
            self.get_html()
        # self.data["url"] = self.url
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
        self.odds_df = None
        
    def get_html(self):
        r = self.get_request()
        self.raw_html = r.text
        tables = pd.read_html(str(r.text))
        self.tables = tables
        for i, table in enumerate(tables):
            table["table_id"] = i
        self.data = pd.concat(tables)
        return self.data
    
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
        odds_df_rows = []
        odds_df = self.tables[1].dropna(subset=["Props.1"]).reset_index(drop=True)
        odds_df = odds_df.rename(columns={"Unnamed: 0": "FighterName"})
        odds_df["match_id"] = odds_df.index//2
        fighter_urls = self.get_fighter_urls()
        odds_df["FighterHref"] = pd.Series(fighter_urls).str[len("https://www.bestfightodds.com/fighters/"):]
        odds_df["EventHref"] = self.url[len("https://www.bestfightodds.com/events/"):]
        return odds_df

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

    def scrape_all_fighter_urls(self):
        root_urls = self.get_root_urls()
        for root_url in root_urls:
            bfs = FighterBFS(root_url, max_iters=self.max_iters)
            bfs.fighter_urls_seen = self.fighter_urls_seen
            self.fighter_urls_seen |= bfs.crawl()
            self.event_urls_seen |= bfs.event_urls_seen
            self.failed_fighter_urls |= bfs.failed_fighter_urls
        url_df = pd.DataFrame(self.fighter_urls_seen, columns=["url"])
        url_df.to_csv("scraped_data/mma/bfo/bfo_fighter_urls_{}.csv".format(dt_now), index=False)

        event_url_df = pd.DataFrame(self.event_urls_seen, columns=["url"])
        event_url_df.to_csv("scraped_data/mma/bfo/bfo_fighter_urls_{}.csv".format(dt_now), index=False)

        fail_df = pd.DataFrame(self.failed_fighter_urls, columns=["url"])
        fail_df.to_csv("scraped_data/mma/bfo/bfo_fighter_urls_{}.csv".format(dt_now), index=False)
        return url_df

    def scrape_all_opening_odds(self, url_df=None):
        if url_df is None:
            url_df = self.scrape_all_fighter_urls()
        start_letter_ind = len("https://www.bestfightodds.com/fighters/")
        url_df["start_letter"] = url_df["url"].str[start_letter_ind]
        for start_letter, grp in url_df.groupby("start_letter"):
            print("{} fighters with name beginning with {}".format(len(grp), start_letter))
            match_df = self.get_fighter_odds(grp["url"])
            path = "scraped_data/mma/bfo/{}_fighter_odds_{}.csv".format(start_letter, dt_now)
            match_df.to_csv(path, index=False)
        return None

    def scrape_all_closing_odds(self, url_df=None):
        if url_df is None:
            url_df = pd.DataFrame(self.event_urls_seen, columns=["url"])
        odds_df = self.get_event_odds(url_df["url"])
        path = "scraped_data/mma/bfo/bfo_event_odds_{}.csv".format(dt_now)
        odds_df.to_csv(path, index=False)
        return None

    def get_event_odds(self, urls):
        odds_df_list = []
        for url in urls:
            print("scraping closing odds from {}".format(url))
            try:
                scraper = EventScraper(url)
                odds_df = scraper.get_odds()
                odds_df_list.append(odds_df)
            except:
                print("couldn't scrape odds from {}".format(url))
                continue 
        return pd.concat(odds_df_list)

    def get_fighter_odds(self, urls):
        # concat fighter odds dfs
        odds_df_list = []
        for url in urls:
            print("scraping odds from {}".format(url))
            try:
                fs = FighterScraper(url)
                odds_df_list.append(fs.get_odds())
            except:
                print("Couldn't scrape odds for {}".format(url))
                continue
        return pd.concat(odds_df_list)

if __name__ == "__main__":
    bfo = BfoOddsScraper(max_iters=np.inf)
    url_df = bfo.scrape_all_fighter_urls()
    bfo.scrape_all_opening_odds(url_df)
    bfo.scrape_all_closing_odds()
    print("done!")
    
