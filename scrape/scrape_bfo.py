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

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

class EmptyResponse(Exception):
    pass

class BFORequest(object):
    
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

class BasePageScraper(BFORequest):

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

class FighterScraper(BasePageScraper):
    
    def __init__(self, url, **request_kwargs):
        super().__init__(url, **request_kwargs)
        self.fighter_urls = None
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
        soup = BeautifulSoup(self.raw_html)
        tbody = soup.find("tbody")
        urls = [link.get("href") for link in tbody.find_all("a")]
        fighter_urls = [("https://www.bestfightodds.com"+u) 
                             for u in urls if u.startswith("/fighters/")]
        self.fighter_urls = set(fighter_urls) - {self.url}
        return self.fighter_urls
    
    def get_odds(self):
        if self.data is None:
            self.get_html()
        odds_df_rows = []
        soup = BeautifulSoup(self.raw_html)
        opp_cells = soup.find_all("th", {"class": "oppcell"})
        opp_cells = [u.find("a").get("href") for u in opp_cells]
        opp_cell_pairs = list(zip(opp_cells[::2], opp_cells[1::2]))
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
    
class FighterBFS(object):
    
    def __init__(self, root_url, max_iters=3):
        self.root_url = root_url
        self.max_iters = max_iters
        self.fighter_urls_seen = set()
        self.failed_fighter_urls = set()
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
                fighter_urls = FighterScraper(url).get_fighter_urls()
                frontier |= (fighter_urls - self.fighter_urls_seen)
                self.fighter_urls_seen |= fighter_urls
            except EmptyResponse:
                print("oy, emptyresponse from {}".format(url))
                self.failed_fighter_urls.add(url)
            except ValueError:
                print("oy, valueerror from {}".format(url))
                self.failed_fighter_urls.add(url)    
            curr_iter += 1
        return self.fighter_urls_seen
                # if exception, add to failed fighter urls

def scrape_all_fighter_urls()
    # fs = FighterScraper("https://www.bestfightodds.com/fighters/Jon-Jones-819")
    # print(fs.get_fighter_urls())
    # fs.data
    # root_url = "https://www.bestfightodds.com/fighters/Jon-Jones-819"
    root_url = "https://www.bestfightodds.com/fighters/Anderson-Silva-38"
    max_iters = np.inf
    # max_iters = 10
    f_bfs = FighterBFS(root_url, max_iters)
    f_bfs.crawl()
    url_df = pd.DataFrame(f_bfs.fighter_urls_seen, columns=["url"])
    url_df.to_csv("scraped_data/mma/bfo_fighter_urls.csv", index=False)

    fail_df = pd.DataFrame(f_bfs.failed_fighter_urls, columns=["url"])
    fail_df.to_csv("scraped_data/mma/failed_bfo_fighter_urls.csv", index=False)

def get_fighter_odds(urls):
    # concat fighter odds dfs
    odds_df_list = []
    for url in urls:
        print("scraping odds from {}".format(url))
        fs = FighterScraper(url)
        odds_df_list.append(fs.get_odds())
    return pd.concat(odds_df_list)

if __name__ == "__main__":
    # scrape_all_fighter_urls()
    bfo_fighter_urls = pd.read_csv("scraped_data/mma/bfo_fighter_urls.csv")
    start_letter_ind = len("https://www.bestfightodds.com/fighters/")
    bfo_fighter_urls["start_letter"] = bfo_fighter_urls["url"].str[start_letter_ind]
    for start_letter, grp in bfo_fighter_urls.groupby("start_letter"):
        print("{} fighters with name beginning with {}".format(len(grp), start_letter))
        odds_df = get_fighter_odds(grp["url"])
        path = "scraped_data/mma/{}_fighter_odds.csv"
        odds_df.to_csv(path, index=False)