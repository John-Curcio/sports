# scrape pages on mixedmartialarts.com
# eg "https://fighters.mixedmartialarts.com/Jon-Jones:54494EFC3CC798A0"

# from scrape.scrape_bfo import BasePageScraper
from scrape_bfo import BasePageScraper, FighterBFS, EmptyResponse
import pandas as pd 
import numpy as np
from bs4 import BeautifulSoup


class MixedPageScraper(BasePageScraper):
    
    def __init__(self, url, **request_kwargs):
        super().__init__(url, **request_kwargs)
        self.raw_html = None
        self.bio_data = None
        self.match_data = None
        
    def get_html(self):
        r = self.get_request()
        self.raw_html = r.text
        return self.raw_html
    
    def get_bio_data(self):
        raw_html = self.raw_html
        if raw_html is None:
            raw_html = self.get_html()
        soup = BeautifulSoup(raw_html)
        fighter_info_table = soup.find("table", {"class": "table fighter-info"})
        bio_data = pd.read_html(str(fighter_info_table))[0]
        bio_data = bio_data.set_index(0).T
        self.bio_data = bio_data
        return bio_data
    
    def get_fighter_urls(self):
        raw_html = self.raw_html
        if raw_html is None:
            raw_html = self.get_html()
        soup = BeautifulSoup(raw_html)
        fighter_info_table = soup.find("div", {"class": "section-wrapper mma-record"})
        urls = [link.get("href") for link in fighter_info_table.find_all("a")]
        opponent_urls = set(urls[::2])
        return opponent_urls
    
    def get_match_data(self):
        raw_html = self.raw_html
        if raw_html is None:
            raw_html = self.get_html()
        soup = BeautifulSoup(raw_html)
        fighter_info_table = soup.find("div", {"class": "section-wrapper mma-record"})
        if fighter_info_table is None:
            return None
        urls = [link.get("href") for link in fighter_info_table.find_all("a")]
        opponent_urls = urls[::2]
        event_urls = urls[1::2]
        match_data = pd.read_html(str(fighter_info_table))[0]
        match_data = match_data.rename(columns={"Unnamed: 2": "FighterResult"})
        match_data = match_data[["Date", "FighterResult", "Opponent", "Weightclass", 
                                 "Method", "Round", "Time"]]
        match_data["OpponentUrl"] = opponent_urls
        match_data["EventUrl"] = event_urls
        self.match_data = match_data
        return match_data

class MixedBfs(FighterBFS):
                
    def crawl(self):
        # just get the URLs of all fighters. Figure out what to do with them later! 
        curr_iter = 0
        frontier = {self.root_url} # bfs involves a queue but frankly idc
        while ((curr_iter < self.max_iters) and len(frontier) > 0):
            url = frontier.pop()
            self.fighter_urls_seen.add(url)
            print("iter={}, |frontier|={}, crawling page {}".format(curr_iter, len(frontier), url))
            try:
                fighter_urls = MixedPageScraper(url).get_fighter_urls()
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

class MixedMartialArtsScraper(object):

    def __init__(self, max_iters=3):
        self.max_iters = max_iters
        self.fighter_urls_seen = set()
        self.failed_fighter_urls = set()

    def get_current_ufc_champions(self):
        url = "https://fighters.mixedmartialarts.com/"
        bps = BasePageScraper(url)
        r = bps.get_request()
        soup = BeautifulSoup(r.text)
        champ_soup = soup.find("div", {"class": "champions"})
        champ_urls = set(link.get("href") for link in champ_soup.find_all("a"))
        return champ_urls

    def scrape_all_fighter_urls(self):
        champ_urls = self.get_current_ufc_champions()
        for url in champ_urls:
            bfs = MixedBfs(url, max_iters=self.max_iters)
            bfs.fighter_urls_seen = self.fighter_urls_seen
            self.fighter_urls_seen = bfs.crawl()
            self.failed_fighter_urls |= bfs.failed_fighter_urls
        url_df = pd.DataFrame(self.fighter_urls_seen, columns=["url"])
        url_df.to_csv("scraped_data/mma/mixedmartialarts/mixedmartialarts_fighter_urls.csv", index=False)

        fail_df = pd.DataFrame(self.failed_fighter_urls, columns=["url"])
        fail_df.to_csv("scraped_data/mma/mixedmartialarts/failed_mixedmartialarts_fighter_urls.csv", index=False)
        return url_df

    def scrape_all_matches(self, url_df=None):
        if url_df is None:
            url_df = self.scrape_all_fighter_urls()
        start_letter_ind = len("https://fighters.mixedmartialarts.com/")
        url_df["start_letter"] = url_df["url"].str[start_letter_ind]
        for start_letter, grp in url_df.groupby("start_letter"):
            print("{} fighters with name beginning with {}".format(len(grp), start_letter))
            match_df = self.get_fighter_matches(grp["url"])
            path = "scraped_data/mma/mixedmartialarts/mixedmartialarts_fighter_matches_{}.csv".format(start_letter)
            match_df.to_csv(path, index=False)
        return None

    def get_fighter_matches(self, urls):
        # concat fighter match dfs
        match_df_list = []
        for url in urls:
            print("scraping matches from {}".format(url))
            try:
                mps = MixedPageScraper(url)
                match_df_list.append(mps.get_match_data())
            except:
                print("Couldn't scrape matches for {}".format(url))
                continue
        return pd.concat(match_df_list)

if __name__ == "__main__":
    msc = MixedMartialArtsScraper(max_iters=3)
    msc.scrape_all_fighter_urls()
    msc.scrape_all_matches()