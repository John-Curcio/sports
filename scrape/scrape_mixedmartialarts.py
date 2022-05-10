# scrape pages on mixedmartialarts.com
# eg "https://fighters.mixedmartialarts.com/Jon-Jones:54494EFC3CC798A0"

# from scrape.scrape_bfo import BasePageScraper
from base_scrape import BasePageScraper, BaseBfs, MultiRootBfs
import pandas as pd 
import numpy as np

class MixedPageScraper(BasePageScraper):
    
    def get_page_urls(self) -> set:
        soup = self.get_soup()
        fighter_info_table = soup.find("div", {"class": "section-wrapper mma-record"})
        urls = [link.get("href") for link in fighter_info_table.find_all("a")]
        opponent_urls = set(urls[::2])
        return opponent_urls
    
    def get_page_data(self) -> pd.DataFrame:
        soup = self.get_soup()
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
        self.data = match_data
        return match_data
    
class MixedBfs(BaseBfs):
    
    def get_neighbor_urls(self, url):
        scraper = MixedPageScraper(url)
        return scraper.get_page_urls()

class MultiRootMixedBfs(MultiRootBfs):

    def get_bfs(self, root_url, max_depth, verbose):
        return MixedBfs(root_url, max_depth, verbose)

    def get_page_data(self, url):
        scraper = MixedPageScraper(url)
        return scraper.get_page_data()


if __name__ == "__main__":
    root_urls = [
        "https://fighters.mixedmartialarts.com/Aljamain-Sterling:05AAFAD78CC5E653",
        "https://fighters.mixedmartialarts.com/Alexander-Volkanovski:80C9865ECBF8EDA4",
        "https://fighters.mixedmartialarts.com/Brandon-Moreno:7E881CECD93918E1",
        "https://fighters.mixedmartialarts.com/Francis-Ngannou:D590C2E5D1E9DAF2",
        "https://fighters.mixedmartialarts.com/Glover-Teixeira:074E3B4BC4A76375",
        "https://fighters.mixedmartialarts.com/Charles-Oliveira:D8F9346A03215D16",
        "https://fighters.mixedmartialarts.com/Israel-Adesanya:E116D6D517539081",
        "https://fighters.mixedmartialarts.com/Kamaru-Usman:8E61AD697A432F19",
        "https://fighters.mixedmartialarts.com/Julianna-Pena:FD04D1864BC96025",
        "https://fighters.mixedmartialarts.com/Amanda-Nunes:15BFA19E4F6C08A5",
        "https://fighters.mixedmartialarts.com/Valentina-Shevchenko:88B6908EDE151E5B",
        "https://fighters.mixedmartialarts.com/Rose-Namajunas:9408C6D89D0DB238",
    ]

    full_scraper = MultiRootMixedBfs(root_urls, max_depth=2, verbose=True)
    full_scraper.crawl_urls()
    url_df = pd.DataFrame(full_scraper.urls_seen, columns=["url"])
    dt = str(pd.to_datetime("today").date())
    path = "scraped_data/mma/mixedmartialarts/all_urls_{}.csv".format(dt)
    url_df.to_csv(path)

    fail_df = pd.DataFrame(full_scraper.failed_urls, columns=["url"])
    path = "scraped_data/mma/mixedmartialarts/failed_urls_{}.csv".format(dt)
    fail_df.to_csv(path, index=False)

    start_letter_ind = len("https://fighters.mixedmartialarts.com/")
    url_df["start_letter"] = url_df["url"].str[start_letter_ind]
    for start_letter, grp in url_df.groupby("start_letter"):
        print("{} fighters with name beginning with {}".format(len(grp), start_letter))
        match_df_list = []
        for url in grp["url"]:
            print("scraping matches from {}".format(url))
            try:
                mps = MixedPageScraper(url)
                match_df_list.append(mps.get_page_data())
            except:
                print("Couldn't scrape matches for {}".format(url))
                continue
        match_df = pd.concat(match_df_list)
        path = "scraped_data/mma/mixedmartialarts/mixedmartialarts_fighter_matches_{}.csv".format(start_letter)
        match_df.to_csv(path, index=False)
    print('done!')



# class MixedMartialArtsScraper(object):

#     def __init__(self, max_iters=3):
#         self.max_iters = max_iters
#         self.fighter_urls_seen = set()
#         self.failed_fighter_urls = set()

#     def get_current_ufc_champions(self):
#         url = "https://fighters.mixedmartialarts.com/"
#         bps = BasePageScraper(url)
#         r = bps.get_request()
#         soup = BeautifulSoup(r.text)
#         champ_soup = soup.find("div", {"class": "champions"})
#         champ_urls = set(link.get("href") for link in champ_soup.find_all("a"))
#         return champ_urls

#     def scrape_all_fighter_urls(self):
#         champ_urls = self.get_current_ufc_champions()
#         for url in champ_urls:
#             bfs = MixedBfs(url, max_iters=self.max_iters)
#             bfs.fighter_urls_seen = self.fighter_urls_seen
#             self.fighter_urls_seen = bfs.crawl()
#             self.failed_fighter_urls |= bfs.failed_fighter_urls
#         url_df = pd.DataFrame(self.fighter_urls_seen, columns=["url"])
#         url_df.to_csv("scraped_data/mma/mixedmartialarts/mixedmartialarts_fighter_urls.csv", index=False)

#         fail_df = pd.DataFrame(self.failed_fighter_urls, columns=["url"])
#         fail_df.to_csv("scraped_data/mma/mixedmartialarts/failed_mixedmartialarts_fighter_urls.csv", index=False)
#         return url_df

#     def scrape_all_matches(self, url_df=None):
#         if url_df is None:
#             url_df = self.scrape_all_fighter_urls()
#         start_letter_ind = len("https://fighters.mixedmartialarts.com/")
#         url_df["start_letter"] = url_df["url"].str[start_letter_ind]
#         for start_letter, grp in url_df.groupby("start_letter"):
#             print("{} fighters with name beginning with {}".format(len(grp), start_letter))
#             match_df = self.get_fighter_matches(grp["url"])
#             path = "scraped_data/mma/mixedmartialarts/mixedmartialarts_fighter_matches_{}.csv".format(start_letter)
#             match_df.to_csv(path, index=False)
#         return None

#     def get_fighter_matches(self, urls):
#         # concat fighter match dfs
#         match_df_list = []
#         for url in urls:
#             print("scraping matches from {}".format(url))
#             try:
#                 mps = MixedPageScraper(url)
#                 match_df_list.append(mps.get_match_data())
#             except:
#                 print("Couldn't scrape matches for {}".format(url))
#                 continue
#         return pd.concat(match_df_list)

# if __name__ == "__main__":
#     msc = MixedMartialArtsScraper(max_iters=3)
#     msc.scrape_all_fighter_urls()
#     msc.scrape_all_matches()