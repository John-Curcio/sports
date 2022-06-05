from base_scrape import BasePageScraper, BaseBfs, MultiRootBfs
import pandas as pd
import numpy as np
from tqdm import tqdm

class UfcStatsEventScraper(BasePageScraper):
    # basically just get weight classes
    # http://ufcstats.com/event-details/253d3f9e97ca149a
    
    def get_page_urls(self) -> set:
        # get urls for each fight
        soup = self.get_soup()
        class_str = "b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click"
        fights = soup.find_all("tr", {"class": class_str})
        fight_urls = [fight.get("data-link") for fight in fights]
        return fight_urls
    
    def _get_date_location(self):
        soup = self.get_soup()
        date, loc = soup.find_all("li", {"class": "b-list__box-list-item"})
        date = ' '.join(date.text.split())
        loc = ' '.join(loc.text.split())
        date = date[len("Date: "):]
        loc = loc[len("Location: "):]
        return date, loc
    
    def _get_img_pngs(self):
        soup = self.get_soup()
        class_str = "b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click"
        fights = soup.find_all("tr", {"class": class_str})
        img_tags = [fight.find("img") for fight in fights]
        img_pngs = [img.get("src") if img else None for img in img_tags]
        return pd.Series(img_pngs).fillna("")

    def get_page_data(self) -> pd.DataFrame:
        soup = self.get_soup()
        self.data = pd.read_html(str(soup))[0]
        date, loc = self._get_date_location()
        self.data["Date"] = date
        self.data["Location"] = loc
        names = self.data["Fighter"].str.split("  ")
        fighters, opponents = names.str[0], names.str[1]
        self.data["FighterName"] = fighters
        self.data["OpponentName"] = opponents
        fighter_links = soup.find_all("a", {"class":"b-link b-link_style_black"})
        fighter_links = [link.get('href') for link in fighter_links]
        self.data["FighterUrl"] = fighter_links[::2]
        self.data["OpponentUrl"] = fighter_links[1::2]
        
        # img pngs (performance of night, fight of night, sub of night, ko of the night)
        self.data["img_png_url"] = self._get_img_pngs()
        self.data["is_title_fight"] = self.data["img_png_url"].str.endswith("belt.png")
        return self.data


class UfcStatsFullScraper(BasePageScraper):
    
    def get_page_data(self):
        return None
    
    def get_page_urls(self) -> set:
        # get urls for each event
        self.url = "http://ufcstats.com/statistics/events/completed?page=all"
        soup = self.get_soup()
        events = soup.find_all("tr", {"class": "b-statistics__table-row"})
        event_urls = [event.find("a", href=True)["href"] for event in events[2:]]
        return event_urls


if __name__ == "__main__":
    scraper = UfcStatsFullScraper("http://ufcstats.com/statistics/events/completed?page=all")
    event_urls = scraper.get_page_urls()
    event_urls = list(event_urls) + [
        "http://ufcstats.com/event-details/6420efac0578988b", # UFC 1: the beginning
    ]
    match_df = []
    for event_url in tqdm(event_urls):
        scraper = UfcStatsEventScraper(event_url)
        match_df.append(scraper.get_page_data())

    match_df = pd.concat(match_df).reset_index(drop=True)

    match_df.to_csv("scraped_data/mma/ufcstats/ufcstats_matches.csv", index=False)
