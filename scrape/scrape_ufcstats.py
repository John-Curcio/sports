from base_scrape import BasePageScraper
import pandas as pd
import numpy as np
import string
from tqdm import tqdm
import re


class UfcEventScraper(BasePageScraper):
    # basically just get weight classes
    # http://ufcstats.com/event-details/253d3f9e97ca149a
    
    def get_fights(self):
        soup = self.get_soup()
        class_str = "b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click"
        tags = soup.find_all("tr", {"class": class_str})
        return [tag.get("data-link") for tag in tags]
    
    def get_page_urls(self) -> set:
        # get urls for each fight
        soup = self.get_soup()
        class_str = "b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click"
        fights = soup.find_all("tr", {"class": class_str})
        fight_urls = {fight.get("data-link") for fight in fights}
        return fight_urls
    
    def get_fighter_urls(self):
        soup = self.get_soup()
        fighter_links = soup.find_all("a", {"class":"b-link b-link_style_black"})
        fighter_links = [link.get('href') for link in fighter_links]
        fighter_links = [link for link in fighter_links if link.startswith("http://ufcstats.com/fighter-details")]
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
        fighter_links = self.get_fighter_urls()
        self.data["FighterUrl"] = fighter_links[::2]
        self.data["OpponentUrl"] = fighter_links[1::2]
        
        # img pngs (performance of night, fight of night, sub of night, ko of the night)
        self.data["img_png_url"] = self._get_img_pngs()
        self.data["is_title_fight"] = self.data["img_png_url"].str.endswith("belt.png")
        
        self.data["FightID"] = self.get_fights()
        return self.data


class MissingStatsException(Exception):
    pass


class UfcFightDetails(BasePageScraper):
    
    def get_page_urls(self) -> set:
        soup = self.get_soup()
        class_str = "b-link b-link_style_black"
        possible_event = soup.find_all("a", {"class": class_str})
        links = [link.get('href') for link in possible_event]
        return links
    
    def get_fighter_urls(self):
        soup = self.get_soup()
        tags = soup.find_all("a", {"class": "b-link b-fight-details__person-link"})
        return [tag["href"] for tag in tags]
    
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
        tags = soup.find_all("div", {"class": "b-fight-details__fight"})
        desc = tags[0].text.strip().replace("  ", "_").replace("\n", "")
        d = re.sub("\_+", "_", desc).split("_")
        return pd.Series({
            "Weight": d[0],
            "Method": d[2],
            "Round": d[4],
            "Time": d[6],
            "Time Format": d[8],
            "Referee": d[10],
            "Details": " ".join(d[12:]),
        })

    def get_page_data(self):
        self.fight_description = self.get_fight_description()
        soup = self.get_soup()
        if "Round-by-round stats not currently available" in soup.text:
            self.totals = None 
            self.strikes = None 
            self.round_strikes = None 
            self.round_totals = None
            return None
        self.data = pd.read_html(str(soup).replace('</p>','_</p>'))
        if len(self.data) != 4:
            raise MissingStatsException(f"Found {len(self.data)} tables instead of 4")
        totals, round_totals, strikes, round_strikes = self.data
        fighter_ids = self.get_fighter_urls()
        self.totals = self.parse_sum_table(totals, fighter_ids)
        self.strikes = self.parse_sum_table(strikes, fighter_ids)
        self.round_strikes = self.parse_round_strikes_table(round_strikes, fighter_ids)
        self.round_totals = self.parse_round_totals_table(round_totals, fighter_ids)
        return self.totals
        #return self.totals.merge(self.strikes, on=["FighterID"], suffixes=("", "_y"))
    

class UfcFighterScraper(BasePageScraper):
    
    def get_page_urls(self):
        soup = self.get_soup()
        # get all the events this guy fought in
        class_str = "b-link b-link_style_black"
        possible_event = soup.find_all("a", {"class": class_str})
        links = [link.get('href') for link in possible_event]
        return links
    
    def get_page_data(self):
        soup = self.get_soup()
        tag = soup.find("ul", {"class": "b-list__box-list"})
        desc = tag.text.strip().replace("  ", "_").replace("\n", "")
        d = re.sub("\_+", "_", desc).split("_")
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
        return [tag.get("data-link") for tag in tags]
    
    def get_events(self):
        urls = pd.Series(self.get_page_urls(), dtype="object")
        return urls.loc[urls.str.startswith("http://ufcstats.com/event-details/")].values
    

class CharFightersScraper(BasePageScraper):
    
    def get_page_urls(self):
        # get set of urls mapping to other pages to scrape
        soup = self.get_soup()
        # TODO I should confirm that urls contain fighter-details
        return {tag["href"] for tag in soup.find("tbody").find_all("a")}
        
    def get_page_data(self):
        return None
    

class UfcUrlScraper(object):
    """
    gets all fighter urls, then gets all event urls and fight urls
    """
    
    def __init__(self):
        self.fighter_urls = None
        self.event_urls = None
        self.fight_urls = None
        
    def get_all_fighter_urls(self):
        self.fighter_urls = set()
        # for c in ["x"]:
        for c in tqdm(string.ascii_lowercase):
            url = f"http://ufcstats.com/statistics/fighters?char={c}&page=all"
            self.fighter_urls |= CharFightersScraper(url).get_page_urls()
        return self.fighter_urls
    
    def get_all_event_and_fight_urls(self):
        self.event_urls = set()
        self.fight_urls = set()
        if self.fighter_urls is None:
            self.get_all_fighter_urls()
        for fighter_url in tqdm(self.fighter_urls):
            try:
                fighter_scraper = UfcFighterScraper(fighter_url)
                curr_event_urls = fighter_scraper.get_events()
                self.event_urls |= set(curr_event_urls)

                curr_fight_urls = fighter_scraper.get_fights()
                self.fight_urls |= set(curr_fight_urls)
            except:
                raise Exception(f"Error encountered on {fighter_url}")
        return self.event_urls, self.fight_urls


class FullUfcScraper(object):
    
    def __init__(self, fighter_urls, event_urls, fight_urls):
        self.fighter_urls = fighter_urls
        self.event_urls = event_urls
        self.fight_urls = fight_urls
        self.totals_df = None
        self.strikes_df = None
        self.round_totals_df = None
        self.round_strikes_df = None
        self.fight_description_df = None
        self.event_data = None
        self.fighter_data = None
        
    def scrape_fights(self):
        totals = []
        strikes = []
        round_totals = []
        round_strikes = []
        descriptions = []
        for fight_url in tqdm(self.fight_urls):
            fight_scraper = UfcFightDetails(fight_url)
            result = fight_scraper.get_page_data()
            if result is not None:
                for df in [fight_scraper.totals, fight_scraper.strikes, 
                        fight_scraper.fight_description,
                        fight_scraper.round_totals, fight_scraper.round_strikes]:
                    df["FightID"] = fight_url
                totals.append(fight_scraper.totals)
                strikes.append(fight_scraper.strikes)
                round_totals.append(fight_scraper.round_totals)
                round_strikes.append(fight_scraper.round_strikes)
                descriptions.append(fight_scraper.fight_description)
        self.totals_df = pd.concat(totals).reset_index(drop=True)
        self.strikes_df = pd.concat(strikes).reset_index(drop=True)
        self.round_totals_df = pd.concat(round_totals).reset_index(drop=True)
        self.round_strikes_df = pd.concat(round_strikes).reset_index(drop=True)
        self.fight_description_df = pd.DataFrame(descriptions)
        return self.round_strikes_df
    
    def scrape_events(self):
        event_data = []
        for event_url in tqdm(self.event_urls):
            event_scraper = UfcEventScraper(event_url)
            event_df = event_scraper.get_page_data()
            event_df["EventUrl"] = event_url
            event_data.append(event_df)
        self.event_data = pd.concat(event_data).reset_index(drop=True)
        return self.event_data
    
    def scrape_fighters(self):
        fighter_data = []
        for fighter_url in tqdm(self.fighter_urls):
            fighter_scraper = UfcFighterScraper(fighter_url)
            curr_fighter_data = fighter_scraper.get_page_data()
            fighter_data.append(curr_fighter_data)
        self.fighter_data = pd.DataFrame(fighter_data)
        return self.fighter_data
    
    def scrape_all(self):
        print("----- scraping fights -----")
        self.scrape_fights()
        print("----- scraping events -----")
        self.scrape_events()
        print("----- scraping fighters -----")
        self.scrape_fighters()


if __name__ == "__main__":
    url_scraper = UfcUrlScraper()
    url_scraper.get_all_event_and_fight_urls()

    full_scraper = FullUfcScraper(
        fighter_urls=url_scraper.fighter_urls,
        event_urls=url_scraper.event_urls,
        fight_urls=url_scraper.fight_urls,
    ) 
    full_scraper.scrape_all()
    folder = "scraped_data/mma/ufcstats/"
    filename_dict = {
        "totals": full_scraper.totals_df,
        "strikes": full_scraper.strikes_df,
        "round_totals": full_scraper.round_totals_df,
        "round_strikes": full_scraper.round_strikes_df,
        "fight_descriptions": full_scraper.fight_description_df,
        "event_data": full_scraper.event_data,
        "fighter_data": full_scraper.fighter_data,
    }
    for filename, df in filename_dict.items():
        path = folder + filename + ".csv"
        df.to_csv(path, index=False)

