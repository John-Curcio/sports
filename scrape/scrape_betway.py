from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver import FirefoxOptions
import time
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

class BetwayParser(object):

    def __init__(self, raw_html):
        self.soup = BeautifulSoup(raw_html, 'html.parser')

    def parse_odds(self):
        # parse odds from page source
        for fight in self.get_upcoming_fights():
            yield self.parse_fight_odds(fight)
        
    def get_upcoming_fights(self):
        # get upcoming fights from page source
        return self.soup.find_all("div", class_="rj-ev-list__ev-card__inner")

    def parse_fight_odds(self, fight_soup):
        button_a, button_b = fight_soup.find_all("div", class_="rj-ev-list__bet-btn__inner")
        name_a = button_a.find("span", class_="rj-ev-list__bet-btn__content rj-ev-list__bet-btn__text")
        name_b = button_b.find("span", class_="rj-ev-list__bet-btn__content rj-ev-list__bet-btn__text")
        odds_a = button_a.find("span", class_="rj-ev-list__bet-btn__content rj-ev-list__bet-btn__odd")
        odds_b = button_b.find("span", class_="rj-ev-list__bet-btn__content rj-ev-list__bet-btn__odd")
        return {
            "FighterName": name_a.text if name_a is not None else None,
            "OpponentName": name_b.text if name_b is not None else None,
            "FighterOdds": odds_a.text if odds_a is not None else None,
            "OpponentOdds": odds_b.text if odds_b is not None else None,
        }

class BetwayScraper(object):

    def __init__(self):
        self.league_urls = self.get_league_urls()
        self.driver = None
        self.data = None

    def get_league_urls(self):
        return [
            "https://dk-bwnjsports.betway.com/sports/mma/ufc/",
            "https://dk-bwnjsports.betway.com/sports/mma/pfl/",
            "https://dk-bwnjsports.betway.com/sports/mma/bellator/",
        ]
    
    def init_update_driver(self, url):
        if self.driver is None:
            opts = FirefoxOptions()
            opts.add_argument("--headless")
            self.driver = webdriver.Firefox(options=opts)
        self.driver.get(url)
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "rj-ev-list__bet-btn__inner ")))

    def close_driver(self):
        self.driver.close()

    def scrape_all_leagues(self):
        """
        Scrape all leagues
        """
        df = []
        for url in self.league_urls:
            self.init_update_driver(url)
            # check if the driver gets redirected
            if self.driver.current_url == url:
                # df.append(self.scrape_league())
                df.append(self.scroll_and_scrape())
            else:
                print("Redirected to {}".format(self.driver.current_url))
        self.close_driver()
        return pd.concat(df).reset_index(drop=True).drop_duplicates()
    
    def scroll_and_scrape(self, scroll_height=1000, scroll_pause_time=0.5):
        """
        Scroll down the page, and scrape its contents at each scroll position
        scroll_height: how much to scroll down each time
        scroll_pause_time: how long to wait after each scroll
        """
        # Get scroll height
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        last_height = self.driver.execute_script("return window.scrollY")
        df = []
        while True:
            # scrape
            df.append(self.scrape_page())
            # Scroll down a bit
            self.driver.execute_script(f"window.scrollTo(0, {last_height + scroll_height});")

            # Wait to load page
            time.sleep(scroll_pause_time)

            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return window.scrollY")
            print(last_height, new_height)
            if new_height == last_height:
                break
            last_height = new_height
        self.data = pd.concat(df).reset_index(drop=True)
        return self.data
    
    def scrape_page(self):
        """
        scrape and parse the current page
        """
        parser = BetwayParser(self.driver.page_source)
        curr_df = pd.DataFrame(parser.parse_odds())
        curr_df["url"] = self.driver.current_url
        return curr_df
    
class DbInterface(object):

    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def init_table_if_not_exists(self):
        # create table if not exists
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS betway_odds (
                FighterName TEXT,
                OpponentName TEXT,
                FighterOdds TEXT,
                OpponentOdds TEXT,
                url TEXT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def insert_data(self, data: pd.DataFrame):
        # insert data into table
        data.to_sql("betway_odds", self.conn, if_exists="append", index=False)

    def close(self):
        # close connection
        self.conn.close()

def scrape_and_write():
    scraper = BetwayScraper()
    data = scraper.scrape_all_leagues()
    db = DbInterface("/home/ubuntu/scrape/market_odds.db")
    db.init_table_if_not_exists()
    db.insert_data(data)
    db.close()

if __name__ == "__main__":
    scrape_and_write()
