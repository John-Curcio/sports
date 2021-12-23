from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import re
from bs4 import BeautifulSoup

from base_classes import GameData, LeagueData, MissingGame
from tqdm import tqdm
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait # available since 2.4.0
from selenium.webdriver.support import expected_conditions as EC # available since 2.26.0
from selenium.common.exceptions import NoSuchElementException, TimeoutException
########################################
# sportsbookreview.com
# Use this to get scores and wager/opener
########################################

class NonClassicGameData(GameData):
    """
    Team identities, date of matchup, scores, wager/opener, and closing odds for each game
    """
    
    def __init__(self, league, game_soup, bet_type, date=None):
        super().__init__(league, game_soup, bet_type, date)
        self.score_A = None
        self.score_B = None
        self.odds["wager"] = {"A": None, "B": None}
        self.odds["opener"] = {"A": None, "B": None}
        for market in self.markets:
            self.odds[market] = {"A": None, "B": None}
        
    def parse_game(self):
        team_names = self.game_soup.find_all("span", {"class": re.compile(r"^participantBox")})
        if len(team_names) <= 1:
            print("only {} team names".format(len(team_names)))
            return self.to_dict(missing=True)
            # raise MissingGame(self.league, self.bet_type, self.date, classic=False)
        self.team_A = team_names[0].text
        self.team_B = team_names[1].text
        # team_score_tags = self.game_soup.find_all("div", {"class": re.compile(r"^(scoreboardColumn-2OtpR finalScore)")})
        team_score_tags = self.game_soup.find_all("div", {"class": re.compile(r"^(scoreboardColumn).*(finalScore).*")})
        #team_score_tags = self.game_soup.find_all("div", {"class": "scores-1-KV5 undefined"})
        if len(team_score_tags) == 0:
            print("couldnt' get scores {} {} {}".format(self.date, self.team_A, self.team_B))
            return self.to_dict(missing=True)
            # raise MissingGame(self.league, self.bet_type, self.date, classic=False)
        team_scores = team_score_tags[0].find_all("div")
        if len(team_scores) <= 1:
            print("couldnt' get scores {} {} {}".format(self.date, self.team_A, self.team_B))
            return self.to_dict(missing=True)
            # raise MissingGame(self.league, self.bet_type, self.date, classic=False)
        self.score_A = team_scores[0].text # away?
        self.score_B = team_scores[1].text # home?
        # print(self.score_A, self.score_B)
        openers = self.game_soup.find_all("span", {"data-cy": "odd-grid-opener-league"}) 
        #### getting the wager and opener ####
        if len(openers) == 4:
            wager_A, wager_B, opener_A, opener_B = [x.text for x in openers]
            # print(wager_A, wager_B, opener_A, opener_B)
            self.odds["wager"]["A"] = wager_A
            self.odds["wager"]["B"] = wager_B
            self.odds["opener"]["A"] = opener_A
            self.odds["opener"]["B"] = opener_B
        #### getting market odds ####
        # <span data-cy="odd-grid-league" class="pointer-2j4Dk margin-2SxKQ"><span class="">-275</span></span>
        odds_tags = self.game_soup.find_all("span", {"data-cy": "odd-grid-league"})
        for tag_A, tag_B, market in zip(odds_tags[::2], odds_tags[1::2], self.markets):
            self.odds[market]["A"] = tag_A.text
            self.odds[market]["B"] = tag_B.text
        return self.to_dict()
            
    def to_dict(self, missing=False):
        D = {
            "missing": missing,
            "date": self.date,
            "team_A": self.team_A,
            "team_B": self.team_B,
            "score_A": self.score_A,
            "score_B": self.score_B,
            "bet_type": self.bet_type,
        }
        for key in self.odds.keys():
            # includes odds for all markets as well as openers, wagers
            D["{}_A".format(key)] = self.odds[key]["A"]
            D["{}_B".format(key)] = self.odds[key]["B"]
        return D
    
class NonClassicLeagueData(LeagueData):
    """
    Takes in a webpage for all games on a given date, gives you everything
    """

    @staticmethod
    def get_empty_df():
        columns = ["date", "team_A", "team_B", "score_A", "score_B", 
                "wager_A", "wager_B", "opener_A", "opener_B", "bet_type"]
        return pd.DataFrame(columns=columns)
    
    def parse_all_games(self):
        results = []
        curr_league = None
        # game banner is <div class="eventMarketGridContainer-3QipG neverWrap-lD_Yj
        # compact-2-t2Y borderGrid-ctRnY">...<\div>
        row_soups = self.soup.find_all('div', class_=re.compile(r'^eventMarketGridContainer'))
        for row_soup in row_soups:
            game = NonClassicGameData(league=self.league, game_soup=row_soup, 
                                      bet_type=self.bet_type, date=self.date)
            try:
                results.append(game.parse_game())
            except MissingGame:
                print(MissingGame)
                continue
        if len(results) == 0:
            return self.get_empty_df()
        return pd.DataFrame(results)
    
#######################################
# Putting it all together
#######################################

class Scraper(object):

    def __init__(self, start_date, end_date, league, bet_types=["pointspread"], 
                max_tries=5, driver="firefox"):
        self.start_date = start_date
        self.end_date = end_date
        self.league = league
        self.bet_types = bet_types
        self.max_tries = max_tries

        if driver.lower().strip() == "phantomjs":
            self.driver = webdriver.PhantomJS()
            self.driver.set_window_size(1120, 550)
        elif driver.lower().strip() == "firefox":
            options = Options()
            options.headless = True
            self.driver = webdriver.Firefox(options=options)
            self.driver = webdriver.Firefox()
        else:
            assert False, driver
        self.data = []
        
    def get_league_date_url(self, datetime, bet_type, classic=False):
        str_date = str(datetime.date()).replace("-", "")
        if classic:
            if bet_type == "pointspread":
                return "https://classic.sportsbookreview.com/betting-odds/%s/?date=%s"%(self.league, str_date)
            return "https://classic.sportsbookreview.com/betting-odds/%s/%s?date=%s"%(self.league, bet_type, str_date)
        return "https://www.sportsbookreview.com/betting-odds/%s/%s/?date=%s" % (self.league, bet_type, str_date)

    def get_league_date_page(self, datetime, bet_type):
        str_date = str(datetime.date()).replace("-", "")
        url = "https://www.sportsbookreview.com/betting-odds/%s/%s/?date=%s" % (self.league, bet_type, str_date)
        self.driver.get(url)
        try:
            # check for whether no games occured on this date
            content = WebDriverWait(self.driver, 4).until(
                EC.presence_of_element_located((By.CLASS_NAME, "noEvents-1qOEP")) 
                # wish i could figure out a better element to track
            )
            html = self.driver.page_source
        except:
            try:
                # okay there's some content here, there was a game
                content = WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "oddsNumber-3fE_m")) 
                    # again, wish i could figure out a better element to track
                )
                html = self.driver.page_source
            except TimeoutException:
                print("yuck timed out")
                print(url)
                html = ""
        soup = BeautifulSoup(html, features="lxml")
        return soup
    
    def close_driver(self):
        self.driver.close()

    def scrape_info(self, datetime, bet_type):
        final_games_df = None
        for _ in range(self.max_tries):
            league_soup = self.get_league_date_page(datetime, bet_type)
            league_data = NonClassicLeagueData(league_soup, self.league, 
                                               bet_type, datetime.date())
            games_df = league_data.parse_all_games()
            if not games_df.isnull().all().any() or games_df.shape[0] == 0:
                return games_df
            if (final_games_df is None or 
                (games_df.isnull().sum().sum() > 
                 final_games_df.isnull().sum().sum())):
                # no it's probably better to union them somehow... whatever
                final_games_df = games_df
        return final_games_df

    def run_scraper(self):
        league_results = []
        for datetime in tqdm(pd.date_range(self.start_date, self.end_date)):
            for bet_type in self.bet_types:
                curr_df = self.scrape_info(datetime, bet_type)
                league_results.append(curr_df)
        return pd.concat(league_results)