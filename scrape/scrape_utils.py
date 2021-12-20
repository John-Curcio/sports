from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import re
from bs4 import BeautifulSoup

from scrape.base_classes import GameData, LeagueData, MissingGame
from tqdm import tqdm
import pandas as pd

########################################
# sportsbookreview.com
# Use this to get scores and wager/opener
########################################

class NonClassicGameData(GameData):
    """
    Team identities, date of matchup, scores, and wager/opener for each game
    """
    
    def __init__(self, league, game_soup, bet_type, date=None):
        super().__init__(league, game_soup, bet_type, date)
        self.score_A = None
        self.score_B = None
        self.odds["wager"] = {"A": None, "B": None}
        self.odds["opener"] = {"A": None, "B": None}
        
    def parse_game(self):
        # print(self.game_soup.prettify())
        team_names = self.game_soup.find_all("span", {"class": re.compile(r"^participantBox")})
        if len(team_names) <= 1:
            raise MissingGame(self.league, self.bet_type, self.date, classic=False)
        self.team_A = team_names[0].text
        self.team_B = team_names[1].text
        # print(self.team_A, self.team_B)
        team_scores = self.game_soup.find_all("div", {"class": re.compile(r"^(scoreboardColumn-2OtpR finalScore)")})
        team_scores = team_scores[0].find_all("div")
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
        return self.to_dict()
            
    def to_dict(self):
        D = {
            "date": self.date,
            "team_A": self.team_A,
            "team_B": self.team_B,
            "score_A": self.score_A,
            "score_B": self.score_B,
            "bet_type": self.bet_type,
        }
        for key in self.odds.keys():
            D["{}_A".format(key)] = self.odds[key]["A"]
            D["{}_B".format(key)] = self.odds[key]["B"]
        return D
    
class NonClassicLeagueData(LeagueData):
    """
    Takes in a webpage for all games on a given date, gives you a collection of GameDatas
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

########################################
# classic.sportsbookreview.com
# Use this to get odds for each game
########################################

class ClassicGameData(GameData):
    """
    Team identities, date of matchup, scores, and odds for a specific game
    """
    
    def __init__(self, league, game_soup, bet_type, date=None):
        super().__init__(league, game_soup, bet_type, date)
        for market in self.markets:
            self.odds[market] = {"A": None, "B": None}
        
    def parse_game(self):
        team_names = self.game_soup.find_all("span", class_="team-name")
        if len(team_names) <= 1:
            raise MissingGame(self.league, self.bet_type, self.date, classic=True)
        # TODO 99% sure this is the right logic for who's home and who's away
        self.team_A = team_names[0].text # looks like away team
        self.team_B = team_names[1].text # looks like home team
        # print(self.team_A, self.team_B)
        
        # get opener, market odds for everything
        market_odds = self.game_soup.find_all("div", {"id": re.compile(r"^(eventLineBook)")})
        
        if len(market_odds) == 8:
            odds_text = [x.text for x in market_odds]
            odds_pairs = list(zip(odds_text[0::2], odds_text[1::2]))
            for i, (odds_A, odds_B) in enumerate(odds_pairs):
                curr_market = self.markets[i]
                self.odds[curr_market]["A"] = odds_A
                self.odds[curr_market]["B"] = odds_B
        return self.to_dict()
            
    def to_dict(self):
        D = {
            "date": self.date,
            "team_A": self.team_A,
            "team_B": self.team_B,
            "bet_type": self.bet_type,
        }
        for key in self.odds.keys():
            D["{}_A".format(key)] = self.odds[key]["A"]
            D["{}_B".format(key)] = self.odds[key]["B"]
        return D

class ClassicLeagueData(LeagueData):
    
    def parse_all_games(self):
        results = []
        curr_league = None
        # game banner is <div class="eventMarketGridContainer-3QipG neverWrap-lD_Yj
        # compact-2-t2Y borderGrid-ctRnY">...<\div>
        for row_soup in self.soup.find_all("div",
                                           class_="event-holder holder-complete"):  # finished games
            game = ClassicGameData(league=self.league, game_soup=row_soup, 
                                   bet_type=self.bet_type, date=self.date)
            try:
                results.append(game.parse_game())
            except MissingGame:
                print(MissingGame)
                continue
        if len(results) == 0:
            return self.get_empty_df()
        return pd.DataFrame(results)

    @staticmethod
    def get_empty_df():
        columns = ["date", "team_A", "team_B", "bet365_A", "bet365_B", "888sport_A", 
                   "888sport_B", "unibet_A", "unibet_B", "betway_A", "betway_B", "bet_type"]
        return pd.DataFrame(columns=columns)
    
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
        else:
            assert False, driver
        self.data = []
        
    def get_league_date_url(self, datetime, bet_type, classic):
        str_date = str(datetime.date()).replace("-", "")
        if classic:
            if bet_type == "pointspread":
                return "https://classic.sportsbookreview.com/betting-odds/%s/?date=%s"%(self.league, str_date)
            return "https://classic.sportsbookreview.com/betting-odds/%s/%s?date=%s"%(self.league, bet_type, str_date)
        return "https://www.sportsbookreview.com/betting-odds/%s/%s/?date=%s" % (self.league, bet_type, str_date)
    
    def get_league_date_page(self, datetime, bet_type, classic=False):
        url = self.get_league_date_url(datetime, bet_type, classic)
        print(url)
        self.driver.implicitly_wait(2) # seconds
        self.driver.get(url)
        html = self.driver.page_source
        soup = BeautifulSoup(html, features="lxml")
        return soup
    
    def close_driver(self):
        self.driver.close()
        
    def _scrape_info_helper(self, datetime, bet_type, classic):
        final_games_df = None
        for _ in range(self.max_tries):
            league_soup = self.get_league_date_page(datetime, bet_type, classic)
            league_data = None
            if classic:
                league_data = ClassicLeagueData(league_soup, self.league, 
                                                bet_type, datetime.date())
            else:
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

    def scrape_info(self, datetime, bet_type):
        print("trying nonclassic")
        nonclassic_df = self._scrape_info_helper(datetime, bet_type, False)
        print("trying classic")
        classic_df = self._scrape_info_helper(datetime, bet_type, True)
        # display(nonclassic_df.head())
        # display(classic_df.head())
        return classic_df.merge(nonclassic_df, on=["date", "team_A", "team_B", "bet_type"], how="outer")
    
    def run_scraper(self):
        league_results = []
        for datetime in pd.date_range(self.start_date, self.end_date):
            for bet_type in self.bet_types:
                print(datetime, bet_type)
                curr_df = self.scrape_info(datetime, bet_type)
                league_results.append(curr_df)
        return pd.concat(league_results)