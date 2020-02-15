from bs4 import BeautifulSoup
import requests
import time
import re

from tqdm import tqdm
import pandas as pd
from datetime import date, timedelta

class ResponseError(Exception):
    """
    Raised when requests fails to get a good response.
    """
    def __init__(self, url):
        """
        :param url: string url that we tried to get a response for and failed.
        """
        self.url = url

class MissingGame(Exception):

    def __init__(self, soup_index):
        self.soup_index = soup_index

class GameData(object):
    """
    Team identities, date of matchup, scores, and odds from the event-holder soup

    I should leave the scores to another source of data, which can also give
    me more detailed game-level data like when points were made and by which
    player. This is just to match point spreads to game ids.
    """

    markets = ["Pinnacle Sports",
               "5Dimes",
               "Bookmaker",
               "BetOnline",
               "Bovada",
               "Heritage",
               "Intertops",
               "YouWager",
               "JustBet",
               "Sportsbetting"]  # TODO expand this

    leagues = ["NFL",
               "NCAAF",
               "MLB",
               "NBA",
               "NCAAB",
               "NHL",
               "MLS",
               "UCL"
               ]

    def __init__(self, league, game_soup, soup_index, date=None):
        """
        :param league: str
        :param game_soup: BeautifulSoup Tag, should have class "event-holder holder-complete"
        :param soup_index: int, indicates which row on the page corresponds to this game.
        :param date: idk how to format this, but uniquely identifies the date. yah TODO TODO TODO TODO TODO
        :param url: str
        """
        self.league = league
        self.soup_index = soup_index
        self.date = date
        # Setting team names and scores
        team_names = game_soup.find_all("span", class_="team-name")
        if len(team_names) <= 1:
            raise MissingGame(soup_index)
        # ought to have some way of checking whether things are just completely blank
        # TODO 99% sure this is the right logic for who's home and who's away
        self.home_team = team_names[1].text
        self.away_team = team_names[0].text
        # self.home_team = clean_team_name(team_names[1].text)
        # self.away_team = clean_team_name(team_names[0].text)
        team_scores = game_soup.find_all("span",
                                         class_="current-score")  # this seems like a better way to go about it.
        self.home_score = team_scores[1].text
        self.away_score = team_scores[0].text
        self.market_spreads = {market: {"home": None, "away": None} for market in self.markets}
        self.market_money_lines = {market: {"home": None, "away": None} for market in self.markets}

    def get_game_id(self):
        return "%s_%s_%s_%s" % (self.league, self.home_team, self.away_team, self.date)

    def _update_odds(self, soup, bet_type):
        if bet_type == "pointspread":
            market_odds = self.market_spreads
        elif bet_type == "money-line":
            market_odds = self.market_money_lines
        else:
            assert False, "Acceptable bet_type: pointspread, money-line. Got %s" % bet_type
        game_soup = self.get_relevant_soup(soup)
        market_odds_tags = game_soup.find_all("div", class_="el-div eventLine-book")
        for market_name, market_tag in zip(self.markets, market_odds_tags):
            foo = market_tag.find_all("b")
            if len(foo) == 2:
                home_val, away_val = foo[1].text, foo[0].text
                market_odds[market_name]["home"] = home_val
                market_odds[market_name]["away"] = away_val
            else:
                # Sometimes sbr shits out on us and doesn't fill in a given market's data
                # whatever, skip it
                market_odds[market_name]["home"] = None
                market_odds[market_name]["away"] = None

    def update_market_spreads(self, soup):
        self._update_odds(soup, "pointspread")
        # some extra parsing, since point spreads come with money lines as well,
        # and the 1/2 character is annoying
        for market_name in self.markets:
            for key in ["home", "away"]:
                val = self.market_spreads[market_name][key]
                if val is not None and "\xa0" in val:
                    spread, money_line = val.split("\xa0")
                    spread = spread.replace("½", ".5")
                    money_line = money_line.replace("½", ".5")
                    self.market_spreads[market_name][key] = (spread, money_line)
                else:
                    self.market_spreads[market_name][key] = (None, None)

    def update_market_money_lines(self, soup):
        self._update_odds(soup, "money-line")

    def get_relevant_soup(self, soup):
        """
        Returns soup specific to this game, given a soup that corresponds to the league
        """
        # league_soups = soup.find_all(attrs={"league": self.league})
        # assert len(league_soups) == 1, "Found %d matches for league %s"%(len(league_soups), self.league)
        # league_soup = league_soups[0]
        game_soups = soup.find_all("div", class_="event-holder holder-complete")  # finished games
        return game_soups[self.soup_index]

    def dictify(self):
        """
        Useful for writing to one long CSV by plugging this all into a pandas dataframe
        """
        result = {"game_id": self.get_game_id(),
                  "league": self.league,
                  "date": self.date,
                  "home_team": self.home_team,
                  "away_team": self.away_team,
                  "home_score": self.home_score,
                  "away_score": self.away_score
                  }
        for market_name in self.markets:
            result[market_name + "_spread_home"] = self.market_spreads[market_name]["home"][0]
            result[market_name + "_spread_away"] = self.market_spreads[market_name]["away"][0]
            result[market_name + "_spread_money_home"] = self.market_spreads[market_name]["home"][1]
            result[market_name + "_spread_money_away"] = self.market_spreads[market_name]["away"][1]

            result[market_name + "_money_home"] = self.market_money_lines[market_name]["home"]
            result[market_name + "_money_away"] = self.market_money_lines[market_name]["away"]
        return result


class DateData(object):
    """
    Class that's great for scraping all the GameDatas for a given date, or multiple dates
    Methods for converting the scraped data to file formats, and splitting stuff
    """
    leagues = GameData.leagues

    def __init__(self):
        self.game_data_list = []
        self.dates_missed = []

    def grab_data_for_date(self, day=11, month=10, year=2019, verbose=False):
        """
        Appends game data for given date to internal game_data_list
        """
        str_date = str(year) + str(month).zfill(2) + str(day).zfill(2)
        spread_soup = get_date_page(day=day, month=month, year=year, bet_type="pointspread")
        money_line_soup = get_date_page(day=day, month=month, year=year, bet_type="money-line")
        for league in self.leagues:
            if len(spread_soup.find_all(attrs={"league": league})) != 1:
                continue
            league_spread_soup = spread_soup.find_all(attrs={"league": league})[0]
            league_money_line_soup = money_line_soup.find_all(attrs={"league": league})[0]
            game_soups = league_spread_soup.find_all("div",
                                                     class_="event-holder holder-complete")  # finished games
            for soup_index in (tqdm(range(len(game_soups))) if verbose
                               else range(len(game_soups))):
                game_soup = game_soups[soup_index]
                try:
                    game_data = GameData(league, game_soup, soup_index, date=str_date)
                    game_data.update_market_spreads(league_spread_soup)
                    game_data.update_market_money_lines(league_money_line_soup)
                    self.game_data_list.append(game_data)
                except MissingGame:
                    #print(MissingGame)
                    continue

    def get_league_game_data(self, league):
        return [game_data for game_data in self.game_data_list if game_data.league == league]

    def as_dataframe(self):
        return pd.DataFrame([game_data.dictify() for game_data in self.game_data_list])

    def to_csv(self, path):
        print("Dates missed:", self.dates_missed)
        return self.as_dataframe().to_csv(path)

    def grab_data_for_date_range(self, start_date, end_date):
        """
        start_date, end_date: datetime.date objects
        """
        delta = timedelta(days=1)
        date_range = [start_date + k * delta for k in range(int((end_date - start_date) / delta))]
        for curr_date in tqdm(date_range):
            try:
                self.grab_data_for_date(day=curr_date.day, month=curr_date.month, year=curr_date.year,
                                   verbose=False)
            except ResponseError:
                self.dates_missed.append(curr_date)


def get_date_page(day=11, month=10, year=2019, bet_type="pointspread"):
    str_date = str(year) + str(month).zfill(2) + str(day).zfill(2)
    url = "https://classic.sportsbookreview.com/betting-odds/%s/?date=%s" % (bet_type, str_date)
    response = requests.get(url)
    # TODO ought to have some timeout error if things are taking too long
    if response.ok:
        soup = BeautifulSoup(response.text, "html.parser")
        return soup
    else:
        raise ResponseError(url)


regex = re.compile('[^a-zA-Z]')
def clean_team_name(team_name):
    # Removes non-alphanumeric characters from team_name, except spaces in the middle
    return regex.sub("", team_name)