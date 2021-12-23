from abc import ABC, abstractmethod

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

    def __init__(self, league, bet_type, date, classic):
        self.league = league 
        self.bet_type = bet_type
        self.date = date 
        self.classic = classic

    def __repr__(self):
        classic_str = "classic" if classic else "nonclassic"
        return "Missing Game: {} {} {} {}".format(
            self.league, self.bet_type, self.date, classic_str,
        )
        
class GameData(ABC):
    
    markets = [
        "bet365",
        "888sport",
        "unibet",
        "betway",
    ]
    
    def __init__(self, league, game_soup, bet_type, date):
        self.league = league
        self.game_soup = game_soup
        self.bet_type = bet_type
        self.date = date
        # initializing empty variables
        self.team_A = None
        self.team_B = None
        self.odds = dict()
        
    @abstractmethod
    def parse_game(self):
        pass
    
class LeagueData(ABC):
    
    def __init__(self, league_soup, league, bet_type, date):
        self.soup = league_soup
        self.league = league
        self.bet_type = bet_type
        self.date = date
    
    @abstractmethod
    def parse_all_games(self):
        pass