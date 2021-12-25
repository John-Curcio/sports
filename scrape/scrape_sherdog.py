"""
Okay I'm gonna have to do BFS through the graph of fights. 

I can either limit the depth of the search, or I can limit the date. 

Classes I need:
* Something for managing the queue/stack (BFS/DFS) of fighters to scrape matches for
    * Has to include and pass constraints for what/whether to scrape
* Fighter class
* Match class
"""

import threading, queue

# q = queue.Queue()

class FightSearch(object):
    """
    Handles breadth-first search
    """

    def __init__(self, max_depth=1, min_date="2021-01-01"):
        self.max_depth = max_depth 
        self.min_date = min_date
        # initialize empty variables 
        self.start_fighter = None
        self.fighter_queue = []
        self.fighters_seen = set()
        self.matches_seen = set()
        self.match_data = []
        self.fighter_data = []

    def search_over_fights(self, start_fighter_url):
        start_fighter = Fighter(start_fighter_url)
        fighter_queue = queue.Queue()
        fighter_queue.put(start_fighter)
        curr_depth = 0
        while (curr_depth < self.max_depth) and not fighter_queue.empty():
            print("curr_depth: {} approx # fighters in queue: {}".format(curr_depth, fighter_queue.qsize()))
            curr_fighter = fighter_queue.pop()
            curr_fighter.scrape_stats()
            self.fighter_data.append(curr_fighter)

            for match in curr_fighter.scrape_matches():
                if (match not in self.matches_seen) and (match.get_date() >= self.min_date):
                    self.match_data.append(match)
                    self.matches_seen.add(match)
                    next_fighter = Fighter(match.get_fighter_B_url())
                    if next_fighter not in self.fighters_seen:
                        fighter_queue.put(next_fighter)
            curr_depth += 1

    def get_match_data(self):
        return pd.DataFrame([m.to_dict() for m in self.match_data])
        
    def get_fighter_data(self):
        return pd.DataFrame([f.to_dict() for f in self.fighter_data])

class Fighter(object):

    def __init__(self, url):
        self.url = url 
        # initializing empty variables
        self.name = None
        self.birthdate = None 
        self.height = None 
        self.weight = None 
        self.weight_class = None 
        self.birthplace = None
        self.association = None

    def scrape_stats(self):
        # get static statistics, like birthdate and height and whatnot.
        pass 

    def scrape_matches(self):
        # get record of all matches that this fighter has been in
        pass

    def to_dict(self):
        return {
            "url": self.url, 
            "name": self.name, 
            "birthdate": self.birthdate,
            "height": self.height,
            "weight": self.weight,
            "weight_class": self.weight_class,
            "birthplace": self.birthplace,
            "association": self.association,
        } 

    ### useful for checking whether we've already scraped data for this fighter
    def __key__(self):
        return self.url

    def __hash__(self):
        return hash(self.__key__())

    def __eq__(self, other):
        if isinstance(other, Fighter):
            return self.__key__() == other.__key__()
        return NotImplemented    

class Match(object):

    def __init__(self, fighter_A, match_soup):
        # self.match_soup = match_soup
        self.fighter_A = fighter_A
        # empty vars 
        self.fighter_B = None
        self.result = None # for fighter_A 
        self.event = None 
        self.date = None 
        self.method = None 
        self.referee = None 
        self.round = None 
        self.time = None 
        self._scrape_match(match_soup)

    def _scrape_match(self, match_soup):
        # populate self.fighter_B, self.result, etc
        raise NotImplemented

    def get_date(self):
        return self.date

    def get_fighter_B_url(self):
        raise NotImplemented

    def to_dict(self):
        return {
            "fighter_A": self.fighter_A,
            "fighter_B": self.fighter_B,
            "result_A": self.result,
            "event": self.event,
            "date": self.date,
            "method": self.method,
            "referee": self.referee,
            "round": self.round,
            "time": self.time,
        }

    ### useful for checking whether we've already scraped data for this fight
    # Either from another fighter or this fighter, somehow
    def __key__(self):
        fighter_1, fighter_2 = sorted([self.fighter_A, self.fighter_B])
        return (self.date, fighter_1, fighter_2)

    def __hash__(self):
        return hash(self.__key__())

    def __eq__(self, other):
        if isinstance(other, Fight):
            return self.__key__() == other.__key__()
        return NotImplemented  



    