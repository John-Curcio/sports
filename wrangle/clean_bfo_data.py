import pandas as pd
import numpy as np
from tqdm import tqdm
from db import base_db_interface

class BfoDataCleaner(object):

    def __init__(self):
        self.bfo_fighter_odds_df = base_db_interface.read("bfo_fighter_odds")
        self.bfo_event_odds_df = base_db_interface.read("bfo_event_odds")

        event_prop_html_df = base_db_interface.read("bfo_event_prop_html")
        prop_df = event_prop_html_df.rename(columns={
            "Unnamed: 0": "title",
        })
        prop_df["title_lower"] = prop_df["title"].str.lower()
        prop_df["EventHref"] = prop_df["url"].str[len("https://www.bestfightodds.com"):]
        self.bfo_prop_odds_raw_df = prop_df

        self.clean_close_df = None
        self.clean_fighter_odds_df = None 
        self.clean_fight_prop_df = None
        self.clean_event_prop_df = None

    def parse_prop_bfo(self):
        """
        Cleans all prop bets scraped from bestfightodds.com. Example pages:
        https://www.bestfightodds.com/events/ufc-285-2738
        https://www.bestfightodds.com/events/pfl-europe-week-1-2836
        https://www.bestfightodds.com/events/ufc-285-2738

        Returns:
            clean_event_prop_df: pd.DataFrame
                Dataframe with one row per event prop bet
            clean_fight_prop_df: pd.DataFrame
                Dataframe with one row per fight prop bet
        """
        prop_df = self.bfo_prop_odds_raw_df

        clean_fight_prop_rows = []
        clean_event_prop_rows = []
        for event_href, event_df in tqdm(prop_df.groupby("EventHref")):
            # get props for overall event
            event_prop_header = event_df["title_lower"] == "event props"
            if event_prop_header.any():
                event_prop_header_ind = event_df.index[event_prop_header][0]
                event_prop_df = event_df.loc[event_prop_header_ind+1:]
            else:
                event_prop_header_ind = event_df.index.max() + 1
                event_prop_df = pd.DataFrame(columns=event_df.columns)
            clean_event_prop_rows.append(event_prop_df)

            # get props for each fight
            event_fights_df = event_df.loc[:(event_prop_header_ind-1)]
            is_fighter = event_fights_df["FighterHref"].notnull()
            # pairs of fighters will have the same event_fight_id, and so 
            # will all following rows until the next pair of fighters
            # is encountered
            event_fight_id = (is_fighter.cumsum() + 1) // 2
            event_fights_df = event_fights_df.assign(event_fight_id=event_fight_id)

            fighter_href = event_fights_df\
                .groupby("event_fight_id")["FighterHref"]\
                .transform("first")
            opponent_href = event_fights_df\
                .groupby("event_fight_id")["FighterHref"]\
                .transform("last")
            
            n_props = event_fights_df\
                .groupby("event_fight_id")["Props.1"]\
                .transform("max")

            fight_prop_df = event_fights_df.loc[event_fights_df["FighterHref"].isnull()]
            if fight_prop_df.shape[0] > 0:
                fight_prop_df = fight_prop_df.assign(
                    FighterHref=fighter_href,
                    OpponentHref=opponent_href,
                    n_props=n_props,
                )
                clean_fight_prop_rows.append(fight_prop_df)
            else:
                # if there are no props for any fights, then we don't need to
                # add anything to clean_fight_prop_rows. On to the next event.
                continue

        clean_event_prop_df = pd.concat(clean_event_prop_rows)\
            .drop(columns=["FighterHref", "Props.1", "Props.2", "Props"])\
            .reset_index(drop=True)
        clean_fight_prop_df = pd.concat(clean_fight_prop_rows)\
            .drop(columns=["Props.1", "Props.2", "Props"])\
            .reset_index(drop=True)

        self.clean_event_prop_df = clean_event_prop_df
        self.clean_fight_prop_df = clean_fight_prop_df
        return (clean_event_prop_df, clean_fight_prop_df)

    def parse_all(self):
        self.clean_close_df = clean_event_bfo(self.bfo_event_odds_df)
        self.clean_fighter_odds_df = clean_fighter_bfo(self.bfo_fighter_odds_df)
        self.parse_prop_bfo()

def parse_american_odds(x:pd.Series):
    """
    Converts American odds to implied probabilities
    """
    x = x.str.replace("▼", "").str.replace("▲", "").astype(float)
    fav_inds = x <= 0
    dog_inds = x > 0
    y = pd.Series(np.nan, index=x.index)
    y.loc[fav_inds] = -1 * x / (100 - x)
    y.loc[dog_inds] = 100 / (100 + x)
    return y

def clean_event_bfo(event_df):
    event_df = event_df.drop(columns=[
        'Props', 'Props.1', 'Props.2', 'table_id',
    ])
    # for col in "DraftKings	BetMGM	Caesars	BetRivers	FanDuel	PointsBet	\
    #         Unibet	BetWay	5D	Ref Bet365".split():
    #     event_df[col] = event_df[col].str.replace("▼", "").str.replace("▲", "").astype(float)
    #     # event_df[col] = parse_american_odds(event_df[col])
    
    fighter_df = event_df.groupby(["match_id", "EventHref"]).first().reset_index()
    opponent_df = event_df.groupby(["match_id", "EventHref"]).last().reset_index()

    close_df = fighter_df.merge(
        opponent_df,
        on=["match_id", "EventHref"],
        how="inner",
        suffixes=("_fighter", "_opponent")
    ).rename(columns={
        "FighterHref_fighter": "FighterID", 
        "FighterHref_opponent": "OpponentID",
        "FighterName_fighter": "FighterName",
        "FighterName_opponent": "OpponentName",
    })
    close_df["FighterID"] = close_df["FighterID"]
    close_df["OpponentID"] = close_df["OpponentID"]
    close_df["EventHref"] = "/events/" + close_df["EventHref"]
    return close_df


def clean_fighter_bfo(bfo_df):
    """
    Get opening and closing money line information from data scraped from 
    the fighter pages on bestfightodds.com. Example page for Islam Makhachev:
    https://www.bestfightodds.com/fighters/Islam-Makhachev-5541
    """
    bfo_df["Date"] = pd.to_datetime(bfo_df["Date"])
    bfo_df["FighterName"] = bfo_df["FighterName"].str.lower().str.strip()
    bfo_df["OpponentName"] = bfo_df["OpponentName"].str.lower().str.strip()
    bfo_df = bfo_df.rename(columns={
        "FighterHref": "FighterID",
        "OpponentHref": "OpponentID",
    }).dropna(subset=[
        "Date", "FighterID", "OpponentID", "FighterOpen", "OpponentOpen",
        "FighterName", "OpponentName",
    ])
    bfo_df["FighterID"] = bfo_df["FighterID"].str.split("/fighters/").str[1]
    bfo_df["OpponentID"] = bfo_df["OpponentID"].str.split("/fighters/").str[1]
    # to get the closing money line, get the "most favorable" odds for each fighter, 
    # then get implied prob most favorable is odds with biggest payout, ie gives 
    # the fighter the lowest prob
    p_fighter = parse_american_odds(bfo_df["FighterOpen"])
    p_opponent = parse_american_odds(bfo_df["OpponentOpen"])
    bfo_df["p_fighter_open_implied"] = p_fighter / (p_fighter + p_opponent)
    bfo_df["p_opponent_open_implied"] = p_opponent / (p_fighter + p_opponent)

    p_fighter_close = np.minimum(
        parse_american_odds(bfo_df["FighterCloseLeft"]),
        parse_american_odds(bfo_df["FighterCloseRight"]),
    )
    p_opponent_close = np.minimum(
        parse_american_odds(bfo_df["OpponentCloseLeft"]),
        parse_american_odds(bfo_df["OpponentCloseRight"]),
    )
    bfo_df["p_fighter_close_implied"] = p_fighter_close / (p_fighter_close + p_opponent_close)
    bfo_df["p_opponent_close_implied"] = p_opponent_close / (p_fighter_close + p_opponent_close)
    return bfo_df

def clean_all_bfo(fighter_df, event_df):
    """
    This function joins fighter_df and event_df. fighter_df contains the opening
    odds for each fight, scraped from the fighters' pages. event_df contains the
    closing odds for each fight, scraped from the event pages.
    """
    open_df = clean_fighter_bfo(fighter_df)
    close_df = clean_event_bfo(event_df)
    close_df = close_df.drop(columns=["FighterName", "OpponentName"])
    # join open_df and close_df
    full_df_1 = open_df.merge(
        close_df,
        how="inner",
        left_on=["FighterID", "OpponentID", "EventHref"],
        right_on=["FighterID", "OpponentID", "EventHref"],
    )
    # if the order of the fighters is different btw open_df and close_df, swap them
    # remember to swap {market}_fighter and {market}_opponent
    rename_dict = {
        "FighterID": "OpponentID",
        "OpponentID": "FighterID",
    }
    for market in ["5D", "Bet365", "BetMGM", "BetRivers", "BetWay", 
        "Caesars", "DraftKings", "FanDuel", "PointsBet", "Ref", "Unibet"]:
        rename_dict[market + "_fighter"] = market + "_opponent"
        rename_dict[market + "_opponent"] = market + "_fighter"
    close_df_switch = close_df.rename(columns=rename_dict)

    full_df_2 = open_df.merge(
        close_df_switch,
        how="inner",
        left_on=["FighterID", "OpponentID", "EventHref"],
        right_on=["FighterID", "OpponentID", "EventHref"],
    ).rename(columns={
        "FighterID_x": "FighterID",
        "OpponentID_x": "OpponentID",
    })
    full_df = pd.concat([full_df_1, full_df_2]).reset_index(drop=True)
    # finally, left join to ensure we have aren't missing any opening odds
    close_cols = [
        '5D_fighter', '5D_opponent',
        'Bet365_fighter', 'Bet365_opponent',
        'BetMGM_fighter', 'BetMGM_opponent',
        'BetRivers_fighter', 'BetRivers_opponent',
        'BetWay_fighter', 'BetWay_opponent',
        'Caesars_fighter', 'Caesars_opponent',
        'DraftKings_fighter', 'DraftKings_opponent',
        'FanDuel_fighter', 'FanDuel_opponent',
        'PointsBet_fighter', 'PointsBet_opponent',
        'Ref_fighter', 'Ref_opponent',
        'Unibet_fighter', 'Unibet_opponent',
    ]
    full_df_clean = open_df.merge(
        full_df[close_cols + ["FighterID", "OpponentID", "EventHref"]],
        on=["FighterID", "OpponentID", "EventHref"],
        how="left",
    )
    return full_df_clean

# if __name__ == "__main__": 
#     fighter_df = pd.read_csv("data/all_fighter_odds_2022-07-16.csv")
#     event_df = pd.read_csv("data/bfo_event_odds_2022-07-16.csv")
#     bfo_df = clean_all_bfo(fighter_df, event_df)
#     bfo_df.to_csv("data/bfo_open_and_close_odds.csv", index=False)
