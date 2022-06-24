import pandas as pd
import numpy as np

def parse_american_odds(x:pd.Series):
    fav_inds = x <= 0
    dog_inds = x > 0
    y = pd.Series(0, index=x.index)
    y.loc[fav_inds] = -1 * x / (100 - x)
    y.loc[dog_inds] = 100 / (100 + x)
    return y

def clean_bfo(bfo_df):
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

if __name__ == "__main__": 
    bfo_df = pd.read_csv("data/all_fighter_odds_2022-05-10.csv")
    bfo_df = clean_bfo(bfo_df)
    bfo_df.to_csv("data/bfo_fighter_odds.csv", index=False)
