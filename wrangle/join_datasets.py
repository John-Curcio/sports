import pandas as pd
import numpy as np
from wrangle.base_maps import *
from db import base_db_interface


class IsomorphismFinder(object):
    """
    Learn map btw FighterIDs in df1 and FighterIDs in df2

    TODO: IIRC we prefer that it's a bijection ofc, but may have 
    to settle for a surjection, and I think it's supposed to be 
    FighterID1 --> FighterID2. But I'm not sure!!!
    """
    
    def __init__(self, df1, df2, manual_map=None):
        self.df1 = self.get_double_df(df1)
        self.df2 = self.get_double_df(df2)
        self.frontier_fighter_id1_vals = pd.concat([self.df1["FighterID"], 
                                                    self.df1["OpponentID"]])
        self.frontier_fighter_id2_vals = pd.concat([self.df2["FighterID"], 
                                                    self.df2["OpponentID"]])
        for df in [self.df1, self.df2]:
            for col in ["FighterName", "OpponentName"]:
                df[col] = self.clean_names(df[col])
        self.fighter_id_map = pd.Series(dtype='object')
        if manual_map is not None:
            index, vals = zip(*manual_map.items()) # gets keys and values of dict respectively
            self.fighter_id_map = pd.Series(vals, index=index, dtype='object')
        self.conflict_fights = None
        
    @staticmethod
    def get_double_df(df):
        # edges are bidirectional
        fight_id = IsomorphismFinder.get_fight_id(df)
        df = df[["Date", "FighterID", "OpponentID", "FighterName", "OpponentName"]]\
            .assign(fight_id = fight_id)\
            .drop_duplicates("fight_id")
        df_complement = df.rename(columns={
            "FighterID":"OpponentID", "OpponentID":"FighterID",
            "FighterName":"OpponentName", "OpponentName":"FighterName",
        })
        df_doubled = pd.concat([df, df_complement]).reset_index(drop=True)
        return df_doubled

    @staticmethod
    def get_fight_id(df):
        max_id = np.maximum(df["FighterID"], df["OpponentID"])
        min_id = np.minimum(df["FighterID"], df["OpponentID"])
        return df["Date"].astype(str) + "_" + min_id + "_" + max_id

    
    @staticmethod
    def get_fight_id(df):
        max_id = np.maximum(df["FighterID"], df["OpponentID"])
        min_id = np.minimum(df["FighterID"], df["OpponentID"])
        return df["Date"].astype(str) + "_" + min_id + "_" + max_id
    
    def _catch_conflicts_in_merge(self, df):
        """
        Function for catching "conflicts" in the merge: cases where
        a FighterID1 maps to multiple FighterID2s. This is not allowed!
        """
        counts = df.groupby("FighterID1")["FighterID2"].nunique()
        if any(counts > 1):
            print(f"Found {sum(counts > 1)} conflicts")
            conflict_fighter_id1s = counts[counts > 1].index
            conflict_fighter_names = df["FighterName"]\
                .loc[df["FighterID1"].isin(conflict_fighter_id1s)]\
                .unique()
            print(f"fighter names with conflicts: {conflict_fighter_names}")
            self.conflict_fights = df.loc[df["FighterID1"].isin(conflict_fighter_id1s)]\
                .sort_values("Date")
            print(self.conflict_fights)
            raise Exception("Clean up conflicts with these FighterIDs in df2, then try again")
        return None
    
    def find_base_map(self):
        """
        This is like the base case of the find_isomorphism loop. We 
        start by inner joining df1 and df2 on Date, FighterName, and 
        OpponentName. These are rows where fighters have the same names
        as in df1 and df2. It is extremely unlikely that in reality, two 
        pairs of fighters with the same names fought on the same day. 
        In find_isomorphism, we take this as ground truth, and then 
        broadcast outwards. 
        """
        cols = ["Date", "FighterName", "OpponentName", "FighterID", "OpponentID"]
        overlapping_fights = self.df1[cols].merge(
            self.df2[cols],
            how="inner", 
            on=["Date", "FighterName", "OpponentName"],
            suffixes=("1", "2"),
        ) 
        ####
        overlapping_fights2 = self.df1[cols].merge(
            self.df2[cols],
            how="inner",
            left_on=["Date", "FighterName", "OpponentName"],
            right_on=["Date", "OpponentName", "FighterName"],
        )
        overlapping_fights = pd.concat([overlapping_fights, overlapping_fights2])
        ####
        self._catch_conflicts_in_merge(overlapping_fights)
        temp_map = overlapping_fights.groupby("FighterID1")["FighterID2"].first()
        self.fighter_id_map = self.fighter_id_map.combine_first(temp_map) 
    
    def find_isomorphism(self, n_iters=3):
        """
        This is where the magic happens. We want to learn the full
        mapping btw IDs in df1 and df2. We do this with a greedy, iterative
        process.
        
        * We start with a base map, which is just inner-joining on 
          fighter names and date. That gives us a subset of the full 
          mapping btw IDs in df1 and df2, which we want to learn.
        * Suppose U1, U2 are unknown fighter IDs in df1 and df2, respectively.
          In df1, U1 fought a, b, c, and d, all of whom we know the mapping for.
          In df2, U2 fought a, b, c, and d as well. And furthermore, U2 did it 
          on the same dates as U1. 
          It's probably the case that U1 and U2 are the same guy!
        """
        self.find_base_map()
        for _ in range(n_iters):
            print(f"iteration {_} of {n_iters}. map has size {len(self.fighter_id_map)} fighters mapped")
            # update mapping greedily
            # find missing fighter_id1 with most fights with known opponent_id1
            # okay, find fighter_id1s with missing fighter_id2s
            # then for each of these, find # fights with known opponent_id2s
            missing_fighter_id1 = ~self.df1["FighterID"].isin(self.fighter_id_map.index)
            known_opponent_id2 = self.df1["OpponentID"].isin(self.fighter_id_map.index)
            df1_sub = self.df1.loc[missing_fighter_id1 & known_opponent_id2]
            # okay, let's figure out what df2 is calling this fighter
            df1_sub = df1_sub.rename(columns={"OpponentID":"OpponentID1"})
            df1_sub["OpponentID2"] = df1_sub["OpponentID1"].map(self.fighter_id_map)
            df_inner = df1_sub.merge(
                self.df2, how="inner", 
                left_on=["Date", "OpponentID2"],
                right_on=["Date", "OpponentID"],
                suffixes=("1", "2"),
            ).rename(columns={"FighterName1": "FighterName", "OpponentName1": "OpponentName"})
            self._catch_conflicts_in_merge(df_inner)
            temp_map = df_inner.groupby("FighterID1")["FighterID2"].first()
            self.fighter_id_map = self.fighter_id_map.combine_first(temp_map)
            if len(df_inner) == 0:
                self.stray_fights = df1_sub
                break
        return self.fighter_id_map
    
    @staticmethod
    def clean_names(names):
        to_replace, value = zip(*NAME_REPLACE_DICT.items()) # gets keys and values of dict respectively
        names = names.fillna("").str.strip().str.lower()\
                .replace(to_replace=to_replace, value=value)
        return names

def join_ufc_and_espn(ufc_df, espn_df, ufc_espn_fighter_id_map):
    """
    fighter_id_map: mapping UFC ID --> ESPN ID
    """
    def get_fight_id(df):
        max_id = np.maximum(df["FighterID"].fillna("unknown"), 
                            df["OpponentID"].fillna("unknown"))
        min_id = np.minimum(df["FighterID"].fillna("unknown"), 
                            df["OpponentID"].fillna("unknown"))
        return df["Date"].astype(str) + "_" + min_id + "_" + max_id
    # okay, let's just create a fight_id for each, then join on fight_id
    ufc_df = ufc_df.assign(
        FighterID=ufc_df["FighterID"].map(ufc_espn_fighter_id_map),
        OpponentID=ufc_df["OpponentID"].map(ufc_espn_fighter_id_map),
    )
    print(ufc_df.loc[ufc_df[["FighterID", "OpponentID"]].isnull().any(1), 
                    ["FighterName", "FighterID", "OpponentName", "OpponentID", "Date"]].sort_values("Date"))
    print(ufc_df.loc[ufc_df[["FighterID", "OpponentID"]].isnull().any(1), ["Date"]].value_counts())
    ufc_df = ufc_df.assign(fight_id=get_fight_id(ufc_df))
    espn_df = espn_df.assign(fight_id=get_fight_id(espn_df))
    # ufc_df = ufc_df.drop(columns=['Date', 'FighterID', 'OpponentID'])
    # ufc_df = ufc_df.drop(columns=['FighterID', 'OpponentID'])\
    #     .rename(columns={"Date": "Date_ufc"})

    def get_deduped(df):
        # remove duplicates, keeping the row with the fewest missing values
        return df.assign(n_missing=df.isnull().sum(1))\
            .sort_values("n_missing", ascending=True)\
            .drop_duplicates(subset="fight_id", keep="first")
    espn_df = get_deduped(espn_df)
    ufc_df = get_deduped(ufc_df)
    # deliberately add duplicates to ufc df: want to make sure fighter
    # and opponent stats are matched up
    col_map = dict()
    for col in ufc_df.columns:
        if col.startswith("Fighter"):
            fighter_col, opp_col = col, "Opponent" + col[len("Fighter"):]
            col_map[fighter_col] = opp_col
            col_map[opp_col] = fighter_col
        if col.endswith("_opp"):
            opp_col, fighter_col = col, col[:-len("_opp")]
            col_map[fighter_col] = opp_col
            col_map[opp_col] = fighter_col
    ufc_df2 = ufc_df.rename(columns=col_map)
    ufc_df = pd.concat([ufc_df, ufc_df2]).reset_index(drop=True)
    ufc_df_cols = [
        "fight_id", "time_dur", "max_time", "weight_bout",
        "method_description", "round_description", "time_description",
        "time_format", "referee", "details_description",
        "weight_class", "method", "round", "location", "img_png_url",
        "is_title_fight", "EventUrl",
        *col_map.keys()
    ]
    return espn_df.merge(ufc_df[ufc_df_cols], 
        on=["fight_id", "FighterID", "OpponentID"], how="left", 
        suffixes=("_espn", "_ufc"))

def join_espn_and_bfo(espn_df, bfo_df, espn_bfo_fighter_id_map):
    def get_fight_id(df):
        max_id = np.maximum(df["FighterID"].fillna("unknown"), 
                            df["OpponentID"].fillna("unknown"))
        min_id = np.minimum(df["FighterID"].fillna("unknown"), 
                            df["OpponentID"].fillna("unknown"))
        return df["Date"].astype(str) + "_" + min_id + "_" + max_id
    # okay, let's just create a fight_id for each, then join on fight_id
    espn_df = espn_df.assign(
        espn_fight_id=espn_df["fight_id"],
        espn_fighter_id=espn_df["FighterID"],
        espn_opponent_id=espn_df["OpponentID"],
        FighterID=espn_df["FighterID"].map(espn_bfo_fighter_id_map),
        OpponentID=espn_df["OpponentID"].map(espn_bfo_fighter_id_map),
    )
    espn_df = espn_df.assign(fight_id=get_fight_id(espn_df))
    bfo_df = bfo_df.assign(fight_id=get_fight_id(bfo_df))\
        .drop(columns=["FighterName", "OpponentName", "Event"])
    # remove duplicates in BFO, keeping the row with the fewest missing values
    bfo_df = bfo_df.assign(n_missing=bfo_df.isnull().sum(1))\
        .sort_values("n_missing", ascending=True)\
        .drop_duplicates(subset="fight_id", keep="first")\
        .drop(columns=["n_missing"])
    # okay, now i deliberately add duplicates. Want to make sure the fighter
    # and opponent are matched with their respective odds!
    rename_dict = {
        "FighterID": "OpponentID",
        "FighterOpen": "OpponentOpen",
        "FighterCloseLeft": "OpponentCloseLeft",
        "FighterCloseRight": "OpponentCloseRight",
        "p_fighter_open_implied": "p_opponent_open_implied",
        "OpponentID": "FighterID",
        "OpponentOpen": "FighterOpen",
        "OpponentCloseLeft": "FighterCloseLeft",
        "OpponentCloseRight": "FighterCloseRight",
        "p_opponent_open_implied": "p_fighter_open_implied",
    }
    # for market in ["5D", "Bet365", "BetMGM", "BetRivers", "BetWay", 
    #     "Caesars", "DraftKings", "FanDuel", "PointsBet", "Ref", "Unibet"]:
    #     rename_dict[market + "_fighter"] = market + "_opponent"
    #     rename_dict[market + "_opponent"] = market + "_fighter"
    # bfo_df2 = bfo_df.rename(columns=rename_dict)
    bfo_duped_df = pd.concat([bfo_df, bfo_df2]).reset_index(drop=True)
    return espn_df.merge(bfo_duped_df, 
                         on=["fight_id", "Date", "FighterID", "OpponentID"], 
                         how="left")

def main():
    espn_df = base_db_interface.read("espn_data")
    ufc_df = base_db_interface.read("ufc_stats_df")
    bfo_df = base_db_interface.read("bfo_open_odds")

    # find mapping btw ufc IDs and espn IDs
    iso_finder = IsomorphismFinder(ufc_df, espn_df, MANUAL_UFC_ESPN_MAP)
    iso_finder.find_isomorphism(n_iters=20)
    
    # okay great, now that we have the mapping, let's join ufc data and espn data
    ufc_espn_df = join_ufc_and_espn(ufc_df, espn_df, iso_finder.fighter_id_map)

    bfo_df_clean = bfo_df.assign(
        FighterID=bfo_df["FighterID"].replace(to_replace=MANUAL_BFO_OVERWRITE_MAP),
        OpponentID=bfo_df["OpponentID"].replace(to_replace=MANUAL_BFO_OVERWRITE_MAP),
    )
    # these fights didn't end up happening
    drop_pairs = [
        ('/fighters/Gabriel-Bonfim-11752', '/fighters/Carlos-Leal-Miranda-7744'),
        ('/fighters/Gabriel-Bonfim-11752', '/fighters/Diego-Dias-11750'),
    ]
    drop_inds = np.any([
        bfo_df_clean["FighterID"].isin(drop_pair) & bfo_df_clean["OpponentID"].isin(drop_pair)
        for drop_pair in drop_pairs
    ], axis=0)
    bfo_df_clean = bfo_df_clean.loc[~drop_inds]

    bfo_iso_finder = IsomorphismFinder(espn_df, bfo_df_clean, MANUAL_ESPN_BFO_MAP)
    bfo_iso_finder.find_isomorphism(n_iters=20)

    bfo_ufc_espn_df = join_espn_and_bfo(ufc_espn_df, bfo_df_clean, bfo_iso_finder.fighter_id_map)
    print(bfo_ufc_espn_df.shape)
    bfo_ufc_espn_df.to_csv("data/full_bfo_ufc_espn_data.csv", index=False)
    return bfo_ufc_espn_df

if __name__ == "__main__":
    main()