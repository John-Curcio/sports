import pandas as pd
import numpy as np
from wrangle.base_maps import *
from db import base_db_interface


class IsomorphismFinder(object):
    """
    Learn map btw FighterIDs in df_canon and FighterIDs in df_aux

    Objective is to learn mapping FighterID_aux --> FighterID_canon.
    df_aux might have multiple FighterIDs that correspond to the same
    FighterID_canon, so we need to learn a surjection.

    Attributes:
    * df_canon 
    * df_aux
    TODO I don't think I do anything with these two attributes, so
    maybe I should get rid of them.
    * frontier_fighter_id_canon_vals
    * frontier_fighter_id_aux_vals
    * fighter_id_map: pd.Series mapping FighterID_aux --> FighterID_canon
    * conflict_fights
    """

    def __init__(self, df_canon, df_aux, manual_map=None):
        """
        * df_canon - dataframe with columns "FighterID", "OpponentID",
            "FighterName", "OpponentName", "Date". This is the "canonical"
            dataframe, i.e. the one with the more reliable FighterIDs.
        * df_aux - dataframe with columns "FighterID", "OpponentID",
            "FighterName", "OpponentName", "Date". This is the "auxiliary"
            dataframe, i.e. the one with the less reliable FighterIDs.
        * manual_map - dictionary mapping FighterID_aux --> FighterID_canon.
            This is a manual mapping that we can use to help the algorithm
            learn the mapping, starting from manual_map as ground truth.
            manual_map will be smaller than the full mapping, because
            manual_map only contains mappings for a few FighterID pairs 
            that we somehow already know.
        """
        # print("line 41:", (df_canon["FighterName"].str.strip() == "").mean())
        # print("line 42:", (df_canon["FighterName"].isnull()).mean())
        # print("line 43:", (df_canon["OpponentName"].str.strip() == "").mean())
        # print("line 44:", (df_canon["OpponentName"].isnull()).mean())
        self.df_canon = self.get_double_df(df_canon)
        self.df_aux = self.get_double_df(df_aux)
        # self.frontier_fighter_id_canon_vals = pd.concat([
        #     self.df_canon["FighterID"],
        #     self.df_canon["OpponentID"]
        # ])
        # self.frontier_fighter_id_aux_vals = pd.concat([
        #     self.df_aux["FighterID"],
        #     self.df_aux["OpponentID"]
        # ])
        # print("line 52:", (self.df_canon["FighterName"] == "").sum())
        for col in ["FighterName", "OpponentName"]:
            self.df_canon[col] = self.clean_names(self.df_canon[col])
            self.df_aux[col] = self.clean_names(self.df_aux[col])
        # print("line 59:", (self.df_canon["FighterName"] == "patrick trey smith").sum())
        # print("line 60:", (self.df_aux["FighterName"] == "patrick trey smith").sum())
        # print("line 56:", (self.df_canon["FighterName"] == "").sum())
        self.fighter_id_map = pd.Series(dtype='object')
        if manual_map is not None:
            index, vals = zip(*manual_map.items())
            self.fighter_id_map = pd.Series(vals, index=index, dtype='object')
        self.conflict_fights = None
        
    @staticmethod
    def get_double_df(df):
        """
        Make sure that each fight is represented exactly twice in the 
        dataframe, once for each fighter as "Fighter" and then again
        as "Opponent". This makes it easier to join.
        Also, drop fights with missing FighterID or OpponentID.
        * df - dataframe with columns "FighterID", "OpponentID",
            "FighterName", "OpponentName", "Date"
        """
        df = df.dropna(subset=["FighterID", "OpponentID"])
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
        """
        Get unique fight ID for each fight in df
        * df - dataframe with columns "FighterID", "OpponentID", "Date"
        """
        max_id = np.maximum(df["FighterID"], df["OpponentID"])
        min_id = np.minimum(df["FighterID"], df["OpponentID"])
        return df["Date"].astype(str) + "_" + min_id + "_" + max_id
    
    def _catch_conflicts_in_merge(self, df):
        """
        So we want to learn a mapping btw FighterID_aux and FighterID_canon.
        We represent this by merging df_aux and df_canon, and for each
        fight, we have FighterID_aux and FighterID_canon for the Fighter
        and the Opponent.
        If the same FighterID_aux maps to multiple FighterID_canons, then
        we have a conflict. This function checks for conflicts.

        TODO: this error message could be a lot more informative!
        """
        # we want to learn a mapping FighterID_aux -> FighterID_canon
        # can't have one FighterID_aux map to multiple FighterID_canon
        counts = df.groupby("FighterID_aux")["FighterID_canon"].nunique()
        if any(counts > 1):
            print(f"Found {sum(counts > 1)} conflicts")
            conflict_fighter_id_auxs = counts[counts > 1].index
            conflict_fighter_names = df["FighterName_canon"]\
                .loc[df["FighterID_canon"].isin(conflict_fighter_id_auxs)]\
                .unique()
            print(f"fighter names with conflicts: {conflict_fighter_names}")
            self.conflict_fights = df.loc[df["FighterID_aux"].isin(conflict_fighter_id_auxs)]\
                .sort_values("Date")
            cols = [
                "Date", "FighterName_canon", "FighterName_aux", 
                "FighterID_aux", #"FighterID_canon",
                "OpponentName_canon", "OpponentName_aux",
                "OpponentID_aux", #"OpponentID_canon"
            ]
            print(self.conflict_fights[cols])
            # print(self.conflict_fights.drop(columns=["fight_id_canon", "fight_id_aux"]))
            raise Exception("Found conflicts")
        return None
    
    def find_base_map(self):
        """
        This is like the base case of the find_isomorphism loop. 
        * We start by inner joining df_aux and df_canon on Date, FighterName, 
        and OpponentName. These are rows where fighters have the same names
        as in df_aux and df_canon. It is extremely unlikely that in reality, two 
        pairs of fighters with the same names fought on the same day. 
        In find_isomorphism, we take this as ground truth, and then 
        broadcast outwards. 
        * 
        """
        cols = ["Date", "FighterName", "OpponentName", "FighterID", "OpponentID"]
        overlapping_fights = self.df_canon[cols].merge(
            self.df_aux[cols],
            how="inner", 
            on=["Date", "FighterName", "OpponentName"],
            suffixes=("_canon", "_aux"),
        )
        self._update_fighter_id_map(overlapping_fights)

    def _update_fighter_id_map(self, df):
        """
        Update self.fighter_id_map based on df, checking for conflicts
        and throwing an error if there are any.
        * df - dataframe with columns "FighterID_aux", "FighterID_canon"
        """
        # check to see whether df has duplicate FighterID_aux -> FighterID_canon mappings
        self._catch_conflicts_in_merge(df)
        # if df has no conflicts within itself, then we can safely
        # use .first() to find the mapping FighterID_aux -> FighterID_canon
        map_update = df.groupby("FighterID_aux")["FighterID_canon"].first()
        # then, check to see whether map_update conflicts with fighter_id_map
        idx = self.fighter_id_map.index.intersection(map_update.index)
        conflicts = self.fighter_id_map.loc[idx].compare(map_update.loc[idx])
        if len(conflicts) > 0:
            print(conflicts)
            raise Exception("Found conflicts btw map_update and fighter_id_map")
        # if no conflicts, then update fighter_id_map
        self.fighter_id_map = self.fighter_id_map.combine_first(map_update)
    
    def get_tournament_dates(self, df):
        """
        A minority of these fights were held in a tournament format, 
        where fighters fought multiple opponents on the same day. This makes
        it harder to learn the mapping btw FighterID_aux and FighterID_canon.
        So we'd like to identify fights that were held in tournament events, 
        and then treat them differently in find_isomorphism.
        """
        # get Date, FighterID counts of opponents
        counts = df.groupby(["Date", "FighterID"])["OpponentID"].nunique()
        # then, for (Date, FighterID) pairs with > 1 opponent, we get 
        # the dates of those fights, which we return
        return counts[counts > 1].index.get_level_values("Date").unique()

    def _find_frontier_of_map(self, df_aux, df_canon):
        """
        Given that self.fighter_id_map maps FighterID_aux to FighterID_canon correctly,
        find the frontier of the map. The frontier is the set of FighterID_auxs
        for which we don't know the corresponding FighterID_canon, but for whom 
        we know the FighterID_canon of their opponents. 

        Returns the result of a merge between df_aux and df_canon, 
        """
        # find mystery fighter_id_auxs with fights with opponents 
        # for whom we know opponent_id_aux -> opponent_id_canon
        # call this the "frontier" since it's at the edge of what we know
        unknown_fighter_id = ~df_aux["FighterID"].isin(self.fighter_id_map.index)
        known_opponent_id = df_aux["OpponentID"].isin(self.fighter_id_map.index)
        df_aux_frontier = df_aux.loc[unknown_fighter_id & known_opponent_id].copy()
        print(f"df_aux_frontier has {len(df_aux_frontier)} rows")
        # okay, now we actually add opponent_id_canon to df_aux_frontier
        # to facilitate the upcoming merge
        df_aux_frontier = df_aux_frontier.rename(columns={"OpponentID":"OpponentID_aux"})
        df_aux_frontier["OpponentID_canon"] = df_aux_frontier["OpponentID_aux"].map(self.fighter_id_map)
        # okay here's where the magic happens. df_aux_frontier is the 
        # set of fights where we know the opponent_id_canon, but not the
        # fighter_id_canon. If we inner join df_aux_frontier and df_canon
        # on date and opponent_id_canon, then those fights are linked. 
        # fighter_id_aux must therefore correspond to fighter_id_canon.
        df_inner = df_aux_frontier.merge(
            df_canon, how="inner", 
            left_on=["Date", "OpponentID_canon"],
            right_on=["Date", "OpponentID"],
            suffixes=("_aux", "_canon"),
        )#.rename(columns={"FighterName_aux": "FighterName", "OpponentName_aux": "OpponentName"})
        print(f"df_inner has {len(df_inner)} rows")
        return df_inner

    def find_isomorphism(self, n_iters=3):
        """
        This is where the magic happens. We want to learn the full
        mapping btw IDs in df_aux --> df_canon, which is stored in self.fighter_id_map. 
        We do this with a greedy, iterative process.

        * "Base" step: We start by running find_base_map(), which is just 
        inner-joining on fighter names and date. That gives us a subset of the full 
        mapping btw IDs in df_aux --> df_canon, which we want to learn.
        * "Propagate" step: Based on this subset of the mapping, we find FighterIDs 
        in df_aux and df_canon that fought a lot of known opponents. We then try to 
        link these FighterIDs to each other. For example:

            * Suppose U_aux, U_canon are unknown fighter IDs in df_aux and df_canon, 
            respectively.
            * In df_aux, U_aux fought a, b, c, and d, all of whom we know the mapping for.
            In df_canon, U_canon fought a, b, c, and d as well. And furthermore, 
            * U_canon did it on the same dates as U_aux. 
            * It's probably the case that U_aux and U_canon are the same guy!
            * So we add U_aux -> U_canon to our mapping.
        
        At the end, we check for conflicts. If there are conflicts, we print
        out the conflicting fights and raise an exception. If not, update 
        the mapping and repeat.

        TODO: this propagation step might be too eager. I might want to try
        to be more conservative, and only add a mapping for one fighter at a time.
        That one fighter would be the one with the most fights with known opponents.
        """
        self.find_base_map()
        # get dates of tournament fights
        aux_tournament_dates = self.get_tournament_dates(self.df_aux)
        canon_tournament_dates = self.get_tournament_dates(self.df_canon)
        tournament_dates = aux_tournament_dates.union(canon_tournament_dates)
        # remove tournament fights from df_aux and df_canon
        df_aux_sub = self.df_aux[~self.df_aux["Date"].isin(tournament_dates)]
        df_canon_sub = self.df_canon[~self.df_canon["Date"].isin(tournament_dates)]
        for _ in range(n_iters):
            print(f"iteration {_} of {n_iters}. map has size {len(self.fighter_id_map)} fighters mapped")
            frontier_of_map_df = self._find_frontier_of_map(df_aux_sub, df_canon_sub)
            print(f"frontier of map has size {len(frontier_of_map_df)} fights")
            # print(f"blank canon names: {(frontier_of_map_df['FighterName_canon'] == '').sum()}")
            self._update_fighter_id_map(frontier_of_map_df)
            # self._catch_conflicts_in_merge(frontier_of_map_df)
            if len(frontier_of_map_df) == 0:
                stray_inds = (
                    ~self.df_aux["FighterID"].isin(self.fighter_id_map.index) &
                    ~self.df_aux["OpponentID"].isin(self.fighter_id_map.index)
                )
                self.stray_fights = df_aux_sub.loc[stray_inds]
                print(f"no more fights in frontier to add to map. map has size {len(self.fighter_id_map)} fighters mapped. {len(self.stray_fights)} fights left unaccounted for.")
                break
        # okay, now we finally include tournament fights
        # Some fighters fought in non-tournament-style fights, so we have 
        # probably learned their mappings already. So now let's try propagating
        # those mappings to the tournament fights.
        print("okay, now we finally include tournament fights")
        for _ in range(n_iters):
            print(f"iteration {_} of {n_iters}. map has size {len(self.fighter_id_map)} fighters mapped")
            frontier_of_map_df = self._find_frontier_of_map(self.df_aux, self.df_canon)
            # print(f"blank canon names: {(frontier_of_map_df['FighterName_canon'] == '').sum()}")
            self._update_fighter_id_map(frontier_of_map_df)
            # self._catch_conflicts_in_merge(frontier_of_map_df)
            if len(frontier_of_map_df) == 0:
                stray_inds = (
                    ~self.df_aux["FighterID"].isin(self.fighter_id_map.index) &
                    ~self.df_aux["OpponentID"].isin(self.fighter_id_map.index)
                )
                self.stray_fights = df_aux_sub.loc[stray_inds]
                print(f"no more fights to map. map has size {len(self.fighter_id_map)} fighters mapped. {len(self.stray_fights)} fights left unaccounted for.")
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
    print("line 323:")
    print(ufc_df["FighterID"].isnull().mean())
    print(ufc_df.loc[ufc_df[["FighterID", "OpponentID"]].isnull().any(1), 
                    ["FighterName", "FighterID", "OpponentName", "OpponentID", "Date"]].sort_values("Date"))
    print(ufc_df.loc[ufc_df[["FighterID", "OpponentID"]].isnull().any(1), ["Date"]].value_counts())
    print("line 328")
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
    espn_df["Date"] = pd.to_datetime(espn_df["Date"])
    bfo_df["Date"] = pd.to_datetime(bfo_df["Date"])
    ufc_df["Date"] = pd.to_datetime(ufc_df["Date"])
    # find mapping btw ufc IDs and espn IDs
    iso_finder = IsomorphismFinder(
        df_aux=ufc_df, df_canon=espn_df, manual_map=MANUAL_UFC_ESPN_MAP
    )
    iso_finder.find_isomorphism(n_iters=20)
    
    # okay great, now that we have the mapping, let's join ufc data and espn data
    ufc_espn_df = join_ufc_and_espn(ufc_df, espn_df, iso_finder.fighter_id_map)
    assert False
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

    bfo_iso_finder = IsomorphismFinder(
        df_canon=espn_df, df_aux=bfo_df_clean, manual_map=MANUAL_BFO_ESPN_MAP
    )
    bfo_iso_finder.find_isomorphism(n_iters=20)

    bfo_ufc_espn_df = join_espn_and_bfo(ufc_espn_df, bfo_df_clean, bfo_iso_finder.fighter_id_map)
    print(bfo_ufc_espn_df.shape)
    base_db_interface.write_replace(bfo_ufc_espn_df, "full_bfo_ufc_espn_data")
    # bfo_ufc_espn_df.to_csv("data/full_bfo_ufc_espn_data.csv", index=False)
    return bfo_ufc_espn_df

if __name__ == "__main__":
    main()