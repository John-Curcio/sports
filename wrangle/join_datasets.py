import pandas as pd
import numpy as np
from wrangle.base_maps import *
from db import base_db_interface

def get_fight_id(fighter_id, opponent_id, date):
    """
    Get unique fight ID for each fight. Arguments must have aligned indices
    * fighter_id
    * opponent_id
    * date
    """
    if fighter_id.isnull().any():
        fighter_id = fighter_id.fillna("unknown")
    if opponent_id.isnull().any():
        opponent_id = opponent_id.fillna("unknown")
    max_id = np.maximum(fighter_id, opponent_id)
    min_id = np.minimum(fighter_id, opponent_id)
    date = pd.to_datetime(date).dt.date
    return date.astype(str) + "_" + min_id + "_" + max_id


class IsomorphismFinder(object):
    """
    Learn map btw FighterIDs in df_canon and FighterIDs in df_aux

    Objective is to learn mapping FighterID_aux --> FighterID_canon.
    df_aux might have multiple FighterIDs that correspond to the same
    FighterID_canon, so we need to learn a surjection.

    Attributes:
    * df_canon 
    * df_aux
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
        self.df_canon = self.get_double_df(df_canon)
        self.df_aux = self.get_double_df(df_aux)
        for col in ["FighterName", "OpponentName"]:
            self.df_canon[col] = self.clean_names(self.df_canon[col])
            self.df_aux[col] = self.clean_names(self.df_aux[col])
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
        fight_id = get_fight_id(df["FighterID"], df["OpponentID"], df["Date"])
        df = df[["Date", "FighterID", "OpponentID", "FighterName", "OpponentName"]]\
            .assign(fight_id = fight_id)\
            .drop_duplicates("fight_id")
        df_complement = df.rename(columns={
            "FighterID":"OpponentID", "OpponentID":"FighterID",
            "FighterName":"OpponentName", "OpponentName":"FighterName",
        })
        df_doubled = pd.concat([df, df_complement]).reset_index(drop=True)
        # remove fights where one of the fighters may have fought twice on the same day
        # this is a rare case, but it happens, and it messes up the join
        # df_doubled = df_doubled.groupby(["FighterID", "Date"]).filter(lambda x: len(x) == 1)
        return df_doubled
    
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
            # print conflicting fighter_id_aux's and the multiple fighter_id_canons they are associated with
            conflict_associations = df.loc[df["FighterID_aux"].isin(conflict_fighter_id_auxs)]\
                [['FighterID_aux', 'FighterID_canon']]\
                    .value_counts()\
                    .reset_index()\
                    .rename(columns={0:"count"})\
                    .sort_values(["FighterID_aux", "count"], ascending=False)
                # .groupby("FighterID_aux")["FighterID_canon"]\
                # .unique()\
                # .apply(lambda x: ", ".join(x))
            print(f"conflict associations: \n{conflict_associations}")
            # print conflicting fighter names

            conflict_fighter_names = df["FighterName_canon"]\
                .loc[df["FighterID_canon"].isin(conflict_fighter_id_auxs)]\
                .unique()
            print(f"fighter names with conflicts: {conflict_fighter_names}")
            self.conflict_fights = df.loc[df["FighterID_aux"].isin(conflict_fighter_id_auxs)]\
                .sort_values("Date")
            cols = [
                # "Date", 
                # "FighterID_canon", "FighterID_aux",
                # "OpponentID_canon", "OpponentID_aux",

                "Date", "FighterName_canon", "FighterName_aux", 
                "FighterID_aux", "FighterID_canon",
                "OpponentName_canon", "OpponentName_aux",
                "OpponentID_aux", "OpponentID_canon"
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
        overlapping_fights["FighterName_canon"] = overlapping_fights["FighterName"]
        overlapping_fights["OpponentName_canon"] = overlapping_fights["OpponentName"]
        overlapping_fights["FighterName_aux"] = overlapping_fights["FighterName"]
        overlapping_fights["OpponentName_aux"] = overlapping_fights["OpponentName"]
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
        in df_aux and df_canon that fought a lot of known opponents. We call this set of 
        FighterIDs the "frontier" of the map, and we then try to 
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
                print(f"no more fights in frontier to add to map. map has size \
                      {len(self.fighter_id_map)} fighters mapped. \
                      {len(self.stray_fights)} fights left unaccounted for.")
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
                print(f"no more fights to map. map has size \
                      {len(self.fighter_id_map)} fighters mapped. \
                      {len(self.stray_fights)} fights left unaccounted for.")
                break
        
        return self.fighter_id_map
    
    @staticmethod
    def clean_names(names):
        to_replace, value = zip(*NAME_REPLACE_DICT.items()) # gets keys and values of dict respectively
        names = names.fillna("").str.strip().str.lower()\
                .replace(to_replace=to_replace, value=value)
        return names


class MapFinder(object):
    """
    class for finding the mapping between two sets of IDs
    in order to join two datasets.
    Provides a handy wrapper around the IsomorphismFinder
    class, which is used to find the mapping.

    Note that MapFinder.aux_to_canon_map maps 
    (FighterID_aux, OpponentID_aux, Date) -> (FighterID_canon, OpponentID_canon, Date)
    whereas IsomorphismFinder.fighter_id_maps
    (FighterID_aux) -> (FighterID_canon)
    """

    def __init__(self, df_aux, df_canon, 
                 aux_suffix, canon_suffix,
                 false_overwrite_canon_map=None,
                 manual_map=None):
        """
        df_aux: auxiliary dataframe
        df_canon: canonical dataframe
        aux_suffix: suffix to add to aux ID columns
        canon_suffix: suffix to add to canon ID columns
        false_overwrite_canon_map: overwrite certain IDs in the canonical dataframe
        manual_map: dict of manual mappings
        """
        self.df_aux = df_aux
        self.df_canon = df_canon
        self.aux_suffix = aux_suffix
        self.canon_suffix = canon_suffix
        self.false_overwrite_canon_map = false_overwrite_canon_map
        self.manual_map = manual_map
        self.iso_finder = None
        self.aux_to_canon_map = None

    def assign_overwritten_ids(self, df, map):
        """
        overwrite certain IDs in the dataframe. Hang onto 
        the original IDs in a new column.
        """
        if map is None:
            map = dict()
        return df.assign(
            FighterID_pre_overwrite=df["FighterID"],
            OpponentID_pre_overwrite=df["OpponentID"],
            FighterID=df["FighterID"].replace(map),
            OpponentID=df["OpponentID"].replace(map),
        )

    def find_map(self, n_iters=20):
        """
        Find the mapping between the two sets of IDs.
        Returns a pd.DataFrame that defines the mapping
        between df_aux -> df_canon. The columns are:
            - FighterID_{aux_suffix}
            - FighterID_{canon_suffix}
            - OpponentID_{aux_suffix}
            - OpponentID_{canon_suffix}
            - Date
        """
        # some pages in some especially dirty sites actually 
        # comprise data for multiple fighters, who usually happen
        # to have the same name. So when we're trying to find the
        # mapping between the two sets of IDs, we actually temporarily
        # merge groups of canonical IDs into a single ID, and then
        # split them back up after we've found the mapping. This depends
        # on the fact that it would be extremely rare and confusing
        # for fans if two fighters with the same name fought on the same
        # day. So we can safely merge them into a single ID.
        df_canon_copy = self.assign_overwritten_ids(
            self.df_canon, self.false_overwrite_canon_map,
        )
        df_aux_copy = self.df_aux.copy()
        iso_finder = IsomorphismFinder(
            df_aux=df_aux_copy, df_canon=df_canon_copy,
            manual_map=self.manual_map,
        )
        self.iso_finder = iso_finder
        iso_finder.find_isomorphism(n_iters=n_iters)
        # now in df_aux_copy, let's add the corresponding canonical IDs
        # use fighter_id_map to find the corresponding canonical FighterIDs (post overwrite)
        f_id_canon_col = "FighterID_"+self.canon_suffix
        o_id_canon_col = "OpponentID_"+self.canon_suffix
        f_id_aux_col = "FighterID_"+self.aux_suffix
        o_id_aux_col = "OpponentID_"+self.aux_suffix
        df_aux_copy[f_id_canon_col] = df_aux_copy["FighterID"].map(iso_finder.fighter_id_map)
        df_aux_copy[o_id_canon_col] = df_aux_copy["OpponentID"].map(iso_finder.fighter_id_map)
        df_aux_copy = df_aux_copy.rename(columns={
            "FighterID": f_id_aux_col,
            "OpponentID": o_id_aux_col,
        })
        # iso_finder.fighter_id_map gave us the mapping 
        # (aux_fighter_id) -> (canon_fighter_id (post overwrite)))
        # now, we're going to use this to inner merge
        # df_aux_copy and df_canon_copy, and then use the result
        # to define the mapping
        # (aux_fighter_id, aux_opponent_id, date) -> (canon_fighter_id, canon_opponent_id, date)
        # (pre-overwrite)
        aux_to_canon_map = df_aux_copy[[
            f_id_aux_col, o_id_aux_col,
            f_id_canon_col, o_id_canon_col,
            "Date",
        ]]
        aux_to_canon_map = aux_to_canon_map.merge(
            df_canon_copy[[
                "FighterID", "OpponentID",
                "FighterID_pre_overwrite", "OpponentID_pre_overwrite",
                "Date",
            ]].rename(columns={
                "FighterID": f_id_canon_col,
                "OpponentID": o_id_canon_col,
            }),
            on=[f_id_canon_col, o_id_canon_col, "Date"],
            how="inner", # inner join drops fights where we don't have a mapping
        )
        # replace the overwritten IDs with the original IDs
        # so now we have the mapping between the original IDs
        aux_to_canon_map = aux_to_canon_map\
            .drop(columns=[f_id_canon_col, o_id_canon_col])\
            .rename(columns={
                "FighterID_pre_overwrite": f_id_canon_col,
                "OpponentID_pre_overwrite": o_id_canon_col,
            })
        # and we're done!
        self.aux_to_canon_map = aux_to_canon_map
        return aux_to_canon_map

def load_espn_df():
    espn_df = base_db_interface.read("clean_espn_data")
    espn_df["Date"] = pd.to_datetime(espn_df["Date"])
    # don't have to clean up names. IsomorphismFinder.__init__()
    # will do that for us
    # drop certain fights
    drop_fight_inds = pd.Series(False, index=espn_df.index)
    for _, row in MANUAL_ESPN_DROP_FIGHTS.iterrows():
        date, fighter_id, opponent_id = row["Date"], row["FighterID"], row["OpponentID"]
        drop_fight_inds = drop_fight_inds | (
            (espn_df["Date"] == date) &
            (espn_df["FighterID"] == fighter_id) & 
            (espn_df["OpponentID"] == opponent_id)
        ) | (
            (espn_df["Date"] == date) &
            (espn_df["FighterID"] == opponent_id) & 
            (espn_df["OpponentID"] == fighter_id)
        )
    espn_df = espn_df.loc[~drop_fight_inds]
    # Fighter history may be split btw two IDs
    return espn_df

def load_ufc_df():
    ufc_df = base_db_interface.read("clean_ufc_data")
    ufc_df["Date"] = pd.to_datetime(ufc_df["Date"])
    drop_inds = ufc_df["FightID"].isin(MANUAL_UFC_DROP_FIGHTS)
    ufc_df = ufc_df.loc[~drop_inds]
    return ufc_df

def load_bfo_df():
    bfo_df = base_db_interface.read("bfo_fighter_odds")
    bfo_df["FighterID"] = bfo_df["FighterHref"].str.split("/fighters/").str[1]
    bfo_df["OpponentID"] = bfo_df["OpponentHref"].str.split("/fighters/").str[1]
    bfo_df["Date"] = pd.to_datetime(bfo_df["Date"])
    ### MANUAL CLEANING ###
    # overwrite certain fighterIDs
    to_replace, value = zip(*MANUAL_BFO_OVERWRITE_MAP.items())
    bfo_df["FighterID"] = bfo_df["FighterID"].replace(to_replace, value)
    bfo_df["OpponentID"] = bfo_df["OpponentID"].replace(to_replace, value)
    return bfo_df

def load_the_historical_odds_df():
    odds_df = base_db_interface.read("the_historical_odds_open_close")\
        .rename(columns={
        "fighter_name": "FighterName",
        "opponent_name": "OpponentName",
    })
    odds_df["FighterID"] = odds_df["FighterName"]
    odds_df["OpponentID"] = odds_df["OpponentName"]
    odds_df["Date"] = pd.to_datetime(pd.to_datetime(odds_df["commence_time"]).dt.date)
    fighter_cols = [
        "FighterName", "FighterID",
        "open_fighter_decimal_odds", "close_fighter_decimal_odds",
    ]
    opponent_cols = [
        "OpponentName", "OpponentID", 
        "open_opponent_decimal_odds", "close_opponent_decimal_odds",
    ]
    complement_col_map = dict()
    for f_col, o_col in zip(fighter_cols, opponent_cols):
        complement_col_map[f_col] = o_col
        complement_col_map[o_col] = f_col
    odds_df_complement = odds_df.rename(columns=complement_col_map)
    return pd.concat([
        odds_df,
        odds_df_complement
    ]).reset_index(drop=True)

def join_the_odds_espn():
    print("loading espn data")
    espn_df = load_espn_df()[[
        "FighterID", "OpponentID",
        "Date",
        "OpponentName", "FighterName",
    ]]
    print("loading the-odds data")
    the_odds_df = load_the_historical_odds_df()[[
        "FighterID", "OpponentID",
        "Date",
        "OpponentName", "FighterName",
    ]].drop_duplicates()
    # add a jitter of +/- 1 day to the-odds data
    # because the-odds dates are sometimes off by a day
    the_odds_df_jitter = pd.concat([
        the_odds_df.assign(
            Date_pre_overwrite=the_odds_df["Date"],
        ),
        the_odds_df.assign(
            Date=the_odds_df["Date"] - pd.Timedelta(days=1),
            Date_pre_overwrite=the_odds_df["Date"],
        ),
        the_odds_df.assign(
            Date=the_odds_df["Date"] + pd.Timedelta(days=1),
            Date_pre_overwrite=the_odds_df["Date"],
        ),
    ]).reset_index()

    map_finder = MapFinder(
        # df_aux=the_odds_df, 
        df_aux=the_odds_df_jitter,
        df_canon=espn_df,
        aux_suffix="the_odds", canon_suffix="espn",
        manual_map=MANUAL_THE_ODDS_ESPN_MAP,
    )
    the_odds_to_espn_map = map_finder.find_map(n_iters=20)
    the_odds_to_espn_map = the_odds_to_espn_map.drop_duplicates()

    # when we join the-odds and espn data, we want to keep the 
    # dates from the-odds data. 
    left = pd.concat([
        the_odds_df_jitter,
        the_odds_df_jitter.assign(
            FighterID=the_odds_df_jitter["OpponentID"],
            OpponentID=the_odds_df_jitter["FighterID"],
        ),
    ])[[
        "FighterID", "OpponentID",
        "Date_pre_overwrite",
        "Date"
    ]]
    the_odds_to_espn_map = left.merge(
        the_odds_to_espn_map,
        left_on=["FighterID", "OpponentID", "Date"],
        right_on=["FighterID_the_odds", "OpponentID_the_odds", "Date"],
        how="inner",
    ).rename(columns={
        "Date_pre_overwrite": "Date_the_odds",
        "Date": "Date_espn",
    })[[
        "FighterID_the_odds", "OpponentID_the_odds",
        "Date_the_odds", "Date_espn",
        "FighterID_espn", "OpponentID_espn",
    ]]
    base_db_interface.write_replace(
        "the_odds_espn_fighter_id_map",
        the_odds_to_espn_map,
    )
    return the_odds_to_espn_map

def find_ufc_espn_mapping():
    print("loading espn data")
    espn_df = load_espn_df()[[
        "FighterID", "OpponentID", 
        "Date", 
        "OpponentName", "FighterName",
    ]]
    print("loading ufc data")
    ufc_df = load_ufc_df()[[
        "FighterID", "OpponentID", 
        "Date", 
        "OpponentName", "FighterName",
    ]]
    print("finding isomorphism")
    # find mapping btw ufc IDs and espn IDs
    iso_finder = IsomorphismFinder(
        df_aux=ufc_df, df_canon=espn_df, manual_map=MANUAL_UFC_ESPN_MAP
    )
    iso_finder.find_isomorphism(n_iters=20)

    # write the mapping to the database. Joining ufc and espn data comes later
    fighter_id_map_df = iso_finder.fighter_id_map.reset_index()\
        .rename(columns={
            "index": "FighterID_ufc",
            0: "FighterID_espn",
        })
    base_db_interface.write_replace(
        "ufc_to_espn_fighter_id_map",
        fighter_id_map_df, 
    )
    # I want to find a mapping 
    # (ufc_fighter_id, ufc_opponent_id, date) -> 
    # (espn_fighter_id, espn_opponent_id, date)
    # so that I can join ufc and espn data and get the orientation right
    ufc_to_espn_map = ufc_df.assign(
        FighterID_espn=ufc_df["FighterID"].map(iso_finder.fighter_id_map),
        OpponentID_espn=ufc_df["OpponentID"].map(iso_finder.fighter_id_map),
    ).rename(columns={
        "FighterID": "FighterID_ufc",
        "OpponentID": "OpponentID_ufc",
    })[[
        "FighterID_ufc", "OpponentID_ufc",
        "FighterID_espn", "OpponentID_espn",
        "Date",
    ]]
    # drop fights where we don't have a mapping
    ufc_to_espn_map = ufc_to_espn_map.loc[
        ufc_to_espn_map["FighterID_espn"].notnull() &
        ufc_to_espn_map["OpponentID_espn"].notnull()
    ]
    # simple as that!
    base_db_interface.write_replace(
        "ufc_to_espn_map",
        ufc_to_espn_map,
    )


def find_bfo_ufc_mapping():
    print("loading ufc data")
    ufc_df = load_ufc_df()[[
        "FighterID", "OpponentID", 
        "Date", 
        "OpponentName", "FighterName",
    ]]
    print("loading bfo data")
    bfo_df = load_bfo_df()[[
        "FighterID", "OpponentID", 
        "Date", 
        "OpponentName", "FighterName",
    ]]
    print("finding isomorphism")
    # some pages in bestfightodds.com actually comprise data for multiple fighters,
    # who happen to have the same name. EG
    # joey gomez:
    # https://www.bestfightodds.com/fighters/Joey-Gomez-6023
    # http://ufcstats.com/fighter-details/0778f94eb5d588a5
    # http://ufcstats.com/fighter-details/3a28e1e641366308
    # So when we're trying to join BFO and UFC data, we temporarily merge the
    # UFC IDs for these fighters into one, and then split them again later.
    ufc_df["FighterID_pre_overwrite"] = ufc_df["FighterID"]
    ufc_df["OpponentID_pre_overwrite"] = ufc_df["OpponentID"]
    ufc_df["FighterID"] = ufc_df["FighterID"].replace(FALSE_OVERWRITE_UFC_MAP)
    ufc_df["OpponentID"] = ufc_df["OpponentID"].replace(FALSE_OVERWRITE_UFC_MAP)
    # find mapping btw bfo IDs and espn IDs
    iso_finder = IsomorphismFinder(
        df_aux=bfo_df, df_canon=ufc_df,
        manual_map=MANUAL_BFO_UFC_MAP
    )
    iso_finder.find_isomorphism(n_iters=20)
    # write the mapping to the database. Joining ufc and espn data comes later
    fighter_id_map_df = iso_finder.fighter_id_map.reset_index()\
        .rename(columns={
            "index": "FighterID_bfo",
            0: "FighterID_ufc",
        })
    base_db_interface.write_replace(
        "bfo_to_ufc_fighter_id_map",
        fighter_id_map_df, 
    )
    # I want to find a mapping
    # (bfo_fighter_id, bfo_opponent_id, date) ->
    # (ufc_fighter_id_pre_overwrite, ufc_opponent_id_pre_overwrite, date)
    # so that I can join bfo and ufc data and get the orientation right

    # use fighter_id_map to find the corresponding ufc FighterIDs (post overwrite)
    bfo_to_ufc_map = bfo_df.assign(
        FighterID_ufc=bfo_df["FighterID"].map(iso_finder.fighter_id_map),
        OpponentID_ufc=bfo_df["OpponentID"].map(iso_finder.fighter_id_map),
    ).rename(columns={
        "FighterID": "FighterID_bfo",
        "OpponentID": "OpponentID_bfo",
    })[[
        "FighterID_bfo", "OpponentID_bfo",
        "FighterID_ufc", "OpponentID_ufc",
        "Date",
    ]]
    # join with ufc_df to get the pre-overwrite FighterIDs
    bfo_to_ufc_map = bfo_to_ufc_map.merge(
        ufc_df[[
            "FighterID", "OpponentID",
            "FighterID_pre_overwrite", "OpponentID_pre_overwrite",
            "Date",
        ]].rename(columns={
            "FighterID": "FighterID_ufc",
            "OpponentID": "OpponentID_ufc",
        }),
        on=["FighterID_ufc", "OpponentID_ufc", "Date"],
        how="inner", # inner join drops fights where we don't have a mapping
    )
    # drop the post-overwrite FighterIDs
    bfo_to_ufc_map = bfo_to_ufc_map\
        .drop(columns=["FighterID_ufc", "OpponentID_ufc"])\
        .rename(columns={
            "FighterID_pre_overwrite": "FighterID_ufc",
            "OpponentID_pre_overwrite": "OpponentID_ufc",
        })

    base_db_interface.write_replace(
        "bfo_to_ufc_map",
        bfo_to_ufc_map,
    )

def join_bfo_espn_ufc(bfo_df, espn_df, ufc_df):
    """
    After we've found the mapping between BFO and ESPN data, as well as 
    the mapping between ESPN and UFC data, we can join the three datasets 
    together. This is the final step before we start doing final feature 
    engineering.
    """
    ufc_to_espn_map = base_db_interface.read("ufc_to_espn_map")
    bfo_to_espn_map = base_db_interface.read("bfo_to_espn_map")

    # join ufc and espn data
    espn_df = espn_df.rename(columns={
        "FighterID": "FighterID_espn",
        "OpponentID": "OpponentID_espn",
    })
    ufc_df = ufc_df.rename(columns={
        "FighterID": "FighterID_ufc",
        "OpponentID": "OpponentID_ufc",
    }).merge(
        ufc_to_espn_map,
        on=["FighterID_ufc", "OpponentID_ufc", "Date"],
        how="inner",
    )
    historical_espn_ufc_df = espn_df.merge(
        ufc_df,
        on=["FighterID_espn", "OpponentID_espn", "Date"],
        how="left",
        suffixes=("","_ufc")
    )
    # don't forget about upcoming fights!
    upcoming_espn_ufc_df = espn_df.merge(
        ufc_df.query("is_upcoming == 1"),
        on=["FighterID_espn", "OpponentID_espn", "Date"],
        how="right",
        suffixes=("","_ufc")
    )
    espn_ufc_df = pd.concat([
        historical_espn_ufc_df,
        upcoming_espn_ufc_df,
    ]).sort_values("Date").reset_index(drop=True)
    espn_ufc_df["is_upcoming"] = espn_ufc_df["is_upcoming"].fillna(0).astype(int)

    # join bfo data with espn_ufc_df
    bfo_espn_df = bfo_df.rename(columns={
        "FighterID": "FighterID_bfo",
        "OpponentID": "OpponentID_bfo",
    }).merge(
        bfo_to_espn_map,
        on=["FighterID_bfo", "OpponentID_bfo", "Date"],
        how="inner",
    )
    bfo_espn_ufc_df = espn_ufc_df.merge(
        bfo_espn_df,
        on=["FighterID_espn", "OpponentID_espn", "Date"],
        how="left",
        suffixes=("","_bfo")
    )
    # Make sure fight_id isn't missing. If not missing, this will 
    # be a no-op. If missing, we'll fill it in. fight_id may be
    # missing for upcoming fights.
    bfo_espn_ufc_df["fight_id"] = get_fight_id(
        bfo_espn_ufc_df["FighterID_espn"],
        bfo_espn_ufc_df["OpponentID_espn"],
        bfo_espn_ufc_df["Date"],
    )

    print("checking for duplicate fights")
    print(bfo_espn_ufc_df["fight_id"].value_counts()[0:10])
    print("bfo_espn_ufc.shape:", bfo_espn_ufc_df.shape)
    print("bfo_espn_ufc_df.shape, with bfo and ufc:", 
        bfo_espn_ufc_df.dropna(
            subset=["FighterID_espn", "FighterID_ufc", "FighterID_bfo"]
        ).shape)
    print("writing joined_bfo_espn_ufc_data")
    base_db_interface.write_replace(
        "joined_bfo_espn_ufc_data",
        bfo_espn_ufc_df,
    )
    return bfo_espn_ufc_df


def find_bfo_espn_mapping():
    bfo_df = load_bfo_df().dropna(subset=[
        # fat lot of good a row will do if it doesn't have IDs or odds
        "FighterID", "OpponentID",
        "FighterOpen", "OpponentOpen",
        "Date",
    ])[[
        "FighterID", "OpponentID", 
        "Date", 
        "OpponentName", "FighterName",
    ]]
    espn_df = load_espn_df()[[
        "FighterID", "OpponentID", 
        "Date", 
        "OpponentName", "FighterName",
    ]]
    # some pages in bestfightodds.com actually comprise data for multiple fighters,
    # who happen to have the same name. 
    # So when we're trying to join BFO and ESPN data, we temporarily merge the
    # ESPN IDs for these fighters into one, and then split them again later.
    temp_espn_df = espn_df.copy()
    temp_espn_df["FighterID_pre_overwrite"] = temp_espn_df["FighterID"]
    temp_espn_df["OpponentID_pre_overwrite"] = temp_espn_df["OpponentID"]
    temp_espn_df["FighterID"] = temp_espn_df["FighterID"].replace(FALSE_OVERWRITE_ESPN_MAP)
    temp_espn_df["OpponentID"] = temp_espn_df["OpponentID"].replace(FALSE_OVERWRITE_ESPN_MAP)

    iso_finder = IsomorphismFinder(
        df_aux=bfo_df, df_canon=temp_espn_df,
        manual_map=MANUAL_BFO_ESPN_MAP,
    )

    iso_finder.find_isomorphism(n_iters=20)

    # use fighter_id_map to find the corresponding ESPN FighterIDs (post overwrite)
    bfo_to_espn_map = bfo_df.assign(
        FighterID_espn=bfo_df["FighterID"].map(iso_finder.fighter_id_map),
        OpponentID_espn=bfo_df["OpponentID"].map(iso_finder.fighter_id_map),
    ).rename(columns={
        "FighterID": "FighterID_bfo",
        "OpponentID": "OpponentID_bfo",
    })[[
        "FighterID_bfo", "OpponentID_bfo",
        "FighterID_espn", "OpponentID_espn",
        "Date",
    ]]

    # join with espn_df to get the pre-overwrite FighterIDs
    bfo_to_espn_map = bfo_to_espn_map.merge(
        temp_espn_df[[
            "FighterID", "OpponentID",
            "FighterID_pre_overwrite", "OpponentID_pre_overwrite",
            "Date",
        ]].rename(columns={
            "FighterID": "FighterID_espn",
            "OpponentID": "OpponentID_espn",
        }),
        on=["FighterID_espn", "OpponentID_espn", "Date"],
        how="inner", # inner join drops fights where we don't have a mapping
    )
    # drop the post-overwrite FighterIDs
    bfo_to_espn_map = bfo_to_espn_map\
        .drop(columns=["FighterID_espn", "OpponentID_espn"])\
        .rename(columns={
            "FighterID_pre_overwrite": "FighterID_espn",
            "OpponentID_pre_overwrite": "OpponentID_espn",
        })

    base_db_interface.write_replace(
        "bfo_to_espn_map",
        bfo_to_espn_map,
    )

def final_clean_step():
    """
    Any last-minute cleaning steps that need to be done before feature engineering
    Namely dropping fights with duplicated opening money lines (thankfully, not very
    many of these cases)
    """
    joined_df = base_db_interface.read("joined_bfo_espn_ufc_data")
    print("joined_df.shape before dropping duplicated opening money lines:", joined_df.shape)
    joined_df["p_stats_null"] = joined_df.isnull().mean(axis=1)
    joined_df = joined_df.sort_values("p_stats_null")\
        .drop_duplicates(
        subset=["FighterID_espn", "OpponentID_espn", "Date"],
        keep="first",
    ).drop(columns=["p_stats_null"])
    print("joined_df.shape after dropping duplicated opening money lines:", joined_df.shape)
    base_db_interface.write_replace(
        "clean_bfo_espn_ufc_data",
        joined_df,
    )