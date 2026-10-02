"""Identity matching algorithms, independent of database connections."""
import pandas as pd
import numpy as np
from wrangle.base_maps import NAME_REPLACE_DICT

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

    def __init__(self, df_canon, df_aux, manual_map=None, day_tol=0, include_tournament_fights=True):
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
        * day_tol - int. Sometimes, two datasets will have the same fight
            occur on slightly different days - an event may run after midnight,
            or each dataset may use a different time zone as a reference. This
            makes the join harder, so we allow a small tolerance in the join.
        * include_tournament_fights - bool. A minority of these fights were held
            in a tournament format, where fighters fought multiple opponents on
            the same day. This makes it harder to learn the mapping btw
            FighterID_aux and FighterID_canon.
        """
        # drop rows with missing FighterID, OpponentID, or Date
        self.df_canon = self.get_double_df(df_canon.dropna(subset=["FighterID", "OpponentID", "Date"]))
        self.df_aux = self.get_double_df(df_aux.dropna(subset=["FighterID", "OpponentID", "Date"]))
        for col in ["FighterName", "OpponentName"]:
            self.df_canon[col] = self.clean_names(self.df_canon[col])
            self.df_aux[col] = self.clean_names(self.df_aux[col])
        self.fighter_id_map = pd.Series(dtype='object')
        if manual_map is not None:
            index, vals = zip(*manual_map.items())
            self.fighter_id_map = pd.Series(vals, index=index, dtype='object')
        self.day_tol = day_tol
        self.include_tournament_fights = include_tournament_fights
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
                [['FighterID_aux', 'FighterID_canon', 'FighterName_canon']]\
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
                .loc[df["FighterID_aux"].isin(conflict_fighter_id_auxs)]\
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
            # on=["Date", "FighterName", "OpponentName"],
            on = ["FighterName", "OpponentName"],
            suffixes=("_canon", "_aux"),
        )
        # I've found several cases where dates differ by 1 day, so I'm
        # going to allow a max difference of 1 day.
        date_diff = (overlapping_fights["Date_canon"] - overlapping_fights["Date_aux"]).dt.days.abs()
        overlapping_fights = overlapping_fights.loc[date_diff <= self.day_tol]\
            .drop(columns=["Date_aux"])\
            .rename(columns={"Date_canon": "Date"})
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
            left_on="OpponentID_canon",
            right_on="OpponentID",
            suffixes=("_aux", "_canon"),
        )
        # I've found several cases where dates differ by 1 day, so I'm
        # going to allow a max difference of 1 day.
        date_diff = (df_inner["Date_aux"] - df_inner["Date_canon"]).dt.days.abs()
        df_inner = df_inner.loc[date_diff <= self.day_tol]\
            .drop(columns=["Date_aux"])\
            .rename(columns={"Date_canon": "Date"})

        # df_inner = df_aux_frontier.merge(
        #     df_canon, how="inner",
        #     left_on=["Date", "OpponentID_canon"],
        #     right_on=["Date", "OpponentID"],
        #     suffixes=("_aux", "_canon"),
        # )#.rename(columns={"FighterName_aux": "FighterName", "OpponentName_aux": "OpponentName"})
        return df_inner

    @staticmethod
    def drop_tournament_fights(df):
        """
        There are some cases where a given Fighter apparently fought multiple
        times on the same day. This is because:
        * the fight was part of a tournament-style event, like early UFCs.
        * one of the fights didn't actually occur. In the BestFightOdds data, we
            see many cases where a fighter has two different opponents on the same
            day. This is because one of the opponents was scheduled to fight the
            fighter, but dropped out due to injury or something. The opponent was
            replaced with a different guy.
        Cases like these can introduce conflicts in fight_isomorphism, so I want to
        drop them from the dataset.
        """
        # get Date, FighterID counts of opponents
        counts = df.groupby(["Date", "FighterID"])["OpponentID"].nunique()\
            .reset_index()\
            .rename(columns={"OpponentID":"OpponentID_count"})
        df = df.merge(counts, how="left", on=["Date", "FighterID"])
        df = df.loc[df["OpponentID_count"] == 1].drop(columns=["OpponentID_count"])
        # we shouldn't have to do this if .get_double_df() has already been called
        # on df, but I'd like this method to be static, so I don't want to
        # make that assumption. If it's already been called, then the following lines
        # will have no effect.
        counts = df.groupby(["Date", "OpponentID"])["FighterID"].nunique()\
            .reset_index()\
            .rename(columns={"FighterID":"FighterID_count"})
        df = df.merge(counts, how="left", on=["Date", "OpponentID"])
        df = df.loc[df["FighterID_count"] == 1].drop(columns=["FighterID_count"])
        return df


    def find_isomorphism(self, n_iters=3):
        """
        This is where the magic happens. We want to learn the full
        mapping btw IDs in df_aux --> df_canon, which is stored in self.fighter_id_map.
        We do this with a greedy, iterative process.

        * "Base" step: We start by running find_base_map(), which is just
        inner-joining on fighter names and date. That gives us a subset of the full
        mapping btw IDs in df_aux --> df_canon, which we want to learn.
        * "Propagate" step: Based on this subset of the mapping, we find FighterIDs
        in df_aux and df_canon that fought known opponents. We call this set of
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
        # remove tournament fights from df_aux and df_canon
        df_aux_sub = self.drop_tournament_fights(self.df_aux)
        df_canon_sub = self.drop_tournament_fights(self.df_canon)
        for _ in range(n_iters):
            print(f"iteration {_} of {n_iters}. map has size {len(self.fighter_id_map)} fighters mapped")
            frontier_of_map_df = self._find_frontier_of_map(df_aux_sub, df_canon_sub)
            print(f"frontier of map has size {len(frontier_of_map_df)} fights")
            # print(f"blank canon names: {(frontier_of_map_df['FighterName_canon'] == '').sum()}")
            self._update_fighter_id_map(frontier_of_map_df)
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
        # Some fighters also fought in non-tournament-style fights, so we have
        # probably learned their mappings already. So now let's try propagating
        # those mappings to the tournament fights.
        if self.include_tournament_fights:
            print("okay, now we finally include tournament fights")
            for _ in range(n_iters):
                print(f"iteration {_} of {n_iters}. map has size {len(self.fighter_id_map)} fighters mapped")
                frontier_of_map_df = self._find_frontier_of_map(self.df_aux, self.df_canon)
                # print(f"blank canon names: {(frontier_of_map_df['FighterName_canon'] == '').sum()}")
                self._update_fighter_id_map(frontier_of_map_df)
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
