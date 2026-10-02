import pandas as pd
import numpy as np
from wrangle.base_maps import *
from db import base_db_interface

from wrangle.identity_matching import get_fight_id, IsomorphismFinder


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
                 manual_map=None,
                 iso_finder_day_tol=0,
                 day_tol=0,
                 include_tournament_fights=True,):
        """
        df_aux: auxiliary dataframe
        df_canon: canonical dataframe
        aux_suffix: suffix to add to aux ID columns
        canon_suffix: suffix to add to canon ID columns
        false_overwrite_canon_map: overwrite certain IDs in the canonical dataframe
        manual_map: dict of manual mappings
        iso_finder_day_tol: day_tol for IsomorphismFinder (finding mapping btw FighterIDs)
        day_tol: day_tol for actually joining fights. Fights differing by a day is
            fairly rare - if we use it in IsomorphismFinder, it may introduce more conflicts
            than I can handle. But when we actually join fights, it's less of an issue.
        """
        self.df_aux = df_aux.dropna(subset=["FighterID", "OpponentID", "Date"])
        self.df_canon = df_canon.dropna(subset=["FighterID", "OpponentID", "Date"])
        self.aux_suffix = aux_suffix
        self.canon_suffix = canon_suffix
        self.false_overwrite_canon_map = false_overwrite_canon_map
        self.manual_map = manual_map
        self.iso_finder_day_tol = iso_finder_day_tol
        self.day_tol = day_tol
        self.include_tournament_fights = include_tournament_fights
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
        * n_iters: number of iterations to run the IsomorphismFinder
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
            day_tol=self.iso_finder_day_tol,
            include_tournament_fights=self.include_tournament_fights,
        )
        self.iso_finder = iso_finder
        iso_finder.find_isomorphism(n_iters=n_iters)
        # now in df_aux_copy, let's add the corresponding canonical IDs
        # use fighter_id_map to find the corresponding canonical FighterIDs (post overwrite)
        f_id_canon_col = "FighterID_"+self.canon_suffix
        o_id_canon_col = "OpponentID_"+self.canon_suffix
        date_canon_col = "Date_"+self.canon_suffix
        f_id_aux_col = "FighterID_"+self.aux_suffix
        o_id_aux_col = "OpponentID_"+self.aux_suffix
        date_aux_col = "Date_"+self.aux_suffix
        df_aux_copy[f_id_canon_col] = df_aux_copy["FighterID"].map(iso_finder.fighter_id_map)
        df_aux_copy[o_id_canon_col] = df_aux_copy["OpponentID"].map(iso_finder.fighter_id_map)
        df_aux_copy = df_aux_copy.rename(columns={
            "FighterID": f_id_aux_col,
            "OpponentID": o_id_aux_col,
            "Date": date_aux_col,
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
            # "Date",
            date_aux_col,
        ]]
        aux_to_canon_map = aux_to_canon_map.merge(
            df_canon_copy[[
                "FighterID", "OpponentID",
                "FighterID_pre_overwrite", "OpponentID_pre_overwrite",
                "Date",
            ]].rename(columns={
                "FighterID": f_id_canon_col,
                "OpponentID": o_id_canon_col,
                "Date": date_canon_col,
            }),
            # on=[f_id_canon_col, o_id_canon_col, "Date"],
            on=[f_id_canon_col, o_id_canon_col],
            how="inner", # inner join drops fights where we don't have a mapping
        )
        date_diff = (aux_to_canon_map[date_aux_col] - aux_to_canon_map[date_canon_col]).dt.days.abs()
        aux_to_canon_map = aux_to_canon_map.loc[date_diff <= self.day_tol]
        # each FighterID_aux should map to one FighterID_canon, with the exception of cases
        # where we had to overwrite FighterID_canon
        assert aux_to_canon_map.query(f"FighterID_pre_overwrite == {f_id_canon_col} ")\
            .groupby([f_id_aux_col])["FighterID_pre_overwrite"].nunique().max() == 1, \
            aux_to_canon_map.query(f"FighterID_pre_overwrite == {f_id_canon_col} ")\
                .groupby([f_id_aux_col])["FighterID_pre_overwrite"].nunique().value_counts()
        # it's allowed for multiple FighterID_canons to point to the same FighterID_aux. Many
        # bestfightodds Fighters have multiple IDs, too many to clean up.

        assert not aux_to_canon_map.isnull().any().any(), aux_to_canon_map.isnull().mean()
        assert aux_to_canon_map.groupby(f_id_aux_col)[f_id_canon_col].nunique().max() == 1, \
            aux_to_canon_map.groupby(f_id_aux_col)[f_id_canon_col].nunique().value_counts()
        # V this assert statement just ends up catching a lot of false positives
        # it's okay for the bestfightodds data to have some duplicate rows, it's just too dirty
        # assert aux_to_canon_map[[f_id_canon_col, o_id_canon_col, date_canon_col]].value_counts().max() == 1, \
        #     aux_to_canon_map[[f_id_canon_col, o_id_canon_col, date_canon_col]].value_counts()
        
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
    # Some espn IDs correspond to the same guy
    to_replace, value = zip(*MANUAL_ESPN_OVERWRITE_MAP.items())
    espn_df["FighterID"] = espn_df["FighterID"].replace(to_replace, value)
    espn_df["OpponentID"] = espn_df["OpponentID"].replace(to_replace, value)
    # Fighter history may be split btw two IDs

    assert (espn_df["fight_id"].value_counts() == 2).all(), espn_df["fight_id"].value_counts()
    return espn_df

def load_ufc_df():
    ufc_df = base_db_interface.read("clean_ufc_data")
    ufc_df["Date"] = pd.to_datetime(ufc_df["Date"])
    drop_inds = ufc_df["FightID"].isin(MANUAL_UFC_DROP_FIGHTS)
    ufc_df = ufc_df.loc[~drop_inds]
    assert (ufc_df["FightID"].value_counts() == 2).all(), ufc_df["FightID"].value_counts()
    return ufc_df

def load_bfo_df():
    # bfo_df = base_db_interface.read("bfo_fighter_odds")
    bfo_df = base_db_interface.read("clean_fighter_odds_data")
    # bfo_df["FighterID"] = bfo_df["FighterHref"].str.split("/fighters/").str[1]
    # bfo_df["OpponentID"] = bfo_df["OpponentHref"].str.split("/fighters/").str[1]
    bfo_df["Date"] = pd.to_datetime(bfo_df["Date"], format="mixed")
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
    ufc_df = load_ufc_df().query("is_upcoming == 0")[[
        "FighterID", "OpponentID", 
        "Date", 
        "EventUrl",
        "OpponentName", "FighterName",
    ]]
    print("finding isomorphism")
    map_finder = MapFinder(
        df_aux=ufc_df, df_canon=espn_df,
        aux_suffix="ufc", canon_suffix="espn",
        manual_map=MANUAL_UFC_ESPN_MAP,
        iso_finder_day_tol=1,
        day_tol=1,
    )
    ufc_to_espn_map = map_finder.find_map(n_iters=20)
    print("ufcstats.com fights successfully mapped:", ufc_to_espn_map.shape)
    assert not ufc_to_espn_map.isnull().any().any(), ufc_to_espn_map.isnull().mean()
    assert ufc_to_espn_map[["FighterID_espn", "OpponentID_espn", "Date_espn"]].value_counts().max() == 1, \
        ufc_to_espn_map[["FighterID_espn", "OpponentID_espn", "Date_espn"]].value_counts()
    assert ufc_to_espn_map[["FighterID_ufc", "OpponentID_ufc", "Date_ufc"]].value_counts().max() == 1, \
        ufc_to_espn_map[["FighterID_ufc", "OpponentID_ufc", "Date_ufc"]].value_counts()
    

    # write the fighter_id map to the database
    fighter_id_map_df = map_finder.iso_finder.fighter_id_map.reset_index()\
        .rename(columns={
            "index": "FighterID_ufc",
            0: "FighterID_espn",
        })
    # There are ~27 fighters in the UFC dataset that don't have a corresponding match in 
    # the ESPN dataset, because they fought only in WEC. This org doesn't appear in the
    # ESPN dataset at all. I'm just going to drop these guys entirely.
    print("ufcstats.com Event URLs with unmatched fighterIDs:")
    print(ufc_df.loc[~ufc_df["FighterID"].isin(fighter_id_map_df["FighterID_ufc"]), "EventUrl"].value_counts())
    assert not fighter_id_map_df.isnull().any().any(), fighter_id_map_df.isnull().mean()
    base_db_interface.write_replace(
        "ufc_to_espn_fighter_id_map",
        fighter_id_map_df, 
    )

    # wrap the actual fight mapping to the database
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

def join_upcoming_fights(bfo_df, ufc_df):
    """
    Find mapping between BFO data to UFC data. These are the only
    two datasets that have upcoming fights.
    Filter down to upcoming fights, then get ESPN FighterIDs for
    each fighter in the upcoming fights.
    """
    bfo_to_ufc_map = base_db_interface.read("bfo_to_ufc_map")
    ufc_to_espn_map = base_db_interface.read("ufc_to_espn_map")
    ufc_df = ufc_df.rename(columns={
        "FighterID": "FighterID_ufc",
        "OpponentID": "OpponentID_ufc",
    }).merge(
        ufc_to_espn_map,
        on=["FighterID_ufc", "OpponentID_ufc", "Date"],
        how="inner",
    )
    # join bfo data
    bfo_ufc_df = bfo_df.rename(columns={
        "FighterID": "FighterID_bfo",
        "OpponentID": "OpponentID_bfo",
    }).merge(
        bfo_to_ufc_map,
        on=["FighterID_bfo", "OpponentID_bfo", "Date"],
        how="inner",
    )
    upcoming_df = ufc_df.query("is_upcoming == 1").merge(
        bfo_ufc_df,
        on=["FighterID_ufc", "OpponentID_ufc", "Date"],
        how="inner"
    )
    print(upcoming_df.shape)
    base_db_interface.write_replace(
        "bfo_ufc_upcoming_fights",
        upcoming_df
    )

def join_bfo_espn_ufc():
    """
    After we've found the mapping between BFO and ESPN data, as well as 
    the mapping between ESPN and UFC data, we can join the three datasets 
    together. This is the final step before we start doing final feature 
    engineering.
    """
    bfo_df = load_bfo_df()
    espn_df = load_espn_df()
    ufc_df = load_ufc_df()
    ufc_to_espn_map = base_db_interface.read("ufc_to_espn_map")
    bfo_to_espn_map = base_db_interface.read("bfo_to_espn_map")
    ufc_to_espn_map["Date_ufc"] = pd.to_datetime(ufc_to_espn_map["Date_ufc"])
    ufc_to_espn_map["Date_espn"] = pd.to_datetime(ufc_to_espn_map["Date_espn"])
    bfo_to_espn_map["Date_bfo"] = pd.to_datetime(bfo_to_espn_map["Date_bfo"])
    bfo_to_espn_map["Date_espn"] = pd.to_datetime(bfo_to_espn_map["Date_espn"])

    # join ufc and espn data
    espn_df = espn_df.rename(columns={
        "FighterID": "FighterID_espn",
        "OpponentID": "OpponentID_espn",
        "Date": "Date_espn",
    })
    ufc_df = ufc_df.rename(columns={
        "FighterID": "FighterID_ufc",
        "OpponentID": "OpponentID_ufc",
        "Date": "Date_ufc",
    }).merge(
        ufc_to_espn_map,
        on=["FighterID_ufc", "OpponentID_ufc", "Date_ufc"],
        how="left",
    )
    historical_espn_ufc_df = espn_df.merge(
        ufc_df,
        on=["FighterID_espn", "OpponentID_espn", "Date_espn"],
        how="left",
        suffixes=("","_ufc")
    )
    assert historical_espn_ufc_df.shape[0] == espn_df.shape[0], (historical_espn_ufc_df.shape, espn_df.shape)
    # one fighter might be unknown, but we can't have cases where BOTH fighters are unknown. If that's the case,
    # something seriously wrong has happened
    assert not historical_espn_ufc_df[["FighterID_espn", "OpponentID_espn"]].isnull().all().any(), \
        historical_espn_ufc_df[["FighterID_espn", "OpponentID_espn"]].isnull().mean()
    # don't forget about upcoming fights!
    # We have to use the ufc_to_espn_map to get the ESPN FighterIDs for upcoming fights.
    # We can't use ufc_to_espn_map because that's only for historical fights; the ESPN data
    # doesn't contain records for upcoming fights.
    fighter_id_map_df = base_db_interface.read("ufc_to_espn_fighter_id_map")
    fighter_id_map = fighter_id_map_df.set_index("FighterID_ufc")["FighterID_espn"]
    upcoming_ufc_df = ufc_df.query("is_upcoming == 1")
    upcoming_ufc_df = upcoming_ufc_df.assign(
        Date_espn = upcoming_ufc_df["Date_espn"].fillna(upcoming_ufc_df["Date_ufc"]),
        FighterID_espn = upcoming_ufc_df["FighterID_espn"].fillna(
            upcoming_ufc_df["FighterID_ufc"].map(fighter_id_map)
        ),
        OpponentID_espn = upcoming_ufc_df["OpponentID_espn"].fillna(
            upcoming_ufc_df["OpponentID_ufc"].map(fighter_id_map)
        ),
    )
    # upcoming_ufc_df is likely to contain some null FighterID_espn and OpponentID_espn.
    # I should address these by filling them in manually using base_maps.MANUAL_UFC_ESPN_MAP
    assert not upcoming_ufc_df[["FighterID_espn", "OpponentID_espn"]].isnull().any().any(), \
        upcoming_ufc_df.loc[upcoming_ufc_df[["FighterID_espn", "OpponentID_espn"]].isnull().any(axis=1),
            ["FighterID_espn", "OpponentID_espn", "FighterID_ufc", "OpponentID_ufc", "Date_espn", "Date_ufc"]]
    
    upcoming_espn_ufc_df = espn_df.merge(
        upcoming_ufc_df,
        on=["FighterID_espn", "OpponentID_espn", "Date_espn"],
        how="right",
        suffixes=("","_ufc")
    )
    espn_ufc_df = pd.concat([
        historical_espn_ufc_df,
        upcoming_espn_ufc_df,
    ]).sort_values("Date_espn").reset_index(drop=True)
    espn_ufc_df["is_upcoming"] = espn_ufc_df["is_upcoming"].fillna(0).astype(int)

    # join bfo data with espn_ufc_df
    bfo_espn_df = bfo_df.rename(columns={
        "FighterID": "FighterID_bfo",
        "OpponentID": "OpponentID_bfo",
        "Date": "Date_bfo",
    }).merge(
        bfo_to_espn_map,
        on=["FighterID_bfo", "OpponentID_bfo", "Date_bfo"],
        how="inner",
    )
    assert bfo_espn_df.shape[0] <= bfo_df.shape[0]
    # bfo_espn_df contains some duplicate fights. This is because of duplicate 
    # rows in bfo_df, owing to varying fighter name spellings across markets or
    # markets recording the date of a fight differently. These odds are still valuable,
    # so I'll mark them as such and figure out what to do with them later.
    duplicate_rows = bfo_espn_df[["FighterID_espn", "OpponentID_espn", "Date_espn"]].duplicated(keep=False)
    bfo_espn_df = bfo_espn_df.assign(duplicate_rows=duplicate_rows)
    # some of these duplicate rows are due to manual ID overwrites
    overwritten_bfo_ids = MANUAL_BFO_OVERWRITE_MAP.keys() | MANUAL_BFO_OVERWRITE_MAP.values()
    has_overwritten_id = bfo_espn_df["FighterID_bfo"].isin(overwritten_bfo_ids) | \
                    bfo_espn_df["OpponentID_bfo"].isin(overwritten_bfo_ids)
    # if it's not an ID overwrite, then the duplicate rows should be due to
    # date mismatches
    sub_df = bfo_espn_df.query("duplicate_rows == True").loc[~has_overwritten_id]
    assert (sub_df.groupby(["FighterID_espn", "OpponentID_espn", "Date_espn"])["Date_bfo"].nunique() == 2).all(), \
        sub_df

    bfo_espn_ufc_df = espn_ufc_df.merge(
        bfo_espn_df,
        on=["FighterID_espn", "OpponentID_espn", "Date_espn"],
        how="left",
        suffixes=("","_bfo")
    )
    # any duplicate rows in bfo_espn_ufc_df should be due to bfo data
    duplicate_rows = bfo_espn_ufc_df[["FighterID_espn", "OpponentID_espn", "Date_espn"]].duplicated(keep=False)
    assert bfo_espn_ufc_df.loc[duplicate_rows]["duplicate_rows"].all(skipna=False), \
        bfo_espn_ufc_df.loc[duplicate_rows]
    bfo_espn_ufc_df = bfo_espn_ufc_df.assign(duplicate_rows = bfo_espn_df["duplicate_rows"].fillna(False))
    # Make sure fight_id isn't missing. If not missing, this will 
    # be a no-op. If missing, we'll fill it in. fight_id may be
    # missing for upcoming fights.
    bfo_espn_ufc_df["fight_id"] = get_fight_id(
        bfo_espn_ufc_df["FighterID_espn"],
        bfo_espn_ufc_df["OpponentID_espn"],
        bfo_espn_ufc_df["Date_espn"],
    )

    print("checking for duplicate fights")
    print(bfo_espn_ufc_df["fight_id"].value_counts()[0:10])
    print("bfo_espn_ufc.shape:", bfo_espn_ufc_df.shape)
    print("bfo_espn_ufc_df.shape, with bfo and ufc:", 
        bfo_espn_ufc_df.dropna(
            subset=["FighterID_espn", "FighterID_ufc", "FighterID_bfo"]
        ).shape)
    assert not bfo_espn_ufc_df.duplicated(keep=False).any(), bfo_espn_ufc_df.loc[bfo_espn_ufc_df.duplicated(keep=False)]
    # joining the bestfightodds data causes some duplicate fights to get carried over, due to 
    # date mismatches. I'll just mark them as such and figure out what to do with them later
    assert bfo_espn_ufc_df.shape[0] >= espn_df.shape[0] + upcoming_ufc_df.shape[0], \
        (bfo_espn_ufc_df.shape, espn_df.shape, upcoming_ufc_df.shape)
    print("writing joined_bfo_espn_ufc_data")
    base_db_interface.write_replace(
        "joined_bfo_espn_ufc_data",
        bfo_espn_ufc_df,
    )
    return bfo_espn_ufc_df


def find_bfo_espn_mapping():
    bfo_df = load_bfo_df()
    # Fight in BFO with missing odds appear to include a high % of fights that
    # didn't actually happen. Whatever coverage we lose by dropping these fights
    # isn't worth the false positives we'd get by keeping them.
    bfo_df = bfo_df.dropna(subset=["FighterOpen", "OpponentOpen"])
    bfo_df = bfo_df[[
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
    map_finder = MapFinder(
        df_aux=bfo_df, df_canon=espn_df,
        aux_suffix="bfo", canon_suffix="espn",
        false_overwrite_canon_map=FALSE_OVERWRITE_ESPN_MAP,
        manual_map=MANUAL_BFO_ESPN_MAP,
        iso_finder_day_tol=1,
        day_tol=1,
        # include_tournament_fights=True,
        include_tournament_fights=False,
    )
    bfo_to_espn_map = map_finder.find_map(n_iters=20)
    print("bfo_to_espn_map.shape:", bfo_to_espn_map.shape)
    assert not bfo_to_espn_map.isnull().any().any(), bfo_to_espn_map.isnull().mean()
    # this should only have one row
    assert bfo_to_espn_map.set_index(["FighterID_espn", "OpponentID_espn", "Date_espn"]).loc[("4084453", "3153129", "2017-07-21")].shape[0] == 1
    # ensure that if a (FighterID_espn, OpponentID_espn, Date_espn) tuple appears
    # in bfo_to_espn_map more than once, then it appears with the same FighterID_bfo.
    # so the duplicate row can safely be dropped.
    espn_mapped_counts = bfo_to_espn_map[["FighterID_espn", "OpponentID_espn", "Date_espn"]].value_counts()
    espn_excess_counts = bfo_to_espn_map.set_index(["FighterID_espn", "OpponentID_espn", "Date_espn"])\
        .loc[espn_mapped_counts > 1]
    assert espn_excess_counts[["FighterID_bfo", "OpponentID_bfo"]].duplicated(keep=False).all(), espn_excess_counts.loc[~espn_excess_counts.duplicated(keep=False)]

    bfo_mapped_counts = bfo_to_espn_map[["FighterID_bfo", "OpponentID_bfo", "Date_bfo"]].value_counts()
    bfo_excess_counts = bfo_to_espn_map.set_index(["FighterID_bfo", "OpponentID_bfo", "Date_bfo"])\
        .loc[bfo_mapped_counts > 1]
    # ensure that if a (FighterID_bfo, OpponentID_bfo, Date_bfo) tuple appears
    # in bfo_to_espn_map more than once, then it appears with the same FighterID_espn.
    # so the duplicate row can safely be dropped.
    assert bfo_excess_counts[["FighterID_espn", "OpponentID_espn"]].duplicated(keep=False).all(), bfo_excess_counts.loc[~bfo_excess_counts.duplicated(keep=False)]

    # There will still be some rows that differ by one day. This is because
    # bestfightodds.com and espn.com have different timezones. We'll just
    # leave these rows in the dataset, they'll still help us in the join.
    bfo_to_espn_map = bfo_to_espn_map.drop_duplicates()
    print("bfo_to_espn_map.shape after dropping duplicates:", bfo_to_espn_map.shape)

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
        subset=["FighterID_espn", "OpponentID_espn", "Date_espn"],
        keep="first",
    ).drop(columns=["p_stats_null"])
    print("joined_df.shape after dropping duplicated opening money lines:", joined_df.shape)
    base_db_interface.write_replace(
        "clean_bfo_espn_ufc_data",
        joined_df,
    )
    # append upcoming fights
    upcoming_df = base_db_interface.read("bfo_ufc_upcoming_fights")
    joined_df = pd.concat([joined_df, upcoming_df]).reset_index()
    base_db_interface.write_replace(
        "clean_bfo_espn_ufc_data_plus_upcoming",
        joined_df,
    )