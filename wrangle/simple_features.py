import numpy as np
import pandas as pd
from tqdm import tqdm
import string
import multiprocessing

def _get_career_stats(fighter_group):
    fighter_id, fighter_group = fighter_group
    # fighter_df = fighter_group.sort_values("Date")
    fighter_df = fighter_group.drop_duplicates(subset="fight_id").sort_values("Date")

    first_fight = fighter_df["Date"].min()
    n_career_fights = np.arange(0, len(fighter_df))
    # if it's his first ufc fight, just give him n_ufc_fights=1. getting
    # signed to the ufc at all has some value
    n_ufc_fights = fighter_df["is_ufc"].cumsum()
    t_since_first_fight = (fighter_df["Date"] - first_fight).dt.days
    t_since_last_fight = fighter_df["Date"].diff().dt.days # .fillna(0)
    total_time_in_cage = fighter_df["time_seconds"].cumsum().shift().fillna(0)
    min_weight = fighter_df["fight_weight"].cummin()
    max_weight = fighter_df["fight_weight"].cummax()
    prev_weight = fighter_df["fight_weight"].shift() # .fillna(0)
    return pd.DataFrame({
        "fight_id": fighter_df["fight_id"],
        "FighterID_espn": fighter_id,
        "n_career_fights": n_career_fights,
        "n_ufc_fights": n_ufc_fights,
        "t_since_first_fight": t_since_first_fight,
        "t_since_prev_fight": t_since_last_fight,
        "total_ufc_cage_time": total_time_in_cage,
        "min_weight": min_weight,
        "max_weight": max_weight,
        "prev_weight": prev_weight,
    })

class Preprocessor(object):
    # create features that are annoying or inconvenient to obtain
    # we can get rid of them and refine them later
    
    def __init__(self, df):
        self.raw_df = df
        self.pp_df = None
        
    def preprocess(self):
        # df = self.assign_fight_time(self.raw_df)
        df = self.raw_df
        df = self.assign_fc(df)
        df = self.assign_parsed_decision(df)
        df = self.assign_weight(df)
        df = self.assign_gender(df)
        df = self.assign_clean_stats(df)
        self.pp_df = self.assign_career_stats(df)
        # we shouldn't be introducing any extra rows
        assert self.pp_df.shape[0] == df.shape[0] == self.raw_df.shape[0]
        return self.pp_df
    
    @staticmethod
    def assign_parsed_decision(df):
        #decision_clean = df["Decision"].copy()
        decision_clean = pd.Series("", index=df.index)
        dec_raw = df["Decision"].fillna("").str.lower()
        # finishes - submissions
        sub_inds = dec_raw.str.contains("submission") | dec_raw.str.contains("sumission")
        # this will get overwritten by "submission (punches) and technical decision"
        decision_clean.loc[sub_inds] = "submission"
        # finishes (tko/ko)
        ko_inds = (
            dec_raw.str.startswith("ko (") |
            (dec_raw == "tko/ko") |
            (dec_raw == "ko/tko") |
            (dec_raw == "ko") | 
            dec_raw.str.startswith("tko") |
            (dec_raw == "submission (punches)") |
            (dec_raw == "submission (slam)")
        )
        decision_clean.loc[ko_inds] = "tko/ko"
        # decisions
        una_dec_inds = (
            (dec_raw == "decision - unanimous") | 
            (dec_raw == "unanimous decision") |
            (dec_raw == "decision")
        )
        decision_clean.loc[una_dec_inds] = "decision - unanimous"
        split_dec_inds = (
            (dec_raw == "split decision") | 
            (dec_raw == "decision - split") |
            (dec_raw == "majority decision") |
            (dec_raw == "decision - majority")
        )
        decision_clean.loc[split_dec_inds] = "decision - split"
        tech_dec_inds = dec_raw.str.startswith("technical decision")
        decision_clean.loc[tech_dec_inds] = "technical decision"
        # draws
        draw_inds = df["FighterResult"] == "D"
        decision_clean.loc[draw_inds] = "draw"
        dq_inds = dec_raw.str.contains("dq") 
        decision_clean.loc[dq_inds] = "dq"
        nc_inds = dec_raw.str.contains("no contest")
        decision_clean.loc[nc_inds] = "nc"

        # submission method
        sub_method = pd.Series("", index=df.index)
        sub_method.loc[sub_inds] = df.loc[sub_inds, "Decision"].str.split("(").str[1].fillna("")#.str[:-1].str.lower()
        return df.assign(
            decision_clean=decision_clean,
            sub_method=sub_method,
        )
        
    def assign_career_stats(self, df):
        df = df.sort_values("Date")
        career_df = []
        with multiprocessing.Pool(2) as pool:
            # wrapping this in a list is necessary because imap returns an iterator, 
            # and we need the append method later.
            # using tqdm to show a progress bar, keep my sanity - tqdm is just a wrapper,
            # only returns another iterable
            fighter_groupby = df.dropna(subset="FighterID_espn").groupby("FighterID_espn")
            career_df = list(tqdm(pool.imap(_get_career_stats, fighter_groupby, chunksize=10)))
        # impute career stats for null FighterID_espn
        # if the fighter is unknown, it must be his first fight
        # inds = df[["FighterID_espn", "OpponentID_espn"]].isnull().any(1)
        inds = df["FighterID_espn"].isnull() # | df["OpponentID_espn"].isnull()
        fight_ids = df.loc[inds, "fight_id"]
        career_df.append(pd.DataFrame({
            "fight_id": fight_ids,
            "FighterID_espn": np.nan,
            "n_career_fights": 0,
            "n_ufc_fights": 0,
            "t_since_first_fight": 0,
            "t_since_prev_fight": np.nan,
            "total_ufc_cage_time": 0,
            "min_weight": df.loc[inds, "fight_weight"],
            "max_weight": df.loc[inds, "fight_weight"],
            "prev_weight": np.nan,
        }))
        # merge career stats into main df
        career_df = pd.concat(career_df).reset_index(drop=True)
        df = df.merge(
            career_df, how="left", on=["fight_id", "FighterID_espn"]
        ).merge(
            career_df, how="left", 
            left_on=["fight_id", "OpponentID_espn"],
            right_on=["fight_id", "FighterID_espn"],
            suffixes=("", "_opp"),
        )
        return df
        
    # @staticmethod
    # def assign_fight_time(df):
    #     time_into_round = df["Time"].replace("-", "0:0").fillna("0:0").str.split(":")
    #     n_rounds = df["Rnd"].replace("-", np.nan).astype(float)
    #     n_seconds = (
    #         (n_rounds - 1) * 5 * 60 +
    #         time_into_round.str[0].astype(int) * 60 +
    #         time_into_round.str[1].astype(int)
    #     )
    #     return df.assign(time_seconds = n_seconds)
        
    @staticmethod
    def assign_weight(df):
        weight_map = {
            "Super Heavyweight": 300, # just punching in something lol
            "Heavyweight": 265, 
            "Light Heavyweight": 205, 
            "Middleweight": 185,
            "Welterweight": 170,
            "Lightweight": 155,
            "Featherweight": 145,
            "Bantamweight": 135,
            "Flyweight": 125,
            "Strawweight": 115,
            "Atomweight": 105,
            "Lightweight - DREAM (70kg)": 155,
            "Women's Lightweight": 155,
            "Women's Featherweight": 145,
            "Women's Bantamweight": 135,
            "Women's Flyweight": 125,
            "Women's Strawweight": 115,
            "Women's Atomweight": 105,
            "Catch Weight": np.nan,
            "Catchweight": np.nan,
            "Open Weight": np.nan,
            "": np.nan,
        }
        return df.assign(fight_weight=df["WT Class"].fillna("").map(weight_map))
        
    @staticmethod
    def assign_gender(df):
        is_fem_fighter = df["WT Class"].fillna("").str.startswith("Women")
        fem_fatales = (
            set(df.loc[is_fem_fighter, "FighterID_espn"]) |
            set(df.loc[is_fem_fighter, "OpponentID_espn"]) 
        )
        is_fem_fighter = (
            df["FighterID_espn"].isin(fem_fatales) |
            df["OpponentID_espn"].isin(fem_fatales)
        )
        return df.assign(gender=is_fem_fighter.map({True:"W", False:"M"}))
    
    @staticmethod
    def assign_fc(df):
        # UFC, ONE, etc
        event_str = df["Event"].fillna("").str.lower().str.strip()
        # M-1 and K-1 are legit leagues
        fc_name_parsed = event_str.str.split(":").str[0]\
            .str.split("- ").str[0]\
            .str.strip().str.rstrip(string.digits) # remove trailing digits
        is_ufc = fc_name_parsed.str.contains("ufc")
        return df.assign(
            is_ufc=is_ufc,
            fc_name_parsed=fc_name_parsed,
        )
    
    @staticmethod
    def assign_clean_stats(df):
        shared_ufc_espn_stats = [
            "KD", "SM", "RV", "SSL", "SSA", 
            "TSL", "TSA", "TDL", "TDA",
        ]
        for stat_name in shared_ufc_espn_stats:
            stat_fighter = df[f"{stat_name}"].fillna(df[f"{stat_name}_ufc"])
            stat_opponent = df[f"{stat_name}_opp"].fillna(df[f"{stat_name}_opp_ufc"])            
            df[stat_name] = stat_fighter
            df[f"{stat_name}_opp"] = stat_opponent
        # SM_fails
        sm_finish = df["decision_clean"] == "submission"
        sm_landed_fighter  = (sm_finish & (df["FighterResult"] == "W")).astype(int)
        df["SML"] = sm_landed_fighter
        df["SMA"] = np.maximum(df["SM"], df["SML"])
        df["SM_fail"] = np.maximum(df["SM"] - sm_landed_fighter, 0)
        
        sm_landed_opponent = (sm_finish & (df["FighterResult"] == "L")).astype(int)
        df["SML_opp"] = sm_landed_opponent
        df["SMA_opp"] = np.maximum(df["SM_opp"], df["SML_opp"])
        df["SM_fail_opp"] = np.maximum(df["SM_opp"] - sm_landed_opponent, 0)
        
        attempt_cols = [
            # stats from both ufcstats.com and espn
            "SSA", "TSA", "TDA",
            # espn stats
            "SCBA", "SCHA", "SCLA",
            "SGBA", "SGHA", "SGLA",
            "SDBA", "SDHA", "SDLA",
            # ufcstats.com stats
            "SHA", "SBA", "SLA", 
            "SDA", "SCA", "SGA",
        ]
        for attempt_col in attempt_cols:
            landed_col = attempt_col[:-1] + "L"
            fail_col = attempt_col[:-1] + "_fail"
            attempt_col_opp = attempt_col + "_opp"
            landed_col_opp = landed_col + "_opp"
            fail_col_opp = fail_col + "_opp"
            df[fail_col]     = np.maximum(0, df[attempt_col]     - df[landed_col])
            df[fail_col_opp] = np.maximum(0, df[attempt_col_opp] - df[landed_col_opp])
        return df