import numpy as np
import pandas as pd
from tqdm import tqdm
import string

class Preprocessor(object):
    # create features that are annoying or inconvenient to obtain
    # we can get rid of them and refine them later
    
    def __init__(self, df):
        self.raw_df = df
        self.pp_df = None
        
    def preprocess(self):
        df = self.assign_fight_time(self.raw_df)
        df = self.assign_fc(df)
        df = self.assign_parsed_decision(df)
        df = self.assign_weight(df)
        df = self.assign_gender(df)
        df = self.assign_clean_stats(df)
        self.pp_df = self.assign_career_stats(df)
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
        fighter_ids = sorted(set(df["espn_fighter_id"]) | set(df["espn_opponent_id"]))
        career_df = []
        for fighter_id in tqdm(fighter_ids):
            inds = (df[["espn_fighter_id", "espn_opponent_id"]] == fighter_id).any(1)
            fight_ids = df.loc[inds, "espn_fight_id"]
            first_fight = df.loc[inds, "Date"].min()
            n_career_fights = np.arange(0, inds.sum())
            # if it's his first ufc fight, just give him n_ufc_fights=1. getting
            # signed to the ufc at all has some value
            n_ufc_fights = df.loc[inds, "is_ufc"].cumsum() # .shift().fillna(0)
            t_since_first_fight = (df.loc[inds, "Date"] - first_fight).dt.days
            t_since_last_fight = df.loc[inds, "Date"].diff().dt.days
            total_time_in_cage = df.loc[inds, "time_seconds"].cumsum().shift().fillna(0)
            min_weight = df.loc[inds, "fight_weight"].cummin()
            max_weight = df.loc[inds, "fight_weight"].cummax()
            prev_weight = df.loc[inds, "fight_weight"].shift()
            career_df.append(pd.DataFrame({
                "espn_fight_id": fight_ids,
                "espn_fighter_id": fighter_id,
                "n_career_fights": n_career_fights,
                "n_ufc_fights": n_ufc_fights,
                "t_since_first_fight": t_since_first_fight,
                "t_since_prev_fight": t_since_last_fight,
                "total_time_in_cage": total_time_in_cage,
                "min_weight": min_weight,
                "max_weight": max_weight,
                "prev_weight": prev_weight,
            }))
        career_df = pd.concat(career_df).reset_index(drop=True)
        df = df.merge(
            career_df, how="left", on=["espn_fight_id", "espn_fighter_id"]
        ).merge(
            career_df, how="left", 
            left_on=["espn_fight_id", "espn_opponent_id"],
            right_on=["espn_fight_id", "espn_fighter_id"],
            suffixes=("", "_opp"),
        )
        return df
        
    @staticmethod
    def assign_fight_time(df):
        time_into_round = df["Time"].replace("-", "0:0").fillna("0:0").str.split(":")
        n_rounds = df["Rnd"].replace("-", np.nan).astype(float)
        n_seconds = (
            (n_rounds - 1) * 5 * 60 +
            time_into_round.str[0].astype(int) * 60 +
            time_into_round.str[1].astype(int)
        )
        return df.assign(time_seconds = n_seconds)
        
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
            set(df.loc[is_fem_fighter, "espn_fighter_id"]) |
            set(df.loc[is_fem_fighter, "espn_opponent_id"]) 
        )
        is_fem_fighter = (
            df["espn_fighter_id"].isin(fem_fatales) |
            df["espn_opponent_id"].isin(fem_fatales)
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
            stat_fighter = df[f"{stat_name}_espn"].fillna(df[f"{stat_name}_ufc"])
            stat_opponent = df[f"{stat_name}_opp_espn"].fillna(df[f"{stat_name}_opp_ufc"])            
            df[stat_name] = stat_fighter
            df[f"{stat_name}_opp"] = stat_opponent
        # SM_fails
        sm_finish = df["decision_clean"] == "submission"
        sm_landed_fighter  = (sm_finish & (df["FighterResult"] == "W")).astype(int)
        df["SM_fail"] = np.maximum(df["SM"] - sm_landed_fighter, 0)
        sm_landed_opponent = (sm_finish & (df["FighterResult"] == "L")).astype(int)
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