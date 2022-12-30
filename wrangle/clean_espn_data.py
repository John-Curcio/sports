import pandas as pd
import numpy as np
from db import base_db_interface
from wrangle.join_datasets import get_fight_id
"""
bfo_fighter_odds       espn_stats             ufc_round_totals
bfo_fighter_urls       ufc_events             ufc_strikes
espn_bio               ufc_fight_description  ufc_totals
espn_matches           ufc_fighters           ufc_upcoming_fights
espn_missed_fighters   ufc_round_strikes
"""


class EspnDataCleaner(object):

    def __init__(self):
        stats_df = base_db_interface.read("espn_stats")
        bio_df = base_db_interface.read("espn_bio")
        match_df = base_db_interface.read("espn_matches")

        for df in [stats_df, match_df]:
            df["Date"] = pd.to_datetime(df["Date"])

        # leave fighterID as number/firstname-lastname
        match_df["OpponentID"] = match_df["OpponentID"].str.split("_/id/").str[1]
        match_df["FighterID"] = match_df["FighterID"].str.split("/").str[0]
        match_df["OpponentID"] = match_df["OpponentID"].str.split("/").str[0]

        stats_df["OpponentID"] = stats_df["OpponentID"].str.split("_/id/").str[1]
        stats_df["FighterID"] = stats_df["FighterID"].str.split("/").str[0]
        stats_df["OpponentID"] = stats_df["OpponentID"].str.split("/").str[0]
        
        self.match_df = match_df 
        self.stats_df = stats_df 
        self.bio_df = bio_df

        self.clean_stats_df = None 
        self.clean_bio_df = None 
        self.clean_match_df = None 

        self.espn_df = None

    def _parse_bios(self):
        bio_df0 = self.bio_df.copy()
        bio_df0["FighterID"] = bio_df0["FighterID"].str.split("/").str[0]
        bio_df0["Name"] = bio_df0["Name"].str.strip().str.lower()

        bio_df0["ReachInches"] = bio_df0["Reach"].fillna('"').str[:-1]
        bio_df0["ReachInches"] = bio_df0["ReachInches"].replace("", np.nan).astype(float)

        def get_height_inches(s):
            # capture group btw ' and "
            height_inches = s.str.extract("\'(.*?)\"", expand=False).astype(float) 
            # capture group before '
            height_feet = s.str.extract("([^']*)", expand=False).astype(float) 
            return height_feet * 12 + height_inches

        ht_wt = bio_df0["HT/WT"].replace(np.nan, "nan'nan\",nan lbs")
        weight_lbs = ht_wt.str.extract("\,(.*?) lbs", expand=False).astype(float)

        bio_df0["WeightPounds"] = weight_lbs
        bio_df0["HeightInches"] = get_height_inches(ht_wt)

        ht = get_height_inches(bio_df0["Height"].replace(np.nan, "nan'nan\",nan lbs"))
        bio_df0["HeightInches"] = bio_df0["HeightInches"].fillna(ht)

        def get_clean_dob(s):
            if s.endswith(")"):
                s = s[:-len(" (67)")]
            return pd.to_datetime(s)

        bio_df0["DOB"] = bio_df0["Birthdate"].fillna("").apply(get_clean_dob)
        bio_df0 = bio_df0.drop(columns=["Birthdate"]) # redundant, I like the name DOB more

        self.clean_bio_df = bio_df0.drop(columns=["Reach", "HT/WT", "Weight", "Height"])
        return self.clean_bio_df

    def _parse_matches(self):
        match_df = self.match_df.assign(fight_id = get_fight_id(
            fighter_id=self.match_df["FighterID"],
            opponent_id=self.match_df["OpponentID"],
            date=self.match_df["Date"]
        ))
        self.clean_match_df = match_df.groupby("fight_id").first()\
            .reset_index()\
            .rename(columns={"Res.":"FighterResult"})
        return self.clean_match_df

    def _parse_stats(self):
        stats_df = self.stats_df.assign(fight_id = get_fight_id(
            fighter_id=self.stats_df["FighterID"],
            opponent_id=self.stats_df["OpponentID"],
            date=self.stats_df["Date"]
        ))
        ### format values
        unclean_cols = [
            'SDBL/A', 'SDHL/A', 'SDLL/A', 'TSL-TSA', '%BODY', 
            '%HEAD', '%LEG', 'TK ACC', 'SR', 'Res.',
        ]

        stats_clean_df = stats_df.drop(columns=unclean_cols)\
            .rename(columns={'SCBL\t': "SCBL", "SGBA\t":"SGBA"})
        stats_df = self.stats_df
        stats_clean_df["SDBL"] = stats_df["SDBL/A"].str.split("/").str[0]
        stats_clean_df["SDBA"] = stats_df["SDBL/A"].str.split("/").str[1]
        stats_clean_df["SDHL"] = stats_df["SDHL/A"].str.split("/").str[0]
        stats_clean_df["SDHA"] = stats_df["SDHL/A"].str.split("/").str[1]
        stats_clean_df["SDLL"] = stats_df["SDLL/A"].str.split("/").str[0]
        stats_clean_df["SDLA"] = stats_df["SDLL/A"].str.split("/").str[1]

        stat_cols = [
            'TSL', 'TSA', 'SSL', 'SSA', 'KD', 
            'SCBL', 'SCBA', 'SCHL', 'SCHA', 'SCLL', 'SCLA', 'RV',
            'TDL', 'TDA', 'TDS', 'SGBL', 'SGBA', 'SGHL', 'SGHA', 'SGLL', 'SGLA',
            'AD', 'ADTB', 'ADHG', 'ADTM', 'ADTS', 'SM', 'SDBL', 'SDBA', 
            'SDHL', 'SDHA', 'SDLL', 'SDLA',
        ]
        stats_clean_df[stat_cols] = stats_clean_df[stat_cols].replace("-", np.nan).astype(float)

        ### drop duplicate and useless rows
        stats_clean_df["all_stats_null"] = stats_clean_df[stat_cols].isnull().all(1)
        stats_clean_df["p_stats_zero"] = (stats_clean_df[stat_cols].fillna(0) == 0).mean(1)
        stats_clean_df["all_stats_null"] |= (stats_clean_df["p_stats_zero"] == 1)

        # ESPN has a LOT of fights where no stats were collected. Either they leave it 
        # blank or impute it as 0. We want to drop these fights because they are not 
        # only useless, but misleading - they make it look like the fighter did nothing!
        # so we drop fights where all stats are null or where all stats are 0.
        # and in the case of duplicate fights, we keep the one with the most stats.
        self.clean_stats_df = stats_clean_df.query("~all_stats_null")\
            .sort_values("p_stats_zero")\
            .groupby(["fight_id", "FighterID", "OpponentID"])\
            .first()\
            .reset_index()\
            .drop(columns=["p_stats_zero"])
        return self.clean_stats_df

    def _get_doubled_matches(self):
        if self.clean_match_df is None:
            self._parse_matches()
        # clean_match_df contains exactly one row per fight

        match_complement_df = self.clean_match_df.rename(columns={
            "FighterID":"OpponentID",
            "OpponentID":"FighterID",
            "Opponent":"Fighter",
        })
        match_complement_df["FighterResult"] = match_complement_df["FighterResult"].replace({
            "W":"L", "L":"W", "D":"D"
        })
        # dropping this Opponent column because it's just a name, not an ID
        doubled_match_df = pd.concat([self.clean_match_df, match_complement_df], axis=0)\
            .reset_index(drop=True)
        # doubled_match_df = doubled_match_df.drop(columns=["Opponent"])
        return doubled_match_df

        
    def parse_all(self):
        print("--- parsing bios ---")
        self._parse_bios()
        print("--- parsing matches ---")
        self._parse_matches()
        print("--- parsing stats ---")
        self._parse_stats()
        print("--- putting it all together ---")
        doubled_match_df = self._get_doubled_matches()
        # for each fight, get the Fighter's stats
        right = self.clean_stats_df.drop(
            columns=["Date", "Event", "OpponentID", "Opponent"]
        )
        match_stats_df = doubled_match_df.merge(
            right,
            how="left",
            on=["fight_id", "FighterID"],
        )
        # now, for each fight, get the Opponent's stats
        right = self.clean_stats_df.drop(
            columns=["Date", "Event", "OpponentID", "Opponent"]
        ).rename(columns={
            "FighterID": "OpponentID",
        })
        match_stats_df = match_stats_df.merge(
            right,
            how="left",
            on=["fight_id", "OpponentID"],
            suffixes=("", "_opp"),
        )

        espn_df = match_stats_df.merge(
            self.clean_bio_df,
            on="FighterID",
            how="left",
        ).merge(
            self.clean_bio_df,
            left_on="OpponentID",
            right_on="FighterID",
            how="left",
            suffixes=("", "_opp"),
        )
        
        espn_df["Date"] = pd.to_datetime(espn_df["Date"])
        espn_df["FighterName"] = espn_df["Name"].fillna(espn_df["Fighter"])
        espn_df["OpponentName"] = espn_df["Name_opp"].fillna(espn_df["Opponent"])
        espn_df = espn_df.drop(columns=["Name", "Name_opp", "Fighter", "Opponent"])
        for col in ["FighterName", "OpponentName"]:
            espn_df[col] = espn_df[col].str.lower().str.strip()

        self.espn_df = espn_df
        return self.espn_df

