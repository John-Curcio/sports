import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from mma_espn_replace import replace_dict, manual_fix_ml_df

class DataCleaner(object):

    def __init__(self, odds_path, stats_path, bio_path, match_path):
        self.odds_df = pd.read_csv(odds_path)
        self.stats_df = pd.read_csv(stats_path)
        self.bio_df = pd.read_csv(bio_path)
        self.match_df = pd.read_csv(match_path)

        self.ml_df = None
        self.totals_df = None
        self.clean_stats_df = None 
        self.clean_bio_df = None 
        self.clean_match_df = None 
        self.ml_stats_df = None

    @staticmethod
    def get_implied_opener_prob(moneyline):
        if moneyline is np.nan:
            return np.nan
        if moneyline == "-":
            return np.nan
        if moneyline.startswith("+"):
            # plus for the underdog
            return 100. / (float(moneyline[1:]) + 100)
        if moneyline.startswith("-"):
            # negative for the favorite
            x = float(moneyline[1:])
            return x / (x + 100)
        raise Exception("can't parse money line: {}".format(moneyline))

    def _parse_moneylines(self):
        ml_df = self.odds_df.query("bet_type == 'money-line'").drop(columns=["Unnamed: 0", "bet_type", "score_A", "score_B"])
        ml_df["date"] = pd.to_datetime(ml_df["date"])
        
        p_A = ml_df["opener_A"].apply(self.get_implied_opener_prob)
        p_B = ml_df["opener_B"].apply(self.get_implied_opener_prob)
        p_A_norm = p_A / (p_A + p_B)
        ml_df["opener_p_A_norm"] = p_A_norm
        # _norm because p_A and p_B may not add to 1. casinos effectively taking less juice
        ml_df["team_A"] = ml_df["team_A"].str.strip().str.lower()
        ml_df["team_B"] = ml_df["team_B"].str.strip().str.lower()
        ml_df0 = ml_df.rename(columns={"team_A": "FighterName", "team_B": "OpponentName", 
                                    "date": "Date", "opener_p_A_norm": "FighterOpener"})
        ml_df0 = manual_fix_ml_df(ml_df0) # manually tweak some rows to find matches btw espn and sbr
        #### matching with fights btw ml_df  and clean_match_df
        # matching on Date=Date, FighterName=FighterName, OpponentName=OpponentName
        ml_df1 = ml_df0.merge(self.clean_match_df, on=["FighterName", "OpponentName", "Date"], how="left")
        # matching on Date=Date, FighterName=OpponentName, OpponentName=FighterName
        ml_df2 = ml_df0.rename(columns={"FighterName": "OpponentName", "OpponentName": "FighterName", "date": "Date"})
        ml_df2 = ml_df2.merge(self.clean_match_df, on=["FighterName", "OpponentName", "Date"], how="left")

        clean_ml_df = ml_df1.copy()
        # FighterID -> OpponentID and vice versa because ml_df2 is the result of swapping
        # note that we don't have to swap socre_A & score_B, opener_A & opener_B, etc
        clean_ml_df["FighterID"] = clean_ml_df["FighterID"].fillna(ml_df2["OpponentID"])
        clean_ml_df["OpponentID"] = clean_ml_df["OpponentID"].fillna(ml_df2["FighterID"])
        ml_df2["FighterResult"] = ml_df2["FighterResult"].replace({"W": "L", "L": "W"})
        for col in ["FighterResult", "Decision", "Rnd", "Time", "Event"]:
            clean_ml_df[col] = clean_ml_df[col].fillna(ml_df2[col])
        ## This should leave about 149 FighterID, OpponentIDs unaccounted for. Almost all of these fights 
        # appear to simply have been cancelled. 
        self.ml_df = clean_ml_df
        return ml_df
        
    def _parse_totals(self):
        # I'm just gonna forget about this for now. Just seems
        # more annoying to parse, let alone model
        return NotImplemented

    def _parse_stats(self):
        bio_df = self.clean_bio_df
        match_df = self.clean_match_df
        if bio_df is None:
            bio_df = self._parse_bios()
        if match_df is None:
            match_df = self._parse_matches()
        stats_df0 = self.stats_df.rename(columns={"SGBA\t": "SGBA", "SCBL\t": "SCBL"})
        stats_df0["OpponentID"] = stats_df0["OpponentID"].str[len("http://www.espn.com/mma/fighter/_/id/"):]
        stat_columns = [col for col in stats_df0.columns if col not in ["Date", "Opponent", "Event", "Res.", "OpponentID", "FighterID"]]
        for col in stat_columns:
            stats_df0[col] = stats_df0[col].replace("-", np.nan)
        for col in ["TSL-TSA", "%BODY", "%HEAD", "%LEG", "TK ACC"]:
            clean_series = stats_df0[col].apply(lambda x: np.nan if x is np.nan else float(x[:-1]) / 100)
            stats_df0[col] = clean_series

        stats_df0["SDBL"] = stats_df0["SDBL/A"].apply(lambda x: np.nan if x is np.nan else int(x.split("/")[0]))
        stats_df0["SDBA"] = stats_df0["SDBL/A"].apply(lambda x: np.nan if x is np.nan else int(x.split("/")[1]))

        stats_df0["SDHL"] = stats_df0["SDHL/A"].apply(lambda x: np.nan if x is np.nan else int(x.split("/")[0]))
        stats_df0["SDHA"] = stats_df0["SDHL/A"].apply(lambda x: np.nan if x is np.nan else int(x.split("/")[1]))

        stats_df0["SDLL"] = stats_df0["SDLL/A"].apply(lambda x: np.nan if x is np.nan else int(x.split("/")[0]))
        stats_df0["SDLA"] = stats_df0["SDLL/A"].apply(lambda x: np.nan if x is np.nan else int(x.split("/")[1]))

        for col in set(stat_columns) - {"SDBL/A", "SDHL/A", "SDLL/A"}:
            stats_df0[col] = pd.to_numeric(stats_df0[col])

        clean_stats_df = stats_df0.merge(bio_df[["FighterID", "Name"]], on="FighterID", how="left")
        clean_stats_df["Date"] = pd.to_datetime(clean_stats_df["Date"])
        clean_stats_df = clean_stats_df.rename(columns={"Res.": "FighterResult"})

        ###### Remove duplicate fights
        # if values are missing, make sure it's obvious!
        temp_cols = [
            'TSL', 'TSA', 'SSL',
            'SSA', #'TSL-TSA', 
            'KD', '%BODY', '%HEAD', '%LEG', 'SCBL',
        'SCBA', 'SCHL', 'SCHA', 'SCLL', 'SCLA', 'RV', 'SR', 'TDL', 'TDA', 'TDS',
        'TK ACC', 'SGBL', 'SGBA', 'SGHL', 'SGHA', 'SGLL', 'SGLA', 'AD', 'ADTB',
        'ADHG', 'ADTM', 'ADTS', 'SM', 'SDBL', 'SDBA', 'SDHL',
        'SDHA', 'SDLL', 'SDLA'
        ]

        stats_missing = clean_stats_df[temp_cols].isnull().all(1) | (clean_stats_df[temp_cols] == 0).all(1)
        clean_stats_df["stats_missing"] = stats_missing
        clean_stats_df.loc[stats_missing, temp_cols] = np.nan

        clean_stats_df["n_null_stats"] = clean_stats_df[temp_cols].isnull().sum(1)
        clean_stats_df["n_zero_stats"] = (clean_stats_df[temp_cols] == 0).sum(1)

        clean_stats_df = clean_stats_df \
            .sort_values(["n_null_stats", "n_zero_stats"], ascending=True) \
            .drop_duplicates(subset=["FighterID", "OpponentID", "Date"]) \
            .drop(columns=["n_null_stats", "n_zero_stats"]) \
            .sort_values(["Date", "FighterID"])
        ######

        self.clean_stats_df = clean_stats_df
        return clean_stats_df

    def _parse_bios(self):
        bio_df0 = self.bio_df.copy()
        bio_df0["Name"] = bio_df0["Name"].str.strip().str.lower()

        bio_df0["ReachInches"] = bio_df0["Reach"].fillna('"').str[:-1]
        bio_df0["ReachInches"] = bio_df0["ReachInches"].replace("", np.nan).astype(float)

        def get_height_inches(s):
            height_inches = s.str.extract("\'(.*?)\"", expand=False).astype(float) # capture group btw ' and "
            height_feet = s.str.extract("([^']*)", expand=False).astype(float) # capture group before '
            return height_feet * 12 + height_inches

        ht_wt = bio_df0["HT/WT"].replace(np.nan, "nan'nan\",nan lbs")
        weight_lbs = ht_wt.str.extract("\,(.*?) lbs", expand=False).astype(float)

        bio_df0["WeightPounds"] = weight_lbs
        bio_df0["HeightInches"] = get_height_inches(ht_wt)

        ht = get_height_inches(bio_df0["Height"].replace(np.nan, "nan'nan\",nan lbs"))
        bio_df0["HeightInches"] = bio_df0["HeightInches"].fillna(ht)

        def get_clean_dob(s):
            if s is np.nan:
                return None
            if s.endswith(")"):
                s = s[:-len(" (67)")]
            return pd.to_datetime(s)

        bio_df0["DOB"] = bio_df0["DOB"].apply(get_clean_dob)

        self.clean_bio_df = bio_df0.drop(columns=["Reach", "HT/WT", "Weight", "Height"])
        return self.clean_bio_df

    def _parse_matches(self):
        bio_df = self.clean_bio_df
        if bio_df is None:
            bio_df = self._parse_bios()
        match_df = self.match_df
        # Getting names for the Fighter
        right = bio_df[["FighterID", "Name"]].rename(columns={"FighterID": "FighterID", "Name": "FighterName"})
        match_df0 = match_df.merge(right, on="FighterID", how="left")
        # getting names for the Opponent
        match_df0["OpponentID"] = match_df0["OpponentID"].str[len("http://www.espn.com/mma/fighter/_/id/"):]
        right = bio_df[["FighterID", "Name"]].rename(columns={"FighterID": "OpponentID", "Name": "OpponentName"})
        match_df0 = match_df0.merge(right, on="OpponentID", how="left")
        
        # removing some nulls
        # go by ascending order of null counts, so that if there's a duplicate
        # the first row will have fewer null columns, and should be kept
        loc_inds = match_df0.isnull().sum(1).sort_values(ascending=True).index # fewer nulls towards the top
        clean_match_df = match_df0.loc[loc_inds].drop_duplicates(["Date", "FighterID", "OpponentID"])
        # final formatting stuff
        clean_match_df["Date"] = pd.to_datetime(clean_match_df["Date"])
        clean_match_df = clean_match_df.rename(columns={"Res.": "FighterResult"})
        self.clean_match_df = clean_match_df
        return clean_match_df

    def _join_ml_and_stats(self):
        stats_df = self.clean_stats_df.rename(columns={"Opponent": "OpponentName"})
        ml_df = self.ml_df.copy()
        stats_df["OpponentName"] = stats_df["OpponentName"].str.strip().str.lower()

        temp_stats_df = stats_df.drop(columns=["Event", "FighterResult", "Name"])
        full_df0 = ml_df.merge(temp_stats_df, on=["FighterID", "OpponentName", "Date"], 
                            how="left")
        temp_stats_df = temp_stats_df.rename(columns={"FighterID": "OpponentID", 
                                                    "OpponentName": "FighterName"})
        full_df = full_df0.merge(temp_stats_df, on=["FighterName", "OpponentID", "Date"], 
                            how="left", suffixes=("_Fighter", "_Opponent"))
        self.ml_stats_df = full_df
        return self.ml_stats_df

    def parse_all(self):
        self._parse_bios()
        self._parse_matches()
        self._parse_stats()
        self._parse_moneylines()
        self._join_ml_and_stats()

if __name__ == "__main__":
    odds_path = "scrape/scraped_data/concated-ufc_2017-04-08_2021-12-20.csv"
    stats_path = "scrape/scraped_data/mma/all_fighter_stats.csv"
    bio_path = "scrape/scraped_data/mma/all_fighter_bios.csv"
    match_path = "scrape/scraped_data/mma/all_matches.csv"
    DC = DataCleaner(odds_path, stats_path, bio_path, match_path)
    DC.parse_all()

    DC.clean_bio_df.to_csv("data/clean_bios.csv", index=False)
    DC.clean_stats_df.to_csv("data/clean_stats.csv", index=False)
    DC.clean_match_df.to_csv("data/clean_matches.csv", index=False)
    DC.ml_df.to_csv("data/ufc_moneylines.csv", index=False)

    DC.ml_stats_df.to_csv("data/moneylines_and_fight_stats.csv", index=False)

    print("finished cleaning data. you can find it in the data/ directory")



