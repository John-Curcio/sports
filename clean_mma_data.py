import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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

    def parse_moneylines(self):
        ml_df = self.odds_df.query("bet_type == 'money-line'").drop(columns=["Unnamed: 0", "bet_type", "score_A", "score_B"])
        ml_df["date"] = pd.to_datetime(ml_df["date"])
        
        p_A = ml_df["opener_A"].apply(self.get_implied_opener_prob)
        p_B = ml_df["opener_B"].apply(self.get_implied_opener_prob)
        p_A_norm = p_A / (p_A + p_B)
        ml_df["opener_p_A_norm"] = p_A_norm
        # _norm because p_A and p_B may not add to 1. casinos effectively taking less juice
        self.ml_df = ml_df
        return ml_df
        
    def parse_totals(self):
        # I'm just gonna forget about this for now. Just seems
        # more annoying to parse, let alone model
        return NotImplemented

    def parse_stats(self):
        bio_df = self.clean_bio_df
        if bio_df is None:
            bio_df = self.parse_bios()
        stats_df0 = self.stats_df.rename(columns={"SGBA\t": "SGBA", "SCBL\t": "SCBL"})

        stat_columns = [col for col in stats_df0.columns if col not in ["Date", "Opponent", "Event", "Res.", "FighterID"]]
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

        self.clean_stats_df = clean_stats_df
        return clean_stats_df

    def parse_bios(self):
        bio_df0 = self.bio_df.copy()
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

    def parse_matches(self):
        bio_df = self.clean_bio_df
        if bio_df is None:
            bio_df = self.parse_bios()
        ####
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
        clean_match_df["Date"] = pd.to_datetime(clean_match_df["Date"])
        self.clean_match_df = clean_match_df
        return clean_match_df

    def parse_all(self):
        self.parse_bios()
        self.parse_stats()
        self.parse_matches()
        self.parse_moneylines()

if __name__ == "__main__":
    odds_path = "scrape/scraped_data/ufc_2017-04-08_2017-12-31.csv"
    stats_path = "scrape/scraped_data/mma/all_fighter_stats.csv"
    bio_path = "scrape/scraped_data/mma/all_fighter_bios.csv"
    match_path = "scrape/scraped_data/mma/all_matches.csv"
    DC = DataCleaner(odds_path, stats_path, bio_path, match_path)
    DC.parse_all()

    DC.clean_bio_df.to_csv("data/clean_bios.csv")
    DC.clean_stats_df.to_csv("data/clean_stats.csv")
    DC.clean_match_df.to_csv("data/clean_matches.csv")
    DC.ml_df.to_csv("data/ufc_moneylines.csv") # TODO still have to get IDs for both guys in the moneylines data...