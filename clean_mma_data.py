import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from mma_espn_replace import replace_dict, manual_fix_ml_df

class DataCleaner(object):

    def __init__(self, stats_path, bio_path, match_path):
        self.stats_df = pd.read_csv(stats_path)
        self.bio_df = pd.read_csv(bio_path)
        self.match_df = pd.read_csv(match_path)

        self.totals_df = None
        self.clean_stats_df = None 
        self.clean_bio_df = None 
        self.clean_match_df = None 

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

    def parse_all(self):
        self._parse_bios()
        self._parse_matches()
        self._parse_stats()

if __name__ == "__main__":
    stats_path = "scrape/scraped_data/mma/espn/espn_stats_2022-05-20.csv"
    bio_path = "scrape/scraped_data/mma/espn/espn_bios_2022-05-20.csv"
    match_path = "scrape/scraped_data/mma/espn/espn_matches_2022-05-20.csv"

    DC = DataCleaner(stats_path, bio_path, match_path)
    DC.parse_all()

    on_cols = ["FighterID", "OpponentID", "Date"]
    stats_df = DC.clean_stats_df.merge(
        DC.clean_match_df[[*on_cols, 'Decision', 'Rnd', 'Time']],
        on=on_cols,
        how="left"
    )

    DC.clean_bio_df.to_csv("data/clean_bios.csv", index=False)
    stats_df.to_csv("data/clean_stats.csv", index=False)
    DC.clean_match_df.to_csv("data/clean_matches.csv", index=False)

    print("finished cleaning data. you can find it in the data/ directory")



