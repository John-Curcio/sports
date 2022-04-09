from tqdm import tqdm
import pandas as pd 
import numpy as np 


# since these are rolling features, data in the test dataset is inherently dependent on the train dataset
# but the train dataset can be used by itself
# that being said I should probably worry about that later. figure out how to prevent data leakage later
# oh fuck okay, i need to get stats for how good the other guy is

class DataPreprocessor(object):

    def __init__(self, stats_df):
        self.stats_df = stats_df 
        self.full_stats_df = None

    def get_preprocessed_df(self):
        # calculate useful stats that aren't necessarily rolling features
        # time in seconds
        stats_df = self.stats_df.copy()
        def parse_time_str(s):
            if s == "-":
                return np.nan
            minutes, seconds = s.split(":")
            return int(minutes)*60 + int(seconds)
        stats_df["time_seconds"] = stats_df["Time"].fillna("-").apply(parse_time_str)
        # could not continue --> TKO or stoppage?
        # overturned / no contest is same thing.
        # draw, no contest, DQ or other weird results should all just get lumped into "other"
        def parse_decision(s):
            if (s.startswith("submission") or 
                s.startswith("sumission") or
                s.startswith("technical submission")
            ):
                return "submission"
            if (s.startswith("tko") or 
                s.startswith("ko") or
                (s == 'could not continue')
            ):
                return "tko_ko"
            if "decision" in s:
                return "decision"
            return "other"

        temp_decision = stats_df["Decision"].fillna("-").str.lower().str.strip()
        stats_df["decision_clean"] = temp_decision.apply(parse_decision)
        # chael sonnen kept getting sprawled on and winding up on his back
        stats_df["TK_fails"] = stats_df["TDA"] - stats_df["TDL"]
        # does he go for an arm bar and then wind up on bottom?
        stats_df["submission_rate"] = (stats_df["decision_clean"] == 'submission').astype(int) / stats_df["SM"]

        stats_df["distance_strikes_landed"] = stats_df[["SDBL", "SDHL", "SDLL"]].sum(1, skipna=False)
        stats_df["clinch_strikes_landed"] = stats_df[["SCBL", "SCHL", "SCLL"]].sum(1, skipna=False)
        stats_df["ground_strikes_landed"] = stats_df[["SGBL", "SGHL", "SGLL"]].sum(1, skipna=False)
        stats_df["standing_strikes"] = stats_df["distance_strikes_landed"] + stats_df["clinch_strikes_landed"]
        # how many strikes result in a guy getting knocked down?
        stats_df["KD_power"] = stats_df["KD"] / stats_df["standing_strikes"]
        self._merge_opp_df(stats_df)
        return self.full_stats_df

    def _merge_opp_df(self, stats_df):
        opp_stats = [
            'TSL', 'TSA', 'SSL',
            'SSA', 'TSL-TSA', 'KD', '%BODY', '%HEAD', '%LEG', 'SCBL',
            'SCBA', 'SCHL', 'SCHA', 'SCLL', 'SCLA', 'RV', 'SR', 'TDL', 'TDA', 'TDS',
            'TK ACC', 'SGBL', 'SGBA', 'SGHL', 'SGHA', 'SGLL', 'SGLA', 'AD', 'ADTB',
            'ADHG', 'ADTM', 'ADTS', 'SM', 'SDBL', 'SDBA', 'SDHL',
            'SDHA', 'SDLL', 'SDLA',
            'time_seconds',
            'TK_fails', 'submission_rate',
            'distance_strikes_landed', 'clinch_strikes_landed', 'standing_strikes',
            'KD_power', 'ground_strikes_landed'
        ]

        opp_df = stats_df[["FighterID", "OpponentID", "Date"] + opp_stats]

        full_stats_df = stats_df.merge(
            opp_df, 
            left_on=["FighterID", "OpponentID", "Date"],
            right_on=["OpponentID", "FighterID", "Date"],
            how="left",
            suffixes=("", "_opp")
        )
        full_stats_df["time_seconds"] = full_stats_df["time_seconds"].fillna(full_stats_df["time_seconds_opp"])
        full_stats_df = full_stats_df.drop(columns=["time_seconds_opp"])

        stat_cols = [
            "TSL", # total strikes landed
            "TDL", # takedowns landed
            "TDS", # takedown slams
            "SSL", # significant strikes landed
            "SM", # submissions
            "RV", # reversals
            "KD",
            #######
            "SGHL", # significant ground head strikes landed
            "SGBL", # significant ground body strikes landed
            "SCBL", # significant clinch body strikes landed
            "SCHL", # significant clinch head strikes landed
            "ADTB", # advance to back
            "ADTM", # advance to mount
            "AD", # advances
            ######
            'TK_fails', 
            'submission_rate',
            'distance_strikes_landed', 'clinch_strikes_landed', 'standing_strikes',
            #'KD_power', 
            'ground_strikes_landed',

        ]
        for stat_col in stat_cols:
            full_stats_df[stat_col+"_per_sec"] = (full_stats_df[stat_col] / 
                                                full_stats_df["time_seconds"])
            full_stats_df[stat_col+"_opp_per_sec"] = (full_stats_df[stat_col+"_opp"] / 
                                                    full_stats_df["time_seconds"])
            full_stats_df[stat_col+"_diff_per_sec"] = (full_stats_df[stat_col+"_per_sec"] - 
                                                    full_stats_df[stat_col+"_opp_per_sec"])
        self.full_stats_df = full_stats_df
        


class FeatureExtractor(object):
    
    def __init__(self, stats_df, max_train_date=pd.to_datetime('2021-06-01')):
        self.stats_df = stats_df
        self.max_train_date = max_train_date
        self.trans_df = None # trans for transformed
        self.feature_diff_df = None
        self.train_df = None
        self.test_df = None
        
        self._cols_to_impute = None
        self._mean_imp_vals = None
        self._median_imp_vals = None
    
    def fit_transform_all(self):
        self._transform_raw()
        
        is_train_ind = self.trans_df["Date"] <= self.max_train_date
        print("fitting to train df")
        self._fit_train(self.trans_df.loc[is_train_ind])
        print("imputing data/new features")
        self._impute_all()
        self._set_feature_diffs()
        is_train_ind = self.feature_diff_df["Date"] <= self.max_train_date
        self.train_df = self.feature_diff_df.loc[is_train_ind]
        self.test_df = self.feature_diff_df.loc[~is_train_ind]

    def _transform_raw(self):
        # this one just calculates rolling features, but doesn't figure out what to do with nans
        # or do any fancy PCA or something
        def groupby_cum_mean(df, group_col, col):
            dummy = ~df[col].isnull()
            numer = df.groupby(group_col)[col].cumsum()
            denom = dummy.groupby(df[group_col]).cumsum()
            return numer / denom
        
        stats_df = self.stats_df
        trans_df = stats_df[["FighterID", "OpponentID", "Event", "Date", "FighterResult"]].copy()
        fighter_groups = stats_df.groupby("FighterID")
        print("aggregating a bunch of cols")
        cols_to_agg = [
            # {stat}_per_sec
            "SSL_per_sec", "TDL_per_sec", "AD_per_sec", "SM_per_sec", "distance_strikes_landed_per_sec", 
            "clinch_strikes_landed_per_sec", "ground_strikes_landed_per_sec", "TK_fails_per_sec", 
            "KD_per_sec", "RV_per_sec",
            # {stat}_diff_per_sec (which is the same as {difference in stats}_per_sec, thanks to arithmetic)
            "SSL_diff_per_sec", "TDL_diff_per_sec", "AD_diff_per_sec", "SM_diff_per_sec", 
            "distance_strikes_landed_diff_per_sec", 
            "clinch_strikes_landed_diff_per_sec", "ground_strikes_landed_diff_per_sec", 
            "TK_fails_diff_per_sec", "KD_diff_per_sec", "RV_diff_per_sec",
            # misc
            "KD_power", "submission_rate",
        ]
        
        for stat_col in tqdm(cols_to_agg):
            trans_df["prev_"+stat_col] = fighter_groups[stat_col].shift()
            trans_df["cum_mean_"+stat_col] = fighter_groups[stat_col].transform(
                lambda x: x.expanding().mean().shift()
            )
            
        print("fight experience")
        #### fight experience
        trans_df["total_ufc_fights"] = fighter_groups["Event"].transform(
            lambda x: x.fillna("").str.contains("UFC").cumsum().shift()
        ).fillna(0)

        trans_df["total_fights"] = fighter_groups["Date"].transform(
            lambda x: pd.Series(1, index=x.index).cumsum() - 1
        )

        trans_df["t_since_last_fight"] = fighter_groups["Date"].transform(
            lambda t: (t - t.shift()).dt.days
        )
        trans_df["t_since_last_fight"] = np.minimum(1.5*365, trans_df["t_since_last_fight"])
        
        print("fight record")
        #### fight record
        trans_df["is_win"] = (stats_df["FighterResult"] == 'W').astype(int)
        trans_df["n_wins"] = trans_df.groupby("FighterID")["is_win"].cumsum()
        trans_df["n_wins"] = trans_df.groupby("FighterID")["n_wins"].shift().fillna(0)
        
        trans_df["is_loss"] = (stats_df["FighterResult"] == 'L').astype(int)
        trans_df["n_losses"] = trans_df.groupby("FighterID")["is_loss"].cumsum()
        trans_df["n_losses"] = trans_df.groupby("FighterID")["n_losses"].shift().fillna(0)
        
        trans_df["t_to_win"] = stats_df["time_seconds"]
        trans_df.loc[stats_df["FighterResult"] != 'W', "t_to_win"] = np.nan
        trans_df["avg_t_to_win"] = groupby_cum_mean(trans_df, group_col="FighterID", col="t_to_win")
        #trans_df["avg_t_to_win"] = trans_df.groupby("FighterID")["t_to_win"].cumsum() 
        trans_df["avg_t_to_win"] = trans_df.groupby("FighterID")["avg_t_to_win"].shift()
        
        trans_df["t_to_lose"] = stats_df["time_seconds"]
        trans_df.loc[stats_df["FighterResult"] != 'L', "t_to_lose"] = np.nan
        trans_df["avg_t_to_lose"] = groupby_cum_mean(trans_df, group_col="FighterID", col="t_to_lose")
        #trans_df["avg_t_to_lose"] = trans_df.groupby("FighterID")["t_to_lose"].expanding().mean()
        trans_df["avg_t_to_lose"] = trans_df.groupby("FighterID")["avg_t_to_lose"].shift()
        
        trans_df = trans_df.drop(columns=["is_win", "is_loss", "t_to_win", "t_to_lose"])
        print("decisiveness of fight record")
        #### decisiveness of fight record
        for d_val in ['decision', 'submission', 'tko_ko']:
            win_by_d  = (stats_df["FighterResult"] == 'W') & (stats_df["decision_clean"] == d_val)
            loss_by_d = (stats_df["FighterResult"] == 'L') & (stats_df["decision_clean"] == d_val)
            
            new_col = "n_wins_by_"+d_val
            trans_df[new_col] = win_by_d.groupby(trans_df["FighterID"]).cumsum()
            trans_df[new_col] = trans_df.groupby("FighterID")[new_col].shift().fillna(0)
            new_col = "n_losses_by_"+d_val
            trans_df[new_col] = loss_by_d.groupby(trans_df["FighterID"]).cumsum()
            trans_df[new_col] = trans_df.groupby("FighterID")[new_col].shift().fillna(0)
        
        is_finish = stats_df["decision_clean"].isin(["submission", "tko_ko"])
        w_by_finish = is_finish & (stats_df["FighterResult"] == 'W')
        l_by_finish = is_finish & (stats_df["FighterResult"] == 'L')
        trans_df["n_wins_by_finish"] = w_by_finish.groupby(trans_df["FighterID"]).cumsum()
        trans_df["n_wins_by_finish"] = trans_df.groupby("FighterID")["n_wins_by_finish"].shift().fillna(0)
        trans_df["n_losses_by_finish"] = l_by_finish.groupby(trans_df["FighterID"]).cumsum()
        trans_df["n_losses_by_finish"] = trans_df.groupby("FighterID")["n_losses_by_finish"].shift().fillna(0)
        print("ufc fight record")
        #### ufc fight record
        trans_df["ufc_win"] = stats_df["Event"].fillna("").str.contains("UFC") & \
                             (stats_df["FighterResult"].fillna("") == 'W')
        trans_df["n_ufc_wins"] = trans_df.groupby("FighterID")["ufc_win"].cumsum()
        trans_df["n_ufc_wins"] = trans_df.groupby("FighterID")["n_ufc_wins"].shift()
        trans_df["ufc_loss"] = stats_df["Event"].fillna("").str.contains("UFC") & \
                              (stats_df["FighterResult"].fillna("") == 'L')
        trans_df["n_ufc_losses"] = trans_df.groupby("FighterID")["ufc_loss"].cumsum()
        trans_df["n_ufc_losses"] = trans_df.groupby("FighterID")["n_ufc_losses"].shift()
        
        trans_df = trans_df.drop(columns=["ufc_win", "ufc_loss"])
        #trans_df["is_ufc"] = trans_df["Event"].fillna("").str.contains("UFC")
        # indicate whether any imputation went into this guy's features
        stats_missing = stats_df[stat_cols].isnull().all(1) | (stats_df[stat_cols] == 0).all(1)
        stats_missing = stats_missing.astype(int)
        trans_df["prev_stats_missing"] = stats_missing.groupby(stats_df["FighterID"]).shift().fillna(1)
        
        self.trans_df = trans_df
        return trans_df
    
    def _fit_train(self, train_df):
        # figure out what values i'm gonna use to impute NaNs with
        # fit PCA, etc
        self._cols_to_impute = train_df.columns[train_df.dtypes.isin(['float', 'int'])]

        # self._cols_to_impute = [
        #     *[col for col in train_df.columns 
        #       if (col.startswith("prev_") and (col != 'prev_stats_missing'))],
        #     # all the prev_{stat} cols
        #     "t_since_last_fight"
        #]
        self._mean_imp_vals = train_df[self._cols_to_impute].mean()
        self._median_imp_vals = train_df[self._cols_to_impute].median()
        # here is where i would do something interesting like PCA
        return None
    
    def _impute_all(self):
        # actually fill in the values
        for col in self._cols_to_impute:
            self.trans_df[col] = self.trans_df[col].fillna(self._mean_imp_vals[col])
        return None
    
    def _set_feature_diffs(self):
        fighter_A_df = self.trans_df
        fighter_B_df = self.trans_df
        feature_cols = set(self.trans_df.columns) - {'FighterID', 'Opponent', 'OpponentID', 
                                                     'Event', 'Date', 'FighterResult', 'is_ufc'}
        feature_cols = list(feature_cols)
        
        A_B_df = fighter_A_df.merge(
            fighter_B_df[feature_cols + ['FighterID', 'OpponentID', 'Date']],
            left_on=('FighterID', 'OpponentID', 'Date'),
            right_on=('OpponentID', 'FighterID', 'Date'),
            suffixes=('_A', '_B'),
            how='inner'
        )
        diff_df = A_B_df[["FighterID_A", "OpponentID_A", "Date", "Event", "FighterResult"]]
        diff_df = diff_df.rename(columns={"FighterID_A":"FighterID", 
                                          "OpponentID_A":"OpponentID"})
        for col in feature_cols:
            diff_df[col] = A_B_df[col+"_A"] - A_B_df[col+"_B"]
            #diff_df[col] = (1 + A_B_df[col+"_A"]) / (1 + A_B_df[col+"_B"])
        diff_df["Event"] = A_B_df["Event"]
        diff_df["FighterResult"] = A_B_df["FighterResult"]
        diff_df["is_ufc"] = diff_df["Event"].fillna("").str.contains("UFC")
        
        self.feature_diff_df = diff_df
        return None
     
# fighters = ['jon jones', 'daniel cormier', 'chael sonnen']
# temp_stats_df = full_stats_df.loc[full_stats_df["Name"].isin(fighters)]
# FE = FeatureExtractor(temp_stats_df)
# FE.fit_transform_all()
# FE.trans_df