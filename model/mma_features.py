from tqdm import tqdm
import pandas as pd 
import numpy as np 
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer

from model.mma_elo_model import EwmaPowers, LogisticEwmaPowers, unknown_fighter_id

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
        stats_df["Date"] = pd.to_datetime(stats_df["Date"])
        def parse_time_str(s):
            if s == "-":
                return np.nan
            minutes, seconds = s.split(":")
            return int(minutes)*60 + int(seconds)
        def parse_round_str(s):
            if s == "-":
                return np.nan
            if s == "20": 
                # a typo that occurred exactly once
                return 2
            return int(s)
        stats_df["time_seconds"] = (
            stats_df["Time"].fillna("-").apply(parse_time_str) + 
            ((stats_df["Rnd"].fillna("-").apply(parse_round_str) - 1) * 5 * 60) 
            # rounds are 5 minutes
        )
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
        result_sign = stats_df["FighterResult"].map({"W": 1, "L":-1, "D": 0})
        decision_score = stats_df["decision_clean"].map({"tko_ko":2, "submission":2, 
                                                         "decision":1, "other":0})
        stats_df["ordinal_fighter_result"] = result_sign * decision_score


        submission_score = stats_df["decision_clean"].map({"submission":1, "decision":0, 
                                                    "other":0, "tko_ko":0})
        tko_ko_score = stats_df["decision_clean"].map({"submission":0, "decision":0, 
                                                    "other":0, "tko_ko":1})
        decision_score = stats_df["decision_clean"].map({"submission":0, "decision":1, 
                                                    "other":0, "tko_ko":0})
        finish_score = stats_df["decision_clean"].map({"submission":1, "decision":0, 
                                                    "other":0, "tko_ko":1})
        stats_df["submission_fighter_result"] = result_sign * submission_score
        stats_df["tko_ko_fighter_result"] = result_sign * tko_ko_score
        stats_df["decision_fighter_result"] = result_sign * decision_score
        stats_df["finish_fighter_result"] = result_sign * finish_score

        # chael sonnen kept getting sprawled on and winding up on his back
        stats_df["TD_fails"] = stats_df["TDA"] - stats_df["TDL"]
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
            'TD_fails', 'submission_rate',
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
        self.full_stats_df = full_stats_df


class SimpleFeatureExtractor(object):
    """
    Extracts things like the following:
    * simple fighter-level features like number of fights, number of fights in the ufc
    * simple relative fighter features like diff in number of fights
    * maybe things that we'll want to make Elo forecasts for, eg difference in sqrt(SSL)
    """
    
    def __init__(self, stats_df):
        self.raw_stats_df = stats_df
        self.trans_df = None
        
    def fit_transform_all(self):
        dd_df = self._dedupe_fights(self.raw_stats_df)
        self.trans_df = self._get_simple_features(dd_df)
        return self.trans_df
        
        
    def _dedupe_fights(self, df):
        temp_df = df.assign(
            fighterA = df[["FighterID", "OpponentID"]].max(1),
            fighterB = df[["FighterID", "OpponentID"]].min(1),
            fighter_is_A = df["FighterID"] > df["OpponentID"],
        )
        return temp_df.drop_duplicates(subset=["Date", "fighterA", "fighterB"]) \
                      .sort_values(["Date", "fighterA", "fighterB"])
    
    def _get_simple_features(self, df):
        # simple things that i needn't get from the fighter stats page
        # eg number of fights, t_since_last_fight
        df = df.assign(
            is_ufc=df["Event"].fillna("").str.contains("UFC"),
            Date=pd.to_datetime(df["Date"]),
        )
        df = df.sort_values("Date")
        fighters = set(df["FighterID"]) | set(df["OpponentID"])
        total_fight_counter = pd.Series(0, index=sorted(fighters))
        total_ufc_fight_counter = pd.Series(0, index=sorted(fighters))
        last_fight_counter = pd.Series(pd.to_datetime("1900-01-01"), index=sorted(fighters))
        feat_df_list = [] # concat this at the end
        for curr_dt, grp in tqdm(df.groupby("Date")):
            fid_vec = grp["FighterID"]
            oid_vec = grp["OpponentID"]
            is_ufc = grp["is_ufc"]
            feat_df_list.append(pd.DataFrame({
                "FighterID":fid_vec,
                "OpponentID":oid_vec,
                "Date":grp["Date"],
                "total_fights":fid_vec.map(total_fight_counter),
                "total_ufc_fights":fid_vec.map(total_ufc_fight_counter),
                "t_since_last_fight":(curr_dt - fid_vec.map(last_fight_counter)).dt.days,
                "total_fights_opp":oid_vec.map(total_fight_counter),
                "total_ufc_fights_opp":oid_vec.map(total_ufc_fight_counter),
                "t_since_last_fight_opp":(curr_dt - oid_vec.map(last_fight_counter)).dt.days,
            }))
            # if somehow this guy takes multiple fights in the same day,
            # just count it as one fight. 
            total_fight_counter[fid_vec] += 1
            total_fight_counter[oid_vec] += 1
            last_fight_counter[fid_vec] = curr_dt
            last_fight_counter[oid_vec] = curr_dt
            ufc_grp = grp.loc[is_ufc]
            total_ufc_fight_counter[ufc_grp["FighterID"]] += 1
            total_ufc_fight_counter[ufc_grp["OpponentID"]] += 1
            # make sure unknown_fighter_id stays unknown!
            total_fight_counter[unknown_fighter_id] = 0
            total_ufc_fight_counter[unknown_fighter_id] = 0
            last_fight_counter[unknown_fighter_id] = pd.to_datetime("1900-01-01")
        feat_df = pd.concat(feat_df_list)
        # cap t_since_last_fight, which can go really long
        # note that because last_fight_counter is initialized to a really early date,
        # this will handle the case where total_fights=0
        feat_df["t_since_last_fight"] = np.minimum(2*365, feat_df["t_since_last_fight"])
        feat_df["t_since_last_fight_opp"] = np.minimum(2*365, feat_df["t_since_last_fight_opp"])
        # compute diffs (idk how i want to --- this...)
        feat_df["t_since_last_fight_diff"] = (feat_df["t_since_last_fight"] - 
                                              feat_df["t_since_last_fight_opp"])
        feat_df["t_since_last_fight_log_diff"] = (np.log(feat_df["t_since_last_fight"]) - 
                                                  np.log(feat_df["t_since_last_fight_opp"]))
        feat_df["total_fights_diff"] = (feat_df["total_fights"] - 
                                        feat_df["total_fights_opp"])
        feat_df["total_fights_sqrt_diff"] = (np.sqrt(feat_df["total_fights"]) - 
                                            np.sqrt(feat_df["total_fights_opp"]))
        feat_df["total_ufc_fights_diff"] = (feat_df["total_ufc_fights"] - 
                                            feat_df["total_ufc_fights_opp"])
        feat_df["total_ufc_fights_sqrt_diff"] = (np.sqrt(feat_df["total_ufc_fights"]) - 
                                                 np.sqrt(feat_df["total_ufc_fights_opp"]))
        return feat_df


class EloFeatureExtractor(object):

    """
    real_elo_target_cols: real-valued fight outcomes. 
        eg my ordinal fight outcome (-2,-1,0,1,2) for loss by finish, loss by dec, draw, etc
    diff_elo_target_cols: forecast difference in sqrts of two fight stats
        eg SSL, TDS, SGHL. sqrt is there because Poisson and istg it helps!
    binary_elo_target_cols: pretty much just Win/Loss
    """
    
    def __init__(self, stats_df, real_elo_target_cols, diff_elo_target_cols,
                 binary_elo_target_cols, elo_alpha=0.6):
        self.raw_stats_df = stats_df
        self.real_elo_target_cols = real_elo_target_cols
        self.diff_elo_target_cols = diff_elo_target_cols
        self.binary_elo_target_cols = binary_elo_target_cols
        self.elo_target_names = sorted(diff_elo_target_cols + 
                                       binary_elo_target_cols +
                                       real_elo_target_cols)
        self.elo_alpha = elo_alpha
        self.elo_df = None
        self.elo_df_dict = None
    
    def fit_transform_all(self):
        # get the target cols
        # get the elo forecasts
        elo_target_df = self._get_diff_elo_targets(self.raw_stats_df)
        self.elo_df = self._get_elo_target_preds(elo_target_df)
        return self.elo_df
    
    def _get_diff_elo_targets(self, df):
        elo_target_df = df.copy()
        for col in self.diff_elo_target_cols:
            # TODO I should make this easier to customize...
            elo_target_df[col+"_diff"] = np.sqrt(df[col]) - np.sqrt(df[col+"_opp"])
            #elo_target_df[col+"_sqrt_diff"] = np.sqrt(df[col]) - np.sqrt(df[col+"_opp"])
        return elo_target_df
        
    def _get_elo_target_preds(self, df):
        # calculate rolling elo scores
        elo_dfs = {
            feat:None for feat in self.elo_target_names
        }
        df = df.sort_values(["Date", "FighterID", "OpponentID"])
        
        for feature in self.elo_target_names:
            curr_ep = None
            temp_df = None
            if feature in self.binary_elo_target_cols:
                curr_ep = LogisticEwmaPowers(target_col=feature, alpha=self.elo_alpha)
                temp_df = df.loc[df[feature].notnull()]
            if feature in self.diff_elo_target_cols:
                curr_ep = EwmaPowers(target_col=feature+"_diff", alpha=self.elo_alpha)
                temp_df = df.loc[df[feature+"_diff"].notnull()]
            if feature in self.real_elo_target_cols:
                curr_ep = EwmaPowers(target_col=feature, alpha=self.elo_alpha)
                temp_df = df.loc[df[feature].notnull()]
            curr_ep.fit(temp_df)
            curr_ep.elo_df["oldEloDiff"] = curr_ep.elo_df["oldFighterElo"] - curr_ep.elo_df["oldOpponentElo"]
            elo_dfs[feature] = curr_ep.elo_df
        self.elo_df_dict = elo_dfs
        # bundle up all these elo features into a single dataframe
        join_cols = ["FighterID", "OpponentID", "Date"]
        full_elo_df = None
        for feature in self.elo_target_names:
            right_df = elo_dfs[feature].rename(columns={
                col: col+feature for col in elo_dfs[feature].columns
                if col not in join_cols
            })
            if full_elo_df is None:
                full_elo_df = right_df
            else:
                full_elo_df = full_elo_df.merge(
                    right_df,
                    how="inner",
                    on=join_cols,
                )
        return full_elo_df


class BioFeatureExtractor(object):
    
    def __init__(self, bio_df):
        self.bio_df = bio_df
        self.aug_bio_df = self._clean_bio_df(bio_df)
        
    def _clean_bio_df(self, bio_df):
        # parse weights, impute
        bio_df = bio_df.assign(DOB=pd.to_datetime(bio_df["DOB"]))
        bio_df["clean_weight_class"] = bio_df["WT Class"].fillna("missing") \
            .str.lower() \
            .str.split("weight").str[0] \
            .str.strip()
        
        avg_class_wts = bio_df.groupby("clean_weight_class")["WeightPounds"].mean()
        # idk seems reasonable
        avg_class_wts["light middle"] = 175.0 
        # avg from googling a handful of fighters in the open weight class
        # zane frazier=230, kazuyuki fujita=245, gerard gordeau=216, paulo cesar silva=386, paul herrera=185
        avg_class_wts["open"] = 250
        # avg from googling again
        # robert duvalle=295,sean o'haire=265,jonathan wiezorek=250
        avg_class_wts["super heavy"] = 270
        # leave this alone tbh - i'll just assume he's the same weight as the other guy
        avg_class_wts["missing"] = np.nan
        
        bio_df["clean_weight"] = bio_df["WeightPounds"] \
            .fillna(bio_df["clean_weight_class"].map(avg_class_wts))
        
        imp_mean = IterativeImputer(random_state=0)
        # using clean_weight instead of WeightPounds because WT Class gives us some info about that
        imp_body_dims = imp_mean.fit_transform(bio_df[["ReachInches", "clean_weight", "HeightInches"]])
        imp_body_dims = pd.DataFrame(imp_body_dims, columns=["imp_reach", "imp_weight", "imp_height"])
        
        bio_df["all_dims_missing"] = bio_df[["ReachInches","clean_weight","HeightInches"]].isnull().all(1)
        bio_df = bio_df.join(imp_body_dims)
        self.aug_bio_df = bio_df
        return self.aug_bio_df
        
        
    def fit_transform_all(self, feat_df):
        # feat_df: dataframe of fighter and opponent features
        sub_bio = self.aug_bio_df[["FighterID", "DOB", "all_dims_missing", 
                                   "imp_reach", "imp_weight", "imp_height"]]

        feat_df = feat_df.merge(
            sub_bio,
            on=["FighterID"],
            how="left"
        ).merge(
            sub_bio.rename(columns={"FighterID":"OpponentID"}),
            on=["OpponentID"],
            how="left",
            suffixes=("", "_opp")
        )

        feat_df["age"] = (feat_df["Date"] - feat_df["DOB"]).dt.days / 365
        feat_df["age_opp"] = (feat_df["Date"] - feat_df["DOB_opp"]).dt.days / 365
        # effect of age on fighter ability is probably not linear but w/e
        feat_df["age_diff"] = (feat_df["DOB"] - feat_df["DOB_opp"]).dt.days / 365
        feat_df["age_diff"] = feat_df["age_diff"].fillna(0)

        feat_df["reach_diff"] = feat_df["imp_reach"] - feat_df["imp_reach_opp"]
        feat_df["weight_diff"] = feat_df["imp_weight"] - feat_df["imp_weight_opp"]
        feat_df["log_weight_diff"] = np.log(feat_df["imp_weight"]) - np.log(feat_df["imp_weight_opp"])
        feat_df["height_diff"] = feat_df["imp_height"] - feat_df["imp_height_opp"]

        zero_imp_inds = feat_df["all_dims_missing"] | feat_df["all_dims_missing_opp"]
        feat_df.loc[zero_imp_inds, ["reach_diff", "weight_diff", 
                                    "height_diff", "log_weight_diff"]] = 0

        return feat_df