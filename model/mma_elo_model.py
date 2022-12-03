import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm 
from sklearn.metrics import log_loss, accuracy_score, mean_squared_error, mean_absolute_error
from sklearn.preprocessing import OneHotEncoder
from scipy.special import expit, logit

unknown_fighter_id = "2557037" # "2557037/unknown-fighter"


class RealEloEstimator(object):
    # This thing is reasonably fast
    
    def __init__(self, target_col, alpha=0.5):
        self.target_col = target_col
        self.alpha = alpha
        self.fighter_ids = None
        self.fighter_ids_with_target = None
        self.useless_fighter_ids = None
        self._fighter_encoder = None
        self._fighter_powers = None
        self.elo_feature_df = None
        
    def transform_fighter_ids(self, fighter_ids:pd.Series):
        return self._fighter_encoder.transform(fighter_ids.values.reshape(-1,1))
    
    def _fit_workhorse(self, df:pd.DataFrame):
        # kind of a cheap name for a function, but this is where the elo update
        # rule is actually applied
        df = df.dropna(subset=[self.target_col])
        fighter_id_mat = self.transform_fighter_ids(df["espn_fighter_id"])
        opponent_id_mat = self.transform_fighter_ids(df["espn_opponent_id"])
        pred_elo_features = np.zeros(len(df))
        fighter_elos = np.zeros(len(df))
        opponent_elos = np.zeros(len(df))
        deltas = np.zeros(len(df))
        y = df[self.target_col].values
        for i in tqdm(range(df.shape[0])):
            fighter_id_vec = fighter_id_mat[i]
            opponent_id_vec = opponent_id_mat[i]
            fighter_elos[i] = self._fighter_powers * fighter_id_vec.T
            opponent_elos[i] = self._fighter_powers * opponent_id_vec.T
            y_hat = fighter_elos[i] - opponent_elos[i]
            pred_elo_features[i] = y_hat
            # half the delta update goes towards updating the fighter's elo, 
            # half to the opponent's elo
            delta = self.alpha * (y[i] - y_hat)
            self._fighter_powers += (0.5 * delta * (fighter_id_vec - opponent_id_vec))
            deltas[i] = delta
        updated_fighter_elos = fighter_elos + (deltas / 2)
        updated_opponent_elos = opponent_elos - (deltas / 2)
        return df[[
            "espn_fight_id", 
            "espn_fighter_id", "espn_opponent_id", 
            self.target_col
        ]].assign(
            pred_target=pred_elo_features,
            fighter_elo=fighter_elos,
            opponent_elo=opponent_elos,
            updated_fighter_elo=updated_fighter_elos,
            updated_opponent_elo=updated_opponent_elos,
        )
    
    def _fit_ffill(self, elo_df:pd.DataFrame):
        elo_df = elo_df.copy()      
        # many of these fighters are guys who competed in cheap leagues their whole
        # careers, and never recorded the target statistic. We impute their elo scores with 0
        df_with_target = elo_df.dropna(subset=[self.target_col])
        fighter_ids_with_target = set(df_with_target["espn_fighter_id"]) | \
                                  set(df_with_target["espn_opponent_id"])
        useless_fighter_ids = set(self.fighter_ids) - fighter_ids_with_target
        self.fighter_ids_with_target = pd.Series(sorted(fighter_ids_with_target))
        if not elo_df[self.target_col].isnull().any():
            # no need to ffill
            self.useless_fighter_ids = pd.Series([], dtype='int64')
            elo_df["pred_target"] = elo_df["fighter_elo"] - elo_df["opponent_elo"]
            return elo_df  
        self.useless_fighter_ids = pd.Series(sorted(useless_fighter_ids))
        is_useless_fighter  = elo_df["espn_fighter_id"].isin(useless_fighter_ids)
        is_useless_opponent = elo_df["espn_opponent_id"].isin(useless_fighter_ids)
        elo_df.loc[is_useless_fighter, ['fighter_elo', 'updated_fighter_elo']] = 0
        elo_df.loc[is_useless_opponent, ['opponent_elo', 'updated_opponent_elo']] = 0
        # for fighters who eventually logged the target statistic, we start from 0,
        # and forward-fill their most recent elo score
        for fighter_id in tqdm(fighter_ids_with_target):
            is_fighter_bool  = elo_df["espn_fighter_id"] == fighter_id
            is_opponent_bool = elo_df["espn_opponent_id"] == fighter_id
            is_fighter_ind = elo_df.index[is_fighter_bool]
            is_opponent_ind = elo_df.index[is_opponent_bool]
            # this is tricky - for fights that occurred after the statistic was recorded,
            # we want to forward-fill the fighter's UPDATED elo scores, not the fighter's
            # elo score prior to the fight where the stat was recorded
            updated_fighter_elo = (
                elo_df["updated_fighter_elo"] * is_fighter_bool +
                elo_df["updated_opponent_elo"] * is_opponent_bool
            ).loc[is_fighter_bool | is_opponent_bool]
            # elo scores always start with 0
            updated_fighter_elo.iloc[0] = np.nan_to_num(updated_fighter_elo.iloc[0], 0)
            updated_fighter_elo = updated_fighter_elo.fillna(method='ffill')
            # fill NaNs in fighter_elo and opponent_elo columns with now-ffilled updated_fighter_elo
            elo_df.loc[is_fighter_ind, 'fighter_elo'] = \
                elo_df.loc[is_fighter_ind, 'fighter_elo']\
                    .fillna(updated_fighter_elo)
            elo_df.loc[is_opponent_ind, 'opponent_elo'] = \
                elo_df.loc[is_opponent_ind, 'opponent_elo']\
                    .fillna(updated_fighter_elo)
            # might as well fill NaNs in updated_fighter_elo and updated_opponent_elo columns
            elo_df.loc[is_fighter_ind, 'updated_fighter_elo'] = updated_fighter_elo.loc[is_fighter_ind]
            elo_df.loc[is_opponent_ind, 'updated_opponent_elo'] = updated_fighter_elo.loc[is_opponent_ind]
        assert elo_df[["fighter_elo", "opponent_elo"]].isnull().any().any() == False, \
            elo_df[["fighter_elo", "opponent_elo"]].isnull().mean()
        elo_df["pred_target"] = elo_df["fighter_elo"] - elo_df["opponent_elo"]
        return elo_df

    def fit_fighter_encoder(self, df):
        self.fighter_ids = sorted(set(df["espn_fighter_id"]) | set(df["espn_opponent_id"]))
        categories = np.array(self.fighter_ids).reshape(-1,1)
        self._fighter_encoder = OneHotEncoder()
        self._fighter_encoder.fit(categories)
        
    def fit(self, df:pd.DataFrame):
        if self.fighter_ids is None:
            self.fit_fighter_encoder(df)
        self._fighter_powers = np.zeros(len(self.fighter_ids))
        
        fitted_elo_df = self._fit_workhorse(df)
        # okay this is the hard part - ffilling
        keep_cols = ["espn_fight_id", "espn_fighter_id", "espn_opponent_id", "Date"]
        elo_df = df[keep_cols].merge(
            fitted_elo_df,
            how="left",
            on=["espn_fight_id", "espn_fighter_id", "espn_opponent_id"],
        )
        self.elo_feature_df = self._fit_ffill(elo_df)
        return self.elo_feature_df
    
    def predict(self, df):
        fighter_id_mat = self.transform_fighter_ids(df["espn_fighter_id"])
        opponent_id_mat = self.transform_fighter_ids(df["espn_opponent_id"])
        # this will be a matrix of size (1, len(df))
        y_hat = self._fighter_powers * (fighter_id_mat.T - opponent_id_mat.T)
        # y_hat.A1 is a flattened 1D array of size (len(df),)
        return pd.DataFrame({
            "espn_fight_id": df["espn_fight_id"],
            "espn_fighter_id": df["espn_fighter_id"],
            "espn_opponent_id": df["espn_opponent_id"],
            "pred_target": y_hat.A1,
        })
    
    def get_fighter_career_elos(self, fighter_id):
        # just for EDA
        is_fighter_bool = self.elo_feature_df["espn_fighter_id"] == fighter_id
        is_opponent_bool = self.elo_feature_df["espn_opponent_id"] == fighter_id
        is_fighting_bool = (
            (self.elo_feature_df["espn_fighter_id"] == fighter_id) |
            (self.elo_feature_df["espn_opponent_id"] == fighter_id)
        )
        fight_dates = self.elo_feature_df.loc[is_fighting_bool, "Date"]
        updated_elos = (
            self.elo_feature_df["updated_fighter_elo"] * is_fighter_bool +
            self.elo_feature_df["updated_opponent_elo"] * is_opponent_bool
        ).loc[is_fighting_bool]
        return pd.DataFrame({
            "espn_fighter_id": fighter_id,
            "fight_date": fight_dates,
            "n_fights": np.arange(0, is_fighting_bool.sum()),
            "updated_elo": updated_elos,
        })


class BinaryEloEstimator(RealEloEstimator):
    # This thing is reasonably fast
    
    def fit(self, df):
        if self.fighter_ids is None:
            self.fit_fighter_encoder(df)
        self._fighter_powers = np.zeros(len(self.fighter_ids))
        fighter_id_mat = self.transform_fighter_ids(df["espn_fighter_id"])
        opponent_id_mat = self.transform_fighter_ids(df["espn_opponent_id"])
        pred_elo_features = np.zeros(len(df))
        fighter_elos = np.zeros(len(df))
        opponent_elos = np.zeros(len(df))
        deltas = np.zeros(len(df))
        y = df[self.target_col].values
        for i in tqdm(range(df.shape[0])):
            fighter_id_vec = fighter_id_mat[i]
            opponent_id_vec = opponent_id_mat[i]
            fighter_elos[i] = self._fighter_powers * fighter_id_vec.T
            opponent_elos[i] = self._fighter_powers * opponent_id_vec.T
            # lol this is the only change
            y_hat = expit(fighter_elos[i] - opponent_elos[i])
            pred_elo_features[i] = y_hat
            delta = np.nan_to_num(self.alpha * (y[i] - y_hat), 0)
            # half the delta update goes towards updating the fighter's elo, 
            # half to the opponent's elo
            self._fighter_powers += (0.5 * delta * (fighter_id_vec - opponent_id_vec))
            deltas[i] = delta
        updated_fighter_elos = fighter_elos + (deltas / 2)
        updated_opponent_elos = opponent_elos - (deltas / 2)
        
        self.elo_feature_df = df[[
            "espn_fight_id", 
            "espn_fighter_id", "espn_opponent_id", 
            "Date", self.target_col
        ]].assign(
            pred_target=pred_elo_features,
            fighter_elo=fighter_elos,
            opponent_elo=opponent_elos,
            updated_fighter_elo=updated_fighter_elos,
            updated_opponent_elo=updated_opponent_elos,
        )
        return self.elo_feature_df

    def predict(self, df):
        fighter_id_mat = self.transform_fighter_ids(df["espn_fighter_id"])
        opponent_id_mat = self.transform_fighter_ids(df["espn_opponent_id"])
        # this will be a matrix of size (1, len(df))
        y_hat = expit(self._fighter_powers * (fighter_id_mat.T - opponent_id_mat.T))
        # y_hat.A1 is a flattened 1D array of size (len(df),)
        return pd.DataFrame({
            "espn_fight_id": df["espn_fight_id"],
            "espn_fighter_id": df["espn_fighter_id"],
            "espn_opponent_id": df["espn_opponent_id"],
            "pred_target": y_hat.A1,
        })
        

class AccEloEstimator(object):
    
    def __init__(self, landed_col, attempt_col, alpha=0.5):
        self.landed_col = landed_col
        self.attempt_col = attempt_col
        self.alpha = alpha
        self.fighter_ids = None
        self.fighter_ids_with_target = None
        self.useless_fighter_ids = None
        self._fighter_encoder = None
        self._fighter_offense_powers = None
        self._fighter_defense_powers = None
        self._power_intercept = None
        self.elo_feature_df = None
        
    def transform_fighter_ids(self, fighter_ids:pd.Series):
        return self._fighter_encoder.transform(fighter_ids.values.reshape(-1,1))
    
    def _fit_power_intercept(self, df:pd.DataFrame):
        keep_inds_fighter = df[[self.landed_col, self.attempt_col]].notnull().all(1)
        keep_inds_opponent = df[[self.landed_col+"_opp", self.attempt_col+"_opp"]].notnull().all(1)

        y = df.loc[keep_inds_fighter, self.landed_col].sum() + \
            df.loc[keep_inds_opponent, self.landed_col+"_opp"].sum()
        n = df.loc[keep_inds_fighter, self.attempt_col].sum() + \
            df.loc[keep_inds_opponent, self.attempt_col+"_opp"].sum()
        p = y / n
        self._power_intercept = logit(p)
    
    def _fit_workhorse(self, df:pd.DataFrame):
        # drop rows where everything is missing
        drop_inds = (
            (df[self.landed_col].isnull() | df[self.attempt_col].isnull()) &
            (df[self.landed_col+"_opp"].isnull() | df[self.attempt_col+"_opp"].isnull())
        )
        df = df.loc[~drop_inds].copy()
        fighter_id_mat = self.transform_fighter_ids(df["espn_fighter_id"])
        opponent_id_mat = self.transform_fighter_ids(df["espn_opponent_id"])
        fighter_offense_elos = np.zeros(len(df))
        fighter_defense_elos = np.zeros(len(df))
        opponent_offense_elos = np.zeros(len(df))
        opponent_defense_elos = np.zeros(len(df))
        
        fighter_offense_deltas = np.zeros(len(df))
        opponent_offense_deltas = np.zeros(len(df))
        
        y_fighter = df[self.landed_col].fillna(0).values
        n_fighter = df[self.attempt_col].fillna(0).values
        # if he made >= 1 attempt, this does nothing. if he made 0 attempts, this does nothing
        p_fighter = y_fighter / np.maximum(n_fighter, 1) 
        p_fighter = np.minimum(p_fighter, 1) # I'm paranoid
        # if stats are missing, just treat it like it's unobserved
        y_opponent = df[self.landed_col+"_opp"].fillna(0).values
        n_opponent = df[self.attempt_col+"_opp"].fillna(0).values
        p_opponent = y_opponent / np.maximum(n_opponent, 1)
        p_opponent = np.minimum(p_opponent, 1)
        for i in tqdm(range(df.shape[0])):
            fighter_id_vec = fighter_id_mat[i]
            opponent_id_vec = opponent_id_mat[i]
            fighter_offense_elos[i] = self._fighter_offense_powers * fighter_id_vec.T
            fighter_defense_elos[i] = self._fighter_defense_powers * fighter_id_vec.T
            opponent_offense_elos[i] = self._fighter_offense_powers * opponent_id_vec.T
            opponent_defense_elos[i] = self._fighter_defense_powers * opponent_id_vec.T
            
            p_fighter_hat = expit(fighter_offense_elos[i] - 
                                    opponent_defense_elos[i] + 
                                    self._power_intercept)
            p_opponent_hat = expit(opponent_offense_elos[i] - 
                                     fighter_defense_elos[i] + 
                                     self._power_intercept)
            
            # delta is oriented in the direction of the fighter i guess
            # update the fighter's offense and opponent's defense
            delta_p_fighter_landed = self.alpha * (y_fighter[i] - (n_fighter[i] * p_fighter_hat))
            self._fighter_offense_powers += (0.5 * delta_p_fighter_landed * fighter_id_vec)
            self._fighter_defense_powers -= (0.5 * delta_p_fighter_landed * opponent_id_vec)
            delta_p_opponent_landed = self.alpha * (y_opponent[i] - (n_opponent[i] * p_opponent_hat))
            self._fighter_offense_powers += (0.5 * delta_p_opponent_landed * opponent_id_vec)
            self._fighter_defense_powers -= (0.5 * delta_p_opponent_landed * fighter_id_vec)

            # w = self.alpha * np.sqrt(n_fighter[i] / (p_fighter_hat * (1 - p_fighter_hat)))
            # delta_p_fighter_landed = w * (p_fighter[i] - p_fighter_hat)
            # self._fighter_offense_powers += (0.5 * delta_p_fighter_landed * fighter_id_vec)
            # self._fighter_defense_powers -= (0.5 * delta_p_fighter_landed * opponent_id_vec)

            # w = self.alpha * np.sqrt(n_opponent[i] / (p_opponent_hat * (1 - p_opponent_hat)))
            # delta_p_opponent_landed = w * (p_opponent[i] - p_opponent_hat)
            
            # self._fighter_offense_powers += (0.5 * delta_p_opponent_landed * opponent_id_vec)
            # self._fighter_defense_powers -= (0.5 * delta_p_opponent_landed * fighter_id_vec)
            
            fighter_offense_deltas[i] = delta_p_fighter_landed
            opponent_offense_deltas[i] = delta_p_opponent_landed
        
        updated_fighter_offense_elos = fighter_offense_elos + (fighter_offense_deltas / 2)
        updated_fighter_defense_elos = fighter_defense_elos - (opponent_offense_deltas / 2)
        updated_opponent_offense_elos = opponent_offense_elos + (opponent_offense_deltas / 2)
        updated_opponent_defense_elos = opponent_defense_elos - (fighter_offense_deltas / 2)
        
        return df[[
            "espn_fight_id", 
            "espn_fighter_id", "espn_opponent_id", 
            self.landed_col, self.attempt_col,
            self.landed_col+"_opp", self.attempt_col+"_opp",
        ]].assign(
            # fighter, opponent elos
            fighter_offense_elo=fighter_offense_elos,
            fighter_defense_elo=fighter_defense_elos,
            opponent_offense_elo=opponent_offense_elos,
            opponent_defense_elo=opponent_defense_elos,
            # predicted targets
            p_fighter_hat = expit(fighter_offense_elos - 
                                    opponent_defense_elos + 
                                    self._power_intercept),
            p_opponent_hat = expit(opponent_offense_elos - 
                                     fighter_defense_elos + 
                                     self._power_intercept),
            # updated fighter, opponent elos
            updated_fighter_offense_elo=updated_fighter_offense_elos,
            updated_fighter_defense_elo=updated_fighter_defense_elos,
            updated_opponent_offense_elo=updated_opponent_offense_elos,
            updated_opponent_defense_elo=updated_opponent_defense_elos,
        )
    
    def _fit_ffill(self, elo_df:pd.DataFrame):
        elo_df = elo_df.copy()
        # many of these fighters are guys who competed in cheap leagues their whole
        # careers, and never recorded the target statistic. We impute their elo scores with 0
        drop_inds = (
            (elo_df[self.landed_col].isnull() | elo_df[self.attempt_col].isnull()) &
            (elo_df[self.landed_col+"_opp"].isnull() | elo_df[self.attempt_col+"_opp"].isnull())
        )
        df_with_target = elo_df.loc[~drop_inds].copy()
        fighter_ids_with_target = set(df_with_target["espn_fighter_id"]) | \
                                  set(df_with_target["espn_opponent_id"])
        useless_fighter_ids = set(self.fighter_ids) - fighter_ids_with_target
        self.fighter_ids_with_target = pd.Series(sorted(fighter_ids_with_target))
        if not elo_df[[self.landed_col, self.attempt_col, 
                    self.landed_col+"_opp", self.attempt_col+"_opp"]].isnull().any().any():
            # no need to ffill
            self.useless_fighter_ids = pd.Series([], dtype='int64')
            elo_df["p_fighter_hat"] = expit(elo_df["fighter_offense_elo"] - elo_df["opponent_defense_elo"])
            elo_df["p_opponent_hat"] = expit(elo_df["opponent_offense_elo"] - elo_df["fighter_defense_elo"])
            return elo_df
        self.useless_fighter_ids = pd.Series(sorted(useless_fighter_ids))
        is_useless_fighter  = elo_df["espn_fighter_id"].isin(useless_fighter_ids)
        is_useless_opponent = elo_df["espn_opponent_id"].isin(useless_fighter_ids)
        elo_df.loc[is_useless_fighter, ['fighter_offense_elo', 'updated_fighter_offense_elo',
                                        'fighter_defense_elo', 'updated_fighter_defense_elo']] = 0
        elo_df.loc[is_useless_opponent, ['opponent_offense_elo', 'updated_opponent_offense_elo',
                                         'opponent_defense_elo', 'updated_opponent_defense_elo']] = 0
        # for fighters who eventually logged the target statistic, we start from 0,
        # and forward-fill their most recent elo score
        for fighter_id in tqdm(fighter_ids_with_target):
            is_fighter_bool  = elo_df["espn_fighter_id"] == fighter_id
            is_opponent_bool = elo_df["espn_opponent_id"] == fighter_id
            is_fighter_ind = elo_df.index[is_fighter_bool]
            is_opponent_ind = elo_df.index[is_opponent_bool]
            # this is tricky - for fights that occurred after the statistic was recorded,
            # we want to forward-fill the fighter's UPDATED elo scores, not the fighter's
            # elo score prior to the fight where the stat was recorded
            for side in ["offense", "defense"]:
                updated_fighter_elo = (
                    elo_df[f"updated_fighter_{side}_elo"] * is_fighter_bool +
                    elo_df[f"updated_opponent_{side}_elo"] * is_opponent_bool
                ).loc[is_fighter_bool | is_opponent_bool]
                # elo scores always start with 0
                updated_fighter_elo.iloc[0] = np.nan_to_num(updated_fighter_elo.iloc[0], 0)
                updated_fighter_elo = updated_fighter_elo.fillna(method='ffill')
                # fill NaNs in fighter_elo and opponent_elo columns with now-ffilled updated_fighter_elo
                elo_df.loc[is_fighter_ind, f"fighter_{side}_elo"] = \
                    elo_df.loc[is_fighter_ind, f"fighter_{side}_elo"]\
                        .fillna(updated_fighter_elo)
                elo_df.loc[is_opponent_ind, f"opponent_{side}_elo"] = \
                    elo_df.loc[is_opponent_ind, f"opponent_{side}_elo"]\
                        .fillna(updated_fighter_elo)
                # might as well fill NaNs in updated_fighter_elo and updated_opponent_elo columns
                elo_df.loc[is_fighter_ind, f"updated_fighter_{side}_elo"] = \
                    updated_fighter_elo.loc[is_fighter_ind]
                elo_df.loc[is_opponent_ind, f"updated_opponent_{side}_elo"] = \
                    updated_fighter_elo.loc[is_opponent_ind]
        assert elo_df[["fighter_offense_elo", "opponent_offense_elo"]].isnull().any().any() == False, \
            elo_df[["fighter_offense_elo", "opponent_offense_elo"]].isnull().mean()
        assert elo_df[["fighter_defense_elo", "opponent_defense_elo"]].isnull().any().any() == False, \
            elo_df[["fighter_defense_elo", "opponent_defense_elo"]].isnull().mean()
        elo_df["p_fighter_hat"] = expit(elo_df["fighter_offense_elo"] - elo_df["opponent_defense_elo"])
        elo_df["p_opponent_hat"] = expit(elo_df["opponent_offense_elo"] - elo_df["fighter_defense_elo"])
        return elo_df

    def fit_fighter_encoder(self, df):
        self.fighter_ids = sorted(set(df["espn_fighter_id"]) | set(df["espn_opponent_id"]))
        categories = np.array(self.fighter_ids).reshape(-1,1)
        self._fighter_encoder = OneHotEncoder()
        self._fighter_encoder.fit(categories)
        
    def fit(self, df:pd.DataFrame):
        self._fit_power_intercept(df)
        if self.fighter_ids is None:
            self.fit_fighter_encoder(df) 
        self._fighter_offense_powers = np.zeros(len(self.fighter_ids))
        self._fighter_defense_powers = np.zeros(len(self.fighter_ids))
        categories = np.array(self.fighter_ids).reshape(-1,1)
        
        fitted_elo_df = self._fit_workhorse(df)
        # okay this is the hard part - ffilling
        keep_cols = ["espn_fight_id", "espn_fighter_id", "espn_opponent_id", "Date"]
        elo_df = df[keep_cols].merge(
            fitted_elo_df,
            how="left",
            on=["espn_fight_id", "espn_fighter_id", "espn_opponent_id"],
        )
        self.elo_feature_df = self._fit_ffill(elo_df)
        return self.elo_feature_df
    
    def predict(self, df):
        fighter_id_mat = self.transform_fighter_ids(df["espn_fighter_id"])
        opponent_id_mat = self.transform_fighter_ids(df["espn_opponent_id"])
        fighter_offense_elos = self._fighter_offense_powers * fighter_id_mat.T
        fighter_defense_elos = self._fighter_defense_powers * fighter_id_mat.T
        opponent_offense_elos = self._fighter_offense_powers * opponent_id_mat.T
        opponent_defense_elos = self._fighter_defense_powers * opponent_id_mat.T

        p_fighter_hat = expit(fighter_offense_elos - 
                                opponent_defense_elos + 
                                self._power_intercept)
        p_opponent_hat = expit(opponent_offense_elos - 
                                 fighter_defense_elos + 
                                 self._power_intercept)
        return pd.DataFrame({
            "espn_fight_id": df["espn_fight_id"],
            "espn_fighter_id": df["espn_fighter_id"],
            "espn_opponent_id": df["espn_opponent_id"],
            "pred_p_fighter_landed": p_fighter_hat.A1,
            "pred_p_opponent_landed": p_opponent_hat.A1,
        })
    
    def get_fighter_career_elos(self, fighter_id):
        # just for EDA
        is_fighter_bool = self.elo_feature_df["espn_fighter_id"] == fighter_id
        is_opponent_bool = self.elo_feature_df["espn_opponent_id"] == fighter_id
        is_fighting_bool = (
            (self.elo_feature_df["espn_fighter_id"] == fighter_id) |
            (self.elo_feature_df["espn_opponent_id"] == fighter_id)
        )
        fight_dates = self.elo_feature_df.loc[is_fighting_bool, "Date"]
        updated_offense_elos = (
            self.elo_feature_df["updated_fighter_offense_elo"] * is_fighter_bool +
            self.elo_feature_df["updated_opponent_offense_elo"] * is_opponent_bool
        ).loc[is_fighting_bool]
        updated_defense_elos = (
            self.elo_feature_df["updated_fighter_defense_elo"] * is_fighter_bool +
            self.elo_feature_df["updated_opponent_defense_elo"] * is_opponent_bool
        ).loc[is_fighting_bool]
        return pd.DataFrame({
            "espn_fighter_id": fighter_id,
            "fight_date": fight_dates,
            "n_fights": np.arange(0, is_fighting_bool.sum()),
            "updated_offense_elo": updated_offense_elos,
            "updated_defense_elo": updated_defense_elos,
        })
