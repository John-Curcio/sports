import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm 
from sklearn.metrics import log_loss, accuracy_score, mean_squared_error, mean_absolute_error
from sklearn.preprocessing import OneHotEncoder
from scipy.special import expit, logit
from abc import ABC, abstractmethod

unknown_fighter_id = "2557037" # "2557037/unknown-fighter"

class BaseEloEstimator(ABC):
    """
    Abstract Base class for Elo estimators.
    Requires a few methods to be implemented:
    - update_powers
    - predict_given_powers
    - fit_initial_params
    """

    def __init__(self, target_col, alpha=0.5):
        self.target_col = target_col
        self.alpha = alpha
        self.fighter_ids = None
        self.fighter_ids_with_target = None
        self.useless_fighter_ids = None
        self._fighter_encoder = None
        self._fighter_powers = None
        self.elo_feature_df = None
        # store the matrices for the fighter ids and opponent ids
        # self.fighter_id_mat = None
        # self.opponent_id_mat = None
        # debugging
        # self.workhorse_df = None

    def transform_fighter_ids(self, fighter_ids:pd.Series):
        return self._fighter_encoder.transform(fighter_ids.values.reshape(-1,1))
    
    def fit_fighter_encoder(self, fighter_ids:pd.Series):
        self.fighter_ids = sorted(set(fighter_ids))
        self._fighter_encoder = OneHotEncoder(handle_unknown="error")
        self._fighter_encoder.fit(fighter_ids.values.reshape(-1,1))

    def fit_initial_params(self, df:pd.DataFrame):
        """
        Fit initial parameters, if any.
        """
        return None
    
    def get_elo_update(self, y_true:np.ndarray, fighter_elo:np.ndarray, opponent_elo:np.ndarray, X: np.ndarray):
        """
        Get update to the powers of the fighters, given the observed target.
        Note that this is a "batch" update, i.e. it updates all the powers
        at once.
        Also, this is a "doubled" dataset, i.e. that each fight is
        represented twice, once for each (Fighter, Opponent) permutation.
        - y_true: the observed target. May be NaN!
        - fighter_elo: the powers of the fighters. Never NaN.
        - opponent_elo: the powers of the opponents. Never NaN.
        - X: the additional features. May be ignored.
        Returns:
        - (fighter_delta, opponent_delta): the updates to the powers of the 
        fighters and opponents, respectively. May not be NaN.
        These will be added to the corresponding indices in self._fighter_powers.
        """
        raise NotImplementedError()
    
    def predict_given_powers(self, fighter_elo:np.ndarray, opponent_elo:np.ndarray, X: np.ndarray) -> np.ndarray:
        """
        Predict the target given the powers of the fighters. df may be used to
        extract additional features.
        """
        raise NotImplementedError()
    
    def extract_features(self, df: pd.DataFrame) -> np.ndarray:
        """
        Extract any additional features from the dataframe that may be used
        in the prediction.
        """
        return np.zeros((len(df), 0))

    def fit(self, df:pd.DataFrame):
        """
        Assuming that the data is "doubled" - i.e. that each fight is
        represented twice, once for each (Fighter, Opponent) permutation.
        Returns a dataframe with the same number of rows as the input df.
        df: pd.DataFrame
        """
        assert (df["fight_id"].value_counts() == 2).all()
        df = df.sort_values(["fight_id", "FighterID_espn", "OpponentID_espn"])\
            .reset_index(drop=True)
        if self.fighter_ids is None:
            self.fit_fighter_encoder(df["FighterID_espn"])
        self._fighter_powers = np.zeros(len(self.fighter_ids))
        # fit initial params, if any
        self.fit_initial_params(df)
        fighter_id_mat = self.transform_fighter_ids(df["FighterID_espn"])
        opponent_id_mat = self.transform_fighter_ids(df["OpponentID_espn"])
        fitted_elo_df = df[[
            "fight_id", "FighterID_espn", "OpponentID_espn", "Date",
            self.target_col,
        ]].assign(
            pred_elo_target=np.nan, fighter_elo=np.nan, opponent_elo=np.nan,
            updated_fighter_elo=np.nan, updated_opponent_elo=np.nan,
        )
        X = self.extract_features(df)
        # loop over dates
        for dt, grp in tqdm(df.groupby("Date")):
            # df's index is simply the row number, so we can use that to
            # index into the matrices. But this is the only place where
            # we are allowed to do that! Inside the methods defined by the
            # inheriting classes, we should only use numpy objects.

            # pseudocode sketch
            # 0. get powers for the fighters in this group
            curr_fighter = fighter_id_mat[grp.index]
            # # get current opponents (len(grp), n_fighters)
            curr_opponent = opponent_id_mat[grp.index]
            # get the powers of the fighters (len(grp),)
            curr_fighter_powers = self._fighter_powers @ curr_fighter.T
            # get the powers of the opponents
            curr_opponent_powers = self._fighter_powers @ curr_opponent.T
            # any current features that we want to use
            curr_X = X[grp.index]
            # 1. predict the target given the current powers
            y_hat = self.predict_given_powers(
                curr_fighter_powers, curr_opponent_powers, curr_X
            )
            # 2. save the predictions
            fitted_elo_df.loc[grp.index, "pred_elo_target"] = y_hat
            # 3. save the current powers
            fitted_elo_df.loc[grp.index, "fighter_elo"] = curr_fighter_powers
            fitted_elo_df.loc[grp.index, "opponent_elo"] = curr_opponent_powers
            # 4. update the powers
            fighter_delta, opponent_delta = self.get_elo_update(
                grp[self.target_col].values, curr_fighter_powers, curr_opponent_powers, curr_X
            )
            self._fighter_powers += fighter_delta @ curr_fighter
            self._fighter_powers += opponent_delta @ curr_opponent
            # 5. save the updated powers
            fitted_elo_df.loc[grp.index, "updated_fighter_elo"] = self._fighter_powers @ curr_fighter.T
            fitted_elo_df.loc[grp.index, "updated_opponent_elo"] = self._fighter_powers @ curr_opponent.T
        self.elo_feature_df = fitted_elo_df.drop(columns=["Date"])
        return fitted_elo_df
    
    def predict(self, test_df: pd.DataFrame):
        # use predict_given_powers to predict on the test data
        # needn't sort the test data by date or anything, since we're not
        # updating the powers
        test_df = test_df.copy() 
        # transform the fighter ids into a (n_fights, n_fighters) matrix
        fighter_id_mat = self.transform_fighter_ids(test_df["FighterID_espn"])
        # transform the opponent ids into a (n_fights, n_fighters) matrix
        opponent_id_mat = self.transform_fighter_ids(test_df["OpponentID_espn"])
        # get the powers of the fighters (len(grp),)
        curr_fighter_powers = self._fighter_powers @ fighter_id_mat.T
        # get the powers of the opponents
        curr_opponent_powers = self._fighter_powers @ opponent_id_mat.T
        # any current features that we want to use
        X = self.extract_features(test_df)
        # 1. predict the target given the current powers
        y_hat = self.predict_given_powers(
            curr_fighter_powers, curr_opponent_powers, X
        )
        # 2. save the predictions
        test_df["pred_elo_target"] = y_hat
        # save fighter and opponent powers too, while we're at it
        test_df["fighter_elo"] = curr_fighter_powers
        test_df["opponent_elo"] = curr_opponent_powers
        return test_df
    
    def fit_predict(self, train_df: pd.DataFrame, test_df: pd.DataFrame):
        """
        Fit the model on the training data and predict on the test data.
        """
        # fit on the training data
        fitted_elo_df = self.fit(train_df)
        # predict on the test data
        test_df = self.predict(test_df)
        return fitted_elo_df, test_df

class RealEloEstimator(BaseEloEstimator):
    """ 
    Use this to estimate elo scores for real-valued, symmetric score differences.
    """

    def fit_initial_params(self, df: pd.DataFrame):
        return None
    
    def predict_given_powers(self, fighter_elo:np.ndarray, opponent_elo:np.ndarray, X: np.ndarray) -> np.ndarray:
        # Simply return the difference in elo scores
        return fighter_elo - opponent_elo
    
    def get_elo_update(self, y_true:np.ndarray, fighter_elo:np.ndarray, opponent_elo:np.ndarray, X: np.ndarray):
        # get predictions
        y_hat = self.predict_given_powers(fighter_elo, opponent_elo, X)
        # the usual update rule is:
        # self._fighter_powers += 0.5 * (alpha * (y_true - y_hat))
        # because df is "doubled", we need to divide by 2
        delta = 0.25 * self.alpha * (y_true - y_hat)
        delta[np.isnan(delta)] = 0
        return (delta, -1 * delta)
 
class BinaryEloEstimator(BaseEloEstimator):
    """
    Use this to estimate elo scores for {0,1}-valued outcomes.
    """
    def fit_initial_params(self, df: pd.DataFrame):
        return None

    def predict_given_powers(self, fighter_elo: np.ndarray, opponent_elo: np.ndarray, X: np.ndarray) -> np.ndarray:
        return expit(fighter_elo - opponent_elo)
    
    def get_elo_update(self, y_true: np.ndarray, fighter_elo: np.ndarray, opponent_elo: np.ndarray, X: np.ndarray):
        # get predictions
        y_hat = self.predict_given_powers(fighter_elo, opponent_elo, X)
        # the usual update rule is the same as for real-valued outcomes:
        # self._fighter_powers += 0.5 * (alpha * (y_true - y_hat))
        # because df is "doubled", we need to divide by 2
        delta = 0.25 * self.alpha * (y_true - y_hat)
        delta[np.isnan(delta)] = 0
        return (delta, -1 * delta)
    
class BinaryEloErrorEstimator(BinaryEloEstimator):
    """
    Use this to estimate elo scores for {0,1}-valued outcomes,
    predicting errors in another column's predictions.
    """
    def __init__(self, target_col: str, init_score_col: str, alpha: float = 0.5):
        """
        Parameters
        ----------
        target_col : str
            The column containing the target variable.
        init_score_col : str
            The column containing the initial predictions, similar to lightGBM's
            init_score parameter. This is the column that we'll be predicting
            errors in. Assume init_score_col is a logit (i.e. it's on the
            log-odds scale).
        alpha : float, optional
            The learning rate, by default 0.5
        """
        super().__init__(target_col, alpha)
        self.init_score_col = init_score_col

    def extract_features(self, df: pd.DataFrame) -> np.ndarray:
        """
        Extract the initial scores from the dataframe.
        """
        return df[self.init_score_col].values

    def predict_given_powers(self, fighter_elo: np.ndarray, opponent_elo: np.ndarray, X: np.ndarray) -> np.ndarray:
        return expit(X + fighter_elo - opponent_elo)
    

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
        fighter_id_mat = self.transform_fighter_ids(df["FighterID_espn"])
        opponent_id_mat = self.transform_fighter_ids(df["OpponentID_espn"])
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
            "fight_id", 
            "FighterID_espn", "OpponentID_espn", 
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
        fighter_ids_with_target = set(df_with_target["FighterID_espn"]) | \
                                  set(df_with_target["OpponentID_espn"])
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
        is_useless_fighter  = elo_df["FighterID_espn"].isin(useless_fighter_ids)
        is_useless_opponent = elo_df["OpponentID_espn"].isin(useless_fighter_ids)
        elo_df.loc[is_useless_fighter, ['fighter_offense_elo', 'updated_fighter_offense_elo',
                                        'fighter_defense_elo', 'updated_fighter_defense_elo']] = 0
        elo_df.loc[is_useless_opponent, ['opponent_offense_elo', 'updated_opponent_offense_elo',
                                         'opponent_defense_elo', 'updated_opponent_defense_elo']] = 0
        # for fighters who eventually logged the target statistic, we start from 0,
        # and forward-fill their most recent elo score
        for fighter_id in tqdm(fighter_ids_with_target):
            is_fighter_bool  = elo_df["FighterID_espn"] == fighter_id
            is_opponent_bool = elo_df["OpponentID_espn"] == fighter_id
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
        self.fighter_ids = sorted(set(df["FighterID_espn"]) | set(df["OpponentID_espn"]))
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
        keep_cols = ["fight_id", "FighterID_espn", "OpponentID_espn", "Date"]
        elo_df = df[keep_cols].merge(
            fitted_elo_df,
            how="left",
            on=["fight_id", "FighterID_espn", "OpponentID_espn"],
        )
        self.elo_feature_df = self._fit_ffill(elo_df)
        return self.elo_feature_df
    
    def predict(self, df):
        fighter_id_mat = self.transform_fighter_ids(df["FighterID_espn"])
        opponent_id_mat = self.transform_fighter_ids(df["OpponentID_espn"])
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
            "fight_id": df["fight_id"],
            "FighterID_espn": df["FighterID_espn"],
            "OpponentID_espn": df["OpponentID_espn"],
            "pred_p_fighter_landed": p_fighter_hat.A1,
            "pred_p_opponent_landed": p_opponent_hat.A1,
        })
    
    def get_fighter_career_elos(self, fighter_id):
        # just for EDA
        is_fighter_bool = self.elo_feature_df["FighterID_espn"] == fighter_id
        is_opponent_bool = self.elo_feature_df["OpponentID_espn"] == fighter_id
        is_fighting_bool = (
            (self.elo_feature_df["FighterID_espn"] == fighter_id) |
            (self.elo_feature_df["OpponentID_espn"] == fighter_id)
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
            "FighterID_espn": fighter_id,
            "fight_date": fight_dates,
            "n_fights": np.arange(0, is_fighting_bool.sum()),
            "updated_offense_elo": updated_offense_elos,
            "updated_defense_elo": updated_defense_elos,
        })
