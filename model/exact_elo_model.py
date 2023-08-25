"""
Elo is simply a stochastic approximation to logistic regression.
So let's just do logistic regression, or linear regression,
to infer fighter skill(s) from fight outcomes (depending on the skill type).
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm 
from sklearn.metrics import log_loss, accuracy_score, mean_squared_error, mean_absolute_error
from sklearn.preprocessing import OneHotEncoder
from scipy.special import expit, logit
from abc import ABC, abstractmethod
from model.mma_elo_model import BaseEloEstimator
from sklearn.linear_model import LinearRegression, LogisticRegression, Ridge
from scipy.sparse import csr_matrix, hstack

unknown_fighter_id = "2557037" # "2557037/unknown-fighter"

class BaseFighterPowerEstimator(ABC):
    """
    Abstract base class for all exact elo estimators. 
    """

    def __init__(self, target_col, weight_decay=0.01, reg_penalty=10, static_feat_cols=None):
        self.target_col = target_col
        self.weight_decay = weight_decay
        self.reg_penalty = reg_penalty
        if static_feat_cols is None:
            static_feat_cols = []
        self.static_feat_cols = static_feat_cols
        self.fighter_ids = None
        self.fighter_ids_with_target = None
        self.useless_fighter_ids = None
        self._fighter_encoder = None
        self.elo_feature_df = None

        self._linear_model = None # for regression

    def transform_fighter_ids(self, fighter_ids:pd.Series):
        return self._fighter_encoder.transform(fighter_ids.values.reshape(-1,1))
    
    def fit_fighter_encoder(self, fighter_ids:pd.Series):
        self.fighter_ids = sorted(set(fighter_ids))
        self._fighter_encoder = OneHotEncoder(handle_unknown="ignore")
        self._fighter_encoder.fit(fighter_ids.values.reshape(-1,1))

    def fit(self, df: pd.DataFrame):
        """
        Assuming that the data is "doubled" - i.e. that each fight is
        represented twice, once for each (Fighter, Opponent) permutation.
        Returns a dataframe with the same number of rows as the input df.
        df: pd.DataFrame
        """
        assert (df["fight_id"].value_counts() == 2).all()
        df = df.sort_values(["fight_id", "FighterID_espn", "OpponentID_espn"])\
            .dropna(subset=[self.target_col])\
            .reset_index(drop=True)
        if self.fighter_ids is None:
            self.fit_fighter_encoder(df["FighterID_espn"])
        self.init_linear_model(df)
        X = self.extract_features(df)
        self.elo_feature_df = df[["fight_id", "FighterID_espn", "OpponentID_espn", 
                                  "Date"]].copy()
        sample_weights = self.get_sample_weights(df)
        y = df[self.target_col].values
        self.fit_linear_model(X, y, sample_weights=sample_weights)
        self.elo_feature_df = self.elo_feature_df.assign(
            pred_elo_target=self.predict_linear_model(X)
        )
        return self.elo_feature_df
    
    def fit_transform_all(self, df: pd.DataFrame, min_date=None):
        """
        Return predictions one date at a time, starting from min_date.
        """
        if min_date is None:
            min_date = df["Date"].min()
        self.fit_fighter_encoder(df["FighterID_espn"])
        date_range = sorted(df.query(f"Date > '{min_date}'")["Date"].unique())
        pred_df = []
        for date in tqdm(date_range):
            train_inds = df["Date"] < date
            test_inds = df["Date"] == date
            train_df = df.loc[train_inds]
            test_df = df.loc[test_inds]
            pred_df.append(
                test_df[["fight_id", "FighterID_espn", "OpponentID_espn",
                        self.target_col]].assign(
                    pred_elo_target=self.fit_predict(train_df, test_df)
                )
            )
        pred_df = pd.concat(pred_df).reset_index(drop=True)
        return pred_df
    
    def predict(self, test_df: pd.DataFrame):
        """
        Predict the outcome of a fight.
        test_df: pd.DataFrame
        """
        X = self.extract_features(test_df)
        test_df = test_df.assign(
            pred_elo_target=self.predict_linear_model(X)
        )
        return test_df
    
    def fit_predict(self, train_df: pd.DataFrame, test_df: pd.DataFrame):
        """
        Fit the model on the train_df, then predict the outcome of the test_df.
        train_df: pd.DataFrame
        test_df: pd.DataFrame
        """
        _ = self.fit(train_df)
        # pred_elo_df = self.predict(test_df)
        # return fitted_elo_df, pred_elo_df[]
        return self.predict(test_df)["pred_elo_target"]

    def get_sample_weights(self, df: pd.DataFrame):
        """
        Return exponentially decreasing sample weights
        w_t = (1 - self.weight_decay) ** (T - t)
        I don't have to recalculate this every time, since 
        weights get normalized anyway
        """
        days_since_max_date = (df["Date"].max() - df["Date"]).dt.days
        months_since_max_date = days_since_max_date / 30.5
        sample_weights = (1 - self.weight_decay) ** months_since_max_date
        return sample_weights

    def extract_features(self, df: pd.DataFrame):
        """
        Extract features from the dataframe, possibly including static features
        """
        X = (
            self.transform_fighter_ids(df["FighterID_espn"]) - 
            self.transform_fighter_ids(df["OpponentID_espn"])
        )
        if len(self.static_feat_cols) > 0:
            X_extra = df[self.static_feat_cols].values
            X = hstack([X, X_extra])
        return X

    def fit_linear_model(self, X, y, sample_weights=None):
        """
        Fit the linear model
        """
        # coef_init = self._linear_model.coef_ if self._linear_model.coef_ is not None else None
        self._linear_model.fit(X, y, #coef_init=coef_init, 
                               sample_weight=sample_weights)
        
    def init_linear_model(self, df: pd.DataFrame):
        """
        Set up the linear model
        """
        raise NotImplementedError()
    
    def predict_linear_model(self, X: np.ndarray) -> np.ndarray:
        """
        Predict the outcome of a fight between fighter_ids and opponent_ids
        """
        return self._linear_model.predict(X)
    
class BinaryFighterPowerEstimator(BaseFighterPowerEstimator):

    def init_linear_model(self, df: pd.DataFrame):
        self._linear_model = LogisticRegression(
            penalty="l2", max_iter=1000,
            C=1/self.reg_penalty, # inverse of regularization strength
            fit_intercept=False,
            warm_start=True,
            solver="lbfgs",
            n_jobs=-1
        )

    def predict_linear_model(self, X: np.ndarray) -> np.ndarray:
        """
        Predict the outcome of a fight between fighter_ids and opponent_ids
        """
        return self._linear_model.predict_proba(X)[:,1]
    
class RealFighterPowerEstimator(BaseFighterPowerEstimator):
    """
    Predict some real-valued outcome, eg diff in strikes landed
    """
    def init_linear_model(self, df: pd.DataFrame):
        self._linear_model = Ridge(
            max_iter=1000,
            alpha=self.reg_penalty,
            fit_intercept=False,
            # Ridge doesn't accept warm_start. It's usually 
            # solved with a linear eqn solver, not gradient descent
            # so it's not a big deal
        )