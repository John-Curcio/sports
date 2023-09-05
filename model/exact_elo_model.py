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
# from concurrent.futures import ProcessPoolExecutor
from joblib import Parallel, delayed

unknown_fighter_id = "2557037" # "2557037/unknown-fighter"

class BaseFighterPowerEstimator(ABC):
    """
    Abstract base class for all exact elo estimators
    """

    def __init__(self, target_col, weight_decay=0.01, reg_penalty=10, static_feat_cols=None):
        self.target_col = target_col
        self.weight_decay = weight_decay
        self.reg_penalty = reg_penalty
        if static_feat_cols is None:
            static_feat_cols = []
        self.static_feat_cols = static_feat_cols
        self.fighter_ids = None
        self._fighter_encoder = None
        self.elo_feature_df = None

        self._linear_model = None

    def fit_fighter_encoder(self, df: pd.DataFrame, fast=False):
        assert (df["fight_id"].value_counts() == 2).all()
        if fast:
            fighter_counts = df["FighterID_espn"].value_counts()
            # this may introduce data leakage - we don't know whether the fighter will fight again in the future
            fighter_ids = fighter_ids.map(lambda x: x if fighter_counts[x] > 1 else "journeyman")
        self.fighter_ids = sorted(df["FighterID_espn"])
        self._fighter_encoder = OneHotEncoder(handle_unknown="ignore")
        self._fighter_encoder.fit(np.array(self.fighter_ids).reshape(-1,1))

    def extract_features(self, df: pd.DataFrame):
        """
        Extract features from the dataframe, possibly including static features
        """
        X = (
            self._fighter_encoder.transform(df["FighterID_espn"].values.reshape(-1,1)) - 
            self._fighter_encoder.transform(df["OpponentID_espn"].values.reshape(-1,1))
        )
        if len(self.static_feat_cols) > 0:
            X_extra = df[self.static_feat_cols].values
            X = hstack([X, X_extra])
        return X

    def fit_transform_all(self, df, min_date=None, fast=False):
        """
        For each date d starting from min_date, fit the model on all data prior to d,
        and predict the outcome of all fights on date d.
        Return a dataframe with the same number of rows as df.query("Date > {min_date}")
        """
        assert (df["fight_id"].value_counts() == 2).all()
        if min_date is None:
            min_date = df["Date"].min()
        date_range = sorted(df["Date"].loc[df["Date"] > min_date].unique())
        pred_df = []
        self.fit_fighter_encoder(df, fast=fast)
        self.init_linear_model(df)
        X = self.extract_features(df).tocsr()
        y = df[self.target_col]
        dt_vec = df["Date"]
        log_w = np.log(1 - self.weight_decay) * (df["Date"].max() - df["Date"]).dt.days / 30.5
        for date in tqdm(date_range):
            train_inds = (dt_vec < date) & y.notnull()
            test_inds = dt_vec == date
            X_train, y_train = X[train_inds, :], y[train_inds]
            X_test = X[test_inds, :]
            w_train = np.exp(log_w[train_inds] - log_w[train_inds].max())
            self.fit_linear_model(X_train, y_train, sample_weights=w_train)
            pred_df.append(
                df.loc[test_inds, ["fight_id", "FighterID_espn", "OpponentID_espn",
                                      self.target_col]].assign(
                    pred_elo_target=self.predict_linear_model(X_test)
                                      )
            )
        pred_df = pd.concat(pred_df).reset_index(drop=True)
        return pred_df
    
    def fit_linear_model(self, X, y, sample_weights=None):
        return self._linear_model.fit(X, y, sample_weight=sample_weights)
    
    def predict_linear_model(self, X):
        # logistic regression will have to override this
        return self._linear_model.predict(X)
    
    def fit_predict(self, train_df, test_df):
        X = self.extract_features(train_df)
        y = train_df[self.target_col]
        log_w = np.log(1 - self.weight_decay) * (train_df["Date"].max() - train_df["Date"]).dt.days / 30.5
        w_train = np.exp(log_w - log_w.max())
        self.fit_linear_model(X, y, sample_weights=w_train)
        X_test = self.extract_features(test_df)
        return test_df[["fight_id", "FighterID_espn", "OpponentID_espn"]].assign(
            pred_elo_target=self.predict_linear_model(X_test)
        )
    
    def init_linear_model(self, df: pd.DataFrame):
        raise NotImplementedError()


class BinaryFighterPowerEstimator(BaseFighterPowerEstimator):

    def init_linear_model(self, df: pd.DataFrame):
        self._linear_model = LogisticRegression(
            penalty="l2", max_iter=1000,
            C=1/self.reg_penalty, # inverse of regularization strength
            fit_intercept=False,
            warm_start=True,
            # warm_start=False,
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