"""
This module contains classes for generating elo-score features from
MMA data. Provides handy wrappers around the RealEloEstimator,
BinaryEloEstimator, etc classes.

To use for training and validation, instantiate a wrapper class, 
then call fit_transform_all() to generate elo features.
To use for training and out-of-sample prediction, instantiate a wrapper 
class, then call fit_predict() to generate elo features for both 
train and test.

Example:
    # For training and validation
    elo_wrapper = RealEloWrapper(elo_alphas={"targetWin": 0.5, "targetKO": 0.5})
    elo_feat_df = elo_wrapper.fit_transform_all(train_df)
    # For training and out-of-sample prediction
    elo_wrapper = RealEloWrapper(elo_alphas={"targetWin": 0.5, "targetKO": 0.5})
    elo_feat_df = elo_wrapper.fit_predict(train_df, test_df)

Notes:
    - The fit_*() methods don't assume that the data is "doubled" - i.e.
    that each fight is represented twice, once for each (Fighter, Opponent) 
    permutation. So in methods like fit_transform_all(), the data is 
    deduped, then in _fit_transform(), the data is doubled! Very silly.
"""

# import numpy as np 
import pandas as pd 
from model.mma_elo_model import RealEloEstimator, BinaryEloEstimator, AccEloEstimator
from sklearn.decomposition import PCA
from scipy.special import expit, logit


class RealEloWrapper(object):
    
    def __init__(self, elo_alphas:dict):
        # elo_alphas maps target_col --> alpha
        self.elo_alphas = elo_alphas
        self.fitted_elo_estimators = dict()
        
    def fit_transform_all(self, df):
        elo_feature_list = []
        df = df.drop_duplicates(subset=["fight_id"])
        elo_feat_df = df[["fight_id"]].copy()
        for target_col, alpha in self.elo_alphas.items():
            print(f"getting elo features for {target_col}")
            elo_estimator = RealEloEstimator(target_col, alpha=alpha)
            elo_estimator.fit(df)
            self.fitted_elo_estimators[target_col] = elo_estimator
            elo_feat_df[f"pred_{target_col}"] = elo_estimator.elo_feature_df["pred_target"]
        return elo_feat_df
    
    def fit_predict(self, train_df, test_df):
        train_feat_df = train_df[["fight_id"]].copy()
        test_feat_df = test_df[["fight_id"]].copy()
        for target_col, alpha in self.elo_alphas.items():
            print(f"getting elo features for {target_col}")
            elo_estimator = RealEloEstimator(target_col, alpha=alpha)
            elo_estimator.fit_fighter_encoder(pd.concat([train_df, test_df]))
            elo_estimator.fit(train_df)
            self.fitted_elo_estimators[target_col] = elo_estimator
            train_feat_df[f"pred_{target_col}"] = elo_estimator.elo_feature_df["pred_target"]
            test_feat_df[f"pred_{target_col}"] = elo_estimator.predict(test_df)["pred_target"]
        return train_feat_df, test_feat_df


class BinaryEloWrapper(object):
    
    def __init__(self, elo_alphas:dict):
        # elo_alphas maps target_col --> alpha
        self.elo_alphas = elo_alphas
        self.fitted_elo_estimators = dict()
        
    def fit_transform_all(self, df):
        elo_feature_list = []
        df = df.drop_duplicates(subset=["fight_id"])
        elo_feat_df = df[["fight_id"]].copy()
        for target_col, alpha in self.elo_alphas.items():
            print(f"getting elo features for {target_col}")
            elo_estimator = BinaryEloEstimator(target_col, alpha=alpha)
            elo_estimator.fit(df)
            self.fitted_elo_estimators[target_col] = elo_estimator
            elo_feat_df[f"pred_{target_col}"] = elo_estimator.elo_feature_df["pred_target"]
            elo_feat_df[f"pred_{target_col}_logit"] = logit(elo_feat_df[f"pred_{target_col}"])
        return elo_feat_df
    
    def fit_predict(self, train_df, test_df):
        train_feat_df = train_df[["fight_id"]].copy()
        test_feat_df = test_df[["fight_id"]].copy()
        for target_col, alpha in self.elo_alphas.items():
            print(f"getting elo features for {target_col}")
            elo_estimator = BinaryEloEstimator(target_col, alpha=alpha)
            elo_estimator.fit_fighter_encoder(pd.concat([train_df, test_df]))
            elo_estimator.fit(train_df)
            self.fitted_elo_estimators[target_col] = elo_estimator
            train_feat_df[f"pred_{target_col}"] = elo_estimator.elo_feature_df["pred_target"]
            test_feat_df[f"pred_{target_col}"] = elo_estimator.predict(test_df)["pred_target"]
            train_feat_df[f"pred_{target_col}_logit"] = logit(train_feat_df[f"pred_{target_col}"])
            test_feat_df[f"pred_{target_col}_logit"]  = logit(test_feat_df[f"pred_{target_col}"])
        return train_feat_df, test_feat_df


class PcaEloWrapper(object):
    
    def __init__(self, n_pca, target_cols, alpha, conditional_var_col="gender"):
        self.n_pca = n_pca
        self.target_cols = target_cols
        self.alpha = alpha
        self.conditional_var_col = conditional_var_col
        self.pca = PCA(whiten=True)
        self.elo_alphas = {f"PC_{i}":alpha for i in range(n_pca)}
        self.elo_wrapper = RealEloWrapper(self.elo_alphas)
        
    def _fit_transform_pca(self, df):
        df = df.drop_duplicates(subset=["fight_id"])
        # okay first we double the df (targets had better be centered at 0 or we have a problem)
        df_sub = df.dropna(subset=self.target_cols)
        temp_pca_train_data = pd.concat([df_sub[self.target_cols], 
                                         -df_sub[self.target_cols]]).reset_index(drop=True)
        pca_train_data = None
        if self.conditional_var_col is None:
            # no heteroscedasticity
            scale_factor = temp_pca_train_data[self.target_cols].std()
            temp_pca_train_data[self.target_cols] /= scale_factor
            self.pca.fit(temp_pca_train_data)
            pca_train_data = self.pca.transform(df_sub[self.target_cols] / scale_factor)
        else:
            # conditional heteroscedasticity
            var_id_vec = pd.concat([df_sub[self.conditional_var_col], 
                                    df_sub[self.conditional_var_col]]).reset_index(drop=True)
            conditional_scale_map = temp_pca_train_data.groupby(var_id_vec).std()
            temp_conditional_scale = conditional_scale_map.loc[var_id_vec].values
            temp_pca_train_data[self.target_cols] /= temp_conditional_scale
            self.pca.fit(temp_pca_train_data)
            conditional_scale = conditional_scale_map.loc[df_sub[self.conditional_var_col]].values
            pca_train_data = self.pca.transform(df_sub[self.target_cols] / conditional_scale)
        pca_cols = ["PC_{}".format(i) for i in range(pca_train_data.shape[1])]
        pca_df = pd.DataFrame(pca_train_data, columns=pca_cols)
        pca_df["fight_id"] = df_sub["fight_id"].values
        pca_df["FighterID_espn"] = df_sub["FighterID_espn"].values
        pca_df["OpponentID_espn"] = df_sub["OpponentID_espn"].values
        pca_df["Date"] = df_sub["Date"].values
        return pca_df
        
    def fit_transform_all(self, df):
        df = df.drop_duplicates(subset=["fight_id"])
        pca_df = self._fit_transform_pca(df)
        pca_df = df[["fight_id", "FighterID_espn", "OpponentID_espn"]].merge(
            pca_df, how="left", on=["fight_id", "FighterID_espn", "OpponentID_espn"],
        )
        return self.elo_wrapper.fit_transform_all(pca_df)

    def fit_predict(self, train_df, test_df):
        train_df = train_df.drop_duplicates(subset=["fight_id"])
        # don't have to drop duplicates for test_df because we're not fitting anything
        train_pca_df = self._fit_transform_pca(train_df)
        train_pca_df = train_df[["fight_id", "FighterID_espn", "OpponentID_espn"]].merge(
            train_pca_df, how="left", on=["fight_id", "FighterID_espn", "OpponentID_espn"],
        )
        return self.elo_wrapper.fit_predict(train_pca_df, test_df)
    
    def get_fighter_career_elos(self, fighter_id):
        pass


class AccEloWrapper(object):
    
    def __init__(self, elo_alphas:dict):
        # elo_alphas maps (landed_col, attempt_col) --> alpha
        self.elo_alphas = elo_alphas
        self.fitted_elo_estimators = dict()
        
    def fit_transform_all(self, df):
        df = df.drop_duplicates(subset=["fight_id"])
        elo_feature_list = []
        elo_feat_df = df[["fight_id"]].copy()
        for (landed_col, attempt_col), alpha in self.elo_alphas.items():
            target_col = f"{landed_col}_{attempt_col}"
            print(f"getting elo features for {landed_col}/{attempt_col}")
            elo_estimator = AccEloEstimator(landed_col=landed_col, attempt_col=attempt_col, alpha=alpha)
            elo_estimator.fit(df)
            self.fitted_elo_estimators[target_col] = elo_estimator
            
            elo_feat_df[f"pred_p_{target_col}_diff"] = (
                elo_estimator.elo_feature_df["p_fighter_hat"] -
                elo_estimator.elo_feature_df["p_opponent_hat"]
            )
            elo_feat_df[f"pred_logit_p_{target_col}_diff"] = (
                logit(elo_estimator.elo_feature_df["p_fighter_hat"]) -
                logit(elo_estimator.elo_feature_df["p_opponent_hat"])
            )
        return elo_feat_df
    
    def fit_predict(self, train_df, test_df):
        train_df = train_df.drop_duplicates(subset=["fight_id"])
        # don't have to drop duplicates for test_df because we're not fitting anything
        train_feat_df = train_df[["fight_id"]].copy()
        test_feat_df = test_df[["fight_id"]].copy()
        for (landed_col, attempt_col), alpha in self.elo_alphas.items():
            target_col = f"{landed_col}_{attempt_col}"
            print(f"getting elo features for {landed_col}/{attempt_col}")
            elo_estimator = AccEloEstimator(landed_col=landed_col, attempt_col=attempt_col, alpha=alpha)
            elo_estimator.fit_fighter_encoder(pd.concat([train_df, test_df]))
            elo_estimator.fit(train_df)
            self.fitted_elo_estimators[target_col] = elo_estimator
            
            train_feat_df[f"pred_p_{target_col}_diff"] = (
                elo_estimator.elo_feature_df["p_fighter_hat"] -
                elo_estimator.elo_feature_df["p_opponent_hat"]
            )
            train_feat_df[f"pred_logit_p_{target_col}_diff"] = (
                logit(elo_estimator.elo_feature_df["p_fighter_hat"]) -
                logit(elo_estimator.elo_feature_df["p_opponent_hat"])
            )
            pred_df = elo_estimator.predict(test_df)
            display(pred_df)
            test_feat_df[f"pred_p_{target_col}_diff"] = (
                pred_df["pred_p_fighter_landed"] - pred_df["pred_p_opponent_landed"]
            )
            test_feat_df[f"pred_logit_p_{target_col}_diff"] = (
                logit(pred_df["pred_p_fighter_landed"]) - logit(pred_df["pred_p_opponent_landed"])
            )
        return train_feat_df, test_feat_df