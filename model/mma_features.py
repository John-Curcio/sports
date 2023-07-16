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
from model.mma_elo_model import RealEloEstimator, BinaryEloEstimator, \
    AccEloEstimator, BinaryEloErrorEstimator
from model.mma_coop_model import RealCoopEstimator, BinaryCoopEstimator
# from model.exact_elo_model import RealExactEloEstimator, BinaryExactEloEstimator, \
#     RealExactEloErrorEstimator, BinaryExactEloErrorEstimator
from model.exact_elo_model import RealFighterPowerEstimator, BinaryFighterPowerEstimator
from sklearn.decomposition import PCA
from scipy.special import expit, logit
from abc import ABC, abstractmethod


class BaseEloWrapper(ABC):

    def __init__(self, elo_alphas:dict):
        # elo_alphas maps target_col --> alpha
        self.elo_alphas = elo_alphas
        self.fitted_elo_estimators = dict()
    
    def get_preprocessed(self, df):
        """
        Returns preprocessed copy of data before fitting the estimator.
        """
        return df.copy()

    def fit_transform_all(self, df):
        """
        Assuming that the data is "doubled" - i.e. that each fight is
        represented twice, once for each (Fighter, Opponent) permutation.
        Returns a dataframe with the same number of rows as the input df.
        df: pd.DataFrame
        """
        assert (df["fight_id"].value_counts() == 2).all()
        prep_df = self.get_preprocessed(df)
        elo_feat_df = df[["FighterID_espn", "OpponentID_espn", "fight_id"]].copy()
        for target_col, alpha in self.elo_alphas.items():
            print(f"getting elo features for {target_col}")
            elo_estimator = self.estimator_class(target_col, alpha)
            elo_estimator.fit(prep_df)
            elo_feat_df = elo_feat_df.merge(
                elo_estimator.elo_feature_df,
                on=["FighterID_espn", "OpponentID_espn", "fight_id"],
                how="left"
            ).rename(columns={
                "pred_elo_target": f"pred_elo_{target_col}",
                "fighter_elo": f"fighter_elo_{target_col}",
                "opponent_elo": f"opponent_elo_{target_col}",
                "updated_fighter_elo": f"updated_fighter_elo_{target_col}",
                "updated_opponent_elo": f"updated_opponent_elo_{target_col}",
            })
            self.fitted_elo_estimators[target_col] = elo_estimator
        return elo_feat_df
    
    def fit_predict(self, train_df, test_df):
        """
        Assuming that train_df is "doubled" - i.e. that each fight is
        represented twice, once for each (Fighter, Opponent) permutation.
        test_df need not be doubled.
        """
        assert (train_df["fight_id"].value_counts() == 2).all()
        prep_train_df = self.get_preprocessed(train_df)
        prep_test_df = self.get_preprocessed(test_df)
        train_feat_df = train_df[["fight_id", "FighterID_espn", "OpponentID_espn"]].copy()
        test_feat_df = test_df[["fight_id", "FighterID_espn", "OpponentID_espn"]].copy()
        for target_col, alpha in self.elo_alphas.items():
            print(f"getting elo features for {target_col}")
            elo_estimator = self.estimator_class(target_col, alpha)
            fighter_ids = pd.concat([
                prep_train_df["FighterID_espn"], prep_train_df["OpponentID_espn"],
                prep_test_df["FighterID_espn"], prep_test_df["OpponentID_espn"]
            ]).unique()
            elo_estimator.fit_fighter_encoder(pd.Series(fighter_ids))
            elo_estimator.fit(prep_train_df)
            train_feat_df = train_feat_df.merge(
                elo_estimator.elo_feature_df,
                on=["FighterID_espn", "OpponentID_espn", "fight_id"],
                how="left"
            ).rename(columns={
                "pred_elo_target": f"pred_elo_{target_col}",
                "fighter_elo": f"fighter_elo_{target_col}",
                "opponent_elo": f"opponent_elo_{target_col}",
                "updated_fighter_elo": f"updated_fighter_elo_{target_col}",
                "updated_opponent_elo": f"updated_opponent_elo_{target_col}",
            })
            test_feat_df = test_feat_df.merge(
                elo_estimator.predict(prep_test_df),
                on=["FighterID_espn", "OpponentID_espn", "fight_id"],
                how="left"
            ).rename(columns={
                "pred_elo_target": f"pred_elo_{target_col}",
                "fighter_elo": f"fighter_elo_{target_col}",
                "opponent_elo": f"opponent_elo_{target_col}"
            })
            self.fitted_elo_estimators[target_col] = elo_estimator
        return train_feat_df, test_feat_df
    

class RealEloWrapper(BaseEloWrapper):
    estimator_class = RealEloEstimator

class BinaryEloWrapper(BaseEloWrapper):
    estimator_class = BinaryEloEstimator

class BaseFighterPowerWrapper(BaseEloWrapper):
    # Use the same signature as the BaseEloWrapper

    def __init__(self, target_cols, static_feat_cols=None, **estimator_kwargs):
        self.target_cols = target_cols
        self.estimator_kwargs = estimator_kwargs
        self.static_feat_cols = static_feat_cols

    def get_preprocessed(self, df):
        """
        Returns preprocessed copy of data before fitting the estimator.
        """
        return df.copy()
    
    def fit_transform_all(self, df, min_date=pd.to_datetime("2023-01-01")):
        """
        Assuming that the data is "doubled" - i.e. that each fight is
        represented twice, once for each (Fighter, Opponent) permutation.
        Returns a dataframe with the same number of rows as the input df.
        df: pd.DataFrame
        """
        assert (df["fight_id"].value_counts() == 2).all()
        prep_df = self.get_preprocessed(df)
        feat_df = df[["FighterID_espn", "OpponentID_espn", "fight_id"]].copy()
        # make sure all the target_cols are in the data
        assert all([col in prep_df.columns for col in self.target_cols])
        for target_col in self.target_cols:
            print(f"fitting {target_col}")
            estimator = self.estimator_class(target_col, static_feat_cols=self.static_feat_cols,
                                             **self.estimator_kwargs)
            curr_pred_df = estimator.fit_transform_all(prep_df, min_date=min_date)
            curr_pred_df = curr_pred_df.rename(columns={"pred_elo_target": f"pred_{target_col}"})
            feat_df = feat_df.merge(curr_pred_df, 
                                    on=["FighterID_espn", "OpponentID_espn", "fight_id"],
                                    how="left")
        return feat_df

class RealFighterPowerWrapper(BaseFighterPowerWrapper):
    estimator_class = RealFighterPowerEstimator

class BinaryFighterPowerWrapper(BaseFighterPowerWrapper):
    estimator_class = BinaryFighterPowerEstimator

# class BaseExactEloWrapper(BaseEloWrapper):

#     def __init__(self, elo_alphas: dict):
#         super().__init__(elo_alphas)

#     def fit_transform_all(self, df):
#         """
#         Assuming that the data is "doubled" - i.e. that each fight is
#         represented twice, once for each (Fighter, Opponent) permutation.
#         Returns a dataframe with the same number of rows as the input df.
#         df: pd.DataFrame
#         """
#         assert (df["fight_id"].value_counts() == 2).all()
#         prep_df = self.get_preprocessed(df)
#         elo_feat_df = df[["FighterID_espn", "OpponentID_espn", "fight_id"]].copy()
#         for target_col, alpha in self.elo_alphas.items():
#             print(f"getting elo features for {target_col}")
#             elo_estimator = self.estimator_class(target_col, weight_decay=(1-alpha))
#             elo_estimator.fit(prep_df)
#             elo_feat_df = elo_feat_df.merge(
#                 elo_estimator.elo_feature_df,
#                 on=["FighterID_espn", "OpponentID_espn", "fight_id"],
#                 how="left"
#             ).rename(columns={
#                 "pred_elo_target": f"pred_elo_{target_col}",
#                 # "fighter_elo": f"fighter_elo_{target_col}",
#                 # "opponent_elo": f"opponent_elo_{target_col}",
#                 # "updated_fighter_elo": f"updated_fighter_elo_{target_col}",
#                 # "updated_opponent_elo": f"updated_opponent_elo_{target_col}",
#             })
#             self.fitted_elo_estimators[target_col] = elo_estimator
#         return elo_feat_df
    
# class RealExactEloWrapper(BaseExactEloWrapper):
#     estimator_class = RealExactEloEstimator

# class BinaryExactEloWrapper(BaseExactEloWrapper):
#     estimator_class = BinaryExactEloEstimator

# class ExactEloErrorWrapper(BaseExactEloWrapper):

#     def __init__(self, elo_alphas: dict, init_score_col: str):
#         super().__init__(elo_alphas)
#         self.init_score_col = init_score_col

#     def fit_transform_all(self, df):
#         """
#         Assuming that the data is "doubled" - i.e. that each fight is
#         represented twice, once for each (Fighter, Opponent) permutation.
#         Returns a dataframe with the same number of rows as the input df.
#         df: pd.DataFrame
#         """
#         assert (df["fight_id"].value_counts() == 2).all()
#         prep_df = self.get_preprocessed(df)
#         elo_feat_df = df[["FighterID_espn", "OpponentID_espn", "fight_id"]].copy()
#         for target_col, alpha in self.elo_alphas.items():
#             print(f"getting elo features for {target_col}")
#             elo_estimator = self.estimator_class(target_col, self.init_score_col, weight_decay=(1-alpha))
#             elo_estimator.fit(prep_df)
#             elo_feat_df = elo_feat_df.merge(
#                 elo_estimator.elo_feature_df,
#                 on=["FighterID_espn", "OpponentID_espn", "fight_id"],
#                 how="left"
#             ).rename(columns={
#                 "pred_elo_target": f"pred_elo_{target_col}",
#                 # "fighter_elo": f"fighter_elo_{target_col}",
#                 # "opponent_elo": f"opponent_elo_{target_col}",
#                 # "updated_fighter_elo": f"updated_fighter_elo_{target_col}",
#                 # "updated_opponent_elo": f"updated_opponent_elo_{target_col}",
#             })
#             self.fitted_elo_estimators[target_col] = elo_estimator
#         return elo_feat_df
        

# class RealExactEloErrorWrapper(ExactEloErrorWrapper):
#     estimator_class = RealExactEloErrorEstimator

# class BinaryExactEloErrorWrapper(ExactEloErrorWrapper):
#     estimator_class = BinaryExactEloErrorEstimator

class RealCoopWrapper(BaseEloWrapper):
    estimator_class = RealCoopEstimator

class BinaryCoopWrapper(BaseEloWrapper):
    estimator_class = BinaryCoopEstimator

class BinaryEloErrorWrapper(BaseEloWrapper):

    def __init__(self, elo_alphas, init_score_col):
        self.elo_alphas = elo_alphas
        self.init_score_col = init_score_col
        self.fitted_elo_estimators = dict()
        self.estimator_class = lambda target_col, alpha: BinaryEloErrorEstimator(
            target_col=target_col, alpha=alpha, init_score_col=init_score_col
        )
            
class PcaEloWrapper(RealEloWrapper):
    
    def __init__(self, n_pca, target_cols, alpha, conditional_var_col="gender"):
        self.n_pca = n_pca
        self.target_cols = target_cols
        self.alpha = alpha
        self.conditional_var_col = conditional_var_col
        self.pca = None
        elo_alphas = {f"PC_{i}":alpha for i in range(n_pca)}
        super().__init__(elo_alphas)

    def _fit_transform_pca(self, df):
        """
        Assuming that the data is "doubled" - i.e. that each fight is
        represented twice, once for each (Fighter, Opponent) permutation.
        Returns a dataframe of PCA fight outcomes.
        df: pd.DataFrame
        """
        assert (df["fight_id"].value_counts() == 2).all()
        self.pca = PCA(whiten=True)
        df_sub = df.dropna(subset=self.target_cols, how="any")
        temp_pca_train_data = df_sub[self.target_cols].copy()
        pca_train_data = None
        if self.conditional_var_col is None:
            # no heteroscedasticity
            scale_factor = temp_pca_train_data[self.target_cols].std()
            temp_pca_train_data[self.target_cols] /= scale_factor
            self.pca.fit(temp_pca_train_data)
            pca_train_data = self.pca.transform(df_sub[self.target_cols] / scale_factor)
        else:
            # conditional heteroscedasticity
            var_id_vec = df_sub[self.conditional_var_col].copy()
            conditional_scale_map = temp_pca_train_data.groupby(var_id_vec).std()
            temp_conditional_scale = conditional_scale_map.loc[var_id_vec].values
            temp_pca_train_data[self.target_cols] /= temp_conditional_scale
            self.pca.fit(temp_pca_train_data)
            conditional_scale = conditional_scale_map.loc[df_sub[self.conditional_var_col]].values
            pca_train_data = self.pca.transform(df_sub[self.target_cols] / conditional_scale)
        pca_cols = ["PC_{}".format(i) for i in range(pca_train_data.shape[1])]
        pca_df = pd.DataFrame(pca_train_data, columns=pca_cols, index=df_sub.index)
        pca_df["fight_id"] = df_sub["fight_id"]
        pca_df["FighterID_espn"] = df_sub["FighterID_espn"]
        pca_df["OpponentID_espn"] = df_sub["OpponentID_espn"]
        # join back to original df
        pca_df = df.merge(
            pca_df, 
            on=["fight_id", "FighterID_espn", "OpponentID_espn"], 
            how="left"
        )
        return pca_df

    def fit_transform_all(self, df):
        """
        Assuming that the data is "doubled" - i.e. that each fight is
        represented twice, once for each (Fighter, Opponent) permutation.
        Returns a dataframe with the same number of rows as the input df.
        df: pd.DataFrame"""
        pca_df = self._fit_transform_pca(df)
        return super().fit_transform_all(pca_df)

    def fit_predict(self, train_df, test_df):
        """
        Assuming that train_df is "doubled" - i.e. that each fight is
        represented twice, once for each (Fighter, Opponent) permutation.
        test_df need not be doubled.
        """
        pca_train_df = self._fit_transform_pca(train_df)
        # needn't fit or apply PCA to test data, since we're just predicting
        # what we'll see along the principal components of the fight outcomes
        # for a given pair of fighters
        return super().fit_predict(pca_train_df, test_df)


class AccEloWrapper(object):
    
    def __init__(self, elo_alphas:dict):
        # elo_alphas maps (landed_col, attempt_col) --> alpha
        self.elo_alphas = elo_alphas
        self.fitted_elo_estimators = dict()
        
    def fit_transform_all(self, df):
        """
        Assuming that the data is "doubled" - i.e. that each fight is
        represented twice, once for each (Fighter, Opponent) permutation.
        """
        assert (df["fight_id"].value_counts() == 2).all()
        df = df.drop_duplicates(subset=["fight_id"])
        # elo_feat_df = df[["fight_id"]].copy()
        elo_feat_df1 = df[["fight_id", "FighterID_espn", "OpponentID_espn"]].copy()
        elo_feat_df2 = df[["fight_id", "FighterID_espn", "OpponentID_espn"]].rename(
            columns={"FighterID_espn": "OpponentID_espn", 
                     "OpponentID_espn": "FighterID_espn"}
        )
        for (landed_col, attempt_col), alpha in self.elo_alphas.items():
            target_col = f"{landed_col}_{attempt_col}"
            print(f"getting elo features for {landed_col}/{attempt_col}")
            elo_estimator = AccEloEstimator(landed_col=landed_col, attempt_col=attempt_col, alpha=alpha)
            elo_estimator.fit(df)
            self.fitted_elo_estimators[target_col] = elo_estimator
            
            elo_feat_df1[f"pred_p_{target_col}_diff"] = (
                elo_estimator.elo_feature_df["p_fighter_hat"] -
                elo_estimator.elo_feature_df["p_opponent_hat"]
            )
            elo_feat_df1[f"pred_logit_p_{target_col}_diff"] = (
                logit(elo_estimator.elo_feature_df["p_fighter_hat"]) -
                logit(elo_estimator.elo_feature_df["p_opponent_hat"])
            )
            # flip predictions for the second df
            elo_feat_df2[f"pred_p_{target_col}_diff"] = (
                -1 * elo_feat_df1[f"pred_p_{target_col}_diff"]
            )
            elo_feat_df2[f"pred_logit_p_{target_col}_diff"] = (
                -1 * elo_feat_df1[f"pred_logit_p_{target_col}_diff"]
            )
        elo_feat_df = pd.concat([elo_feat_df1, elo_feat_df2]).reset_index(drop=True)
        return elo_feat_df
    
    def fit_predict(self, train_df, test_df):
        """
        train_df: pd.DataFrame, assumed to be "doubled"
        test_df: pd.DataFrame, not necessarily doubled
        """
        train_df = train_df.drop_duplicates(subset=["fight_id"])
        # don't have to drop duplicates for test_df because we're not fitting anything
        train_feat_df1 = train_df[["fight_id", "FighterID_espn", "OpponentID_espn"]].copy()
        train_feat_df2 = train_df[["fight_id", "FighterID_espn", "OpponentID_espn"]].rename(
            columns={"FighterID_espn": "OpponentID_espn",
                        "OpponentID_espn": "FighterID_espn"}
        )
        test_feat_df = test_df[["fight_id"]].copy()
        for (landed_col, attempt_col), alpha in self.elo_alphas.items():
            target_col = f"{landed_col}_{attempt_col}"
            print(f"getting elo features for {landed_col}/{attempt_col}")
            elo_estimator = AccEloEstimator(landed_col=landed_col, attempt_col=attempt_col, alpha=alpha)
            elo_estimator.fit_fighter_encoder(pd.concat([train_df, test_df]))
            elo_estimator.fit(train_df)
            self.fitted_elo_estimators[target_col] = elo_estimator
            
            train_feat_df1[f"pred_p_{target_col}_diff"] = (
                elo_estimator.elo_feature_df["p_fighter_hat"] -
                elo_estimator.elo_feature_df["p_opponent_hat"]
            )
            train_feat_df1[f"pred_logit_p_{target_col}_diff"] = (
                logit(elo_estimator.elo_feature_df["p_fighter_hat"]) -
                logit(elo_estimator.elo_feature_df["p_opponent_hat"])
            )
            # flip predictions for the second df
            train_feat_df2[f"pred_p_{target_col}_diff"] = (
                -1 * train_feat_df1[f"pred_p_{target_col}_diff"]
            )
            train_feat_df2[f"pred_logit_p_{target_col}_diff"] = (
                -1 * train_feat_df1[f"pred_logit_p_{target_col}_diff"]
            )
            pred_df = elo_estimator.predict(test_df)
            display(pred_df)
            test_feat_df[f"pred_p_{target_col}_diff"] = (
                pred_df["pred_p_fighter_landed"] - pred_df["pred_p_opponent_landed"]
            )
            test_feat_df[f"pred_logit_p_{target_col}_diff"] = (
                logit(pred_df["pred_p_fighter_landed"]) - logit(pred_df["pred_p_opponent_landed"])
            )
        train_feat_df = pd.concat([train_feat_df1, train_feat_df2]).reset_index(drop=True)
        return train_feat_df, test_feat_df