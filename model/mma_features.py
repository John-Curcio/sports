# import numpy as np 
import pandas as pd 
from model.mma_elo_model import RealEloEstimator, BinaryEloEstimator
from sklearn.decomposition import PCA

class RealEloWrapper(object):
    
    def __init__(self, elo_alphas:dict):
        # elo_alphas maps target_col --> alpha
        self.elo_alphas = elo_alphas
        self.fitted_elo_estimators = dict()
        
    def fit_transform_all(self, df):
        elo_feature_list = []
        elo_feat_df = df[["espn_fight_id"]].copy()
        for target_col, alpha in self.elo_alphas.items():
            print(f"getting elo features for {target_col}")
            elo_estimator = RealEloEstimator(target_col)
            elo_estimator.fit(df)
            self.fitted_elo_estimators[target_col] = elo_estimator
            elo_feat_df[f"pred_{target_col}"] = elo_estimator.elo_feature_df["pred_target"]
        return elo_feat_df
    
    def fit_predict(self, train_df, test_df):
        train_feat_df = train_df[["espn_fight_id"]].copy()
        test_feat_df = test_df[["espn_fight_id"]].copy()
        for target_col, alpha in self.elo_alphas.items():
            print(f"getting elo features for {target_col}")
            elo_estimator = RealEloEstimator(target_col, alpha)
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
        elo_feat_df = df[["espn_fight_id"]].copy()
        for target_col, alpha in self.elo_alphas.items():
            print(f"getting elo features for {target_col}")
            elo_estimator = BinaryEloEstimator(target_col)
            elo_estimator.fit(df)
            self.fitted_elo_estimators[target_col] = elo_estimator
            elo_feat_df[f"pred_{target_col}"] = elo_estimator.elo_feature_df["pred_target"]
            elo_feat_df[f"pred_{target_col}_logit"] = logit(elo_feat_df[f"pred_{target_col}"])
        return elo_feat_df
    
    def fit_predict(self, train_df, test_df):
        train_feat_df = train_df[["espn_fight_id"]].copy()
        test_feat_df = test_df[["espn_fight_id"]].copy()
        for target_col, alpha in self.elo_alphas.items():
            print(f"getting elo features for {target_col}")
            elo_estimator = BinaryEloEstimator(target_col, alpha)
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
        self.elo_wrapper = EloWrapper(self.elo_alphas)
        
    def _fit_transform_pca(self, df):
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
        pca_df["espn_fight_id"] = df_sub["espn_fight_id"].values
        pca_df["espn_fighter_id"] = df_sub["espn_fighter_id"].values
        pca_df["espn_opponent_id"] = df_sub["espn_opponent_id"].values
        pca_df["Date"] = df_sub["Date"].values
        return pca_df
        
    def fit_transform_all(self, df):
        pca_df = self._fit_pca(df)
        pca_df = df[["espn_fight_id", "espn_fighter_id", "espn_opponent_id"]].merge(
            pca_df, how="left", on=["espn_fight_id", "espn_fighter_id", "espn_opponent_id"],
        )
        return self.elo_wrapper.fit_transform_all(pca_df)

    def fit_predict(self, train_df, test_df):
        train_pca_df = self._fit_transform_pca(train_df)
        train_pca_df = train_df[["espn_fight_id", "espn_fighter_id", "espn_opponent_id"]].merge(
            train_pca_df, how="left", on=["espn_fight_id", "espn_fighter_id", "espn_opponent_id"],
        )
        return self.elo_wrapper.fit_predict(train_pca_df, test_df)
    
    def get_fighter_career_elos(self, fighter_id):
        pass