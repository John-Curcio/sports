from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import log_loss

class LogisticRegressionOnEloFeatures(object):
    
    def __init__(self, alpha=0.3, binary_features=["Win"], 
                 continuous_features=["SSL_sqrt_diff", "TDL_sqrt_diff"]):
        self.alpha = alpha
        self.binary_features = binary_features
        self.continuous_features = continuous_features
        self.feature_names = sorted(binary_features + continuous_features)
        self.elo_dfs = {
            feat: None for feat in binary_features + continuous_features
        }
        self._scaler = StandardScaler(with_mean=False)
        self._model = LogisticRegression(fit_intercept=False)
        
    def transform(self, df):
        for feature in self.feature_names:
            curr_ep = None
            if feature in self.binary_features:
                curr_ep = LogisticEwmaPowers(target_col=feature, alpha=self.alpha)
            if feature in self.continuous_features:
                curr_ep = EwmaPowers(target_col=feature, alpha=self.alpha)
            temp_df = df.loc[~df[feature].isnull()]
            curr_ep.fit(temp_df)
            self.elo_dfs[feature] = curr_ep.elo_df   
        join_cols = ["FighterID", "OpponentID", "Date"]
        full_elo_df = None
        for feature in self.feature_names:
            right_df = self.elo_dfs[feature].rename(columns={
                col: col+feature for col in self.elo_dfs[feature]
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
    
    def get_elo_diffs(self, elo_df):
        X_col_list = []
        for col in self.feature_names:
            X_col_list.append(
                elo_df["oldFighterElo"+col] - elo_df["oldOpponentElo"+col]
            )
        X = np.array(X_col_list).T
        return X
    
    def fit(self, trans_df, target_col):
        y = trans_df[target_col]
        X = self.get_elo_diffs(trans_df)
        X = self._scaler.fit_transform(X)
        return self._model.fit(X, y)
        
    def predict(self, trans_df):
        X = self.get_elo_diffs(trans_df)
        X = self._scaler.transform(X)
        return self._model.predict(X)
        
    def predict_proba(self, trans_df):
        X = self.get_elo_diffs(trans_df)
        X = self._scaler.transform(X)
        return self._model.predict_proba(X)
        
    def score(self, trans_df, target_col):
        y = trans_df[target_col]
        X = self.get_elo_diffs(trans_df)
        X = self._scaler.transform(X)
        return self._model.score(X, y)
    
    def score_log_loss(self, trans_df, target_col):
        y = trans_df[target_col]
        X = self.get_elo_diffs(trans_df)
        X = self._scaler.transform(X)
        y_pred = self._model.predict_proba(X)[:,0]
        return log_loss(y_pred=y_pred, y_true=y)