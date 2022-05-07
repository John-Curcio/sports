import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import seaborn as sns 
from sklearn.metrics import log_loss, accuracy_score

class TimeSeriesCrossVal(object):
    # Just regular time series cross validation,
    # but ensures dates don't end up in multiple folds

    def __init__(self, n_splits=4):
        self.n_splits = n_splits
        self.fold_pred_df = None

    def get_folds(self, df):
        df = df.sort_values(["Date", "FighterID", "OpponentID"])
        dates = sorted(df["Date"].unique())
        n_dates_per_fold = len(dates) // (self.n_splits + 1)
        for i in range(self.n_splits + 1):
            start = i * n_dates_per_fold
            stop = min(start + n_dates_per_fold, len(dates)-1)
            min_date = dates[start]
            max_date = dates[stop]
            inds = (df["Date"] >= min_date) & (df["Date"] < max_date)
            yield df.loc[inds]
            
    def get_cross_val_preds(self, model, df):
        train_df = pd.DataFrame()
        fold_pred_df_list = []        
        for i, test_df in enumerate(self.get_folds(df)):
            if len(train_df) > 0:
                print("training on date range:", 
                      train_df["Date"].dt.date.min(), 
                      train_df["Date"].dt.date.max())
                y_pred = model.fit_predict(train_df, test_df)
                y_pred_df = test_df.assign(
                    y_pred=y_pred,
                    test_fold=i,
                )
                fold_pred_df_list.append(y_pred_df)
            train_df = pd.concat([train_df, test_df])
        self.fold_pred_df = pd.concat(fold_pred_df_list)
        return self.fold_pred_df
    
    def score_preds(self, score_fn_dict=None):
        if score_fn_dict is None:
            score_fn_dict = dict()
        score_fn_dict.update({
            "log_loss": lambda fold_df: log_loss(y_pred=fold_df["y_pred"], 
                                                 y_true=fold_df["targetWin"]),
            "accuracy_score": lambda fold_df: accuracy_score(y_pred=fold_df["y_pred"].round(), 
                                                             y_true=fold_df["targetWin"]),
            "ml_log_loss": lambda fold_df: log_loss(y_pred=fold_df["p_fighter_implied"], 
                                                    y_true=fold_df["targetWin"]),
        })
        metrics_df_list = []
        for i, fold_df in self.fold_pred_df.groupby("test_fold"):
            if len(fold_df) > 0:
                curr_metrics = {
                    "test_fold": i,
                    "min_test_date": fold_df["Date"].min(),
                    "max_test_date": fold_df["Date"].max(),
                    "n_test_days": fold_df["Date"].nunique(),
                    "n_test_fights": len(fold_df),
                }
                for score_nm, score_fn in score_fn_dict.items():
                    curr_metrics[score_nm] = score_fn(fold_df)
                metrics_df_list.append(curr_metrics)
        self.metrics_df = pd.DataFrame(metrics_df_list)
        return self.metrics_df
    

def naive_returns(fold_df):
    # payout in addition to wager
    f_payout = (1/fold_df["p_fighter"])
    o_payout = (1/fold_df["p_opponent"])
    # expected return is positive
    f_bet = fold_df["y_pred"] > fold_df["p_fighter"]
    o_bet = (1 - fold_df["y_pred"]) > fold_df["p_opponent"]
    f_won = test_df["targetWin"] == 1
    o_won = test_df["targetWin"] == 0
    
    f_gains = (f_bet * f_won * (f_payout - 1)) # gains over the initial wager
    o_gains = (o_bet * o_won * (o_payout - 1)) # gains over the initial wager
    f_losses = (-1 * f_bet * o_won)
    o_losses = (-1 * o_bet * f_won)
    return f_gains.sum() + o_gains.sum() + f_losses.sum() + o_losses.sum()

def eval_kelly(fold_df):
    y_pred = fold_df["y_pred"]
    # b is % of wager gained on a win (not counting original wager)
    b_fighter = (1/fold_df["p_fighter"]) - 1
    b_opponent = (1/fold_df["p_opponent"]) - 1
    
    kelly_bet_fighter = y_pred + ((y_pred - 1) / b_fighter)
    kelly_bet_opponent = (1 - y_pred) + ((1 - y_pred - 1) / b_opponent)
    kelly_bet_fighter = np.maximum(0, kelly_bet_fighter)
    kelly_bet_opponent = np.maximum(0, kelly_bet_opponent)
    
    f_won = fold_df["targetWin"] == 1
    o_won = fold_df["targetWin"] == 0
    
    fighter_return = (kelly_bet_fighter * b_fighter * f_won) - (kelly_bet_fighter * o_won)
    opponent_return = (kelly_bet_opponent * b_opponent * o_won) - (kelly_bet_opponent * f_won)
    
    total_returns = 1 + fighter_return + opponent_return
    return np.prod(total_returns) 
