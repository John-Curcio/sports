import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import seaborn as sns 
from sklearn.metrics import log_loss, accuracy_score

class TimeSeriesCrossVal(object):
    # Just regular time series cross validation,
    # but ensures dates don't end up in multiple folds

    def __init__(self, n_splits=4, n_dates_per_fold=None, 
                 min_test_date=None, p_fighter_implied_col="p_fighter_implied",
                 test_date_col="Date"):
        self.n_splits = n_splits
        self.n_dates_per_fold = n_dates_per_fold
        self.min_test_date = min_test_date
        self.p_fighter_implied_col = p_fighter_implied_col
        self.test_date_col = test_date_col
        self.fold_pred_df = None

    def get_folds(self, df):
        dates = sorted(df[self.test_date_col].unique())
        n_dates_per_fold = self.n_dates_per_fold
        if self.min_test_date is None:
            self.min_test_date = min(dates)
        dates = [d for d in dates if d >= self.min_test_date]
        if self.n_dates_per_fold is None:
            n_dates_per_fold = len(dates) // (self.n_splits + 2)
        date_starts = dates[::n_dates_per_fold]
        date_ends = dates[n_dates_per_fold::n_dates_per_fold]
        date_ends.append(dates[-1] + pd.Timedelta(days=1))
        # date_ends[-1] = dates[-1] + pd.Timedelta(days=1) # make sure last fold ends on last date
        for date_start, date_end in zip(date_starts, date_ends):
            inds = (df[self.test_date_col] >= date_start) & (df[self.test_date_col] < date_end)
            yield df.loc[inds]
            
    def get_cross_val_preds(self, model, df):
        train_df = pd.DataFrame()
        if self.min_test_date is not None:
            train_df = df.loc[df[self.test_date_col] < self.min_test_date]
            df = df.loc[df[self.test_date_col] >= self.min_test_date]
        fold_pred_df_list = [] 
        for i, test_df in enumerate(self.get_folds(df)):
            if len(train_df) > 0:
                print("training on date range:", 
                      train_df[self.test_date_col].dt.date.min(), 
                      train_df[self.test_date_col].dt.date.max())
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
            "accuracy_score": lambda fold_df: accuracy_score(y_pred=fold_df["y_pred"].round(), 
                                                             y_true=fold_df["targetWin"]),
            "log_loss": lambda fold_df: log_loss(y_pred=fold_df["y_pred"], 
                                                 y_true=fold_df["targetWin"], labels=[0,1]),
            "ml_log_loss": lambda fold_df: log_loss(y_pred=fold_df[self.p_fighter_implied_col], 
                                                    y_true=fold_df["targetWin"], labels=[0,1]),
            "ml_accuracy_score": lambda fold_df: accuracy_score(y_pred=fold_df[self.p_fighter_implied_col].round(),
                                                                y_true=fold_df["targetWin"]),
            "naive_returns": naive_returns,
            "kelly_returns": eval_kelly,
        })
        metrics_df_list = []
        for i, fold_df in self.fold_pred_df.groupby("test_fold"):
            if len(fold_df) > 0:
                curr_metrics = {
                    "test_fold": i,
                    "min_test_date": fold_df[self.test_date_col].min(),
                    "max_test_date": fold_df[self.test_date_col].max(),
                    "n_test_days": fold_df[self.test_date_col].nunique(),
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
    f_won = fold_df["targetWin"] == 1
    o_won = fold_df["targetWin"] == 0
    
    f_gains = (f_bet * f_won * (f_payout - 1)) # gains over the initial wager
    o_gains = (o_bet * o_won * (o_payout - 1)) # gains over the initial wager
    f_losses = (-1 * f_bet * o_won)
    o_losses = (-1 * o_bet * f_won)
    net_gains = f_gains.sum() + o_gains.sum() + f_losses.sum() + o_losses.sum()
    net_bets = f_bet.sum() + o_bet.sum()
    if net_bets == 0:
        return 0
    return net_gains / net_bets

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
