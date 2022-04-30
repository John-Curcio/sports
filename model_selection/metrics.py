import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns 

from sklearn.metrics import log_loss, accuracy_score

def score_accuracy(model, test_df):
    y_pred = model.predict(test_df).round()
    y_true = test_df["targetWin"]
    return accuracy_score(y_pred=y_pred, y_true=y_true)

def score_log_loss(model, test_df):
    y_pred = model.predict(test_df)
    y_true = test_df["targetWin"]
    return log_loss(y_pred=y_pred, y_true=y_true)

def score_kelly_returns(model, test_df):
    y_pred = model.predict(test_df)
    KB = KellyBet(y_pred, test_df)
    total_return = KB.returns_df["total_return"].prod()
    return total_return

class KellyBet(object):
    """
    This is an unrealistic simulation
    But my model is profitable if this shows profit
    """

    def __init__(self, y_pred, test_df):
        # b is % of wager gained on a win (not counting original wager)
        # https://en.wikipedia.org/wiki/Kelly_criterion#Gambling_formula
        b_fighter = (1/test_df["p_fighter"]) - 1
        b_opponent = (1/test_df["p_opponent"]) - 1
        
        kelly_bet_fighter = y_pred + ((y_pred - 1) / b_fighter)
        kelly_bet_opponent = (1 - y_pred) + ((1 - y_pred - 1) / b_opponent)
        kelly_bet_fighter = np.maximum(0, kelly_bet_fighter)
        kelly_bet_opponent = np.maximum(0, kelly_bet_opponent)
        
        f_won = test_df["targetWin"] == 1
        o_won = test_df["targetWin"] == 0
        
        fighter_return = (kelly_bet_fighter * b_fighter * f_won) - \
            (kelly_bet_fighter * o_won)
        opponent_return = (kelly_bet_opponent * b_opponent * o_won) - \
            (kelly_bet_opponent * f_won)
        
        total_returns = 1 + fighter_return + opponent_return
        self.returns_df = test_df[["FighterID", "OpponentID", "Date"]].assign(
            p_fighter=y_pred,
            p_fighter_implied=test_df["p_fighter_implied"],
            b_fighter=b_fighter,
            b_opponent=b_opponent,
            kelly_bet_fighter=kelly_bet_fighter,
            kelly_bet_opponent=kelly_bet_opponent,
            fighter_won=f_won,
            opponent_won=o_won,
            total_return=total_returns,
        )

    def plot_diagnostics(self):
        returns_df = self.returns_df 

        plt.plot(returns_df.index, returns_df["total_return"])
        plt.title("Kelly returns over fights")
        plt.axhline(y=1.0)
        plt.xticks(rotation=45)
        plt.show()

        plt.plot(returns_df.index, returns_df["total_return"].cumprod())
        plt.title("Portfolio value over fights \
        (imaginary world where returns per fight compound)")
        plt.axhline(y=1.0)
        plt.xticks(rotation=45)
        plt.show()

        returns_df = returns_df.groupby("Date")["total_return"].prod().reset_index()
        returns_df["cum_return"] = returns_df["total_return"].cumprod()
        sns.lineplot(x="Date", y="cum_return", data=returns_df)
        plt.title("Portfolio value over time \
        (imaginary world where returns per fight compound)")
        plt.axhline(y=1.0)
        plt.xticks(rotation=45)
        plt.show()


    
        