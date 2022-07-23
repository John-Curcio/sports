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

# def score_kelly_returns(model, test_df, max_bankroll_fraction=1):
#     y_pred = model.predict(test_df)
#     KB = KellyBet(y_pred, test_df, max_bankroll_fraction)
#     total_return = KB.returns_df["total_return"].prod()
#     return total_return


class MultiKellyPM(object):

    def __init__(self, pred_df, max_bankroll_fraction=1, groupby_col="Date",
        fighter_ml_col="FighterOpen", opponent_ml_col="OpponentOpen", parse_ml=True):
        """
        pred_df: pd.DataFrame with columns y_pred, Date, win_target, 
            `fighter_ml_col`, `opponent_ml_col`, espn_fight_id, espn_fighter_id, 
            espn_opponent_id
        max_bankroll_fraction: maximum % of bankroll to risk on any one fight
        fighter_ml_col: column of pred_df containing money line for fighter
        opponent_ml_col: column of pred_df containing money line for opponent
        """
        if parse_ml:
            self.pred_df = pred_df.assign(
                fighter_payout=self.get_payouts_from_moneylines(pred_df[fighter_ml_col]),
                opponent_payout=self.get_payouts_from_moneylines(pred_df[opponent_ml_col]),
            )
        else:
            self.pred_df = pred_df.rename(columns={
                fighter_ml_col: "fighter_payout",
                opponent_ml_col: "opponent_payout",
            })
        self.max_bankroll_fraction = max_bankroll_fraction
        self.groupby_col = groupby_col
        self.fight_return_df = None
        self.event_return_df = None

    @staticmethod
    def get_payouts_from_moneylines(ml_vec):
        is_fav = ml_vec < 0
        is_dog = ml_vec > 0
        payout = pd.Series(np.nan, index=ml_vec.index)
        # favorite: you have to bet X to get a $100 payout, so payout is 100 / X
        payout.loc[is_fav] = -100 / ml_vec.loc[is_fav]
        # underdog: you bet $100 to get X payout, so payout is X / 100
        payout.loc[is_dog] = ml_vec.loc[is_dog] / 100
        return payout 

    def get_all_returns(self):
        returns_df_list = []
        temp_pred_df = self.pred_df.dropna(subset=["fighter_payout", "opponent_payout"])
        for curr_date, grp in temp_pred_df.groupby(self.groupby_col):
            weight_df = self.get_portfolio_weights(grp)
            return_df = weight_df.assign(win_target=grp["win_target"])
            fighter_won = return_df["win_target"] == 1
            opponent_won = return_df["win_target"] == 0
            # calculating returns
            # in the case of a draw, money is returned, and return is 0
            return_df["fighter_return"] = (
                (return_df["fighter_bet"] * return_df["fighter_payout"] * fighter_won )
                - (return_df["fighter_bet"] * opponent_won)
            )
            return_df["opponent_return"] = (
                (return_df["opponent_bet"] * return_df["opponent_payout"] * opponent_won )
                - (return_df["opponent_bet"] * fighter_won)
            )
            return_df["return"] = (
                return_df["fighter_return"] + return_df["opponent_return"]
            )
            return_df["Date"] = curr_date
            returns_df_list.append(return_df)
        fight_return_df = pd.concat(returns_df_list).reset_index(drop=True)
        self.fight_return_df = fight_return_df
        self.event_return_df = fight_return_df.groupby(self.groupby_col)["return"].sum()
        return self.event_return_df.reset_index()

    def get_portfolio_weights(self, df):
        b_fighter = df["fighter_payout"].fillna(0)
        b_opponent = df["opponent_payout"].fillna(0)
        p_fighter = df["y_pred"]
        p_opponent = 1 - df["y_pred"]
        kelly_bet_fighter = np.maximum(0, p_fighter - (p_opponent / b_fighter))
        kelly_bet_opponent = np.maximum(0, p_opponent - (p_fighter / b_opponent))
        
        # i want to check that i'm never betting on both guys
        check_vec = (kelly_bet_fighter > 0) & (kelly_bet_opponent > 0)
        assert not check_vec.any()

        # size bets proportional to kelly criterion for one bet
        n_bets = (kelly_bet_fighter > 0).sum() + (kelly_bet_opponent > 0).sum()
        fighter_bet = np.minimum(self.max_bankroll_fraction, 
                                 kelly_bet_fighter / n_bets).fillna(0)
        opponent_bet = np.minimum(self.max_bankroll_fraction, 
                                  kelly_bet_opponent / n_bets).fillna(0)
        # again, check that i'm never betting on both guys
        assert fighter_bet.sum() + opponent_bet.sum() < 1
        return df[["espn_fight_id", self.groupby_col, "y_pred",
                   "espn_fighter_id", "espn_opponent_id", 
                   "fighter_payout", "opponent_payout"]].assign(
            fighter_bet = fighter_bet,
            opponent_bet = opponent_bet,
        )
    
    @staticmethod
    def plot_diagnostics(event_return_df, x_col="Date"):
        event_return_df["portfolio_value"] = (event_return_df["return"] + 1).cumprod()
        sns.lineplot(
            x=x_col, y="portfolio_value",
            data=event_return_df
        )
        avg_return = event_return_df["return"].mean()
        plt.title("Portfolio value over time - assume returns compound by %s\
\naverage return: %.4f"%(x_col, avg_return))
        plt.xticks(rotation=45)
        plt.show()


    
        