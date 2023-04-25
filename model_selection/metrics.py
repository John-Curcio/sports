import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns 
from abc import ABC, abstractmethod

from sklearn.metrics import log_loss, accuracy_score

def score_accuracy(model, test_df):
    y_pred = model.predict(test_df).round()
    y_true = test_df["targetWin"]
    return accuracy_score(y_pred=y_pred, y_true=y_true)

def score_log_loss(model, test_df):
    y_pred = model.predict(test_df)
    y_true = test_df["targetWin"]
    return log_loss(y_pred=y_pred, y_true=y_true)

def american_to_decimal_odds(ml_vec: pd.Series) -> pd.Series:
    """
    Decimal odds are shown as one number, which is the amount a 
    winning bet would collect on a $1 bet. If the odds are listed 
    as 6, a winning bet would receive $5 profit and the original 
    $1 bet. Anything between 1 and 2 is a favorite bet and 2 
    is an even money bet.
    
    ml_vec: pd.Series of money lines in American odds
    """
    ml_vec = ml_vec.astype(float)
    is_fav = ml_vec < 0
    is_dog = ml_vec > 0
    payout = pd.Series(np.nan, index=ml_vec.index)
    # favorite: you have to bet X to get a $100 payout, so payout is (100 / X) + 1
    payout.loc[is_fav] = (-100 / ml_vec.loc[is_fav]) + 1
    # underdog: you bet $100 to get X payout, so payout is (X / 100) + 1
    payout.loc[is_dog] = (ml_vec.loc[is_dog] / 100) + 1
    return payout


class TradingSimulator(object):
    """
    So TradingSimulator iterates over ts_col and passes upcoming 
    fights to PortfolioManager, then calculates returns from completed 
    fights from PortfolioManager's allocations. Record returns and 
    increment PortfolioManager's bankroll accordingly.
    """
    def __init__(self, PortfolioManagerClass, fighter_ml_col, opponent_ml_col):
        self.fighter_ml_col = fighter_ml_col
        self.opponent_ml_col = opponent_ml_col
        self.pm = PortfolioManagerClass(fighter_ml_col=fighter_ml_col, opponent_ml_col=opponent_ml_col)
        self.returns_df = None

    def simulate_trading(self, pred_df, bet_ts_col="Date", payout_ts_col="Date"):
        """
        pred_df: pd.DataFrame with columns y_pred, win_target, 
            `fighter_ml_col`, `opponent_ml_col`, fight_id, FighterID_espn, 
            OpponentID_espn, `bet_ts_col`, `payout_ts_col`
        bet_ts_col: column of pred_df containing ts for when bets are placed
        payout_ts_col: column of pred_df containing ts for when bets are paid out
        """
        self.pred_df = pred_df.assign(
            fighter_dec_odds=american_to_decimal_odds(pred_df[self.fighter_ml_col]),
            opponent_dec_odds=american_to_decimal_odds(pred_df[self.opponent_ml_col]),
        )
        self.bet_ts_col = bet_ts_col
        self.payout_ts_col = payout_ts_col
        self._simulate_trading()

    def _simulate_trading(self):
        """
        Simulates trading over all tses in `pred_df`.
        """
        returns_df_list = []
        ts_range = pd.date_range(
            start=self.pred_df[self.bet_ts_col].min(),
            end=self.pred_df[self.payout_ts_col].max(),
        )
        for curr_ts in ts_range:
            # TODO this block is extremely confusing because it's not
            # clear what the columns are for each df. Ought to enforce
            # some kind of standardization.

            # Get upcoming fights to bet on
            upcoming_df = self._get_upcoming_fights(curr_ts)
            # Bet on upcoming fights
            self.pm.place_bets(upcoming_df)
            # Get completed fights
            completed_fights = self._get_completed_fights(curr_ts)
            # Get portfolio weights
            stake_df = self.pm.get_stakes(completed_fights)
            # Get returns
            return_df = self._get_returns(completed_fights, stake_df)
            returns_df_list.append(return_df)
            # Update bankroll
            self.pm.update_bankroll_with_returns(return_df)
        self.returns_df = pd.concat(returns_df_list).reset_index(drop=True)
    
    def _get_upcoming_fights(self, curr_ts):
        """
        Returns upcoming fights for `curr_ts`.
        Subset of pred_df
        """
        upcoming_df = self.pred_df[
            (self.pred_df[self.bet_ts_col] == curr_ts) &
            (self.pred_df[self.payout_ts_col] >= curr_ts)
        ]
        return upcoming_df
    
    def _get_completed_fights(self, curr_ts):
        """
        Returns completed fights for `curr_ts`.
        Subset of pred_df
        """
        completed_df = self.pred_df[
            (self.pred_df[self.payout_ts_col] == curr_ts)
        ]
        return completed_df
    
    def _get_returns(self, completed_df, stake_df):
        """
        Returns returns for `completed_df` with `stake_df`.
        completed_df: pd.DataFrame with columns y_pred, win_target,
            `fighter_ml_col`, `opponent_ml_col`, fight_id, FighterID_espn,
            OpponentID_espn, `bet_ts_col`, `payout_ts_col`
        stake_df: pd.DataFrame with columns fight_id, FighterID_espn,
            OpponentID_espn, fighter_stake, opponent_stake
        """
        # Get returns for completed fights
        completed_df = completed_df.merge(
            stake_df[["fight_id", "FighterID_espn", "OpponentID_espn", 
                      "fighter_stake", "opponent_stake",
                      "p_bankroll_fighter", "p_bankroll_opponent"]], 
            on=["fight_id", "FighterID_espn", "OpponentID_espn"]
        )
        # possible collect: how much you get back if you bet $1, including stake
        completed_df["fighter_possible_collect"] = (
            completed_df["fighter_dec_odds"] * completed_df["fighter_stake"]
        )
        completed_df["opponent_possible_collect"] = (
            completed_df["opponent_dec_odds"] * completed_df["opponent_stake"]
        )
        # calculate profit: amount collected, minus stake
        # if win_target is null, then assume fight was a draw, and profit is 0
        completed_df["fighter_profit"] = (
            completed_df["fighter_possible_collect"] * completed_df["win_target"]
            - completed_df["fighter_stake"]
        ).fillna(0)
        completed_df["opponent_profit"] = (
            completed_df["opponent_possible_collect"] * (1 - completed_df["win_target"])
            - completed_df["opponent_stake"]
        ).fillna(0)
        completed_df["fight_profit"] = (
            completed_df["fighter_profit"] + completed_df["opponent_profit"]
        )
        return completed_df[[
            "fight_id", "FighterID_espn", "OpponentID_espn", 
            self.fighter_ml_col, self.opponent_ml_col,
            "fighter_dec_odds", "opponent_dec_odds",
            "fighter_stake", "opponent_stake",
            "p_bankroll_fighter", "p_bankroll_opponent",
            "fighter_possible_collect", "opponent_possible_collect",
            "win_target", "y_pred",
            "fight_profit",
            "fighter_profit", "opponent_profit",
            # payout_ts_col and bet_ts_col could be the same
            *{self.payout_ts_col, self.bet_ts_col}
        ]]


class Portfolio(object):
    """
    Class for keeping track of the actual portfolio itself.
    Basically just a wrapper around a pandas dataframe, with 
    an extra attribute for cash holdings
    """

    def __init__(self, initial_bankroll = 1):
        self.cash = initial_bankroll
        self.data = pd.DataFrame([], columns=[
            "fight_id", "FighterID_espn", "OpponentID_espn",
            "fighter_stake", "opponent_stake",
            "p_bankroll_fighter", "p_bankroll_opponent",
            "fighter_dec_odds", "opponent_dec_odds",
        ])

    def add_bet(self, row):
        """
        row: pd.Series or dict with columns fight_id, FighterID_espn, OpponentID_espn,
            fighter_stake, opponent_stake, fighter_dec_odds, opponent_dec_odds
        """
        self.data = self.data.append(row, ignore_index=True)
        self.cash -= row["fighter_stake"] + row["opponent_stake"]

    def del_bet(self, fight_id, fighter_id, opponent_id):
        # drop row with fight_id, fighter_id, opponent_id
        self.data = self.data[
            (self.data["fight_id"] != fight_id) &
            (self.data["FighterID_espn"] != fighter_id) &
            (self.data["OpponentID_espn"] != opponent_id)
        ]

    def update_portfolio_with_bets(self, df):
        """
        Update portfolio after bets have been placed.
        """
        # get portfolio rows matching fight_id, fighter_id, opponent_id
        # by merging df with self.data
        prev_portfolio_shape = self.data.shape
        self.data = pd.concat([
            self.data,
            df[self.data.columns],
        ]).reset_index(drop=True)
        assert self.data["fight_id"].nunique() == self.data.shape[0]
        # self.data = df[["fight_id", "FighterID_espn", "OpponentID_espn",
        #          "fighter_stake", "opponent_stake",
        #          "fighter_dec_odds", "opponent_dec_odds"]].merge(
        #     self.data,
        #     on=["fight_id", "FighterID_espn", "OpponentID_espn"],
        #     how="outer"
        # )
        assert self.data.shape[0] == prev_portfolio_shape[0] + df.shape[0]
        # update bankroll
        self.cash -= (df["fighter_stake"] + df["opponent_stake"]).sum()

    def update_bankroll_with_returns(self, df):
        """
        Update portfolio after a fight has been completed.
        """
        # get portfolio rows matching fight_id, fighter_id, opponent_id
        # by merging df with self.data
        df = df[["fight_id", "FighterID_espn", "OpponentID_espn",
                #  "fighter_dec_odds", "opponent_dec_odds",
                 "win_target"]].merge(
            self.data,
            on=["fight_id", "FighterID_espn", "OpponentID_espn"],
            how="inner"
        )
        # calculate profit
        fighter_profit = df["fighter_stake"] * df["fighter_dec_odds"] * df["win_target"]
        opponent_profit = df["opponent_stake"] * df["opponent_dec_odds"] * (1 - df["win_target"])
        # update bankroll
        self.cash += (fighter_profit + opponent_profit).sum()
        # delete row
        prev_portfolio_shape = self.data.shape
        for _, row in df.iterrows():
            fight_id = row["fight_id"]
            fighter_id = row["FighterID_espn"]
            opponent_id = row["OpponentID_espn"]
            self.del_bet(fight_id, fighter_id, opponent_id)
        assert self.data.shape[0] == prev_portfolio_shape[0] - df.shape[0]


class PortfolioManager(ABC):
    """
    Abstract base class for portfolio managers. 
    Maintain a portfolio of bets, and update it as new bets are made
    and old bets are paid out.
    """
    def __init__(self, fighter_ml_col="FighterOpen", opponent_ml_col="OpponentOpen"):
        """
        fighter_ml_col: column of pred_df containing money line for fighter
        opponent_ml_col: column of pred_df containing money line for opponent
        """
        self.fighter_ml_col = fighter_ml_col
        self.opponent_ml_col = opponent_ml_col
        self.portfolio = Portfolio()

    @abstractmethod
    def calculate_bets(self, upcoming_df):
        """
        upcoming_df: pd.DataFrame with fight_id, FighterID_espn, OpponentID_espn,
            fighter_ml_col, opponent_ml_col, y_pred
        returns pd.DataFrame with fight_id, FighterID_espn, OpponentID_espn,
            fighter_stake, opponent_stake
        """
        raise NotImplementedError()

    def place_bets(self, upcoming_df):
        # set columns fighter_stake, opponent_stake,
        # p_bankroll_fighter, p_bankroll_opponent
        upcoming_df = self.calculate_bets(upcoming_df)
        upcoming_df["p_bankroll_fighter"] = upcoming_df["fighter_stake"] / self.portfolio.cash
        upcoming_df["p_bankroll_opponent"] = upcoming_df["opponent_stake"] / self.portfolio.cash
        self.portfolio.update_portfolio_with_bets(upcoming_df)

    def get_stakes(self, df=None):
        """
        df: pd.DataFrame with fight_id, FighterID_espn, OpponentID_espn
        returns pd.DataFrame with fight_id, FighterID_espn, OpponentID_espn,
            fighter_stake, opponent_stake
        """
        result = self.portfolio.data.merge(
            df[["fight_id", "FighterID_espn", "OpponentID_espn"]],
            on=["fight_id", "FighterID_espn", "OpponentID_espn"],
            how="inner"
        )
        assert result.shape[0] == df.shape[0], (result.shape, df.shape)
        return result 

    def update_bankroll_with_returns(self, df):
        self.portfolio.update_bankroll_with_returns(df)


class MultiKellyPM(object):

    def __init__(self, pred_df, max_bankroll_fraction=1, groupby_col="Date",
        fighter_ml_col="FighterOpen", opponent_ml_col="OpponentOpen", parse_ml=True):
        """
        pred_df: pd.DataFrame with columns y_pred, Date, win_target, 
            `fighter_ml_col`, `opponent_ml_col`, fight_id, FighterID_espn, 
            OpponentID_espn
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

    def get_portfolio_weights(self, df=None):
        if df is None:
            df = self.pred_df
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
        return df[["fight_id", self.groupby_col, "y_pred",
                   "FighterID_espn", "OpponentID_espn", 
                   "fighter_payout", "opponent_payout"]].assign(
            fighter_kelly_bet = kelly_bet_fighter,
            opponent_kelly_bet = kelly_bet_opponent,
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
        plt.xs(rotation=45)
        plt.show()


    
        