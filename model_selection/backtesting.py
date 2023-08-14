import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns 
from abc import ABC, abstractmethod
from tqdm import tqdm

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
    def __init__(self, portfolio_manager, fighter_ml_col, opponent_ml_col):
        self.fighter_ml_col = fighter_ml_col
        self.opponent_ml_col = opponent_ml_col
        self.pm = portfolio_manager
        self.returns_df = None

    def simulate_trading(self, pred_df, bet_ts_col="Date", payout_ts_col="Date"):
        """
        pred_df: pd.DataFrame with columns y_pred, win_target, 
            `fighter_ml_col`, `opponent_ml_col`, fight_id, FighterID_espn, 
            OpponentID_espn, `bet_ts_col`, `payout_ts_col`, win_target
        bet_ts_col: column of pred_df containing ts for when bets are placed
        payout_ts_col: column of pred_df containing ts for when bets are paid out
        """
        pred_df = pred_df.drop_duplicates(subset=["fight_id"])
        self.pred_df = pred_df.assign(
            fighter_dec_odds=american_to_decimal_odds(pred_df[self.fighter_ml_col]),
            opponent_dec_odds=american_to_decimal_odds(pred_df[self.opponent_ml_col]),
        )
        # risk-free probabilities for each fighter. These should sum to >= 1
        self.pred_df = self.pred_df.assign(
            p_fighter_rf=1 / self.pred_df["fighter_dec_odds"],
            p_opponent_rf=1 / self.pred_df["opponent_dec_odds"],
        )
        self.bet_ts_col = bet_ts_col
        self.payout_ts_col = payout_ts_col
        self._simulate_trading()

    def _simulate_trading(self):
        """
        Simulates trading over all tses in `pred_df`.
        """
        returns_df_list = []
        ts_range = sorted(
            set(self.pred_df[self.bet_ts_col].unique()) |
            set(self.pred_df[self.payout_ts_col].unique())
        )
        for curr_ts in tqdm(ts_range):
            # TODO this block is extremely confusing because it's not
            # clear what the columns are for each df. Ought to enforce
            # some kind of standardization.

            # Get upcoming fights to bet on
            upcoming_fights_df = self._get_upcoming_fights(curr_ts)
            # Bet on upcoming fights
            self.pm.place_bets(upcoming_fights_df)
            # Get completed fights
            completed_fights_df = self._get_completed_fights(curr_ts)
            # Get portfolio weights
            stake_df = self.pm.get_stakes(completed_fights_df)
            # Get returns
            return_df = self._get_returns(completed_fights_df, stake_df)
            returns_df_list.append(return_df)
            # Update bankroll
            self.pm.update_bankroll_with_returns(return_df)
            # Add one more helpful column to return_df
            return_df["final_portfolio_value"] = self.pm.portfolio.portfolio_value
        self.returns_df = pd.concat(returns_df_list).reset_index(drop=True)
        return self.returns_df
    
    def _get_upcoming_fights(self, curr_ts):
        """
        Returns upcoming fights for `curr_ts`.
        Subset of pred_df
        """
        upcoming_fights_df = self.pred_df[
            (self.pred_df[self.bet_ts_col] == curr_ts) &
            (self.pred_df[self.payout_ts_col] >= curr_ts)
        ]
        return upcoming_fights_df
    
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
            "p_fighter_rf", "p_opponent_rf",
            "fighter_stake", "opponent_stake",
            "p_bankroll_fighter", "p_bankroll_opponent",
            "fighter_possible_collect", "opponent_possible_collect",
            "win_target", "y_pred",
            "fight_profit",
            "fighter_profit", "opponent_profit",
            # payout_ts_col and bet_ts_col could be the same
            *{self.payout_ts_col, self.bet_ts_col}
        ]]
    
    def plot_diagnostics(self):
        """
        Plot portfolio value over time.
        """
        return_df = self.returns_df
        x_col = self.payout_ts_col
        event_return_df = return_df.groupby(x_col)["final_portfolio_value"].max()\
            .reset_index()\
            .rename(columns={"final_portfolio_value": "portfolio_value"})
        sns.lineplot(
            x=x_col, y="portfolio_value",
            data=event_return_df
        )
        # geometric growth rate
        log_returns = np.log(event_return_df["portfolio_value"]).diff().dropna()
        geometric_avg_growth_rate = np.exp(log_returns.mean()) - 1
        std = log_returns.std()
        t_statistic = log_returns.mean() / (std / np.sqrt(len(log_returns)))
        plt.title("Portfolio value over time - assume returns compound by %s\
\ngeometric average return: %.4f \nstd of returns: %.4f \nt-statistic: %.4f"%(
            x_col, geometric_avg_growth_rate, std, t_statistic))
        plt.xticks(rotation=45)
        plt.show()

#         growth_rates = event_return_df["portfolio_value"].pct_change().dropna()
#         harmonic_avg_growth_rate = 1 / np.mean(1 / (1 + growth_rates))
#         harmonic_avg_return = harmonic_avg_growth_rate - 1
#         std = growth_rates.std()
#         t_statistic = harmonic_avg_return / (std / np.sqrt(len(growth_rates)))
#         plt.title("Portfolio value over time - assume returns compound by %s\
# \nharmonic average return: %.4f \nstd of returns: %.4f \nt-statistic: %.4f"%(
#             x_col, harmonic_avg_return, std, t_statistic))
#         plt.xticks(rotation=45)
#         plt.show()


class Portfolio(object):
    """
    Class for keeping track of the actual portfolio itself.
    Basically just a wrapper around a pandas dataframe, with 
    an extra attribute for cash holdings.
    Doesn't do any of the actual bet selection, nor does it track the
    portfolio holdings over time, just keeps track of
    the current state of the portfolio.
    """

    def __init__(self, initial_bankroll = 1):
        self.cash = initial_bankroll
        self.portfolio_value = initial_bankroll
        self.data = pd.DataFrame([], columns=[
            "fight_id", "FighterID_espn", "OpponentID_espn",
            "fighter_stake", "opponent_stake",
            "p_bankroll_fighter", "p_bankroll_opponent",
            "fighter_dec_odds", "opponent_dec_odds",
            "p_fighter_rf", "p_opponent_rf",
        ])

    def del_bet(self, fight_id, fighter_id, opponent_id):
        # drop row with fight_id, fighter_id, opponent_id
        self.data = self.data[
            (self.data["fight_id"] != fight_id) |
            (self.data["FighterID_espn"] != fighter_id) |
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
        assert self.data.shape[0] == prev_portfolio_shape[0] + df.shape[0]
        # update bankroll
        self.cash -= (df["fighter_stake"] + df["opponent_stake"]).sum()

    def update_bankroll_with_returns(self, return_df):
        """
        Update portfolio after a fight has been completed.
        """
        # get portfolio rows matching fight_id, fighter_id, opponent_id
        # by merging df with self.data
        return_df = return_df[["fight_id", "FighterID_espn", "OpponentID_espn",
                 "fighter_profit", "opponent_profit"]].merge(
            self.data,
            on=["fight_id", "FighterID_espn", "OpponentID_espn"],
            how="inner"
        )
        # update bankroll
        self.cash += return_df[["fighter_profit", "opponent_profit", 
                                "fighter_stake", "opponent_stake"]].sum().sum()
        self.portfolio_value += return_df[["fighter_profit", "opponent_profit"]].sum().sum()
        # delete row
        prev_portfolio_shape = self.data.shape
        for _, row in return_df.iterrows():
            fight_id = row["fight_id"]
            fighter_id = row["FighterID_espn"]
            opponent_id = row["OpponentID_espn"]
            self.del_bet(fight_id, fighter_id, opponent_id)
        # check that portfolio shape is correct. We should be deleting
        # fights that were in our portfolio that have now been paid out.
        if self.data.shape[0] != prev_portfolio_shape[0] - return_df.shape[0]:
            print("self.data.shape[0]: %s, prev_portfolio_shape[0]: %s, return_df.shape[0]: %s"%(
                self.data.shape[0], prev_portfolio_shape[0], return_df.shape[0]
            ))
            assert False


class PortfolioManager(ABC):
    """
    Abstract base class for portfolio managers. 
    Maintain a portfolio of bets, and update it as new bets are made
    and old bets are paid out.
    """
    def __init__(self, fighter_ml_col="FighterOpen", opponent_ml_col="OpponentOpen",
                 initial_bankroll=1):
        """
        fighter_ml_col: column of pred_df containing money line for fighter
        opponent_ml_col: column of pred_df containing money line for opponent
        """
        self.fighter_ml_col = fighter_ml_col
        self.opponent_ml_col = opponent_ml_col
        self.portfolio = Portfolio(initial_bankroll=initial_bankroll)

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
        upcoming_df["p_bankroll_fighter"] = upcoming_df["fighter_stake"] / self.portfolio.portfolio_value
        upcoming_df["p_bankroll_opponent"] = upcoming_df["opponent_stake"] / self.portfolio.portfolio_value
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

class MultiKellyPortfolioManager(PortfolioManager):

    def __init__(self, max_bankroll_fraction=1, *args, **kwargs):
        """
        max_bankroll_fraction: maximum % of bankroll to risk on any one fight
        """
        super().__init__(*args, **kwargs)
        self.max_bankroll_fraction = max_bankroll_fraction

    def calculate_bets(self, upcoming_df):
        """
        upcoming_df: pd.DataFrame with fight_id, FighterID_espn, OpponentID_espn,
            fighter_ml_col, opponent_ml_col, y_pred
        returns pd.DataFrame with fight_id, FighterID_espn, OpponentID_espn,
            fighter_stake, opponent_stake
        """
        p_fighter = upcoming_df["y_pred"]
        p_opponent = 1 - p_fighter
        b_fighter = upcoming_df["fighter_dec_odds"] - 1
        b_opponent = upcoming_df["opponent_dec_odds"] - 1
        kelly_bet_fighter = np.maximum(0, p_fighter - (p_opponent / b_fighter))
        kelly_bet_opponent = np.maximum(0, p_opponent - (p_fighter / b_opponent))

        # i want to check that i'm never betting on both guys
        check_vec = (kelly_bet_fighter > 0) & (kelly_bet_opponent > 0)
        # assert not check_vec.any()

        # size bets proportional to kelly criterion for one bet
        n_bets = (kelly_bet_fighter > 0).sum() + (kelly_bet_opponent > 0).sum()
        fighter_bet = np.minimum(self.max_bankroll_fraction, 
                                 kelly_bet_fighter / n_bets).fillna(0)
        opponent_bet = np.minimum(self.max_bankroll_fraction, 
                                  kelly_bet_opponent / n_bets).fillna(0)
        # again, check that i'm never betting on both guys
        assert fighter_bet.sum() + opponent_bet.sum() < 1
        return upcoming_df.assign(
            fighter_stake=fighter_bet * self.portfolio.cash,
            opponent_stake=opponent_bet * self.portfolio.cash,
        )



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
        plt.xticks(rotation=45)
        plt.show()


    
        