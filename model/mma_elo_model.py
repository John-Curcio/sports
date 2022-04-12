import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm 
from sklearn.metrics import log_loss, accuracy_score, mean_squared_error, mean_absolute_error

def sigmoid(x):
    return 1/(1+np.exp(-x))

def inv_sigmoid(x): # just log odds
    return np.log(x) - np.log(1 - x)

unknown_fighter_id = "2557037/unknown-fighter"

class EwmaPowers(object):
    
    def __init__(self, target_col, alpha=0.5, unknown_fighter_elo=-1):
        self.target_col = target_col
        self.alpha = alpha
        self.elo_df = None
        self.deduped_df = None
        self.fighter_elo_df = dict()
        self.unknown_fighter_elo = unknown_fighter_elo
        
    def _dedupe_fights(self, df):
        temp_df = df.assign(
            fighterA = df[["FighterID", "OpponentID"]].max(1),
            fighterB = df[["FighterID", "OpponentID"]].min(1),
            fighter_is_A = df["FighterID"] > df["OpponentID"],
        )
        return temp_df.drop_duplicates(subset=["Date", "fighterA", "fighterB"]) \
                      .sort_values(["Date", "fighterA", "fighterB"])
    
    def fit(self, df):
        df = self._dedupe_fights(df)
        self.deduped_df = df
        self.fighter_elo_dict = {fid:0.0 for fid in set(df["FighterID"]) | set(df["OpponentID"])}
        self.fighter_elo_dict[unknown_fighter_id] = self.unknown_fighter_elo
        elo_list = []
        for i, row in tqdm(df.iterrows()):
            old_A_elo = self.fighter_elo_dict[row["FighterID"]]
            old_B_elo = self.fighter_elo_dict[row["OpponentID"]]
            # treat it as if it's continuous
            y_A_B = row[self.target_col]
            y_hat_A_B = old_A_elo - old_B_elo
            delta = self.alpha * (y_A_B - y_hat_A_B) / 2
            new_A_elo = old_A_elo + delta
            new_B_elo = old_B_elo - delta
            if row["FighterID"] == unknown_fighter_id:
                new_A_elo = self.unknown_fighter_elo
            if row["OpponentID"] == unknown_fighter_id:
                new_B_elo = self.unknown_fighter_elo
            elo_list.append({
                "FighterID": row["FighterID"],
                "OpponentID": row["OpponentID"],
                "oldFighterElo": old_A_elo,
                "oldOpponentElo": old_B_elo,
                "predTarget": y_hat_A_B,
                "target": row[self.target_col],
                "Date": row["Date"],
                "newFighterElo": new_A_elo,
                "newOpponentElo": new_B_elo,
            })
            self.fighter_elo_dict[row["FighterID"]] = new_A_elo
            self.fighter_elo_dict[row["OpponentID"]] = new_B_elo
        self.elo_df = pd.DataFrame(elo_list)
        
    def score(self):
        mse = mean_squared_error(self.elo_df["predTarget"], self.elo_df["target"])
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(self.elo_df["predTarget"], self.elo_df["target"])
        corr = np.corrcoef(self.elo_df["predTarget"], self.elo_df["target"])[0,1]
        print("rmse: %.4f   mae: %.4f   corr: %.4f"%(rmse, mae, corr))
        return {"alpha":self.alpha, "rmse": rmse, "mae": mae, "corr": corr}

class LogisticEwmaPowers(EwmaPowers):
    ### This requires binary target!! 
    ### I don't know what to do with draws yet, or at least how to handle it in the score fn...
    
    def fit(self, df):
        df = self._dedupe_fights(df)
        self.deduped_df = df
        self.fighter_elo_dict = {fid:0.0 for fid in set(df["FighterID"]) | set(df["OpponentID"])}
        self.fighter_elo_dict[unknown_fighter_id] = self.unknown_fighter_elo
        elo_list = []
        for i, row in tqdm(df.iterrows()):
            old_A_elo = self.fighter_elo_dict[row["FighterID"]]
            old_B_elo = self.fighter_elo_dict[row["OpponentID"]]
            # treat it as if it's continuous
            y_A_B = row[self.target_col]
            y_hat_A_B = sigmoid(old_A_elo - old_B_elo)
            y_hat_A_B_new = self.alpha * y_A_B + (1 - self.alpha) * y_hat_A_B
            delta = inv_sigmoid(y_hat_A_B_new) - (old_A_elo - old_B_elo)
            delta /= 2
            new_A_elo = old_A_elo + delta
            new_B_elo = old_B_elo - delta
            if row["FighterID"] == unknown_fighter_id:
                new_A_elo = self.unknown_fighter_elo
            if row["OpponentID"] == unknown_fighter_id:
                new_B_elo = self.unknown_fighter_elo
            elo_list.append({
                "FighterID": row["FighterID"],
                "OpponentID": row["OpponentID"],
                "oldFighterElo": old_A_elo,
                "oldOpponentElo": old_B_elo,
                "predTarget": y_hat_A_B,
                "target": row[self.target_col],
                "Date": row["Date"],
                "newFighterElo": new_A_elo,
                "newOpponentElo": new_B_elo,
            })
            self.fighter_elo_dict[row["FighterID"]] = new_A_elo
            self.fighter_elo_dict[row["OpponentID"]] = new_B_elo
        self.elo_df = pd.DataFrame(elo_list)
        
    def score(self):
        acc = accuracy_score((self.elo_df["predTarget"] >= 0.5).astype(int), self.elo_df["target"])
        loss = log_loss(y_pred=self.elo_df["predTarget"], y_true=self.elo_df["target"])
        print("acc: %.4f    xentropy: %.4f"%(acc, loss))
        return {"alpha": self.alpha, "acc": acc, "xentropy": loss}

class AlphaGridSearch(object):
    
    def __init__(self, df, target_col, binary_target=False):
        self.df = df
        self.target_col = target_col
        self.binary_target = binary_target
        self.metric_df = None
        
    def grid_search(self, alpha_range=np.arange(0.1, 0.9, 0.1), plot=True):
        metric_list = []
        ep = None
        for alpha in alpha_range:
            if self.binary_target:
                ep = LogisticEwmaPowers(target_col=self.target_col, alpha=alpha)
            else:
                ep = EwmaPowers(target_col=self.target_col, alpha=alpha)
            ep.fit(self.df)
            curr_metric = ep.score()
            metric_list.append(curr_metric)
        self.metric_df = pd.DataFrame(metric_list)
        if plot:
            for metric_col in sorted(set(self.metric_df.columns) - {"alpha"}):
                sns.lineplot(x="alpha", y=metric_col, data=self.metric_df, label=metric_col)
            plt.title("metrics for elo/ewma power forecast on %s"%self.target_col)
            plt.legend()
            plt.show()
        return self.metric_df
    
if __name__ == "__main__":
    stats_df = pd.read_csv("data/full_stats_df.csv")
    ssl_df = stats_df.loc[~stats_df["SSL_diff_per_sec"].isnull()]
    ep = EwmaPowers(target_col="SSL_diff_per_sec", alpha=0.16)
    ep.fit(ssl_df)
    print("ep.alpha: %.4f   ep.target_col: %s"%(ep.alpha, ep.target_col))
    ep.score()

    result_df = stats_df.copy()
    result_df["target"] = (result_df["FighterResult"] == "W").astype(int)
    result_df = result_df.loc[result_df["FighterResult"] != "D"]
    ep = LogisticEwmaPowers(target_col="target", alpha=0.16)
    ep.fit(result_df)
    print("ep.alpha: %.4f   ep.target_col: %s"%(ep.alpha, ep.target_col))
    ep.score()

    AGS = AlphaGridSearch(ssl_df, "SSL_diff_per_sec", binary_target=False)
    AGS.grid_search(plot=True)

    AGS = AlphaGridSearch(result_df, "target", True)
    AGS.grid_search(plot=True)