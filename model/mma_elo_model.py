import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm 
from sklearn.metrics import log_loss, accuracy_score

class EloModel(object):
    
    def __init__(self, k_factor=1):
        self.k_factor = k_factor
        self.elo_df = None
        self.fighter_elo_dict = dict()
        
    def _dedupe_fights(self, df):
        temp_df = df.assign(
            fighterA=df[["FighterID", "OpponentID"]].max(1),
            fighterB=df[["FighterID", "OpponentID"]].min(1),
        )
        return temp_df.drop_duplicates(["Date", "fighterA", "fighterB"])
        
    def fit(self, df):
        df = self._dedupe_fights(df)
        self.fighter_elo_dict = {fid:0.0 for fid in set(df["FighterID"]) | set(df["OpponentID"])}
        # okay this might be complicated by the duplication of rows...
        # whatever I think I'll deal with that later lol. 
        # well it should be taken care of in feature_diff_df. If each fight is represented exactly once 
        # then we're good
        elo_list = []
        for i, row in tqdm(df.sort_values("Date").iterrows()):
            old_fighter_elo = self.fighter_elo_dict[row["FighterID"]]
            old_opponent_elo = self.fighter_elo_dict[row["OpponentID"]]
            pred_p_fighter_wins = sigmoid(old_fighter_elo - old_opponent_elo)
            sign = {"W": 1, "L": -1, "D": 0}[row["FighterResult"]]
            new_fighter_elo =  old_fighter_elo  + (sign * self.k_factor * (1 - pred_p_fighter_wins))
            new_opponent_elo = old_opponent_elo - (sign * self.k_factor * (1 - pred_p_fighter_wins))
            elo_list.append({
                "fighter_id": row["FighterID"],
                "opponent_id": row["OpponentID"],
                "Date": row["Date"],
                "old_fighter_elo": old_fighter_elo,
                "old_opponent_elo": old_opponent_elo,
                "pred_p_fighter_wins": pred_p_fighter_wins,
                "fighter_result": row["FighterResult"],
                "new_fighter_elo": new_fighter_elo,
                "new_opponent_elo": new_opponent_elo,
            })
            self.fighter_elo_dict[row["FighterID"]] = new_fighter_elo
            self.fighter_elo_dict[row["OpponentID"]] = new_opponent_elo
        self.elo_df = pd.DataFrame(elo_list)
        
    def score(self):
        sub_df = self.elo_df.query("fighter_result != 'D'")
        y_true = (sub_df["fighter_result"] == 'W').astype(int)
        y_pred = sub_df["pred_p_fighter_wins"]
        xentropy_val = log_loss(y_true=y_true, y_pred=y_pred)
        print("binary cross-entropy: %.4f"%xentropy_val)
        
        acc = accuracy_score(y_true=y_true, y_pred=y_pred.round())
        print("acc: %.4f"%acc)
        return {"xentropy": xentropy_val, "acc": acc}
        
    def predict(self, test_df):
        # fights we haven't seen
        fighter_elo_vec  = test_df["FighterID"].apply(lambda x: self.fighter_elo_dict[x])
        opponent_elo_vec = test_df["OpponentID"].apply(lambda x: self.fighter_elo_dict[x])
        
        pred_p_fighter_wins = sigmoid(fighter_elo_vec - opponent_elo_vec)
        return test_df.assign(y_pred=pred_p_fighter_wins)
        