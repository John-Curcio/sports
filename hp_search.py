import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from model.mma_features import PcaEloWrapper, BinaryEloWrapper, AccEloWrapper
from sklearn.metrics import log_loss
from model.mma_log_reg_stan import SimpleSymmetricModel


class HyperParamTester(object):

    def __init__(self, model):
        self.df = self.prep_dataset()
        self.model = model
        self.static_feat_df = self.get_static_feat_df()

    def test_hp_range(self, n_draws):
        results = []
        for _ in range(n_draws):
            pca_elo_alpha = np.random.uniform(0, 1)
            binary_elo_alpha = np.random.uniform(0, 1)
            acc_elo_alpha = np.random.uniform(0, 1)
            n_pca = np.random.choice(range(6, 20))
            print(f"""testing the following params:
            pca elo alpha: {pca_elo_alpha}
            bin elo alpha: {binary_elo_alpha}
            acc elo alpha: {acc_elo_alpha}
            n pca components: {n_pca}""")
            curr_result = self.test_hp(
                pca_elo_alpha, binary_elo_alpha, acc_elo_alpha, n_pca
            )
            print(curr_result)
            results.append(curr_result)
        return pd.DataFrame(results)
    
    def test_hp(self, pca_elo_alpha=0.5, binary_elo_alpha=0.5, acc_elo_alpha=0.5, n_pca=16):
        acc_elo_feat_df = self.get_acc_elo_df(acc_elo_alpha)
        pca_elo_feat_df = self.get_pca_elo_df(pca_elo_alpha, n_pca)
        bin_elo_feat_df = self.get_binary_elo_df(binary_elo_alpha)

        feat_df = self.static_feat_df.merge(
            pca_elo_feat_df, on="espn_fight_id", how="left",
        ).merge(
            bin_elo_feat_df, on="espn_fight_id", how="left"
        ).merge(
            acc_elo_feat_df, on="espn_fight_id", how="left"
        )

        feat_cols = [
            "log_height_diff", 
            "log_reach_diff", 
            "age_diff", 
            # "t_since_last_fight_log_diff", 
            "sqrt_n_career_fights_diff",
            "log_t_since_first_fight_diff",
            "min_weight_diff",
            # binary elo cols
            "pred_win_target_logit",
            "pred_fighter_finish_logit",
            # acc elo cols
            'pred_logit_p_SML_SMA_diff', 
            'pred_logit_p_TDL_TDA_diff',
            'pred_logit_p_KD_SSL_diff', 'pred_logit_p_TDS_TDL_diff',
        ] + [
            # pca elo cols
            f"pred_PC_{i}" for i in range(n_pca)
        ]
        
        misc_cols = [
            "espn_fight_id",
            "espn_fighter_id",
            "espn_opponent_id",
            "Date",
            "FighterResult",
            "win_target",
            "p_fighter_open_implied",
            "p_fighter_close_implied",
            "FighterOpen", 
            "OpponentOpen",
        ]

        feat_df = feat_df[misc_cols + feat_cols]
        result = self.eval_model(feat_df, feat_cols)
        result["pca_elo_alpha"] = pca_elo_alpha
        result["binary_elo_alpha"] = binary_elo_alpha
        result["acc_elo_alpha"] = acc_elo_alpha
        result["n_pca"] = n_pca
        return result

    def eval_model(self, feat_df, feat_cols):
        train_df = feat_df.dropna(subset=["win_target"])
        print(train_df[feat_cols].isnull().mean())
        y_hat = self.model.fit_predict(train_df, train_df, feat_cols=feat_cols)
        
        xce = log_loss(y_true=train_df["win_target"], y_pred=y_hat)

        k = len(feat_cols)
        log_likelihood = len(train_df) * xce * -1

        aikake = 2 * k - 2 * log_likelihood # xce is avg negative log likelihood
        return {
            "in_sample_xce": xce,
            "n_params": k,
            "aikake": aikake,
        }

    def get_pca_elo_df(self, elo_alpha, n_pca=16):
        pca_ew = PcaEloWrapper(n_pca=n_pca, target_cols=self.diff_cols, alpha=elo_alpha)
        pca_elo_feat_df = pca_ew.fit_transform_all(self.df)
        return pca_elo_feat_df

    def get_binary_elo_df(self, elo_alpha):
        elo_alphas = {col: elo_alpha for col in self.bin_cols}
        bin_ew = BinaryEloWrapper(elo_alphas)
        bin_elo_feat_df = bin_ew.fit_transform_all(self.df)
        return bin_elo_feat_df

    def get_acc_elo_df(self, elo_alpha):
        elo_alphas = {
            (landed_col, attempted_col): elo_alpha 
            for landed_col, attempted_col in zip(self.landed_cols, self.attempted_cols)
        }
        acc_ew = AccEloWrapper(elo_alphas)
        acc_elo_feat_df = acc_ew.fit_transform_all(self.df)
        print(acc_elo_feat_df.isnull().mean())
        return acc_elo_feat_df

    def get_static_feat_df(self):
        # pca_elo_feat_df.shape, df.shape
        feat_df = self.df.dropna(subset=["p_fighter_open_implied"]).copy()
        feat_df["log_height_diff"] = (
            np.log(feat_df["HeightInches"]) - np.log(feat_df["HeightInches_opp"])
        ).fillna(0) # if missing from either fighter, impute with 0
        feat_df["log_reach_diff"] = (
            np.log(feat_df["ReachInches"]) - np.log(feat_df["ReachInches_opp"])
        ).fillna(0) # if missing from either fighter, impute with 0
        feat_df["age_diff"] = (feat_df["DOB"] - feat_df["DOB_opp"]).dt.days.fillna(0)
        feat_df["t_since_last_fight_log_diff"] = (
            # if it's the first fight, impute with mean
            np.log(np.maximum(1, feat_df['t_since_prev_fight'].fillna(258))) - 
            np.log(np.maximum(1, feat_df['t_since_prev_fight_opp'].fillna(258)))
        )

        feat_df["log_t_since_first_fight_diff"] = (
            np.log(np.maximum(1, feat_df['t_since_first_fight'])) - 
            np.log(np.maximum(1, feat_df['t_since_first_fight_opp']))
        )
        feat_df["sqrt_n_career_fights_diff"] = (
            np.sqrt(feat_df["n_career_fights"]) - np.sqrt(feat_df["n_career_fights_opp"])
        )

        feat_df["min_weight_diff"] = (
            np.log(feat_df["min_weight"]) - np.log(feat_df["min_weight_opp"])
        ).fillna(0)
        return feat_df

    def prep_dataset(self):
        df = pd.read_csv("data/full_bfo_ufc_espn_data_clean.csv", parse_dates=["Date", "DOB", "DOB_opp"])
        stat_landed_cols = [
            'SCBL', 'SCHL', 'SCLL', 'SGBL', 'SGHL', 'SGLL', 'SDBL', 'SDHL', 'SDLL',
            'SHL', 'SBL', 'SLL', 'SDL', 'SCL', 'SGL', 'SSL', 'TSL', 'TDL',
        ]
        stat_failed_cols = [
            'SM_fail', 'SS_fail', 'TS_fail', 'TD_fail', 'SCB_fail', 'SCH_fail',
            'SCL_fail', 'SGB_fail', 'SGH_fail', 'SGL_fail', 'SDB_fail', 'SDH_fail',
            'SDL_fail', 'SH_fail', 'SB_fail', 'SL_fail', 'SD_fail', 'SC_fail',
            'SG_fail',
        ]
        misc_stat_cols = [
            'KD', 'RV', 'AD', 'ADTB', 'ADHG', 'ADTM', 'ADTS', 'ctrl_seconds', 
        ]
        diff_cols = []
        for stat_col in stat_landed_cols + stat_failed_cols + misc_stat_cols:
            diff_col = f"diff_sqrt_{stat_col}"
            df[diff_col] = np.sqrt(df[stat_col]) - np.sqrt(df[stat_col+"_opp"])
            diff_cols.append(diff_col)
        self.diff_cols = diff_cols

        # defining some binary features
        df["win_target"] = df["FighterResult"].replace({"W":1, "L":0, "D":np.nan})

        fight_finish = df["decision_clean"].isin(["submission", "tko/ko"]).replace({True:1, False:np.nan})
        df["fighter_finish"] = fight_finish * df["win_target"]
        self.bin_cols = ["win_target", "fighter_finish"]
        
        sm_finish = df["decision_clean"] == "submission"
        sm_landed_fighter  = (sm_finish & (df["FighterResult"] == "W")).astype(int)
        df["SML"] = sm_landed_fighter
        df["SMA"] = np.maximum(df["SM"], df["SML"])

        sm_landed_opponent = (sm_finish & (df["FighterResult"] == "L")).astype(int)
        df["SML_opp"] = sm_landed_opponent
        df["SMA_opp"] = np.maximum(df["SM_opp"], df["SML_opp"])
        self.landed_cols = ["SML", "TDL", "KD", "TDS"]
        self.attempted_cols = ["SMA", "TDA", "SSL", "TDL"]
        return df.query("Date <= '2021-01-01'")

if __name__ == "__main__":
    mod = SimpleSymmetricModel(feat_cols=None, target_col="win_target", 
                                p_fighter_implied_col="p_fighter_open_implied",
                                beta_prior_std=1.0, mcmc=False)
    hp_tester = HyperParamTester(mod)
    result_df = hp_tester.test_hp_range(n_draws=30)
    print(result_df.sort_values("in_sample_xce"))
    result_df.to_csv("data/hp_results.csv", index=False)