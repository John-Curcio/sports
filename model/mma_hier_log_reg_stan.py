import pystan 
import numpy as np 
import pandas as pd 
from sklearn.decomposition import PCA
from model.mma_log_reg_stan import SimpleSymmetricModel, PcaSymmetricModel, logit, inv_logit

hier_code = """

data {
    int<lower=0> n;                     // number of data points in training data
    int<lower=0> n2;                    // number of data points in test data
    int<lower=1> d;                     // explanatory variable dimension
    int<lower=0,upper=1> y[n];          // response variable
    real<lower=0> beta_prior_std;       // prior scale on beta mean across groups
    real<lower=0> intra_group_std;      // prior scale on beta, std dev of group's beta around mean
    
    vector[n] is_m;      // 0 if woman, 1 if man
    vector[n2] is_m2;    // 0 if woman, 1 if man
    
    matrix[n, d] X;                     // explanatory variable
    vector[n] ml_logit;                   // logit of the opening money line

    matrix[n2, d] X2;                   // test data
    vector[n2] ml_logit2;                 // test data

}

parameters {
    vector[d] beta_m;
    vector[d] beta_w;
}

transformed parameters {
    vector[n] eta;
    vector[n2] eta2;
    eta = (
        ml_logit + 
        ((X * beta_m) .* is_m) + 
        ((X * beta_w) .* (1 - is_m))
    );      // linear predictor
    eta2 = (
        ml_logit2 + 
        ((X2 * beta_m) .* is_m2) + 
        ((X2 * beta_w) .* (1 - is_m2))
    );   // linear predictor for test data
}

model {
    beta_m ~ normal(0, beta_prior_std);
    beta_w ~ normal(beta_m, intra_group_std); // damn i hope this works

    y ~ bernoulli_logit(eta);
}

generated quantities {
    vector[n2] y_pred;
    
    y_pred = inv_logit(eta2);  // y values predicted for test data
}
"""

class HierPcaSymmetricModel(PcaSymmetricModel):    
    
    def __init__(self, feat_cols, beta_prior_std=0.1, intra_group_std=0.1, 
                 n_pca=8, mcmc=False, num_chains=4, num_samples=1000):
        super().__init__(feat_cols, beta_prior_std, n_pca, mcmc, num_chains, num_samples)
        self.intra_group_std = float(intra_group_std)
        self.code = hier_code
        
    def fit_predict(self, train_df, test_df, feat_cols=None):
        if self.stan_model is None:
            self._load_stan_model()
        if not feat_cols:
            feat_cols = self.feat_cols
        scale_ = np.sqrt((train_df[feat_cols]**2).mean(0))
        self.scale_ = scale_
        X_train = train_df[feat_cols] / scale_
        X_test = test_df[feat_cols] / scale_
        
        X_pca_train = self.pca.fit_transform(X_train)
        X_pca_test = self.pca.transform(X_test)

        y_train = train_df["targetWin"]
        y_test = test_df["targetWin"]

        ml_train = logit(train_df["p_fighter_implied"])
        ml_test = logit(test_df["p_fighter_implied"])
        
        is_m_train = train_df["gender"].map({"M":1, "W":0})
        is_m_test = test_df["gender"].map({"M":1, "W":0})
        
        data = {
            "n": train_df.shape[0],
            "n2": test_df.shape[0],
            "d": self.n_pca,
            "y": y_train.astype(int).values,
            "beta_prior_std": self.beta_prior_std,
            "intra_group_std": self.intra_group_std,
            "is_m": is_m_train.values,
            "is_m2": is_m_test.values,
            "X": X_pca_train,
            "ml_logit": ml_train.values,
            "X2": X_pca_test,
            "ml_logit2": ml_test.values,
        }
        if self.mcmc:
            fit = self._fit_mcmc(data)
            return fit["y_pred"].mean(0) 
        fit = self._fit_opt(data)
        return fit["y_pred"]