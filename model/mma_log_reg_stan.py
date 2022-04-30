import stan 
import numpy as np 
import pandas as pd 
from sklearn.decomposition import PCA

code = """

data {
    int<lower=0> n;                     // number of data points in training data
    int<lower=0> n2;                    // number of data points in test data
    int<lower=1> d;                     // explanatory variable dimension
    int<lower=0,upper=1> y[n];          // response variable
    real<lower=0> beta_prior_std;       // prior scale on beta

    matrix[n, d] X;                     // explanatory variable
    vector[n] ml_logit;                   // logit of the opening money line

    matrix[n2, d] X2;                   // test data
    vector[n2] ml_logit2;                 // test data

}

parameters {
    vector[d] beta;
}

transformed parameters {
    vector[n] eta;
    vector[n2] eta2;
    eta = ml_logit + (X * beta);      // linear predictor
    eta2 = ml_logit2 + (X2 * beta);   // linear predictor for test data
}

model {
    for(i in 1:d){
        beta[i] ~ normal(0, beta_prior_std);
        //beta[i] ~ cauchy(0, beta_prior_std); //prior for slopes following gelman 2008
    }

    // observation model
    y ~ bernoulli_logit(eta);
}

generated quantities {
    vector[n2] y_pred;
    y_pred = inv_logit(eta2);  // y values predicted for test data
}
"""

def logit(x):
    return np.log(x) - np.log(1-x)

def inv_logit(x):
    return 1 / (1 + np.exp(-x))

class SimpleSymmetricModel(object):

    def __init__(self, feat_cols, beta_prior_std=0.1, n_pca=8, num_chains=4, num_samples=1000):
        self.feat_cols = feat_cols
        self.beta_prior_std = float(beta_prior_std)
        self.code = code
        self.scale_ = None
        self.fit = None
        self.num_chains = num_chains
        self.num_samples = num_samples
        
    def _fit(self, data):
        posterior = stan.build(self.code, data=data, random_seed=1)
        fit = posterior.sample(num_chains=self.num_chains, num_samples=self.num_samples)
        self.fit = fit
        return fit
        
    def fit_predict(self, train_df, test_df, feat_cols=None):
        if not feat_cols:
            feat_cols = self.feat_cols
        scale_ = (train_df[feat_cols]**2).mean(0)
        self.scale_ = scale_
        X_train = train_df[feat_cols] / scale_
        X_test = test_df[feat_cols] / scale_

        y_train = train_df["targetWin"]
        y_test = test_df["targetWin"]

        ml_train = logit(train_df["p_fighter_implied"])
        ml_test = logit(test_df["p_fighter_implied"])
        
        data = {
            "n": train_df.shape[0],
            "n2": test_df.shape[0],
            "d": X_pca_train.shape[1],
            "y": y_train.astype(int).values,
            "beta_prior_std": self.beta_prior_std,
            "X": X_pca_train,
            "ml_logit": ml_train.values,
            "X2": X_pca_test,
            "ml_logit2": ml_test.values,
        }

        fit = self._fit(data)
        return fit["y_pred"].mean(1)

class PcaSymmetricModel(SimpleSymmetricModel):
    
    def __init__(self, feat_cols, beta_prior_std=0.1, n_pca=8, num_chains=4, num_samples=1000):
        super().__init__(feat_cols, beta_prior_std, num_chains, num_samples)
        self.n_pca = n_pca
        self.pca = PCA(n_components=n_pca, whiten=True)
        
    def fit_predict(self, train_df, test_df, feat_cols=None):
        if not feat_cols:
            feat_cols = self.feat_cols
        scale_ = (train_df[feat_cols]**2).mean(0)
        self.scale_ = scale_
        X_train = train_df[feat_cols] / scale_
        X_test = test_df[feat_cols] / scale_
        
        # pca happens here
        X_pca_train = self.pca.fit_transform(X_train)
        X_pca_test = self.pca.transform(X_test)

        y_train = train_df["targetWin"]
        y_test = test_df["targetWin"]

        ml_train = logit(train_df["p_fighter_implied"])
        ml_test = logit(test_df["p_fighter_implied"])
        
        data = {
            "n": train_df.shape[0],
            "n2": test_df.shape[0],
            "d": X_pca_train.shape[1],
            "y": y_train.astype(int).values,
            "beta_prior_std": self.beta_prior_std,
            "X": X_pca_train,
            "ml_logit": ml_train.values,
            "X2": X_pca_test,
            "ml_logit2": ml_test.values,
        }

        fit = self._fit(data)
        return inv_logit(fit["eta2"]).mean(1)
        #return fit["y_pred"].mean(1)

