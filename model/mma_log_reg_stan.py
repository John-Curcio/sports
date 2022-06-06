import pystan 
import pickle
import hashlib
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


class BaseStanModel(object):
    
    def _load_stan_model(self):
        # persistent hash: https://stackoverflow.com/a/2511075
        code_hash = int(hashlib.md5(self.code.encode('ascii')).hexdigest(), 16)
        path = f"stan_builds/{code_hash}.pkl"
        try:
            with open(path, 'rb') as f:
                self.stan_model = pickle.load(f)
        except:
            print(f"couldn't pickle.load from path {path}, so we'll compile and dump there")
            self.stan_model = pystan.StanModel(model_code=self.code)
            with open(path, 'wb') as f:
                pickle.dump(self.stan_model, f)
                
    def _fit_mcmc(self, data):
        self.fit = self.stan_model.sampling(data=data, iter=self.num_samples, 
                                       chains=self.num_chains)
        return self.fit
    
    def _fit_opt(self, data):
        self.fit = self.stan_model.optimizing(data=data)
        return self.fit


class SimpleSymmetricModel(BaseStanModel):
        
    def __init__(self, feat_cols, beta_prior_std=0.1, mcmc=False, num_chains=4, num_samples=1000):
        self.feat_cols = feat_cols
        self.beta_prior_std = float(beta_prior_std)
        self.code = code
        self.scale_ = None
        self.fit = None
        self.mcmc = mcmc
        self.num_chains = num_chains
        self.num_samples = num_samples
        self.stan_model = None
        
    def fit_predict(self, train_df, test_df, feat_cols=None):
        if not feat_cols:
            feat_cols = self.feat_cols
        if self.stan_model is None:
            self._load_stan_model()
        # If there are PCA feats, I might as well include them in the scaling
        scale_ = np.sqrt((train_df[feat_cols]**2).mean(0))
        self.scale_ = scale_
        X_train = train_df[feat_cols] / scale_
        X_test = test_df[feat_cols] / scale_

        y_train = train_df["targetWin"]

        ml_train = logit(train_df["p_fighter_implied"])
        ml_test = logit(test_df["p_fighter_implied"])
        
        X_ml_train = np.concatenate([X_train, ml_train.values.reshape(-1,1)], axis=1)
        X_ml_test = np.concatenate([X_test, ml_test.values.reshape(-1,1)], axis=1)
        
        data = {
            "n": train_df.shape[0],
            "n2": test_df.shape[0],
            "d": X_ml_train.shape[1],
            "y": y_train.astype(int).values,
            "beta_prior_std": self.beta_prior_std,
            "X": X_ml_train,
            "ml_logit": ml_train.values,
            "X2": X_ml_test,
            "ml_logit2": ml_test.values,
        }
        if self.mcmc:
            fit = self._fit_mcmc(data)
            return fit["y_pred"].mean(0) 
        fit = self._fit_opt(data)
        return fit["y_pred"]