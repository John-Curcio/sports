from model.base import BaseClassifier, BaseFeatureExtractor

from statsmodels.discrete.discrete_model import Logit
from sklearn.linear_model import LogisticRegression

class SkLogRegClassifier(LogisticRegression):
    """
    Yeah whatever lol. Just phoning this one in
    """

    def __init__(self, **sklearn_kwargs):
        self._model = LogisticRegression(fit_intercept=False, **sklearn_kwargs)

    def fit(self, X, y):
        self._model.fit(X, y)

    def predict(self, X):
        self._model.predict(X)

