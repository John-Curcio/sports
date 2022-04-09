"""
Okay uhhhh 

Need a fit and predict class I guess

Need something different for classification vs regression

Maybe focus on classification first
"""

from abc import ABC, abstractmethod


class BaseClassifier(ABC):

    @abstractmethod
    def fit(self, X, y):
        pass 

    @abstractmethod
    def predict(self, X):
        pass

class BaseFeatureExtractor(ABC):
    """
    Something for extracting rolling time-series features. eg how'd we do for the last game?

    I think because of the rolling dependences, we may demand a fit_predict method. 
    Extract all the features all at once. Idk wtf to do in the case of predicting an entire 
    bracket, I'll figure that out later
    """

    @abstractmethod
    def fit_predict(self, data, min_test_date):
        pass