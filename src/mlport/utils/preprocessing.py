import pandas as pd
import numpy as np
import copy

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler


class CrossSectionalScaler(BaseEstimator,TransformerMixin):
    '''
    Stock cross sectional standardization for each date
    '''
    def __init__(self, scaler=StandardScaler(),num_threads=None):
        self.num_threads = num_threads
        self.scaler = scaler
        
    def fit(self, X,y=None):
        return self
        
    def transform(self, X,y=None):
        result = self.run(X,X.index.get_level_values('date').unique(),
                        scaler=self.scaler)
        self.is_fitted_ = True
        return result

    def run(self,X,molecule,scaler=StandardScaler(),index_col = ['stock','date']):
        current_X = X.loc[X.index.get_level_values('date').isin(molecule)].copy()
        for date in current_X.index.get_level_values('date').unique():
            today_data = current_X.xs(date,level='date',drop_level=False)
            current_X.loc[today_data.index,:] = scaler.fit_transform(today_data)
        return current_X