#%%
import pandas as pd
import numpy as np
from ..decorator import matrix_op

@matrix_op
def momentum(data: pd.DataFrame, lag_start: int, lag_end: int=0) -> pd.DataFrame:
    mom = np.log(data.shift(lag_end) / data.shift(lag_start))
    return mom


@matrix_op
def volatility(data: pd.DataFrame, window: int) -> pd.DataFrame:
    ret = np.log(data / data.shift(1))
    vol = ret.rolling(window).std()
    return vol
