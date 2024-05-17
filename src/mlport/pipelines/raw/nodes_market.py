#%%
import pandas as pd
import yfinance as yf
import numpy as np
from dateutil.relativedelta import relativedelta
import datetime as dt
import os
from ..decorator import matrix_op

def update_index_values(dropna: bool = True):
    """
    Get JCI data from Yahoo Finance
    """
    start_date = (dt.date.today() - relativedelta(year=10)).strftime('%Y-%m-%d')
    data = yf.download(tickers='^JKSE', 
                       start=start_date,
                       interval="1d") 
    data = data[['Adj Close']]
    data = data.reset_index()
    data.columns = ['date','index_values_jci']  
    data = data.set_index('date')
    return data


def update_date_list(data: pd.DataFrame, dropna: bool = True):
    """
    Get trading date data
    """
    dates = data.reset_index()[['date']]
    return dates


def get_market_data(stock_listed: pd.DataFrame) -> pd.DataFrame:
    
    # Get data
    tickers = (stock_listed['stock'] + '.JK').unique().tolist()#[:5]
    start_date = stock_listed.loc[0,'date']
    end_date = stock_listed.loc[:,'date'].iloc[-1]
    data = yf.download(tickers=tickers, start=start_date, end=end_date, interval="1d")

    # Reformat
    data.index = data.reset_index()['Date']
    data.index = data.index.rename('date')
    data.columns = data.columns.set_levels(data.columns.levels[1].str.replace('.JK', ''), level=1)
    data.columns = data.columns.rename(['feature','stock'])
    data.columns = data.columns.set_levels(['adj_close', 'close', 'high', 'low', 'open', 'volume'],level=0)
    
    # Impute Volume
    data.loc[:,pd.IndexSlice['volume',:]] = data.loc[:,pd.IndexSlice['volume',:]].fillna(0)
    
    # Validation
    data = drop_outliers(data)
    
    # Impute Price
    data = data.ffill()
    
    # Melt
    data = data.unstack().unstack(level=0)
    data.index = data.index.rename(['stock', 'date'])
    data = data.swaplevel(0, 1, 0).sort_index()
    
    return data


def drop_outliers(data):
    result = data.swaplevel(axis=1)
    ref = result.xs('adj_close', level=1, axis=1)
    start_id = 0
    end_id = 240
    while start_id < ref.index.shape[0]:
        
        if end_id > ref.index.shape[0]:
            start_id = ref.index.shape[0] - 240
        
        temp = ref.iloc[start_id:end_id]
        mean = temp.mean()
        std = temp.std()
        outliers = (temp < mean-5*std) | (temp>mean+5*std)
        outlier_stocks = outliers.columns[outliers.sum(0) > 0]
        for stock in outlier_stocks:
            
            outlier_date = outliers.index[outliers[stock]]
            # print(f'Correcting {stock} on {outlier_date.to_list()}')
            result.loc[outlier_date, stock] = np.nan
   
        start_id += 240
        end_id += 240
        
    return result.swaplevel(axis=1)


def select(data: pd.DataFrame, column: str) -> pd.DataFrame:
    return data[[column]]


@matrix_op
def calculate_daily_return(data: pd.DataFrame) -> pd.DataFrame:
    return data.pct_change()
