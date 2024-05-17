#%%
import pandas as pd
import numpy as np
import requests
from io import BytesIO
from zipfile import ZipFile


def update_stock_listed(listed, delisted, excluded, date_list):
    """
    Create an index which consist of date and all listed stocks for each date
    """
    stock_list = listed['stock'].values
    stock_listed = pd.DataFrame(0, index=date_list['date'], columns=stock_list)
    for _, row in listed.iterrows():
        stock_listed.loc[row['listing_date']:, row['stock']] = 1

    for _, row in delisted.iterrows():
        stock_listed.loc[row['date']:, row['stock']] = 0

    stock_listed = stock_listed.fillna(0)
    stock_listed = stock_listed.astype(int)
    stock_listed.columns.name = 'stock'
    stock_listed.index.name = 'date'
    stock_listed = stock_listed.drop(excluded['stock'].values, axis=1)
    stock_listed = stock_listed.reset_index()
    
    index_ref = stock_listed.melt(id_vars='date', var_name='stock')
    index_ref.index.name = 'index'
    index_ref = index_ref.set_index(['date', 'stock']).sort_index()
    index_ref = index_ref[index_ref == 1].dropna()
    index_ref = index_ref.reset_index()[['date','stock']]
    
    return index_ref


def parse_date(date):
    MONTH2NUM = {'JAN':'01',
                 'FEB':'02',
                 'MAR':'03',
                 'APR':'04',
                 'MAY':'05',
                 'JUN':'06',
                 'JUL':'07',
                 'AUG':'08',
                 'SEP':'09',
                 'OCT':'10',
                 'NOV':'11',
                 'DEC':'12'}
    if date is not np.nan:
        temp = date.split('-')
        if temp[1].isnumeric():
            date = '-'.join(temp[::-1])
        else:
            temp[1] = MONTH2NUM[temp[1]]
            
            date = '-'.join(temp[::-1])
    return date


def get_stock_info():
    '''
    Get stock listing and sector
    '''
    dls = "https://www.ksei.co.id/Download/efek/efekSaham.txt.zip"
    resp = requests.get(dls, verify=False)
    zipfile = ZipFile(BytesIO(resp.content))
    file = zipfile.namelist()[0]
    df = pd.read_csv(zipfile.open(file), delimiter='|')
    df.loc[:,'Listing Date'] = df.loc[:,'Listing Date'].apply(lambda x : parse_date(x))
    df.loc[:,'Eff. Isin Date'] = df.loc[:,'Eff. Isin Date'].apply(lambda x : parse_date(x))   
    df = df[df['Stock Exchange'].notnull()]
    
    df = df.rename(
        {
            'Securities Code':'stock', 
            'Sec. sector':'sector',
            'Listing Date':'listing_date'
            }, 
        axis=1)
    return df