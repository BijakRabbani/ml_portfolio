#%%
import pandas as pd
from .model_lr import create_model

def build_train_data(*args, **kwargs):
    '''
    Create training ready dataset.
    Currently, automatically filters to first day of month.
    Label is put on the last column.
    '''
    config = kwargs['config']
    data = pd.concat(args, axis=1)
    dates = data.index.get_level_values('date').unique()
    dates = dates[-config['training_horizon']:]
    fom = []
    for i in range(1,len(dates)):
        if dates[i].month != dates[i-1].month:
             fom.append(dates[i])
    data = data.loc[pd.IndexSlice[fom,:]]
    return data


def train(data):
    '''
    Training the model.
    For now, model is defined inside this function, will change latter.
    '''
    print(f'Training using data from {data.index[0][0]} to {data.index[-1][0]}')
    model = create_model()
    y = data.iloc[:,[-1]]
    y = y.dropna()
    X = data.iloc[:,:-1]
    X = X.loc[y.index]
    model.fit(X, y)
    return model


