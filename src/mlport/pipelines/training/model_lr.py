from mlport.utils.preprocessing import CrossSectionalScaler
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn import set_config

set_config(transform_output="pandas")

def create_model():
    imputer = SimpleImputer(strategy='constant', fill_value=0)
    scaler = CrossSectionalScaler(StandardScaler())
    model = LinearRegression()
    pipe = Pipeline([
        ('prep', imputer),
        ('scaler',scaler),
        ('model', model)
        ],
        )
    params = {}
    pipe.set_params(**params)
    return pipe