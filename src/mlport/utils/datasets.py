from typing import Any, Dict
from kedro.io import AbstractDataset
import pandas as pd
import numpy as np
from datetime import datetime as dt, timedelta
import os
import joblib

    
class ParquetData(AbstractDataset):
    """
    """
    def __init__(self, name: str, group: str):
        self.name = name
        self.group = group


    def _load(self) -> pd.DataFrame:
        """
        """
        data = pd.read_parquet(
            f"data/{self.group}/{self.name}.parquet",
            )
        return data

    def _save(self, data: pd.DataFrame) -> None:
        """
        """
        data.to_parquet(
            f"data/{self.group}/{self.name}.parquet",
            index=True
            )
    
    def _describe(self) -> Dict[str, Any]:
        """"""
        return dict(name=self.name)
    

class IndexedParquetData(AbstractDataset):
    """
    """
    def __init__(self, name: str, group: str, date=None):
        self.name = name
        self.group = group
        self.date = date
        
    def read_index(self):
        index = pd.read_parquet(
            f"data/raw/stock_listed.parquet",
            )
        return index
        
    def _load(self) -> pd.DataFrame:
        """
        """
        index = self.read_index()
        data = pd.read_parquet(
            f"data/{self.group}/{self.name}.parquet",
            )
        data = pd.concat([index, data], axis=1).set_index(['date','stock'])
        if self.date is not None:
            data = data.loc[pd.IndexSlice[:self.date,:]]
        return data

    def _save(self, data: pd.DataFrame) -> None:
        """
        """
        index = self.read_index()
        index = index.set_index(['date','stock'])
        data = data.reindex(index.index)
        data.columns = [self.name]
        data.to_parquet(
            f"data/{self.group}/{self.name}.parquet",
            index=False
            )
    
    def _describe(self) -> Dict[str, Any]:
        """"""
        return dict(filepath=self.name)
    

class ModelData(AbstractDataset):
    """
    """
    def __init__(self, name: str, date: str):
        self.name = name
        self.date = date

    def _load(self) -> pd.DataFrame:
        """
        """
        files = os.listdir(f'data/model/{self.name}')
        file_date = np.sort([file.split('/')[-1].split('.')[0] for file in files])
        last_date = file_date[np.sort(file_date) <= self.date][-1]
        filepath = f'data/model/{self.name}/{last_date}.joblib'
        joblib.load(filepath)

    def _save(self, model: pd.DataFrame) -> None:
        """
        """
        filepath = f'data/model/{self.name}/{self.date}.joblib'
        print(joblib.dump(model, filepath))        
        
    def _describe(self) -> Dict[str, Any]:
        """"""
        return dict(name=self.name)