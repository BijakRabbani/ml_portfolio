from typing import Any, Dict
from kedro.io import AbstractDataset
import pandas as pd
import numpy as np
from datetime import datetime as dt, timedelta
import os
  
    
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
    
