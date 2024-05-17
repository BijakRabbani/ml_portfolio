import pandas as pd
import numpy as np
from typing import Any, Dict, Set
from kedro.io import AbstractDataset, MemoryDataset
from kedro.io import DataCatalog as dC
from .utils.datasets import (
    ParquetData,
    )
import yaml

dataset_dict = {
        'parquet': ParquetData,
    }


def load_catalog(date=None):
    with open('conf/base/catalog.yml') as stream:
        data_info = yaml.safe_load(stream)
        
    with open('conf/base/feature_catalog.yaml') as stream:
        feature_info = yaml.safe_load(stream)
    
    data_info.update(feature_info)
    
    catalog = {}
    for key in data_info:
        if 'args' in data_info[key]['datatype'].keys():
            args = data_info[key]['datatype']['args']
        else:
            args = {}
            
        if data_info[key]['datatype']['type'] in ['gcs_indexedparquet', 'model']:
            args['date'] = date
            
        type_key = data_info[key]['datatype']['type']
        dataset = dataset_dict[type_key](**args)
        catalog[key] = dataset
    catalog_class = DataCatalog(catalog)
    return catalog_class


class DataCatalog(dC):

    def __init__(
            self,
            data_sets: Dict[str, AbstractDataset] = None,
            feed_dict: Dict[str, Any] = None,
            layers: Dict[str, Set[str]] = None,
    ) -> None:
        """
        DataCatalog extension. Adding several functions.
        """
        self.container = {}
        super(DataCatalog, self).__init__(data_sets, feed_dict, layers)
    

    def get(self, features: str, dates=None, **kwargs) -> pd.DataFrame:
        """
        """
        df = self.load_from_container(features, **kwargs)

        # Filter by date
        if dates is not None:
            if dates == 'last':
                dates = [df.index.get_level_values('date')[-1]]
            
            idx = pd.IndexSlice
            df = df.loc[idx[dates,:]]
                
        return df


    def load_from_container(self, features: str, **kwargs) -> pd.DataFrame:
        
        # Load feature
        if features in self.container.keys():
            df = self.container[features]
        else:
            df = self.load(features, **kwargs)
        
        # Store to container
        if isinstance(features, list):
            for feature in features:
                if feature not in self.container.keys():
                    self.container[feature] = df[feature]
        else:
            if features not in self.container.keys():
                self.container[features] = df
                
        return df