
from dataclasses import dataclass, InitVar
from typing import Optional, Union, List, Type
import torch

import pandas as pd

from . import Dataset


@dataclass
class PandasDataset(Dataset):
    """
    Dataset constructed from pandas Dataframe or a list of pandas dataframe

    Parameters
    ----------
    dataframes
        Single ``pd.DataFrame`` or a list of ``pd.Dataframe
    target
        Name of the column that contains the ``target`` time series.
        For multivariate targets, a list of column names should be provided.
    start_date
        The start date of the dataframe
    freq
        The frequency of the provided dataframe, e.g. ``D`` for intraday 
    future_length
        The range of the provided dataset
    """

    dataframes: InitVar[
        Union[
        pd.DataFrame, 
        List[pd.DataFrame],
        ]
    ]

    target: InitVar[Union[str, List[str]]] = "target"
    start_date: str = None
    freq: Optional[str] = None
    future_length: Optional[int] = 1
    dtype: Type = torch.float32

    def __post_init__(self, dataframes, target):
        if isinstance(dataframes, pd.DataFrame):
            self.X = dataframes.drop(target, axis=1).values
            self.y = dataframes[target].values
            self.timestamp = dataframes.index
    

    def __len__(self) -> int:
        return len(self.timestamp)
    
    
 



    




