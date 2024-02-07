from __future__ import annotations
from typing import Optional, List

import numpy as np 
import pandas as pd

from torch_models.metadata.data_processors.processor_taq import TAQIntradayProcessor


class DataProcessor:
    """
    A unified data processor class for downloading data, clean data, and feature generation

    Parameters
    ----------
    data_source
        Database source of the data
    data_path
        Path to save the downloaded data
    
    """

    def __init__(self, data_source, data_path, **kwargs):

        if data_source == "taq_clickhouse":
            self.processor = TAQIntradayProcessor()

        else:
            raise ValueError("Data Source Not Supported Yet")
  
        self.data_path = data_path
       

    def download_data(
        self, 
        ticker,
        start_date,
        end_date,
        trade_cols: Optional[List] = None,
        quote_cols: Optional[List] = None,
        **kwargs 
    ) -> pd.Dataframe:
        """
        Download data 
        """
        return self.processor.load_data(
            data_path=self.data_path,
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            trade_cols = trade_cols,
            quote_cols = quote_cols 
        )

    
    def preprocess(
        self, 
        trades: pd.DataFrame, 
        quotes: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Preprocess the data
        """
        return self.processor.preprocess(trades, quotes)
    

    def gen_taq_features(
        self, 
        df: pd.DataFrame,
        **kwargs
    ) -> pd.DataFrame:
        """
        Generate microeconomics features for the trade and quote data
        """
        mode = kwargs.get("mode", "calendar")
        deltas = kwargs.get("deltas", [])
        prediction_length = kwargs.get("prediction_length", 0)
        return self.processor.gen_taq_features(df, mode, prediction_length, deltas)