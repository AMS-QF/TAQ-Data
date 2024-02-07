from __future__ import annotations

from typing import Optional, List, Tuple
import sys
from tqdm import tqdm
import numpy as np
import pandas as pd

from data_preprocessing.get_data import get_trades, get_quotes
from .features_taq import CalendarFeatureGenerator
from .utils import convert_participant_timestamps, drop_irreg_hours, generate_trade_side

class TAQIntradayProcessor:
    """
    Provides methods for reconstructing intraday trades and quotes events for 
    a specified ticker and date
    """

    def __init__(self):
        pass


    def load_data(
        self, 
        data_path: str,
        ticker: str,
        start_date: str,
        end_date: str,
        trade_cols: Optional[List[str]] = None,
        quote_cols: Optional[List[str]] = None,
        row_limit: Optional[str] = None
    ):
        """
        Download the Clickhouse TAQ data
        """
        
        if not trade_cols:
            trade_cols = 'ALL'
        if not quote_cols:
            quote_cols = 'ALL'
        
        if trade_cols is not None and not all(col in trade_cols for col in ['Time', 'Symbol']):
            raise ValueError("'Time' and 'Symbol' must be included in trade_cols")
        
        if quote_cols is not None and not all(col in quote_cols for col in ['Time', 'Symbol']):
            raise ValueError("'Time' and 'Symbol' must be included in quote_cols")

        if not row_limit:
            row_limit = str(sys.maxsize)
        
        try:
            get_trades(data_path, ticker, start_date, end_date, row_limit, trade_cols)
            get_quotes(data_path, ticker, start_date, end_date, row_limit, quote_cols)
    
        except Exception as e:
            print(f"An error occurred: {e}")
        
        return 
    

    def preprocess(
        self,
        trades: pd.DataFrame,
        quotes: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Preprocess trades and quotes and reconstruct the event
        """
        clean_t = self._clean_trades(trades)
        clean_q = self._clean_quotes(quotes)
        return self._reconstruct_event(clean_t, clean_q)

    
    def gen_taq_features(
            self,
            df: pd.DataFrame,
            mode: str,
            prediction_length: float,
            deltas: List[Tuple]      
    )-> pd.DataFrame:
        """
        Generate features for high frequency taq data based on the following article 
        https://www.nber.org/papers/w30366

        Parameters
        ----------
        df:
            Dataframe of reconstructed trades and quotes events
        mode:
            nature of the generated feature
            only 'calendar' is supported now
        deltas:
            List of tuples for generating features from lookback intervals
        prediction_length:
            Length (in seconds) to generate prediction return
    
        """
        
        assert len(deltas) >= 1, "Invalid length for lookback intervals"

        self.deltas = deltas

        assert prediction_length > 0, "Prediction length must be positive in seconds"
        
        return self._generate_taq_features(df, mode, prediction_length)
        



    def _clean_trades(self, trades: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the trade data
            - convert participate timestamp
            - drop hours
            - drop invalid trade price and volume
        """
        new_trades = trades.copy()
        new_trades['Participant_Timestamp'] = convert_participant_timestamps(new_trades['Date'], new_trades['Participant_Timestamp'])
        new_trades = drop_irreg_hours(new_trades)
        new_trades['Trade_Side'] = generate_trade_side(new_trades['Trade_Price'].values)
        new_trades['Type'] = 'T'

        invalid_trades_idx = []
        # invalid trades
        if "Trade_Price" in new_trades:
            invalid_trades_idx.extend(new_trades[new_trades["Trade_Price"] <= 0].index)
        if "Trade_Volume" in new_trades.columns:
            invalid_trades_idx.extend(new_trades[new_trades["Trade_Volume"] <= 0].index)
        if "Trade_Reporting_Facility" in new_trades.columns:
            invalid_trades_idx.extend(new_trades[new_trades["Trade_Reporting_Facility"] == "D"].index)

        new_trades.drop(invalid_trades_idx, inplace=True)
        new_trades.drop(['Time', 'Date', 'Trade_Reporting_Facility'], axis=1, inplace=True)

        return new_trades
    

    def _clean_quotes(self, quotes: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the quotes data
            - convert participate timestamp
            - drop hours
            - drop invalid 
        """

        new_quotes = quotes.copy()
        new_quotes['Participant_Timestamp'] = convert_participant_timestamps(new_quotes['Date'], new_quotes['Participant_Timestamp'])
        new_quotes = drop_irreg_hours(new_quotes)
        new_quotes['Type'] = 'Q'

        invalid_quotes_idx = []

        if "Bid_Price" in new_quotes.columns:
            invalid_quotes_idx.extend(new_quotes[new_quotes["Bid_Price"] <= 0].index)
        if "Offer_Price" in new_quotes.columns:
            invalid_quotes_idx.extend(new_quotes[new_quotes["Offer_Price"] <= new_quotes["Bid_Price"]].index)
        
        new_quotes.drop(invalid_quotes_idx, inplace=True)
        new_quotes.drop(['Time', 'Date'], axis=1, inplace=True)
        return new_quotes


    def _reconstruct_event(self, trades: pd.DataFrame, quotes: pd.DataFrame) -> pd.DataFrame:
        """
        merge trade and quotes data on Participant Timestamp
        assign MOX identifier
        assign trade LAQ and compute midprice
        """
        event = pd.concat([trades, quotes])
        event.sort_values(by=['Participant_Timestamp'], inplace=True)
        event.reset_index(drop=True, inplace=True)

        event['MOX'] = pd.factorize(event['Participant_Timestamp'])[0]

        event['Active_Quotes'] = pd.NA
        quotes_df = event[event['Type'] == 'Q']
        event.loc[event['Type']=='Q', 'Active_Quotes'] = 'A'
        event.loc[quotes_df.duplicated('MOX', keep=False) & (event['Type'] == 'Q'), 'Active_Quotes'] = 'M'

        for col in ['Bid_Price', 'Bid_Size', 'Offer_Price', 'Offer_Size']:
            event.loc[(event['Active_Quotes'] == 'A') | (event['Type'] == 'T'), col] = event[(event['Active_Quotes'] == 'A') | (event['Type'] == 'T')][col].ffill()

        event.drop(['Active_Quotes'], axis=1, inplace=True)
        event['Mid_Price'] = (event['Bid_Price'] + event['Offer_Price'])/2

        return event

    
    def _generate_taq_features(
        self,  
        df:pd.DataFrame, 
        mode: str,
        prediction_length: float
    ) -> pd.DataFrame:
        """
        Helper function to generating the taq features
        """
        if mode == 'calendar':
            fg = CalendarFeatureGenerator()
        else:
            raise ValueError("Mode not implemented")
        
        fg.getForwardReturn(df, prediction_length)
        print(f"====Finish computing {prediction_length}s Return====")
     
        for delta1, delta2 in tqdm(self.deltas, desc="Generating Features"):
            fg.calc_time_deltas(df, delta1, delta2)
            # Volume and Duration
            fg.getBreath(df)
            fg.getImmediacy(df)
            fg.getVolumeAll(df)
            fg.getVolumeAvg(df)
            fg.getVolumeMax(df)

            # Return and Imbalance
            fg.getLambda(df)
            fg.getLobImbalance(df)
            fg.getTxnImbalance(df)
            fg.getPastReturn(df)
        
            # Speed and Cost
            fg.getQuotedSpread(df)
            fg.getEffectiveSpread(df)

            # Optionally, you can print or log after each complete set of features
            tqdm.write("Completed features for one set of deltas.")

        return fg.features_to_df(df)



        


    

