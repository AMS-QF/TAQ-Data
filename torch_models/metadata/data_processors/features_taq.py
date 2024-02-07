from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Optional


class CalendarFeatureGenerator:
    """
    Generate Calendar mode features for taq data
    """

    def __init__(self):
        self.feature_map = {}

    def reset(self):
        self.feature_map = {}
        return

    def features_to_df(self, df:pd.DataFrame) -> pd.DataFrame:
        feature_df = pd.DataFrame(self.feature_map)
        feature_df.index = pd.DatetimeIndex(df['Participant_Timestamp'])
        return feature_df
        
    
    def getForwardReturn(self, df:pd.DataFrame, prediction_length:float):
        if 'Return' in self.feature_map:
            return self.feature_map['Return']
        else:
            pts = pd.to_datetime(df['Participant_Timestamp'].values)
            forward_idx = np.searchsorted(pts, pts + pd.Timedelta(seconds=prediction_length), side='right')
            self.feature_map['Return'] = pd.Series([df.iloc[start:end]['Trade_Price'].mean()/df.iloc[start]['Mid_Price'] - 1 \
                                       for start, end in zip(df.index, forward_idx)])
            return self.feature_map['Return']
        

    def calc_time_deltas(self, df:pd.DataFrame, delta1:float, delta2:float):
        """
        Calculate start/end index for lookback interval
        """
        self.delta1, self.delta2 = delta1, delta2
        new_df = df.copy()
        pts = pd.to_datetime(new_df['Participant_Timestamp'].values)
        self.start_idx = np.searchsorted(pts, pts - pd.Timedelta(seconds=delta2), side='right')
        self.end_idx = np.searchsorted(pts, pts - pd.Timedelta(seconds=delta1), side='right') - 1
    
        return
    

    #Breath
    def getBreath(self, df:pd.DataFrame) -> pd.Series:
        if 'Breath' in self.feature_map:
            return self.feature_map['Breath']
        else:
            self.feature_map['Breath'] = pd.Series([df.iloc[start:end+1]['Type'].eq('T').sum() \
                                       for start, end in zip(self.start_idx, self.end_idx)])
            return self.feature_map['Breath']

    #Immediacy
    def getImmediacy(self, df:pd.DataFrame) -> pd.Series:
        if 'Immediacy' in self.feature_map:
            return self.feature_map['Immediacy']
        else:
            breath = self.getBreath(df)
            delta_diff = self.delta2-self.delta1
            self.feature_map['Immediacy'] = np.where(breath == 0, np.nan, (delta_diff) / breath)
            return self.feature_map['Immediacy']
    
    #VolumeAll
    def getVolumeAll(self, df:pd.DataFrame) -> pd.Series:
        if 'VolumeAll' in self.feature_map:
            return self.feature_map['VolumeAll']
        else:
            self.feature_map['VolumeAll'] = pd.Series([df.iloc[start:end+1]['Trade_Volume'].sum() \
                                          for start, end in zip(self.start_idx, self.end_idx)])
            return self.feature_map['VolumeAll']
    
    # VolumeAvg
    def getVolumeAvg(self, df:pd.DataFrame) -> pd.Series:
        if 'VolumeAvg' in self.feature_map:
            return self.feature_map['VolumeAvg']
        else:
            volumeAll = self.getVolumeAll(df)
            breath = self.getBreath(df)
            self.feature_map['VolumeAvg'] = volumeAll/breath
            return self.feature_map['VolumeAvg']
    
    # VolumeMax
    def getVolumeMax(self, df:pd.DataFrame) -> pd.Series:
        if 'VolumeMax' in self.feature_map:
            return self.feature_map['VolumeMax']
        else:
            self.feature_map['VolumeMax'] = pd.Series([max(df.iloc[start:end+1]['Trade_Volume'], default=0) \
                                          for start, end in zip(self.start_idx, self.end_idx)])
            return self.feature_map['VolumeMax']
        
    
    # Lambda
    def getLambda(self, df:pd.DataFrame) -> pd.Series:
        if 'Lambda' in self.feature_map:
            return self.feature_map['Lambda']
        else:
            volumeAll = self.getVolumeAll(df)
            p_change = pd.Series([df.iloc[end]['Mid_Price']-df.iloc[start]['Mid_Price'] \
                                  for start, end in zip(self.start_idx, self.end_idx)])
            
            self.feature_map['Lambda'] = np.where(volumeAll != 0, p_change / volumeAll, np.nan)
            return self.feature_map['Lambda']
    
    
    # LobImbalance
    def getLobImbalance(self, df:pd.DataFrame) -> pd.Series:
        if 'LobImbalance' in self.feature_map:
            return self.feature_map['LobImbalance']
        else:
            temp_df = df.copy()
            temp_df['Imbalance'] = (temp_df['Offer_Size'] - temp_df['Bid_Size']) / (temp_df['Offer_Size'] + temp_df['Bid_Size'])
            self.feature_map['LobImbalance'] = pd.Series([temp_df.iloc[start:end+1]['Imbalance'].mean() \
                                                          for start, end in zip(self.start_idx, self.end_idx)])
            return self.feature_map['LobImbalance']
    

    # TxnImbalance
    def getTxnImbalance(self, df:pd.DataFrame) -> pd.Series:
        if 'TxnImbalance' in self.feature_map:
            return self.feature_map['TxnImbalance']
        else:
            volumeAll = self.getVolumeAll(df)
            temp_df = df.copy()
            temp_df['Vt_Dir'] = temp_df['Trade_Volume'] * temp_df['Trade_Side']
            sum_Vt_Dir = pd.Series([temp_df.iloc[start:end+1]['Vt_Dir'].sum() for start, end in zip(self.start_idx, self.end_idx)])
            self.feature_map['TxnImbalance'] = np.where(volumeAll != 0, sum_Vt_Dir / volumeAll, np.nan)
            return self.feature_map['TxnImbalance']
    

    # PastReturn
    def getPastReturn(self, df:pd.DataFrame) -> pd.Series:
        if 'PastReturn' in self.feature_map:
            return self.feature_map['PastReturn']
        else:
            p_return = pd.Series([df.iloc[start:end+1]['Trade_Price'].mean() / df.iloc[end]['Mid_Price']\
                                for start, end in zip(self.start_idx, self.end_idx)])
            
            self.feature_map['PastReturn'] = 1 - p_return
            return self.feature_map['PastReturn']
    

    #QuotedSpread
    def getQuotedSpread(self, df:pd.DataFrame) -> pd.Series:
        if 'QuotedSpread' in self.feature_map:
            return self.feature_map['QuotedSpread']
        else:
            temp_df = df.copy()
            temp_df['Spread'] = (temp_df['Offer_Price'] - temp_df['Bid_Price'])/temp_df['Mid_Price']
            self.feature_map['QuotedSpread'] = pd.Series([temp_df.iloc[start:end+1]['Spread'].mean() \
                                                          for start, end in zip(self.start_idx, self.end_idx)])
            return self.feature_map['QuotedSpread']
    
    
    #EffectiveSpread
    def getEffectiveSpread(self, df:pd.DataFrame) -> pd.Series:
        if 'EffectiveSpread' in self.feature_map:
            return self.feature_map['EffectiveSpread']
        else:
            temp_df = df.copy()
            temp_df['Spread'] = np.log(temp_df['Trade_Price']/temp_df['Mid_Price'])*temp_df['Trade_Side']\
                                            *temp_df['Trade_Volume']*temp_df['Trade_Price']
            temp_df['Vt_P'] = temp_df['Trade_Volume'] * temp_df['Trade_Price']
            self.feature_map['EffectiveSpread'] = pd.Series([np.where(temp_df.iloc[start:end+1]['Vt_P'].sum() != 0, 
                                    np.divide(temp_df.iloc[start:end+1]['Spread'].sum(), temp_df.iloc[start:end+1]['Vt_P'].sum()), 
                                    np.nan) 
                           for start, end in zip(self.start_idx, self.end_idx)])
            return self.feature_map['EffectiveSpread']


