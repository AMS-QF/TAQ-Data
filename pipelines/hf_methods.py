
from sklearn.base import BaseEstimator, TransformerMixin, OneToOneFeatureMixin
from sortedcollections import OrderedSet
import time
from generators import *



class CleanData(BaseEstimator, TransformerMixin):
    
    def __init__(self, dropped_after_hourse=True, droped_irregular_hours=True):
        self.dropped_after_hourse = dropped_after_hourse
        self.droped_irregular_hours = droped_irregular_hours
        
    
    def fit(self, X, y=None):
        if 'Participant_Timestamp' and 'Date' in X.columns:
            print ('test')
            self.part_timestamp = convertParticipantTimestamp(X['Participant_Timestamp'], X['Date'])
        else:
            self.part_timestamp = X['Participant_Timestamp']
        return self
    
    
    def transform(self, X):    
        
        # remove rows of all NA
        X = X.dropna(axis=0, how="all")
        X.drop(['Unnamed: 0','Time', 'Date', 'YearMonth'], axis=1, inplace=True, errors='ignore')
        X['Participant_Timestamp'] = self.part_timestamp
        X.index = self.part_timestamp
       
        # drop after hours if specified
        if self.dropped_after_hourse:
            after_idx = []
            for t in X.index:
                str_t = t.strftime("%H:%M:%S")
                if str_t < "09:00:00" or str_t > "16:00:00":
                    after_idx.append(t)
            X.drop(after_idx, inplace=True)
     
        
        # drop irregular hours if specified
        if self.droped_irregular_hours:
            irreg_idx = []
            for t in X.index:
                str_t = t.strftime("%H:%M:%S")
                if str_t < "09:15:00" or str_t > "15:45:00":
                    irreg_idx.append(t)
            X.drop(irreg_idx, axis=0, inplace=True)
        
        
        X = X.sort_index()
 
        return X
    

class PreprocessData(BaseEstimator, TransformerMixin):
    
    def __init__(self):
        return
        
    
    def fit(self, X, y=None):
        self.invalid_idx = []
        
        # invalid trades
        self.invalid_idx.extend(X[X['Trade_Price'] < 0].index) 
        self.invalid_idx.extend(X[X['Trade_Volume'] < 0].index)
        self.invalid_idx.extend(X[X['Trade_Reporting_Facility'] == 'D'].index)
        
        # invalid quotes
        self.invalid_idx.extend(X[X['Bid_Price'] < 0].index)
        self.invalid_idx.extend(X[X['Offer_Price'] < X['Bid_Price']].index)
        return self
    
    
    def transform(self, X):    
        
        #drop invalid trades and quotes
        X.drop(self.invalid_idx, inplace=True)
        
        #assign mox identifier
        X['MOX'] = generate_mox_identifier(X['Participant_Timestamp'])
        
        #labeling natural best bid/ask or LMQ as valid (True), otherwise False
        valid_quotes = ~X.duplicated(subset=['MOX'], keep='last')
        X['Valid_Quotes'] = X['Is_Quote'] & valid_quotes #Vectorization
        
        #assign trading directions
        X['Trade_Sign'] = generate_trade_side(X['Trade_Price'])
        
        #assign time in float (measure in seconds)
        X['Participant_Timestamp_f'] = X['Participant_Timestamp'].apply(lambda t : t.timestamp())
        
        return X


class FeatureGeneration(BaseEstimator, TransformerMixin):
    
    def __init__(self, params=None):
        self.params = params
        return
        
    
    def fit(self, df, y=None):
        
        df['Trade_Volume'] = df['Trade_Volume'].apply(lambda t: t if not np.isnan(t) else 0)
        df.reset_index(drop=True, inplace=True)
        
        return self
    
    
    def transform(self, df):  
        
        # 1.generating response variable
        
        # transaction return
        df['Trans_Return'] = generate_transaction_return(df, self.params['return_span'], self.params['clock_mode'])
        
        # 2.generating predictors
        
        # Volume and Duration
        
        
        # Return and Imbalance
        df = parent_generator_ret_imb(df, self.params['deltas'], mode='calendar')
        
        # Speed and Cost
        
        return df