# import the necessary libraries and credentials
import os
from datetime import timedelta
from collections import OrderedDict
from sklearn.base import BaseEstimator, TransformerMixin
import clickhouse_connect
from dotenv import load_dotenv
import pandas as pd
import numpy as np

load_dotenv()  # load the contents of the .env file into the environment

# read the credentials from the environment variables
host = os.getenv("host")
server_user = os.getenv("server_user")
server_password = os.getenv("server_password")
db_user = os.getenv("db_user")
db_pass = os.getenv("db_pass")

# use the credentials to connect to the database
client = clickhouse_connect.get_client(host=host, port=3306, username=db_user, password=db_pass)


class PreprocessData(BaseEstimator, TransformerMixin):
    
    def __init__(self, dropped_after_hourse=True, droped_irregular_hours=True):
        self.dropped_after_hourse = dropped_after_hourse
        self.droped_irregular_hours = droped_irregular_hours
        
    
    def fit(self, X, y=None):
        return self
    
    def generate_mox_identifier(self, df):
        """Generate MOX Identifier
        """
        # get participant timestamps
        participant_timestamps = df.index
        # convert timestamps to float
        fl_participant_timestamps = [float(ts.timestamp()*1000) for ts in participant_timestamps]
        # generate unique index for each timestamp
        time_mox_mapping = OrderedDict((ts, mox_idx) for mox_idx, ts in enumerate(set(fl_participant_timestamps)))
        # generate the mox_identifiers
        mox_identifiers = [time_mox_mapping[t] for t in fl_participant_timestamps]

        df['MOX_Identifiers'] = mox_identifiers

        return df
    
    def transform(self, X):
        cols = X.columns
        if 'Unnamed: 0' in cols:
            X.drop(['Unnamed: 0'], inplace=True, axis=1)
        if 'Time' in cols:
            X.drop(['Time'], inplace=True, axis=1)
        
        # parse date
        X['Date'] = pd.to_datetime(X['Date'])

        # function to split the timestamp into hours, minutes, seconds, and microseconds
        def split_timestamp(ts):
            parts = str(ts).split('.')
            return parts[0].zfill(6), parts[1].ljust(6, '0') if len(parts) > 1 else '000000'

        # apply the function and convert to datetime
        X['Participant_Timestamp'] = X['Participant_Timestamp'].apply(split_timestamp)
        X['Participant_Timestamp'] = pd.to_datetime(X['Participant_Timestamp'].apply(lambda x: x[0] + x[1]), format="%H%M%S%f")

        # convert datetime to index
        X["Participant_Timestamp"] = X["Date"].apply(lambda x: x) + X["Participant_Timestamp"].apply(
            lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second, microseconds=x.microsecond)
        )
        X.index = X["Participant_Timestamp"].values
        
        # remove rows of all NA
        X = X.dropna(axis=1, how="all")
        
        # remove invalid trades
        X.drop(X[X['Trade_Price'] < 0].index, inplace=True)
        X.drop(X[X['Trade_Volume'] < 0].index, inplace=True)
        X.drop(X[X['Trade_Reporting_Facility'] == 'D'].index, inplace=True)
        
        # remove invalid quotes
        X.drop(X[X['Bid_Price'] < 0].index, inplace=True)
        X.drop(X[X['Offer_Price'] < X['Bid_Price']].index, inplace=True)
        
        # drop after hours if specified
        if self.dropped_after_hourse:
            afterhours_idx = []
            for t in X.index:
                str_t = t.strftime("%H:%M:%S")
                if str_t < "09:00:00" or str_t > "16:00:00":
                    afterhours_idx.append(t)
            X.drop(afterhours_idx, inplace=True)
            
        # remove first and last 15 minutes of regular trading hours
        if self.droped_irregular_hours:
            irregular_idx = []
            for t in X.index:
                str_t = t.strftime("%H:%M:%S")
                if str_t < "09:45:00" or str_t > "15:45:00":
                    irregular_idx.append(t)
            X.drop(irregular_idx, inplace=True)
        
        #sort data according to index
        X = X.sort_index()
        
        #assign MOX Identifiers
        X = self.generate_mox_identifier(X)


def get_trades(query):
    """Execute a SQL query and return the results as a list of dictionaries."""
    results = client.command(query)

    # Replace 'raw_data' with the variable containing your data as a list
    raw_data = results

    # Preprocess the raw data
    rows = []
    current_row = []
    for item in raw_data:
        if '\n' in item:
            # Split the item at the newline character
            split_item = item.split('\n')

            # Add the first part of the split item to the current row
            current_row.append(split_item[0])

            # Add the current row to the list of rows
            rows.append(current_row)

            # Start a new row with the second part of the split item
            current_row = [split_item[1]]
        else:
            # Add the item to the current row
            current_row.append(item)

    # Create the DataFrame
    columns = [
        'Time', 'Exchange', 'Symbol', 'Sale_Condition', 'Trade_Volume', 'Trade_Price',
        'Trade_Stop_Stock_Indicator', 'Trade_Correction_Indicator', 'Sequence_Number',
        'Trade_Id', 'Source_of_Trade', 'Trade_Reporting_Facility', 'Participant_Timestamp',
        'Trade_Reporting_Facility_TRF_Timestamp', 'Trade_Through_Exempt_Indicator',
        'Date', 'YearMonth'
    ]

    trades = pd.DataFrame(rows, columns=columns)
    
    # Replace '\\N' with NaN in all columns
    for col in trades.columns:
        trades[col] = trades[col].replace('\\N', np.nan)
    
    # Convert the Time column to datetime
    trades['Time'] = pd.to_datetime(trades['Time']).dt.tz_localize(None)
    
    # Format columns to the proper data type
    trades['Exchange'] = trades['Exchange'].astype('str')
    trades['Symbol'] = trades['Symbol'].astype('str')
    trades['Trade_Volume'] = trades['Trade_Volume'].astype('int')
    trades['Trade_Price'] = trades['Trade_Price'].astype('float')
    trades['Trade_Stop_Stock_Indicator'] = trades['Trade_Stop_Stock_Indicator'].astype('str')
    trades['Trade_Correction_Indicator'] = trades['Trade_Correction_Indicator'].astype('int')
    trades['Sequence_Number'] = trades['Sequence_Number'].astype('int')
    trades['Trade_Id'] = trades['Trade_Id'].astype('int')
    trades['Source_of_Trade'] = trades['Source_of_Trade'].astype('str')
    trades['Trade_Reporting_Facility'] = trades['Trade_Reporting_Facility'].astype('str')
    trades['Participant_Timestamp'] = trades['Participant_Timestamp'].astype('float64')
    trades['Trade_Reporting_Facility_TRF_Timestamp'] = trades['Trade_Reporting_Facility_TRF_Timestamp'].astype('float64')
    trades['Trade_Through_Exempt_Indicator'] = trades['Trade_Through_Exempt_Indicator'].astype('int')
    trades['Date'] = pd.to_datetime(trades['Date'])
    trades['YearMonth'] = trades['YearMonth'].astype('str')


    return trades

def load_and_preprocess_data(query, dropped_after_hourse=True, droped_irregular_hours=True):
    # Load the raw trades data
    raw_data = get_trades(query)
    
    # Create a PreprocessData instance
    preprocessor = PreprocessData(dropped_after_hourse=dropped_after_hourse, droped_irregular_hours=droped_irregular_hours)
    
    # Preprocess the raw data
    processed_data = preprocessor.transform(raw_data)
    
    return processed_data

# Example Queries

# Select apple trades from January of 2017 to April of 2017
query = "SELECT * FROM TRADESDB.trades2017view WHERE (Symbol = 'AAPL') AND (Date = '2017-01-05')"
data = load_and_preprocess_data(query)


