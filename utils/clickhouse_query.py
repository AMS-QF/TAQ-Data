# import the necessary libraries and credentials
import os
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
    trades['Participant_Timestamp'] = trades['Participant_Timestamp'].astype('int64')
    trades['Trade_Reporting_Facility_TRF_Timestamp'] = trades['Trade_Reporting_Facility_TRF_Timestamp'].astype('float64')
    trades['Trade_Through_Exempt_Indicator'] = trades['Trade_Through_Exempt_Indicator'].astype('int')
    trades['Date'] = pd.to_datetime(trades['Date'])
    trades['YearMonth'] = trades['YearMonth'].astype('str')

    trades['Participant_Timestamp'] = pd.to_datetime(trades['Participant_Timestamp'], format='%H%M%S%f').dt.time

    # Ensure "Date" column is in datetime format
    trades['Date'] = pd.to_datetime(trades['Date'])

    # Ensure "Participant_Timestamp" is of type str
    trades['Participant_Timestamp'] = trades['Participant_Timestamp'].astype(str)

    # Create new datetime column "DateTime"
    trades['DateTime'] = pd.to_datetime(trades['Date'].dt.strftime('%Y-%m-%d') + ' ' + trades['Participant_Timestamp'])


    return trades

def get_quotes(query):
    """Execute a SQL query and return the results as a pandas DataFrame."""
    results = client.command(query)  # Assuming `client` is your ClickHouse client

    if results:
        # Convert the result into a pandas DataFrame
        quotes = pd.DataFrame(results, columns=results[0].keys())

        # Replace 'NULL' with np.nan in all columns
        quotes.replace('NULL', np.nan, inplace=True)

        # Convert columns to the appropriate data types
        quotes['Time'] = pd.to_datetime(quotes['Time']).dt.tz_localize(None)
        quotes['Date'] = pd.to_datetime(quotes['Date'])
        numeric_cols = ['Bid_Price', 'Bid_Size', 'Offer_Price', 'Offer_Size',
                        'National_BBO_Indicator', 'FINRA_ADF_MPID_Indicator',
                        'FINRA_ADF_Market_Participant_Quote_Indicator']
        quotes[numeric_cols] = quotes[numeric_cols].apply(pd.to_numeric)
        str_cols = ['Exchange', 'Symbol', 'Quote_Condition', 'Sequence_Number',
                    'FINRA_BBO_Indicator', 'Quote_Cancel_Correction', 'Source_Of_Quote',
                    'Retail_Interest_Indicator', 'Short_Sale_Restriction_Indicator',
                    'LULD_BBO_Indicator', 'SIP_Generated_Message_Identifier', 'NBBO_LULD_Indicator',
                    'Participant_Timestamp', 'FINRA_ADF_Timestamp', 'Security_Status_Indicator',
                    'YearMonth']
        quotes[str_cols] = quotes[str_cols].astype(str)

        return quotes

    return pd.DataFrame()  # Return an empty DataFrame if no results




"""Example Query

# import helper functions
from utils.clickhouse_query import *

# Note - there is restriction to 1,000,000 rows per day/per user - so it's wise to limit the query to a specific time range for testing purposes - aggregation can also be used to reduce the number of rows returned
# Here is a way to restrict the query to a specific time range
start_hour = 9
end_hour = 10

# Define the query - this query grabs trades data from AAPL on 2017-01-05 between 9am and 11am
query = f'''
    SELECT * 
    FROM TRADESDB.trades2017view 
    WHERE (Symbol = 'AAPL') 
    AND (Date = '2017-01-05') 
    AND (toHour(Time) BETWEEN {start_hour} AND {end_hour})
    AND Trade_Volume > 0
    AND Trade_Price > 0
'''

# Execute the query and store the resulting dataframe
data = get_trades(query)

"""



    

