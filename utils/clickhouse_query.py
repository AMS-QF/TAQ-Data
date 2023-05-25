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

"""Example Queries

# select apple trades from January of 2017 to April of 2017
query = "SELECT * FROM TRADESDB.trades2017view WHERE (Symbol = 'AAPL') AND (Date = '2017-01-05')"

"""



    

