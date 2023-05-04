# import the necessary libraries and credentials
import os
import clickhouse_connect
from dotenv import load_dotenv
import pandas as pd

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
    
    trades = trades.dropna(axis=1, how="all")

    #trades = trades[trades["Trade_Volume"] > 0]

    #trades = trades[trades["Trade_Price"] > 0]


    return trades

        
"""Example Queries

# select apple trades from January of 2017 to April of 2017
query = "SELECT * FROM TRADESDB.trades2017view WHERE (Symbol = 'AAPL') AND (Date = '2017-01-05')"

"""
