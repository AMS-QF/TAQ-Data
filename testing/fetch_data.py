import sys

import pandas as pd

sys.path.append("../")

# import functions directly
from data_preprocessing.load_data import connect_to_db, get_quotes, get_trades


def get_data(symbol="AAPL", start_date="2021-08-03", end_date="2021-08-04"):

    # connect to database ( if you connect somewhere outside of load_data.py, you will need to pass the path to the .env file)
    conn = connect_to_db("../../.env")

    # get trades and quotes data
    pathTrades = get_trades(conn, symbol, start_date, end_date)
    pathQuotes = get_quotes(conn, symbol, start_date, end_date)

    # initialize dataframes
    tradesDf = pd.DataFrame()
    quotesDf = pd.DataFrame()

    # load data
    for path in pathTrades:
        tradesDf = tradesDf.append(pd.read_csv(path))
    for path in pathQuotes:
        quotesDf = quotesDf.append(pd.read_csv(path))

    # concatenate dataframes and sort by timestamp
    dataDf = pd.concat([tradesDf, quotesDf], axis=0)

    return dataDf
