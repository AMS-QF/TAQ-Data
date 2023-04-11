import pandas as pd

import data_preprocessing.load_data as ld


def tname(symbol="AAPL", start_date="2020-01-01", end_date="2020-01-02"):
    # connect to database
    conn = ld.connect_to_db()

    # get trades and quotes data
    pathTrades = ld.get_trades(conn, symbol, start_date, end_date)
    pathQuotes = ld.get_quotes(conn, symbol, start_date, end_date)

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