# python data_preprocessing/run_jobs.py  --symbol AAPL --start_date 2021-01-01 --end_date 2021-01-02
import data_preprocessing.load_data as ld
import pandas as pd


def get_data(symbol = "AAPL", start_date = "2021-01-01", end_date = "2021-01-02"):
    # connect to database
    conn = ld.connect_to_db()

    return conn

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

if __name__ == "__main__":
    dataDf = get_data()
    print(dataDf)