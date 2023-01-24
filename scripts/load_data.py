import configobj
import pandas as pd
from fabric import Connection
from preprocess import clean_quotes, clean_trades
from query_helpers import client_connection


def connect_to_db():
    """Connect to the database"""

    config = configobj.ConfigObj("../.env")
    host = config["host"]
    server_user = config["server_user"]
    server_password = config["server_password"]
    db_user = config["db_user"]
    db_pass = config["db_pass"]

    conn = client_connection(host, server_user, server_password, db_user, db_pass)

    return conn


def get_trades(conn: Connection, exchange: str, symbol: str, start: str, end: str, data_dir: str = None):
    """Get trades from the database"""

    result, path = conn.client_get_trades(exchange, symbol, start, end, data_dir)
    print(path)
    trades = pd.read_csv("../" + path)

    trades = clean_trades(trades)
    trades.to_csv(path)

    return trades, path


def get_quotes(conn: Connection, exchange: str, symbol: str, start: str, end: str, data_dir: str = None):
    """Get quotes from the database"""

    results, path = conn.client_get_quotes(exchange, symbol, start, end, data_dir)

    quotes = pd.read_csv(path)

    quotes = clean_quotes(quotes)
    quotes.to_csv(path)

    return quotes, path


def get_sample_trades(
    conn: Connection, exchange="N", symbol="AAPL", start_date="2021-01-01", end_date="2021-01-31", data_dir="data"
):
    """Get a sample of trades from the database"""

    trades, path = get_trades(conn, exchange, symbol, start_date, end_date, data_dir)

    return trades, path


def get_sample_quotes(
    conn: Connection, exchange="N", symbol="AAPL", start_date="2021-01-01", end_date="2021-01-31", data_dir="data"
):
    """Get a sample of quotes from the database"""

    quotes, path = get_quotes(conn, exchange, symbol, start_date, end_date)

    return quotes, path


# python3 load_data.py
if __name__ == "__main__":
    conn = connect_to_db()

    trades, path = get_sample_trades(conn)

    print(f"Trades saved to {path}")

    quotes, path = get_sample_quotes(conn)

    print(f"Quotes saved to {path}")
