import argparse

import configobj
from fabric import Connection

from query_helpers import client_connection


def connect_to_db():
    """Connect to the database"""

    config = configobj.ConfigObj(".env")
    host = config["host"]
    server_user = config["server_user"]
    server_password = config["server_password"]
    db_user = config["db_user"]
    db_pass = config["db_pass"]

    conn = client_connection(host, server_user, server_password, db_user, db_pass)

    return conn


def get_trades(conn: Connection, symbol: str, start: str, end: str, data_dir: str = None):
    """Get trades from the database"""

    path_list = conn.get_trades_range(symbol, start, end, data_dir)

    return path_list


def get_quotes(conn: Connection, symbol: str, start: str, end: str, data_dir: str = None):
    """Get quotes from the database"""

    path_list = conn.get_quotes_range(symbol, start, end, data_dir)

    return path_list


def get_sample_trades(conn: Connection, symbol="AAPL", start_date="2021-01-01", end_date="2021-01-31", data_dir=None):
    """Get a sample of trades from the database"""

    trades, path = get_trades(conn, symbol, start_date, end_date, data_dir)

    return trades, path


def get_sample_quotes(conn: Connection, symbol="AAPL", start_date="2021-01-01", end_date="2021-01-31", data_dir=None):
    """Get a sample of quotes from the database"""

    quotes, path = get_quotes(conn, symbol, start_date, end_date, data_dir)

    return quotes, path


# python data_preprocessing/get_data.py --exchange N --symbol AAPL --start_date 2021-01-01 --end_date 2021-01-31 --data_dir AAPL
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--symbol", type=str, default="AAPL")
    parser.add_argument("--start_date", type=str, default="2021-01-01")
    parser.add_argument("--end_date", type=str, default="2021-01-31")
    parser.add_argument("--data_dir", type=str, default=None)

    args = parser.parse_args()

    conn = connect_to_db()

    path_list = get_trades(conn, args.symbol, args.start_date, args.end_date, args.data_dir)

    print(f"Trades saved to {path_list}")

    path_list = get_quotes(conn, args.symbol, args.start_date, args.end_date, args.data_dir)

    print(f"Quotes saved to {path_list}")
