import gc
import logging
import os
from datetime import timedelta

import pandas as pd
import pandas_market_calendars as mcal
from fabric import Connection


class client_connection:
    """
    Class which represents client connection to remote server

    Runs commands for raw trade and quote data, generate features

    """

    def __init__(self, host, username, password, user_username, user_password):

        self.host = host
        self.router_username = username
        self.router_password = password

        self.path = "TAQNYSE-Clickhouse"

        self.user_username = user_username
        self.user_password = user_password

        # setup remote connection to server
        self.conn = Connection(
            host=self.host,
            user=self.router_username,
            connect_timeout=None,
            connect_kwargs={"password": self.router_password},
        )
        logging.basicConfig(level=logging.INFO)

    def run_command(self, command, timeout=None, keep_alive=30):
        """Run a command on the remote server"""

        command = f"{command} --keepalive={keep_alive}"
        result = self.conn.run(command, hide=True, warn=True, timeout=timeout)
        return result

    def client_get_trades(self, symbol, start, end, dir_name=None):
        """Get trades from the database via remote execution of server_helpers.py"""

        if dir_name is None:
            dir_name = f"data/raw_data/temp/{symbol}_trades.csv"
        else:
            dir_name = f"data/raw_data/temp/{dir_name}/{symbol}_trades.csv"

        conda_command = "source ../../opt/anaconda3/bin/activate query_user"
        with self.conn.prefix(conda_command):

            command = f" python3 {self.path}/server_helpers/trade_server_helpers.py {self.user_username} {self.user_password}  {symbol} {start} {end}"
            print(f"Trade Query for {symbol} {start} {end}")
            self.run_command(command, keep_alive=30)

            # get the file from the server saving to our local directory
            df = self.conn.get(f"{self.path}/query_results.csv", local=dir_name)

        return df, dir_name

    def client_get_quotes(self, symbol, start, end, dir_name=None):
        """Get quotes from the database via remote execution of server_helpers.py"""

        if dir_name is None:
            dir_name = f"data/raw_data/temp/{symbol}_quotes.csv"
        else:
            dir_name = f"data/raw_data/temp/{dir_name}/{symbol}_quotes.csv"

        conda_command = "source ../../opt/anaconda3/bin/activate query_user"
        with self.conn.prefix(conda_command):

            command = f" python3 {self.path}/server_helpers/quote_server_helpers.py {self.user_username} {self.user_password} {symbol} {start} {end}"
            print(f"Quote Query for {symbol} {start} {end}")
            self.run_command(command, keep_alive=30)

            # get the file from the server saving to our local directory
            df = self.conn.get(f"{self.path}/query_results.csv", local=dir_name)

        return df, dir_name

    def get_quotes_range(self, symbol, start, end, dir_name=None):
        """Get quotes for a range of dates by calling client_get_quotes for each day (preventing timeouts)"""
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        # get market days
        market_days = mcal.get_calendar("NYSE").valid_days(start_date=start, end_date=end)
        market_days = [x.date() for x in market_days]

        current_dt = start
        path_list = []
        while current_dt < end:

            if current_dt.date() not in market_days:
                current_dt = current_dt + timedelta(days=1)
                continue

            current_dt_str = str(current_dt.date())
            next_dt_str = str((current_dt + timedelta(days=1)).date())
            self.client_get_quotes(symbol, current_dt_str, next_dt_str, dir_name)

            # create directory if it doesn't exist
            isExist = os.path.exists(f"data/raw_data/{current_dt.date()}")
            if not isExist:
                os.makedirs(f"data/raw_data/{current_dt.date()}")

            day_quotes = pd.read_csv(f"data/raw_data/temp/{symbol}_quotes.csv", low_memory=False, on_bad_lines="skip")

            if len(day_quotes) > 0:
                day_quotes.to_csv(f"data/raw_data/{current_dt.date()}/{symbol}_quotes.csv")
                path_list.append(f"data/raw_data/{current_dt.date()}/{symbol}_quotes.csv")
                print(f"Saved Quotes for {symbol} on {current_dt}")

            del day_quotes
            gc.collect()
            current_dt = current_dt + timedelta(days=1)

        self.conn.close()
        self.conn._sftp = None
        print(" ")
        return path_list

    def get_trades_range(self, symbol, start, end, dir_name=None):
        """Get trades for a range of dates by calling client_get_trades for each day (preventing timeouts)"""
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)

        # get list of market days
        market_days = mcal.get_calendar("NYSE").valid_days(start_date=start, end_date=end)
        market_days = [x.date() for x in market_days]

        current_dt = start

        path_list = []
        while current_dt < end:

            if current_dt.date() not in market_days:
                current_dt = current_dt + timedelta(days=1)
                continue

            current_dt_str = str(current_dt.date())
            next_dt_str = str((current_dt + timedelta(days=1)).date())
            self.client_get_trades(symbol, current_dt_str, next_dt_str, dir_name)

            # create directory if it doesn't exist
            isExist = os.path.exists(f"data/raw_data/{current_dt.date()}")
            if not isExist:
                os.makedirs(f"data/raw_data/{current_dt.date()}")

            day_trades = pd.read_csv(f"data/raw_data/temp/{symbol}_trades.csv", low_memory=False, on_bad_lines="skip")

            if len(day_trades) > 0:
                day_trades.to_csv(f"data/raw_data/{current_dt.date()}/{symbol}_trades.csv")
                path_list.append(f"data/raw_data/{current_dt.date()}/{symbol}_trades.csv")
                print(f"Saved trades for {symbol} on {current_dt}")

            del day_trades
            gc.collect()

            current_dt = current_dt + timedelta(days=1)

        self.conn.close()
        self.conn._sftp = None
        print(" ")
        return path_list
