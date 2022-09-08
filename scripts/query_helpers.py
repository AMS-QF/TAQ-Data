import pandas as pd
import numpy as np
import logging
import io
from fabric import Connection


class client_connection:
    """
    Class which represents client connection to remote server

    Runs commands for raw trade and quote data, generate features

    """

    def __init__(self, host, username, password, user_username, user_password):

        # want to encrypt these in some way
        self.host = host
        self.router_username = username
        self.router_password = password

        self.path = "TAQNYSE-Clickhouse"

        self.user_username = user_username
        self.user_password = user_password

        self.conn = Connection(
            host=self.host,
            user=self.router_username,
            connect_kwargs={"password": self.router_password},
        )
        logging.basicConfig(level=logging.INFO)

    def run_command(self, command):
        result = self.conn.run(command)
        return result

    def client_get_trades(self, exchange, symbol, start, end):
        directory = f"data/{symbol}_trades.csv"
        conda_command = "source ../../opt/anaconda3/bin/activate query_user"
        with self.conn.prefix(conda_command):

            command = f" python3 {self.path}/trades/server_helpers.py {self.user_username} {self.user_password} {exchange} {symbol} {start} {end}"
            print(f"Trade Query for {exchange} {symbol} {start} {end}")
            data = self.run_command(command)

            df = self.conn.get(f"{self.path}/trades/query_results.csv", local=directory)

        self.conn.close()
        return df

    def client_get_quotes(self, exchange, symbol, start, end):
        directory = f"data/{symbol}_quotes.csv"
        conda_command = "source ../../opt/anaconda3/bin/activate query_user"
        with self.conn.prefix(conda_command):

            command = f" python3 {self.path}/quotes/server_helpers.py {self.user_username} {self.user_password} {exchange} {symbol} {start} {end}"
            print(f"Quote Query for {exchange} {symbol} {start} {end}")
            data = self.run_command(command)

            df = self.conn.get(f"{self.path}/quotes/query_results.csv", local=directory)

        self.conn.close()
        return df
