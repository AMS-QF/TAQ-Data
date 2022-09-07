import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import sys
import sqlalchemy as db
from sqlalchemy.sql import func
import logging
from fabric import Connection


class client_connection:
    """
    Class which represents client connection to remote server

    Runs commands for raw trade and quote data, generate features

    """

    def __init__(self, router_ip, username, password, user_username, user_password):

        # want to encrypt these in some way
        self.router_ip =   router_ip
        self.router_username = "query_user"  # username
        self.router_password =  password

        self.user_username = user_username
        self.user_password = user_password

        self.path = "TAQNYSE-Clickhouse"

        self.ssh = paramiko.SSHClient()
        logging.basicConfig(level=logging.INFO)

    def run_command(self, command):
        self.ssh.load_system_host_keys()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        total_attempts = 3
        for attempt in range(total_attempts):
            try:
                string = str(f"Attempt to connect:{attempt}")
                logging.info(string)
                # Connect to router using username/password authentication.
                self.ssh.connect(
                    self.router_ip,
                    username=self.router_username,
                    password=self.router_password,
                    look_for_keys=False,
                )
                # Run command.
                ssh_stdin, ssh_stdout, ssh_stderr = self.ssh.exec_command(
                    "conda activate query_user",  get_pty=True
                )

                ssh_stdin, ssh_stdout, ssh_stderr = self.ssh.exec_command(command,  get_pty=True)

                output = ssh_stdout.readlines()

                # Close connection.
                self.ssh.close()

                return output

            except Exception as error_message:
                string = str(f"Unable to Connect {error_message}")
                logging.warning(string)
                return pd.DataFrame([])

    def construct_lob(self, exchange, symbol, start, end, n_levels=10):
        """
        Fetches Raw quotes and then constructs the full lOB up to level n_levels

        """

        quotes = self.get_quotes(exchange, symbol, start, end)

        return

    def client_get_trades(self, exchange, symbol, start, end):
        command = f"python3 {self.path}/trades/server_helpers.py {self.user_username} {self.user_password} {exchange} {symbol} {start} {end}"
        data = self.run_command(command)

        return data

    def client_get_trade_features(self, exchange, symbol, start, end, bbo=False):

        pass

    def client_get_quotes(self, exchange, symbol, start, end):
        command = f"python3 {self.path}/quotes/server_helpers.py  {self.user_username} {self.user_password} {exchange} {symbol} {start} {end}"
        print(command)
        data = self.run_command(command)
        print(data)
        return data

    def client_get_quote_features(self, exchange, symbol, start, end, bbo=False):
        pass


conn = client_connection(
    "router", "server_user", "server_pass", "Jason_Remote", "Password"
)
conn.client_get_quotes("K", "AAPL", "2020-01-04", "2020-01-11")
