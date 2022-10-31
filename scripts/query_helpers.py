import pandas as pd
import numpy as np
import gc
import logging
import io
from fabric import Connection
from datetime import timedelta



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

        
        return df

    def client_get_quotes(self, exchange, symbol, start, end):
        directory = f"data/{symbol}_quotes.csv"
        conda_command = "source ../../opt/anaconda3/bin/activate query_user"
        with self.conn.prefix(conda_command):

            command = f" python3 {self.path}/quotes/server_helpers.py {self.user_username} {self.user_password} {exchange} {symbol} {start} {end}"
            print(f"Quote Query for {exchange} {symbol} {start} {end}")
            data = self.run_command(command)

            df = self.conn.get(f"{self.path}/quotes/query_results.csv", local=directory)

        
        return df

    def get_quotes_range(self,exchange,symbol,start,end):
        start=pd.to_datetime(start)
        end=pd.to_datetime(end)
        
        current_dt=start
        
        while current_dt<end:
            current_dt_str=str(current_dt.date())
            next_dt_str=str((current_dt+timedelta(days=1)).date())
            result=self.client_get_quotes(exchange,symbol,current_dt_str,next_dt_str)
            
            day_quotes=pd.read_csv(f'data/{symbol}_quotes.csv')
            day_quotes.to_csv(f'data/{symbol}_quotes_{current_dt.date()}.csv')
            del day_quotes
            gc.collect()
            print(f"Saved Quotes for {symbol} on {current_dt}")
            current_dt=current_dt+timedelta(days=1)
        
        self.conn.close()
        return
            
    def get_trades_range(self,exchange,symbol,start,end):
        start=pd.to_datetime(start)
        end=pd.to_datetime(end)
        
        current_dt=start
        
        while current_dt<end:
            current_dt_str=str(current_dt.date())
            next_dt_str=str((current_dt+timedelta(days=1)).date())
            result=self.client_get_trades(exchange,symbol,current_dt_str,next_dt_str)
            
            day_trades=pd.read_csv(f'data/{symbol}_trades.csv')
            if len(day_trades)>0:
                day_trades.to_csv(f'data/{symbol}_trades_{current_dt.date()}.csv')
                print(f"Saved trades for {symbol} on {current_dt}")
                
            del day_trades
            gc.collect()
            
            
            current_dt=current_dt+timedelta(days=1)
        
        self.conn.close()
        return