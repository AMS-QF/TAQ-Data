# import the necessary libraries and credentials
import os
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()  # load the contents of the .env file into the environment

# read the credentials from the environment variables
host = os.getenv("host")
server_user = os.getenv("server_user")
server_password = os.getenv("server_password")
db_user = os.getenv("db_user")
db_pass = os.getenv("db_pass")

# use the credentials to connect to the database
client = clickhouse_connect.get_client(host=host, port=3306, username=db_user, password=db_pass)

def sql_query(query):
    """Execute a SQL query and return the results as a list of dictionaries."""
    results = client.command(query)
    return results

        
"""Example Queries

# select apple trades from January of 2017 to April of 2017
query = "SELECT * FROM TRADESDB.trades2017view WHERE (Symbol = 'AAPL') AND (Date = '2017-01-05')"

"""

