import paramiko
from scp import SCPClient
from dotenv import load_dotenv
import os

def get_trades(symbols, start_date, end_date, row_limit):
    # load the contents of the .env file into the environment
    load_dotenv()

    # read the credentials from the environment variables
    host = os.getenv("host")
    server_user = os.getenv("server_user")
    server_password = os.getenv("server_password")
    db_user = os.getenv("db_user")
    db_pass = os.getenv("db_pass")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    scp = None

    try:
        ssh.connect(host, username=db_user, password=server_password)

        for symbol in symbols:
            # Execute a command to change directory and list files
            command = f'source /opt/anaconda3/etc/profile.d/conda.sh && conda activate query_user && cd TAQNYSE-Clickhouse && cd server_helpers && \
            python3 trade_server_helpers.py "{db_user}" "{db_pass}" "{symbol}" "{start_date}" "{end_date}" "{row_limit}"'
            stdin, stdout, stderr = ssh.exec_command(command)



            print(f"Output for symbol {symbol}:")
            for line in stdout:
                print('... ' + line.strip('\n'))

            print(f"Errors for symbol {symbol}:")
            for line in stderr:
                print('... ' + line.strip('\n'))

            # SCPCLient takes a paramiko transport as an argument
            scp = SCPClient(ssh.get_transport())

            # fetch the remote file 'trade_results.csv' from the directory 'TAQNYSE-Clickhouse'
            # and save it to the data directory in the pipelines folder
            scp.get('TAQNYSE-Clickhouse/trade_results.csv', f'../pipelines/data/trades_{symbol}_{start_date.replace("-", "")}-{end_date.replace("-", "")}.csv')

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if scp is not None:
            # close the SCP session
            scp.close()
        ssh.close()

def get_quotes(symbols, start_date, end_date, row_limit):
    # load the contents of the .env file into the environment
    load_dotenv()

    # read the credentials from the environment variables
    host = os.getenv("host")
    server_user = os.getenv("server_user")
    server_password = os.getenv("server_password")
    db_user = os.getenv("db_user")
    db_pass = os.getenv("db_pass")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    scp = None

    try:
        ssh.connect(host, username=server_user, password=server_password)

        for symbol in symbols:
            # Execute a command to change directory and list files
            command = f'source /home/{server_user}/anaconda3/etc/profile.d/conda.sh && conda activate query_user && cd TAQNYSE-Clickhouse && cd server_helpers && \
                python3 quote_server_helpers.py "{server_user}" "testpassword321" "{symbol}" "{start_date}" "{end_date}" "{row_limit}"'
            stdin, stdout, stderr = ssh.exec_command(command)

            print(f"Output for symbol {symbol}:")
            for line in stdout:
                print('... ' + line.strip('\n'))

            print(f"Errors for symbol {symbol}:")
            for line in stderr:
                print('... ' + line.strip('\n'))

            # SCPCLient takes a paramiko transport as an argument
            scp = SCPClient(ssh.get_transport())

            # fetch the remote file 'trade_results.csv' from the directory 'TAQNYSE-Clickhouse'
            # and save it to the data directory in the pipelines folder
            scp.get('TAQNYSE-Clickhouse/quote_results.csv', f'../pipelines/data/quotes_{symbol}_{start_date.replace("-", "")}-{end_date.replace("-", "")}.csv')


    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if scp is not None:
            # close the SCP session
            scp.close()
        ssh.close()

def get_ref(symbols, start_date, end_date, row_limit):
    # load the contents of the .env file into the environment
    load_dotenv()

    # read the credentials from the environment variables
    host = os.getenv("host")
    server_user = os.getenv("server_user")
    server_password = os.getenv("server_password")
    db_user = os.getenv("db_user")
    db_pass = os.getenv("db_pass")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    scp = None

    try:
        ssh.connect(host, username=server_user, password=server_password)

        for symbol in symbols:
            # Execute a command to change directory and list files
            command = f'source root/anaconda3/conda.sh && conda activate query_user && cd TAQNYSE-Clickhouse && cd server_helpers && \
                python3 refdata_server_helpers.py "{server_user}" "{server_password}" "{symbol}" "{start_date}" "{end_date}" "{row_limit}"'
            stdin, stdout, stderr = ssh.exec_command(command)

            print(f"Output for symbol {symbol}:")
            for line in stdout:
                print('... ' + line.strip('\n'))

            print(f"Errors for symbol {symbol}:")
            for line in stderr:
                print('... ' + line.strip('\n'))

            # SCPCLient takes a paramiko transport as an argument
            scp = SCPClient(ssh.get_transport())

            # fetch the remote file 'trade_results.csv' from the directory 'TAQNYSE-Clickhouse'
            # and save it to the data directory in the pipelines folder
            scp.get('TAQNYSE-Clickhouse/refdata_results.csv', f'../pipelines/data/ref_{symbol}_{start_date.replace("-", "")}-{end_date.replace("-", "")}.csv')

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if scp is not None:
            # close the SCP session
            scp.close()
        ssh.close()