import os
from pathlib import Path

import paramiko
from dotenv import load_dotenv
from scp import SCPClient


def get_trades(symbols, start_date, end_date, row_limit):
    load_dotenv()

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
            command = f'source /home/{server_user}/anaconda3/etc/profile.d/conda.sh && conda activate query_user && cd TAQNYSE-Clickhouse && cd server_helpers && \
                python3 trade_server_helpers.py "{db_user}" "{db_pass}" "{symbol}" "{start_date}" "{end_date}" "{row_limit}"'
            stdin, stdout, stderr = ssh.exec_command(command)

            print(f"Output for symbol {symbol}:")
            for line in stdout:
                print("... " + line.strip("\n"))

            print(f"Errors for symbol {symbol}:")
            for line in stderr:
                print("... " + line.strip("\n"))

            scp = SCPClient(ssh.get_transport())

            script_path = Path(__file__).resolve().parent
            project_root = script_path.parent
            save_path = (
                project_root / "data" / f'trades_{symbol}_{start_date.replace("-", "")}-{end_date.replace("-", "")}.csv'
            )
            save_path.parent.mkdir(parents=True, exist_ok=True)

            scp.get("TAQNYSE-Clickhouse/trade_results.csv", str(save_path))

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if scp is not None:
            scp.close()
        ssh.close()


def get_quotes(symbols, start_date, end_date, row_limit):
    load_dotenv()

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
            command = f'source /home/{server_user}/anaconda3/etc/profile.d/conda.sh && conda activate query_user && cd TAQNYSE-Clickhouse && cd server_helpers && \
                python3 quote_server_helpers.py "{db_user}" "{db_pass}" "{symbol}" "{start_date}" "{end_date}" "{row_limit}"'
            stdin, stdout, stderr = ssh.exec_command(command)

            print(f"Output for symbol {symbol}:")
            for line in stdout:
                print("... " + line.strip("\n"))

            print(f"Errors for symbol {symbol}:")
            for line in stderr:
                print("... " + line.strip("\n"))

            scp = SCPClient(ssh.get_transport())

            script_path = Path(__file__).resolve().parent
            project_root = script_path.parent
            save_path = (
                project_root / "data" / f'quotes_{symbol}_{start_date.replace("-", "")}-{end_date.replace("-", "")}.csv'
            )
            save_path.parent.mkdir(parents=True, exist_ok=True)

            scp.get("TAQNYSE-Clickhouse/quote_results.csv", str(save_path))

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if scp is not None:
            scp.close()
        ssh.close()
