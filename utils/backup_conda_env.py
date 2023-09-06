
import paramiko
from scp import SCPClient
from dotenv import load_dotenv
import os

def backup_conda_environment():
    # load the contents of the .env file into the environment
    load_dotenv()

    # read the credentials from the environment variables
    host = os.getenv("host")
    server_user = os.getenv("server_user")
    server_password = os.getenv("server_password")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    scp = None

    try:
        ssh.connect(host, username=server_user, password=server_password)

        # Execute a command to backup the conda environment to a .yml file
        command = f'source /opt/anaconda3/etc/profile.d/conda.sh && conda activate query_user && conda env export > environment.yml'
        stdin, stdout, stderr = ssh.exec_command(command)

        # Wait for the command to complete
        stdout.channel.recv_exit_status()

        # SCPCLient takes a paramiko transport as an argument
        scp = SCPClient(ssh.get_transport())

        # fetch the remote file 'environment.yml' (conda environment backup) 
        scp.get('environment.yml', 'environment.yml')

        print("Conda environment backed up and downloaded successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if scp is not None:
            # close the SCP session
            scp.close()
        ssh.close()

# Call the function to backup and download the conda environment
backup_conda_environment()
