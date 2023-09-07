import paramiko
from scp import SCPClient
from dotenv import load_dotenv
import os

def install_conda_environment():
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

        # Upload the local file 'environment.yml' to the server
        scp = SCPClient(ssh.get_transport())
        scp.put('environment.yml', 'environment.yml')
        
        # Execute a command to install the conda environment from the .yml file
        command = 'source /opt/anaconda3/etc/profile.d/conda.sh && conda env create -f environment.yml'
        stdin, stdout, stderr = ssh.exec_command(command)

        # Wait for the command to complete
        stdout.channel.recv_exit_status()

        print("Conda environment installed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if scp is not None:
            # close the SCP session
            scp.close()
        ssh.close()

# Call the function to install the conda environment
install_conda_environment()
