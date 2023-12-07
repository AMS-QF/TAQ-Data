# TAQ-Query-Scripts
Included are the client side-scripts for access to the TAQ-Clickhouse Database remotely with Python.

Instructions for access through the SQL UI DBeaver are  included in Accessing the TAQ-Clickhouse Database PDF

### Local Setup
- Pull the repo 'TAQ-Query-Scripts-Public' from Github
- Setup the conda environment using the environment.yml file
- Create a directory called `data` in the root directory of the repo
- Create subdirectories called `raw_data` and `features` in the data directory of the repo
- Create an .env file in the root directory of the repo with the following variables:

```
    host="ppolak5.ams.stonybrook.edu."
    server_user= 
    server_password= 
    db_user= 
    db_pass=
```

### Conda Tips
- `environment.yml` is a file that contains all the dependencies for the conda environment
    - Will have to update path name on this yml file
- `query_user_environment.yml` is a file that contains all the dependencies for the conda environment for the query user on the server
- To install datatable it is required to install from source repo using `pip install git+https://github.com/h2oai/datatable`
- If conda environment is not working, try to update conda using `conda update -n base -c defaults conda`. 

### Sample Data
Sample data of 1000 trade and quote samples are included within the `sample_data` directory. The sample data is stored as gzip compressed csv files. All tutorials  will use this sample data.

Feel free to create a directory for your own research called `personal_research` in the root directory of the repo. This directory is ignored by git and can be used to store your own scripts and data
        
**Do not republish, distribute or utilize the sample data found in this repo for any purposes other than academic research**


### Internally Setting up a new user
- Requires server username and password with DBUserDev permissions- contact Victor Poon
- Server user groups requried - docker, condausers, TAQDatabaseCoreDev, TAQDatabaseUsers
- Requires Database username and password
- Database user groups QUERY_USER, taq_group


More detailed sketch
- TAQ-Query-Scripts triggers conda env and execute scripts in TAQNYSE-Clickhouse to programmatically query from DB
- Save file to local directory and transfer file to local machine through SCP