# TAQ-Data
Included are the client side scripts for access to the TAQ-Clickhouse Database remotely with Python.

Instructions for access through the SQL UI DBeaver are  included in Accessing the TAQ-Clickhouse Database PDF

### Local Setup
- Pull the repo 'TAQ-Query-Scripts-Public' from GitHub
- Setup the conda environment using the environment.yml file
- Create a directory called `data` in the root directory of the repo
- Create a .env file in the root directory of the repo with the following variables:

```
    host="ppolak5.ams.stonybrook.edu."
    server_user= 
    server_password= 
    db_user= 
    db_pass=
```

### Conda Tips
- `environment.yml` is a file that contains all the dependencies for the conda environment
    - Will have to update the path name on this yml file
- `query_user_environment.yml` is a file that contains all the dependencies for the conda environment for the query user on the server
- If the conda environment is not working, try to update conda using `conda update -n base -c defaults conda`. 


Feel free to create a directory for your research called `personal_research` in the root directory of the repo. This directory is ignored by Git and can be used to store your scripts and data
        
**Do not republish, distribute, or utilize the sample data found in this repo for any purposes other than academic research**


