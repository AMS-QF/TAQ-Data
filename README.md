# TAQ-Query-Scripts
Included are the client side-scripts for access to the TAQ-Clickhouse Database

### Setup

Create a directory called **data** in the root directory of the repo

Create an .env file in the root directory of the repo with the following variables:
 
    host="ppolak5.ams.stonybrook.edu."
    server_user= 
    server_password= 
    db_user= 
    db_pass=

Create a conda env using the environment.yml file

    conda env create -f environment.yml


Directories:
**scripts**
    - query_helpers: scripts to ssh into the server and clickhouse database executing queries from database and returning df to data directory
    - preprocess: scripts to preprocess data from db to user-friendly format
    - load_data: scripts to load data from data directory into user-specific directory
        


**Do not republish, distribute or utilize the sample data found in this repo for any purposes other than academic research**
