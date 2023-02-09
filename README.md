# TAQ-Query-Scripts
Included are the client side-scripts for access to the TAQ-Clickhouse Database remotely. Functionality is also included to clean and load the data into a user-specific directory.

### Setup

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

- Create a conda env using the environment.yml file

    `conda env create -f environment.yml`

- Setup Pre-commit Hooks for formatting

    `conda install isort autoflake black pre-commit`
    `pre-commit install`


### Example Usage

Here we will show how to access the TAQ-Clickhouse database remotely, clean data, and generate features from the data with one command.

Command has the following format:

`python run_jobs.py  --symbol <symbol> --start_date <start_date> --end_date <end_date>`

Example:

```python run_jobs.py  --symbol "AAPL" --start_date "2020-01-01" --end_date "2020-01-03"```

Notes
- Sometimes the connection will be reset by peer; Hitting enter in the terminal will restart the connection
- To check whether a query has finished you can login into the TAQ server with a single terminal and search for running jobs (within the docker container)
        
**Do not republish, distribute or utilize the sample data found in this repo for any purposes other than academic research**
