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

Here we will show how to access the TAQ-Clickhouse database remotely and generate features from the data with one command.

Command has the following format:

`python run_jobs.py --exchange <exchange> --symbol <symbol> --start_date <start_date> --end_date <end_date>`

Example:

```python run_jobs.py --exchange N --symbol AAPL --start_date 2021-01-01 --end_date 2021-01-31```
        
**Do not republish, distribute or utilize the sample data found in this repo for any purposes other than academic research**
