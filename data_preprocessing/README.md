
### Data Preprocessing

This directory contains the scripts required to remote access TAQ data and clean locally via command line

### Local Configuration



#### Data Access
To access the data remotely,  run the following command:

```python data_preprocessing/load_data.py  --symbol <symbol> --start_date <start_date> --end_date <end_date>```

Example:

```python data_preprocessing/load_data.py  --symbol AAPL --start_date 2021-01-01 --end_date 2021-01-02```


### Data Cleaning

To clean the data locally (from this directory), run the following command:

```python clean_data.py --input_file <input_file> ```

Example

```python clean_data.py --input_file ../data/raw_data/2020-01-02/F_quotes.csv```