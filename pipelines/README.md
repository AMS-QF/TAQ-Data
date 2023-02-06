 ### Pipeline for TAQ Features

 Given cleaned TAQ data; we will want to construct pipelines for a variet of tasks. Some of which include

 - [] Event Reconstruction
 - [] Feature Normalization and Selection


### Event Reconstruction

Event reconstruction is the process of reconstructing the events that occurred during a given time period. This is done by aggregating the TAQ data into dataframe

An example of this is shown below:

```python event_reconstruction.py --input_files ../data/raw_data/2020-01-02/AMZN_trades_cleaned.csv ../data/raw_data/2020-01-02/AMZN_quotes_cleaned.csv```
