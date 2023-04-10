import getData
from preprocessData import PreprocessData

from sklearn.pipeline import make_pipeline

def get_clean_data(symbol="AAPL", start_date="2020-01-01", end_date="2020-01-02"):

    # get data
    allData = getData.get_data(symbol, start_date, end_date)

    # intialize preprocessing pipeline
    process_pipeline = make_pipeline(PreprocessData())

    # clean data
    df_clean = process_pipeline.fit_transform(allData)

    return df_clean
