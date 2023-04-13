import getData
import preprocessData
from sklearn.pipeline import make_pipeline


def get_clean_data(symbol="AAPL", start_date="2021-08-03", end_date="2021-08-04"):

    # get data
    allData = getData.get_data(symbol, start_date, end_date)

    # intialize preprocessing pipeline
    process_pipeline = make_pipeline(preprocessData.PreprocessData())

    # clean data
    df_clean = process_pipeline.fit_transform(allData)

    return df_clean
