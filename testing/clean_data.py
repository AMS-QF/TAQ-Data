# sys main path
import sys

sys.path.append("../")
from sklearn.pipeline import make_pipeline

# import functions directly
from testing.fetch_data import get_data
from testing.preprocess_data import Data_Preprocessor


def get_clean_data(symbol="AAPL", start_date="2021-08-03", end_date="2021-08-04"):

    # get data
    allData = get_data(symbol, start_date, end_date)

    # initialize preprocessing pipeline
    process_pipeline = make_pipeline(Data_Preprocessor())

    # clean data
    df_clean = process_pipeline.fit_transform(allData)

    return df_clean
