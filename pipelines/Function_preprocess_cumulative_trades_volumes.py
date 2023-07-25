import pandas as pd
import numpy as np

def preprocess_cumulative_trades_volumes(df):
    # followed by preprocess pipeline, after we get clean data
    df["Participant_Timestamp_f"] = df["Participant_Timestamp"].apply(
        lambda t: t.timestamp()
    )  # convert timestamp into float format
    # create a test dataframe, including the head 500 rows.
    
    df = df.sort_values(by=["Participant_Timestamp"])

    # generate two columns one is trade volume and one is number of trades from the beginning of the dataframe to each row, thorough out the whole dataframe

    df["Cumulative_Trade_Volume"] = df["Participant_Timestamp_f"].apply(
        lambda t: sum(
            df.fillna(0)[
                df["Participant_Timestamp_f"].between(
                    df["Participant_Timestamp_f"][0], t, inclusive="left"
                )
            ]["Trade_Volume"]
        )
    )

    df["Cum_Trades"] = (df.fillna(0)["Trade_Price"] != 0).cumsum()

    return df