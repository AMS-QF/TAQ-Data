def preprocess_features(df_clean, num_rows=500):
    # followed by preprocess pipeline, after we get clean data
    df_clean["Participant_Timestamp_f"] = df_clean["Participant_Timestamp"].apply(
        lambda t: t.timestamp()
    )  # convert timestamp into float format
    # create a test dataframe, including the head 500 rows.
    test_copy = df_clean[:num_rows].copy()
    test_copy = test_copy.sort_values(by=["Participant_Timestamp"])

    # generate two columns one is trade volume and one is number of trades from the beginning of the dataframe to each row, thorough out the whole dataframe

    test_copy["Cumulative_Trade_Volume"] = test_copy["Participant_Timestamp_f"].apply(
        lambda t: sum(
            test_copy.fillna(0)[
                test_copy["Participant_Timestamp_f"].between(
                    test_copy["Participant_Timestamp_f"][0], t, inclusive="left"
                )
            ]["Trade_Volume"]
        )
    )

    test_copy["Cum_Trades"] = (test_copy.fillna(0)["Trade_Price"] != 0).cumsum()

    return test_copy
