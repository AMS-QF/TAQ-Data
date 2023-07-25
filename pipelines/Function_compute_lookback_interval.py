def compute_lookback_interval(df, t, delta1, delta2, mode):
    """Compute the lookback interval for a given time window.
    Parameters
    df : pandas.DataFrame: The input dataframe.
    t : datetime.datetime: The input timestamp.
    delta1 : int: The first input parameter.
    delta2 : int: The second input parameter.
    mode : str: The input mode.
    """

    assert delta1 < delta2, "Invalid input value of delta1 and delta2"

    # save start and end time of the dataset
    start = df["Participant_Timestamp_f"][0]
    end = df["Participant_Timestamp_f"][-1]

    t = t.timestamp()
    if mode == "calendar":
        time = pd.Series([t, t - delta1, t - delta2])

        # check whether three timestamps are located in the range of the dataset
        assert time.between(start, end, inclusive="both").all() == True, "Invalid Time Input"

        # return part of the dataframe that located in the time interval for further processing
        backward_window = df[df["Participant_Timestamp_f"].between(t - delta2, t - delta1, inclusive="right")]

        return backward_window

    elif mode == "transaction":
        time = pd.Series([t])
        # check whether the input timestamp are located in the range of the dataset
        assert time.between(start, end, inclusive="both").all() == True, "Invalid Time Input"

        # cut off the data later than the input timestamp
        filtered_data = df[df["Participant_Timestamp_f"] <= t]

        # check whether the input numbe of transactions are integers
        assert isinstance(delta1, int) & isinstance(delta2, int), "Please Input delta1 and delta2 as Integers"

        # check whether the input number of transactions exceeds the largest possible value till the input timestamp T
        assert delta1 & delta2 <= filtered_data["Cum_Trades"][-1], "Invalid input value of delta1 and delta2"

        # generate a dataframe including all timestamps t such that number of transactions among (t, T] are between delta1 and delta2 (quote timestamps included)
        backward_window = df[
            df["Cum_Trades"].between(
                filtered_data["Cum_Trades"][-1] - delta2,
                filtered_data["Cum_Trades"][-1] - delta1,
                inclusive="right",
            )
        ]

        return backward_window

    elif mode == "volume":
        time = pd.Series([t])
        # check whether the input timestamp are located in the range of the dataset
        assert time.between(start, end, inclusive="both").all() == True, "Invalid Time Input"

        filtered_data = df[df["Participant_Timestamp_f"] <= t]

        # check whether the input number of trade volume exceeds the largest possible value till the input timestamp T
        assert (
            delta1 & delta2 <= filtered_data["Cumulative_Trade_Volume"][-1]
        ), "Invalid input value of delta1 and delta2"

        # generate a dataframe including all timestamps t such that trade volume among (t, T] are between delta1 and delta2 (quote timestamps included)
        backward_window = df[
            df["Cumulative_Trade_Volume"].between(
                filtered_data["Cumulative_Trade_Volume"][-1] - delta2,
                filtered_data["Cumulative_Trade_Volume"][-1] - delta1,
                inclusive="right",
            )
        ]

        return backward_window

    else:
        raise ValueError("Invalid Mode Input")