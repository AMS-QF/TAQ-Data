from datetime import datetime, timedelta

import pandas as pd


def clean_trades(trades):
    if "Time" not in trades.columns:
        trades = trades.rename(columns={trades.columns[1]: "Time"})
    # parse date and pt
    trades["date"] = trades["Time"].apply(lambda x: str(x[:11]))
    trades.index = trades["date"] + trades["Participant_Timestamp"].astype(str)
    trades = trades.drop(columns=["Participant_Timestamp"])
    trades = trades.rename(columns={trades.columns[0]: "Participant_Timestamp", "Time": "SIP_Timestamp"})

    trades.index = trades.index.str[:-3]
    time = pd.Series(pd.to_datetime(trades.index.str[11:].str.zfill(12), format="%H%M%S%f"))
    date = pd.Series(pd.to_datetime(trades.index.str[:11]))
    trades.index = date.apply(lambda x: x) + time.apply(
        lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second, microseconds=x.microsecond)
    )

    trades = trades.sort_index()

    trades = trades.dropna(axis=1, how="all")

    trades = trades[trades["Trade_Volume"] > 0]

    trades = trades[trades["Trade_Price"] > 0]

    grouped_trades = trades.groupby("date").groups

    # drop trade data outside of market hours

    for day in grouped_trades.keys():
        subset = trades[trades["date"] == day]
        grouped_trades[day] = subset[subset.index < datetime.strptime(f"{day} 16:00:00", "%Y-%m-%d %H:%M:%S")]
        grouped_trades[day] = subset[subset.index > datetime.strptime(f"{day} 09:30:00", "%Y-%m-%d %H:%M:%S")]

    new_trades = pd.concat(list(grouped_trades.values())).sort_index()

    return new_trades


def clean_quotes(quotes, drop_after_hours=True):
    if "Time" not in quotes.columns:
        quotes = quotes.rename(columns={quotes.columns[1]: "Time"})
    # parse date and pt
    quotes["date"] = quotes["Time"].apply(lambda x: str(x[:11]))
    quotes.index = quotes["date"] + quotes["Participant_Timestamp"].astype(str)
    quotes = quotes.drop(columns=["Participant_Timestamp", "date"])
    quotes = quotes.rename(columns={quotes.columns[0]: "Participant_Timestamp", "Time": "SIP_Timestamp"})

    # convert pt to valid ts
    quotes.index = quotes.index.str[:-3]
    time = pd.Series(pd.to_datetime(quotes.index.str[11:].str.zfill(12), format="%H%M%S%f"))
    date = pd.Series(pd.to_datetime(quotes.index.str[:11]))
    quotes.index = date.apply(lambda x: x) + time.apply(
        lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second, microseconds=x.microsecond)
    )

    quotes = quotes.sort_index()

    quotes = quotes.dropna(axis=1, how="all")

    quotes = quotes[quotes["Offer_Price"] > quotes["Bid_Price"]]  # removed quotes with invalid spreads
    quotes = quotes[quotes["Bid_Price"] > 0]  # bid and offer price >0

    # drop after hours for quotes, preserve if want to prepend lob
    if drop_after_hours:
        quotes["date"] = quotes.index.date

        grouped_quotes = quotes.groupby("date").groups

        # drop trade data outside of market hours

        for day in grouped_quotes.keys():
            subset = quotes[quotes["date"] == day]
            grouped_quotes[day] = subset[subset.index < datetime.strptime(f"{day} 16:00:00", "%Y-%m-%d %H:%M:%S")]
            grouped_quotes[day] = subset[subset.index > datetime.strptime(f"{day} 09:30:00", "%Y-%m-%d %H:%M:%S")]
        new_quotes = pd.concat(list(grouped_quotes.values())).sort_index()

        return new_quotes
    else:
        return quotes


# grossman transcnction costs
def chunk_clean(path, quotes=True):
    counter = 1

    # remove .csv if present
    if ".csv" in path:
        path = path.replace(".csv", "")

    path_list = []

    for df in pd.read_csv(f"{path}.csv", iterator=True, chunksize=100000, low_memory=False):

        if quotes:
            cleaned_data = clean_quotes(df)
        else:
            cleaned_data = clean_trades(df)

        if counter == 1:
            pd.DataFrame(columns=cleaned_data.columns).to_csv(f"{path}_cleaned.csv", index=False)

        print(f"{100000*counter} rows cleaned")

        cleaned_data.to_csv(f"{path}_cleaned.csv", mode="a", header=False)
        cleaned_path = f"{path}_cleaned.csv"
        path_list.append(cleaned_path)
        counter += 1

    return path_list
