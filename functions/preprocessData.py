import warnings
from datetime import timedelta

import pandas as pd

warnings.filterwarnings("ignore")
from sklearn.base import BaseEstimator, TransformerMixin
from sortedcollections import OrderedSet


class PreprocessData(BaseEstimator, TransformerMixin):
    def __init__(self, dropped_after_hourse=True, droped_irregular_hours=True):
        self.dropped_after_hourse = dropped_after_hourse
        self.droped_irregular_hours = droped_irregular_hours

    def fit(self, X, y=None):
        return self

    def generate_mox_identifier(self, df):
        """Generate MOX Identifier"""
        # get participant timestamps
        participant_timestamps = df.index
        # convert timestamps to float
        fl_participant_timestamps = [float(ts.timestamp() * 1000) for ts in participant_timestamps]
        # generate unique index for each timestamp
        time_mox_mapping = {ts: mox_idx for mox_idx, ts in enumerate(OrderedSet(fl_participant_timestamps))}
        # generate the mox_identifiers
        mox_identifiers = [time_mox_mapping[t] for t in fl_participant_timestamps]

        df["MOX_Identifiers"] = mox_identifiers

        return df

    # generates individual OFI event at each quote update
    def generate_OFI_event(self, df):
        df["OFI_Event"] = (
            df.Bid_Price.ge(df.Bid_Price.shift()).astype(int) * df.Bid_Size
            - df.Bid_Price.le(df.Bid_Price.shift()).astype(int) * df.Bid_Size.shift()
            - df.Offer_Price.le(df.Offer_Price.shift()).astype(int) * df.Offer_Size
            + df.Offer_Price.ge(df.Offer_Price.shift()).astype(int) * df.Offer_Size.shift()
        )
        return df

    # generates OFI over a time window (default 15s)
    def generate_OFI(self, df, time_window="15s"):
        df["OFI"] = df["OFI_Event"].rolling(time_window, min_periods=1).sum()
        return df

    # generates midprice at each quote update
    def generate_midprice(self, df):
        df["Mid_Price"] = (df["Bid_Price"] + df["Offer_Price"]) / 2
        return df

    def transform(self, X):
        cols = X.columns
        if "Unnamed: 0" in cols:
            X.drop(["Unnamed: 0"], inplace=True, axis=1)
        if "Time" in cols:
            X.drop(["Time"], inplace=True, axis=1)

        # parse date and participant timestamp
        X["Date"] = pd.to_datetime(X["Date"])
        X["Participant_Timestamp"] = pd.to_datetime(
            X["Participant_Timestamp"].astype(str).str.zfill(15), format="%H%M%S%f"
        )

        # convert datetime to index
        X["Participant_Timestamp"] = X["Date"].apply(lambda x: x) + X["Participant_Timestamp"].apply(
            lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second, microseconds=x.microsecond)
        )
        X.index = X["Participant_Timestamp"].values

        # remove rows of all NA
        X = X.dropna(axis=1, how="all")

        # remove invalid trades
        X.drop(X[X["Trade_Price"] < 0].index, inplace=True)
        X.drop(X[X["Trade_Volume"] < 0].index, inplace=True)
        X.drop(X[X["Trade_Reporting_Facility"] == "D"].index, inplace=True)

        # remove invalid quotes
        X.drop(X[X["Bid_Price"] < 0].index, inplace=True)
        X.drop(X[X["Offer_Price"] < X["Bid_Price"]].index, inplace=True)

        # drop after hours if specified
        if self.dropped_after_hourse:
            afterhours_idx = []
            for t in X.index:
                str_t = t.strftime("%H:%M:%S")
                if str_t < "09:00:00" or str_t > "16:00:00":
                    afterhours_idx.append(t)
            X.drop(afterhours_idx, inplace=True)

        # remove first and last 15 minutes of regular trading hours
        if self.droped_irregular_hours:
            irregular_idx = []
            for t in X.index:
                str_t = t.strftime("%H:%M:%S")
                if str_t < "09:45:00" or str_t > "15:45:00":
                    irregular_idx.append(t)
            X.drop(irregular_idx, inplace=True)

        # sort data according to index
        X = X.sort_index()

        # generate mid price
        X = self.generate_midprice(X)

        # assign MOX Identifiers
        X = self.generate_mox_identifier(X)

        # generate OFI event
        X = self.generate_OFI_event(X)

        return X
