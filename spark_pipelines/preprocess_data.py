from .generators import generate_mox_identifier, generate_trade_side
from sklearn.base import BaseEstimator, TransformerMixin


class PreprocessData(BaseEstimator, TransformerMixin):
    def __init__(self):
        return

    def fit(self, X, y=None):
        self.invalid_idx = []

        # invalid trades
        self.invalid_idx.extend(X[X["Trade_Price"] < 0].index)
        self.invalid_idx.extend(X[X["Trade_Volume"] < 0].index)
        self.invalid_idx.extend(X[X["Trade_Reporting_Facility"] == "D"].index)

        # invalid quotes
        self.invalid_idx.extend(X[X["Bid_Price"] < 0].index)
        self.invalid_idx.extend(X[X["Offer_Price"] < X["Bid_Price"]].index)
        return self

    def transform(self, X):
        # drop invalid trades and quotes
        X.drop(self.invalid_idx, inplace=True)

        # assign mox identifier
        X["MOX"] = generate_mox_identifier(X["Participant_Timestamp"])

        # labeling natural best bid/ask or LMQ as valid (True), otherwise False
        valid_quotes = ~X.duplicated(subset=["MOX"], keep="last")
        X["Valid_Quotes"] = X["Is_Quote"] & valid_quotes  # Vectorization

        # assign trading directions
        X["Trade_Sign"] = generate_trade_side(X["Trade_Price"])

        # assign time in float (measure in seconds)
        X["Participant_Timestamp_f"] = X["Participant_Timestamp"].apply(lambda t: t.timestamp())

        return X
