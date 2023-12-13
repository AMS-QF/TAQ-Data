from .generators import convertParticipantTimestamp
from sklearn.base import BaseEstimator, TransformerMixin


class CleanData(BaseEstimator, TransformerMixin):
    def __init__(self, dropped_after_hourse=True, droped_irregular_hours=True):
        self.dropped_after_hourse = dropped_after_hourse
        self.droped_irregular_hours = droped_irregular_hours

    def fit(self, X, y=None):
        if "Participant_Timestamp" and "Date" in X.columns:
            print("test")
            self.part_timestamp = convertParticipantTimestamp(X["Participant_Timestamp"], X["Date"])
        else:
            self.part_timestamp = X["Participant_Timestamp"]
        return self

    def transform(self, X):
        # remove rows of all NA
        X = X.dropna(axis=0, how="all")
        X.drop(
            ["Unnamed: 0", "Time", "Date", "YearMonth"],
            axis=1,
            inplace=True,
            errors="ignore",
        )
    
        X["Participant_Timestamp"] = self.part_timestamp
        X.index = self.part_timestamp

        # drop after hours if specified
        if self.dropped_after_hourse:
            after_idx = []
            for t in X.index:
                str_t = t.strftime("%H:%M:%S")
                if str_t < "09:00:00" or str_t > "16:00:00":
                    after_idx.append(t)
            X.drop(after_idx, inplace=True)

        # drop irregular hours if specified
        if self.droped_irregular_hours:
            irreg_idx = []
            for t in X.index:
                str_t = t.strftime("%H:%M:%S")
                if str_t < "09:15:00" or str_t > "15:45:00":
                    irreg_idx.append(t)
            X.drop(irreg_idx, axis=0, inplace=True)

        X = X.sort_index()

        return X
