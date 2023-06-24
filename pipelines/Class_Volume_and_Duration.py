""" Feature Generation Class for Pipeline """

from sklearn.base import BaseEstimator, TransformerMixin

from pipelines.lookback_interval import compute_lookback_interval


class Volume_and_Duration(BaseEstimator, TransformerMixin):
    """Volume and Duration Class generates volume and duration features for a given time window.
    Parameters
    df : pandas.DataFrame: The input dataframe.
    t : datetime.datetime: The input timestamp.
    delta1 : int: The first input parameter.
    delta2 : int: The second input parameter.
    m : str: The input mode.
    """

    def __init__(self, df, t, delta1, delta2, mode):
        self.df = df
        self.t = t
        self.delta1 = delta1
        self.delta2 = delta2
        self.mode = mode

    def fit(self, X, y=None):
        return self

    def compute_breadth(self):
        """Compute the breadth of the lookback interval."""
        return compute_lookback_interval(self.df, self.t, self.delta1, self.delta2, self.mode)["Trade_Price"].count()

    def compute_inmediacy(self):
        """Compute the inmediacy of the lookback interval."""
        return (
            len(
                compute_lookback_interval(self.df, self.t, self.delta1, self.delta2, self.mode)[
                    "Participant_Timestamp_f"
                ].value_counts()
            )
            / self.compute_breadth()
        )

    def compute_volume(self):
        """Compute the volume of the lookback interval."""
        return compute_lookback_interval(self.df, self.t, self.delta1, self.delta2, self.mode)["Trade_Volume"].sum()

    def compute_avg_volume(self):
        """Compute the average volume of the lookback interval."""
        return self.compute_volume() / self.compute_breadth()

    def compute_max_volume(self):
        """Compute the maximum volume of the lookback interval."""
        return compute_lookback_interval(self.df, self.t, self.delta1, self.delta2, self.mode)["Trade_Volume"].max()
