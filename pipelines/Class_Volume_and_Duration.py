#define a class that generate predictor variables according to page 12 of paper in case of concadinating them into pipeline
# !Remark: it does not generate a new line of feature to the dataframe since it may not always valid for different parameters sets.
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
    
    def fit(self, df, y=None):
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