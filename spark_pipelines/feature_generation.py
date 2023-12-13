from generators import generate_transaction_return, np, parent_generator_ret_imb
from sklearn.base import BaseEstimator, TransformerMixin


class FeatureGeneration(BaseEstimator, TransformerMixin):
    def __init__(self, params=None):
        self.params = params
        return

    def fit(self, df, y=None):
        df["Trade_Volume"] = df["Trade_Volume"].apply(lambda t: t if not np.isnan(t) else 0)
        df.reset_index(drop=True, inplace=True)

        return self

    def transform(self, df):
        # 1.generating response variable

        assert "return_span" in self.params.keys(), "return_span is not in params"
        assert "clock_mode" in self.params.keys(), "clock_mode is not in params"

        # transaction return
        df["Trans_Return"] = generate_transaction_return(df, self.params["return_span"], self.params["clock_mode"])

        # 2.generating predictors

        # Volume and Duration

        # Return and Imbalance
        df = parent_generator_ret_imb(df, self.params["deltas"], mode="calendar")

        # Speed and Cost

        return df
