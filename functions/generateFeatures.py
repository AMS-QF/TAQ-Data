############################################################
# note: this function is not yet operational due to some   #
# bugs. Please refrain from using it to generate features. #
# It will be updated soon.                                 #
############################################################

import warnings

import numpy as np
import pandas as pd
from sklearn.compose import make_column_transformer
from sklearn.pipeline import make_pipeline

warnings.filterwarnings("ignore")
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import make_column_transformer

from feature_generation.generators import parent_generator


class GenerateTradeFeatures(BaseEstimator, TransformerMixin):
    def __init__(self, features):
        self.features = features
        return

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        trade_features = X.columns
        for f in self.features:
            if f not in trade_features:
                X, _ = parent_generator(X, f)
        return X


class GenerateQuoteFeatures(BaseEstimator, TransformerMixin):
    def __init__(self, features):
        self.features = features
        return

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        quote_features = X.columns
        for f in self.features:
            if f not in quote_features:
                X, _ = parent_generator(X, f)
        return X


def generate_features(
    data,
    trade_features_to_generate=["Trade_Side"],
    quote_features_to_generate=[["Effective_Spread", "Midprice", "Microprice", "Imbalance"]],
):
    raw_trade_features, raw_quote_features = data.columns.values[:14], data.columns.values[14:]

    # Initialize trade and quote feature pipelines
    trade_pipeline = make_pipeline(GenerateTradeFeatures(trade_features_to_generate))
    quote_pipeline = make_pipeline(GenerateQuoteFeatures(quote_features_to_generate))

    # Create column transformer
    generating_features = make_column_transformer(
        (trade_pipeline, raw_trade_features), (quote_pipeline, raw_quote_features)
    )

    df_copy = data.copy()

    data_prepared = generating_features.fit_transform(df_copy)

    column_names = np.concatenate(
        (raw_trade_features, trade_features_to_generate, raw_quote_features, quote_features_to_generate), axis=0
    )

    data_prepared_fr = pd.DataFrame(
        data_prepared,
        # The columns parameter specifies the column names for the DataFrame
        # and is set to the output of preprocessing.get_feature_names_out().
        columns=column_names,
        # The index parameter sets the index of the DataFrame to the index of the housing data,
        # preserving the original data's indices.
        index=df_copy.index,
    )

    return data_prepared_fr
