import numpy as np
import pandas as pd

# Generators are named generate_<feature_name>
# This is called by the parent_generator function which requires following output

# def generate_<feature_name>(df: pd.DataFrame) -> pd.DataFrame:
#     Generate <feature_name>
#     # generate feature
#     df["<feature_name>"] = <feature_name>
#     # return the dataframe and the list of features generated
#     return df, ["<feature_name>"]


def generate_effective_spread_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Effective BBO Spread"""
    df["Effective_Spread_BBO"] = df["Best_Offer_Price"] - df["Best_Bid_Price"]

    return df, ["Effective_Spread_BBO"]


def generate_realized_spread_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Realized BBO Spread"""

    df["Realized_Spread_BBO"] = pd.Series(np.nan, index=df.index)

    # Look into Realized Spread (accounts for market impact)

    return df, ["Realized_Spread_BBO"]


def generate_midprice_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO MidPrice"""
    df["Midprice_BBO"] = (df["Best_Offer_Price"] + df["Best_Bid_Price"]) / 2

    return df, ["Midprice_BBO"]


def generate_microprice_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO Microprice"""
    df["Microprice_BBO"] = (
        df["Best_Offer_Price"] * df["Best_Offer_Size"] + df["Best_Bid_Price"] * df["Best_Bid_Size"]
    ) / (df["Best_Offer_Size"] + df["Best_Bid_Size"])

    return df, ["Microprice_BBO"]


def generate_imbalance_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO Imbalance"""
    df["Imbalance_BBO"] = df["Best_Bid_Size"] / df["Best_Offer_Size"]

    return df, ["Imbalance_BBO"]


def generate_trade_side(df: pd.DataFrame) -> pd.DataFrame:
    """Classify trade side"""

    df["Trade_Side"] = pd.Series(np.nan, index=df.index)
    # Look into Signing Algorithms
    return df, ["Trade_Side"]


def generate_prevailing_nbbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Prevailing Best Bid Price"""

    df = df.sort_index()

    # create a copy of the dataframe
    merged_df = df.copy()

    # fill in the missing values
    merged_df = merged_df.ffill()
    merged_df = merged_df.dropna()

    column_dict = {
        "Prevailing_Best_Bid_Price": "Best_Bid_Price",
        "Prevailing_Best_Bid_Size": "Best_Bid_Size",
        "Prevailing_Best_Offer_Price": "Best_Offer_Price",
        "Prevailing_Best_Offer_Size": "Best_Offer_Size",
    }

    # create a new dataframe with the required columns
    for column in column_dict.keys():
        df[column] = merged_df[column_dict[column]]
    return df, list(column_dict.keys())


def generate_mox_identifier(df: pd.DataFrame) -> pd.DataFrame:
    """Generate MOX Identifier"""

    df_copy = df.copy()
    df_copy.index = pd.to_datetime(df_copy.index)

    # round the index to the nearest millisecond
    grouped_df = df_copy.groupby(df_copy.index.map(lambda t: t.round("1us")))

    # assign a unique identifier to each group
    for i, (name, group) in enumerate(grouped_df):
        df_copy.loc[group.index, "MOX_Identifier"] = i

    df["MOX_Identifier"] = df_copy["MOX_Identifier"].values

    return df, ["MOX_Identifier"]

    # Look into MOX Identifier
    return df, ["MOX_Identifier"]


def generate_price_impact(df: pd.DataFrame) -> pd.DataFrame:

    df["Price_Impact"] = pd.Series(np.nan, index=df.index)
    # Look into Price Impact
    return df, ["Price_Impact"]


def generate_imbalance_weighted_effective_spread_bbo(df: pd.DataFrame) -> pd.DataFrame:

    df["Imbalance_Weighted_Effective_Spread_BBO"] = pd.Series(np.nan, index=df.index)
    # Look into Imbalance Weighted Effective Spread
    return df, ["Imbalance_Weighted_Effective_Spread_BBO"]


def parent_generator(df: pd.DataFrame, feature_to_generate: str) -> pd.DataFrame:
    """Wrapper function to call the correct feature generator function"""
    # This is a mapping of the feature name to the function that generates it
    feature_mappping = {
        "Effective_Spread_BBO": generate_effective_spread_bbo,
        "Realized_Spread_BBO": generate_realized_spread_bbo,
        "Imbalance_BBO": generate_imbalance_bbo,
        "Imbalance_Weighted_Effective_Spread_BBO": generate_imbalance_weighted_effective_spread_bbo,
        "Midprice_BBO": generate_midprice_bbo,
        "Microprice_BBO": generate_microprice_bbo,
        "Trade_Side": generate_trade_side,
        "Prevailing_NBBO": generate_prevailing_nbbo,
        "MOX_Identifier": generate_mox_identifier,
        "Price_Impact": generate_price_impact,
    }

    return feature_mappping[feature_to_generate](df)
