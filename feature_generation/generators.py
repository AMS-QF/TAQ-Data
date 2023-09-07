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


def generate_effective_spread(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Effective Spread"""
    df["Effective_Spread"] = df["Offer_Price"] - df["Bid_Price"]

    return df, ["Effective_Spread"]


def generate_realized_spread(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Realized  Spread"""

    df["Realized_Spread"] = pd.Series(np.nan, index=df.index)

    # Look into Realized Spread (accounts for market impact)

    return df, ["Realized_Spread"]


def generate_midprice(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO MidPrice"""
    df["Midprice"] = (df["Offer_Price"] + df["Bid_Price"]) / 2

    return df, ["Midprice"]


def generate_microprice(df: pd.DataFrame) -> pd.DataFrame:
    """Generate  Microprice"""
    df["Microprice"] = (df["Offer_Price"] * df["Offer_Size"] + df["Bid_Price"] * df["Bid_Size"]) / (
        df["Offer_Size"] + df["Bid_Size"]
    )

    return df, ["Microprice"]


def generate_imbalance(df: pd.DataFrame) -> pd.DataFrame:
    """Generate  Imbalance"""
    df["Imbalance"] = df["Bid_Size"] / df["Offer_Size"]

    return df, ["Imbalance"]


# def generate_prevailing_nbbo(df: pd.DataFrame) -> pd.DataFrame:
#     """Generate Prevailing Best Bid Price"""

#     df = df.sort_index()

#     # create a copy of the dataframe
#     merged_df = df.copy()

#     # fill in the missing values
#     merged_df = merged_df.ffill()
#     merged_df = merged_df.dropna()

#     column_dict = {
#         "Prevailing_Best_Bid_Price": "Best_Bid_Price",
#         "Prevailing_Best_Bid_Size": "Best_Bid_Size",
#         "Prevailing_Best_Offer_Price": "Best_Offer_Price",
#         "Prevailing_Best_Offer_Size": "Best_Offer_Size",
#     }

#     # create a new dataframe with the required columns
#     for column in column_dict.keys():
#         df[column] = merged_df[column_dict[column]]
#     return df, list(column_dict.keys())


def generate_price_impact(df: pd.DataFrame) -> pd.DataFrame:

    df["Price_Impact"] = pd.Series(np.nan, index=df.index)
    # Look into Price Impact
    return df, ["Price_Impact"]


def generate_imbalance_weighted_effective_spread(df: pd.DataFrame) -> pd.DataFrame:

    df["Imbalance_Weighted_Effective_Spread"] = pd.Series(np.nan, index=df.index)
    # Look into Imbalance Weighted Effective Spread
    return df, ["Imbalance_Weighted_Effective_Spread"]


def parent_generator(df: pd.DataFrame, feature_to_generate: str) -> pd.DataFrame:
    """Wrapper function to call the correct feature generator function"""
    # This is a mapping of the feature name to the function that generates it

    # idea- separate features into base feature and then model-specific generators (e.g. linear regression coefficients for price impact)
    feature_mappping = {
        "Effective_Spread": generate_effective_spread,
        "Realized_Spread": generate_realized_spread,
        "Imbalance": generate_imbalance,
        "Imbalance_Weighted_Effective_Spread": generate_imbalance_weighted_effective_spread,
        "Midprice": generate_midprice,
        "Microprice": generate_microprice,
        # "Prevailing_NBBO": generate_prevailing_nbbo,
        "MOX_Identifier": generate_mox_identifier,
        "Price_Impact": generate_price_impact,
    }

    return feature_mappping[feature_to_generate](df)
