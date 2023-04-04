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


def generate_trade_side(df: pd.DataFrame) -> pd.DataFrame:
    """Classify trade side using tick test"""
    trade_direction_dic = {
    "uptick": 1,
    "zero-uptick": 1,
    "downtick": -1,
    "zero-downtick": -1,
    "NaN": np.nan
    }
    pre_price, pre_cat = 0, 0
    trade_cats = []
    for p in df["Trade_Price"].values:
        if pd.isna(p):
            trade_cats.append("NaN")
        else:
            if p > pre_price:
                trade_cats.append("uptick")
                pre_cat = "uptick"
            elif p < pre_price:
                trade_cats.append("downtick")
                pre_cat = "downtick"
            else:
                if pre_cat == "downtick":
                    trade_cats.append("zero-downtick")
                    pre_cat = "zero-downtick"
                else:
                    # question: what about previous one is zero-uptick?
                    trade_cats.append("zero-uptick")
                    pre_cat = "zero-uptick"

            pre_price = p

    trade_signs = [trade_direction_dic[c] for c in trade_cats]

    df["Trade_Side"] = trade_signs
    
    return df, ["Trade_Side"]


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
    feature_mappping = {
        "Effective_Spread": generate_effective_spread,
        "Realized_Spread": generate_realized_spread,
        "Imbalance": generate_imbalance,
        "Imbalance_Weighted_Effective_Spread": generate_imbalance_weighted_effective_spread,
        "Midprice": generate_midprice,
        "Microprice": generate_microprice,
        "Trade_Side": generate_trade_side,
        # "Prevailing_NBBO": generate_prevailing_nbbo,
        "MOX_Identifier": generate_mox_identifier,
        "Price_Impact": generate_price_impact,
    }

    return feature_mappping[feature_to_generate](df)
