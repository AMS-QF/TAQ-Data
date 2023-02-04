import pandas as pd

# Below are the functions that will be used to generate features
# To add a new feature, add a new function below and add the function name to the list_features.py file

# Functions are named generate_<feature_name>
# This is called by the parent_generator function


def generate_bbo_effective_spread(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Effective BBO Spread"""
    df["BBO_Spread"] = df["Best_Offer_Price"] - df["Best_Bid_Price"]

    return df


def generate_bbo_realized_spread(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Realized BBO Spread"""

    # Look into Realized Spread (accounts for market impact)

    return df


def generate_midprice_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO MidPrice"""
    df["Midprice_BBO"] = (df["Best_Offer_Price"] + df["Best_Bid_Price"]) / 2

    return df


def generate_microprice_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO Microprice"""
    df["Microprice_BBO"] = (
        df["Best_Offer_Price"] * df["Best_Offer_Size"] + df["Best_Bid_Price"] * df["Best_Bid_Size"]
    ) / (df["Best_Offer_Size"] + df["Best_Bid_Size"])

    return df


def generate_imbalance_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO Imbalance"""
    df["Imbalance_BBO"] = df["Best_Bid_Size"] / df["Best_Offer_Size"]

    return df


def generate_trade_side(df: pd.DataFrame) -> pd.DataFrame:
    """Classify trade side"""

    # Look into Signing Algorithms
    return df


def generate_prevailing_nbbo(df: pd.DataFrame) -> pd.DataFrame:

    # Look into Prevailing NBBO
    return df


def generate_price_impact(df: pd.DataFrame) -> pd.DataFrame:

    # Look into Price Impact
    return df


def parent_generator(df: pd.DataFrame, feature_to_generate: str) -> pd.DataFrame:
    """Wrapper function to call the correct feature generator function"""
    # This is a mapping of the feature name to the function that generates it
    feature_mappping = {
        "Effective_Spread_BBO": generate_bbo_effective_spread,
        "Realized_Spread_BBO": generate_bbo_realized_spread,
        "Midprice_BBO": generate_midprice_bbo,
        "Microprice_BBO": generate_microprice_bbo,
        "Imbalance_BBO": generate_imbalance_bbo,
        "Trade_Side": generate_trade_side,
        "Prevailing_NBBO": generate_prevailing_nbbo,
        "Price_Impact": generate_price_impact,
    }

    return feature_mappping[feature_to_generate](df)
