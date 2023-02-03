import pandas as pd

# Below are the functions that will be used to generate features
# To add a new feature, add a new function below and add the function name to the list_features.py file

# Functions are named generate_<feature_name>
# This is called by the parent_generator function


def generate_spread(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Spread"""
    df["Spread"] = df["Offer_Price"] - df["Bid_Price"]

    return df


def generate_bbo_spread(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO Spread"""
    df["BBO_Spread"] = df["Best_Offer_Price"] - df["Best_Bid_Price"]

    return df


def generate_midprice(df: pd.DataFrame) -> pd.DataFrame:
    """Generate MidPrice"""
    df["Midprice"] = (df["Offer_Price"] + df["Bid_Price"]) / 2

    return df


def generate_midprice_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO MidPrice"""
    df["Midprice_BBO"] = (df["Best_Offer_Price"] + df["Best_Bid_Price"]) / 2

    return df


def generate_microprice(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Microprice"""
    df["Microprice"] = (df["Offer_Price"] * df["Offer_Size"] + df["Bid_Price"] * df["Bid_Size"]) / (
        df["Offer_Size"] + df["Bid_Size"]
    )

    return df


def generate_microprice_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO Microprice"""
    df["Microprice_BBO"] = (
        df["Best_Offer_Price"] * df["Best_Offer_Size"] + df["Best_Bid_Price"] * df["Best_Bid_Size"]
    ) / (df["Best_Offer_Size"] + df["Best_Bid_Size"])

    return df


def generate_imbalance(df: pd.DataFrame) -> pd.DataFrame:
    """Generate Imbalance"""
    df["Imbalance"] = df["Bid_Size"] / df["Offer_Size"]

    return df


def generate_imbalance_bbo(df: pd.DataFrame) -> pd.DataFrame:
    """Generate BBO Imbalance"""
    df["Imbalance_BBO"] = df["Best_Bid_Size"] / df["Best_Offer_Size"]

    return df


def parent_generator(df: pd.DataFrame, feature_to_generate: str) -> pd.DataFrame:
    """Wrapper function to call the correct feature generator function"""
    # This is a mapping of the feature name to the function that generates it
    feature_mappping = {
        "Spread": generate_spread,
        "Spread_BBO": generate_bbo_spread,
        "Midprice": generate_midprice,
        "Midprice_BBO": generate_midprice_bbo,
        "Microprice": generate_microprice,
        "Microprice_BBO": generate_microprice_bbo,
        "Imbalance": generate_imbalance,
        "Imbalance_BBO": generate_imbalance_bbo,
    }

    return feature_mappping[feature_to_generate](df)
