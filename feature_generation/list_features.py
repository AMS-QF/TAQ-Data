import pandas as pd


def list_features(raw_data=False, trades=True, quotes=True, names_only=False):
    """List all features available"""

    trade_features = {}
    quote_features = {}

    if raw_data:
        if trades:

            raw_trade_features = {
                "Exchange": "Exchange of Trade",
                "Symbol": "Symbol pf Trade",
                "Trade_Volume": "Trade Volume",
                "Trade_Price": "Trade Price",
                "Sale_Condition": "Condition of the Trade",
                "Source_of_Trade": "Source which generated the Trade",
                "Trade_Correction_Indicator": "Indicates if the trade is a correction",
                "Sequence_Number": "Sequence Number of the Trade",
                "Trade_Id": "Trade ID",
                "Trade_Reporting_Facility": "Facility which reported the Trade",
                "Trade_Through_Exempt_Indicator": "Indicates if the trade is exempt from Trade Through Rule",
            }
            trade_features.update(raw_trade_features)

        if quotes:
            raw_quote_features = {
                "Exchange": "Exchange of the Quote",
                "Symbol": "Symbol of Quote",
                "Bid_Price": "Best Bid Price on Exchange",
                "Bid_Size": "Size of the Best Bid on Exchange",
                "Offer_Price": "Best Offer Price on Exchange",
                "Offer_Size": "Size of the Best Offer on Exchange",
                "Quote_Condition": "Condition of the Quote",
                "Sequence_Number": "Sequence Number of the Quote",
                "Finra_Bbo_Indicator": "Indicates if the quote is a FINRA BBO",
                "Source_of_Quote": "Source which generated the Quote",
                "Best_Bid_Exchange": "Exchange with the National Best Bid",
                "Best_Bid_Price": "National Best Bid Price",
                "Best_Bid_Size": "Size of the National Best Bid",
                "Best_Bid_FINRA_Market_Maker_ID": "FINRA Market Maker ID of the National Best Bid",
                "Best_Offer_Exchange": "Exchange with the National Best Offer",
                "Best_Offer_Size": "Size of the National Best Offer",
                "Best_Offer_FINRA_Market_Maker_ID": "FINRA Market Maker ID of the National Best Offer",
                "LULD_Indicator": "Indicates if the quote is a LULD quote",
                "LULD_NBBO_Indicator": "Indicates if the quote is a LULD NBBO",
                "SIP_Generated_Message_Identifier": "Indentifies associated SIP messages",
                "FINRA_ADF_MPID_Indicator": "Indicates if the quote is a FINRA ADF quote",
                "Security_Status_Indicator": "Reports trading suspension status of the security",
                "Quote_Cancel_Correction": "Indicates if the quote is a correction",
                "National_BBO_Ind": "Indicates if the quote is a National BBO",
            }
            quote_features.update(raw_quote_features)

    if trades:
        """Generate Trade Features"""

        generated_trade_features = {
            "Trade_Side": "Side of the Trade",
            "Prevailing_Best_Bid_Price": "National Best Bid at the time of the Trade",
            "Prevailing_Best_Offer_Price": "National Best Offer at the time of the Trade",
            "Prevailing_Best_Bid_Size": "Size of the National Best Bid at the time of the Trade",
            "Prevailing_Best_Offer_Size": "Size of the National Best Offer at the time of the Trade",
            "Price_Impact": "Percentage of the Trade which excecuted outside National Best Bid and National Best Offer",
        }

        trade_features.update(generated_trade_features)

    if quotes:
        """Generate Quote Features"""

        generated_quote_features = {
            "Midprice_BBO": "Midprice of the National Best Bid and National Best Offer",
            "Microprice_BBO": "Microprice of the National Best Bid and National Best Offer",
            "Effective_Spread_BBO": "Effective Spread of the National Best Bid and National Best Offer",
            "Realized_Spread_BBO": " Spread of the National Best Bid and National Best Offer accounting for impact cost",
            "Imbalance_Weighted_Effective_Spread_BBO": "Imbalance Weighted Effective Spread of the National Best Bid and National Best Offer",
            "Imbalance_BBO": "Ratio between the National Best Bid and National Best Offer",
        }

        quote_features.update(generated_quote_features)

    if names_only:
        return list(trade_features.keys()), list(quote_features.keys())

    else:
        pd.DataFrame.from_dict(trade_features, orient="index", columns=["Description"]), pd.DataFrame.from_dict(
            quote_features, orient="index", columns=["Description"]
        )


# python scripts/feature_gen/list_features.py
if __name__ == "__main__":
    trade_features, quote_features = list_features(raw_data=True, trades=True, quotes=True)
    print("Trade Features:")
    print(trade_features)

    print("#############################################")

    print("Quote Features:")
    print(quote_features)

    print("#############################################")
