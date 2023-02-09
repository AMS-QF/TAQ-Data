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
                "Time": "Time",
                "Exchange": "Exchange",
                "Symbol": "Symbol",
                "Bid_Price": "Bid_Price",
                "Bid_Size": "Bid_Size",
                "Offer_Price": "Offer_Price",
                "Offer_Size": "Offer_Size",
                "Quote_Condition": "Quote_Condition",
                "Sequence_Number": "Sequence_Number",
                "National_BBO_Ind": "National_BBO_Indicator",
                "FINRA_BBO_Indicator": "FINRA_BBO_Indicator",
                "FINRA_ADF_MPID_Indicator": "FINRA_ADF_MPID_Indicator",
                "Quote_Cancel_Correction": "Quote_Cancel_Correction",
                "Source_of_Quote": "Source_of_Quote",
                "Retail_Interest_Indicator": "Retail_Interest_Indicator",
                "Short_Sale_Restriction_Indicator": "Short_Sale_Restriction_Indicator",
                "LULD_BBO_Indicator": "LULD_BBO_Indicator",
                "SIP_Generated_Message_Identifier": "SIP_Generated_Message_Identifier",
                "National_BBO_LULD_Indicator": "NBBO_LULD_Indicator",
                "Participant_Timestamp": "Participant_Timestamp",
                "FINRA_ADF_Timestamp": "FINRA_ADF_Timestamp",
                "FINRA_ADF_Market_Participant_Quote_Indicator": "FINRA_ADF_Market_Participant_Quote_Indicator",
                "Security_Status_Indicator": "Security_Status_Indicator",
            }
            quote_features.update(raw_quote_features)

    if trades:
        """Generate Trade Features"""

        generated_trade_features = {
            "Trade_Side": "Side of the Trade",
            "Prevailing_NBBO": "National Best Bid and National Best Offer at the time of the Trade",
            "Price_Impact": "Percentage of the Trade which excecuted outside National Best Bid and National Best Offer",
            "MOX_Identifier": "Indicates all trades and quotes from executable order",
        }

        trade_features.update(generated_trade_features)

    if quotes:
        """Generate Quote Features"""

        generated_quote_features = {
            "Midprice": "Midprice of the given quote",
            "Microprice": "Microprice of the given quote",
            "Effective_Spread": "Effective Spread of the   given quote",
            "Realized_Spread": " Spread of the quote accounting for impact cost",
            "Imbalance": "Ratio between the bid and offer size",
            "Imbalance_Weighted_Effective_Spread": "Imbalance Weighted Effective Spread of the quote",
            "MOX_Identifier": "Indicates all trades and quotes from executable order up to millisecond precision",
        }

        quote_features.update(generated_quote_features)

    if names_only:
        return list(trade_features.keys()), list(quote_features.keys())

    else:
        pd.DataFrame.from_dict(trade_features, orient="index", columns=["Description"]), pd.DataFrame.from_dict(
            quote_features, orient="index", columns=["Description"]
        )


# python feature_generation/list_features.py
if __name__ == "__main__":
    trade_features, quote_features = list_features(raw_data=True, trades=True, quotes=True)
    print("Trade Features:")
    print(trade_features)

    print("#############################################")

    print("Quote Features:")
    print(quote_features)

    print("#############################################")
