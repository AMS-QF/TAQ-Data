from __future__ import annotations


# directories
DATA_SAVE_DIR = "data"
TRAINED_MODEL_DIR = "trained_models"

# date format %YYYY-mm-dd"
START_DATE = "2020-01-06"
END_DATE = "2020-01-07"

# trades/quotes subcolumns
TRADE_COLUMNS = ['Time', 'Symbol', 'Date', 'Participant_Timestamp', 'Trade_Volume', 'Trade_Price', 'Trade_Reporting_Facility']
QUOTE_COLUMNS = ['Time', 'Symbol', 'Date', 'Participant_Timestamp', 'Bid_Price', 'Bid_Size', 'Offer_Price', 'Offer_Size']