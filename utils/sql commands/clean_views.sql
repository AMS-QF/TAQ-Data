CREATE VIEW IF NOT EXISTS COMBINEDDB.combined2017view AS
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    Sale_Condition, 
    Trade_Volume, 
    Trade_Price, 
    Trade_Stop_Stock_Indicator, 
    Trade_Correction_Indicator, 
    Sequence_Number, 
    Trade_Id, 
    Source_of_Trade, 
    Trade_Reporting_Facility, 
    Participant_Timestamp, 
    Trade_Reporting_Facility_TRF_Timestamp, 
    Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    NULL as Bid_Price,
    NULL as Bid_Size,
    NULL as Offer_Price,
    NULL as Offer_Size,
    NULL as Quote_Condition,
    NULL as National_BBO_Indicator,
    NULL as FINRA_BBO_Indicator,
    NULL as FINRA_ADF_MPID_Indicator,
    NULL as Quote_Cancel_Correction,
    NULL as Source_Of_Quote,
    NULL as Retail_Interest_Indicator,
    NULL as Short_Sale_Restriction_Indicator,
    NULL as LULD_BBO_Indicator,
    NULL as SIP_Generated_Message_Identifier,
    NULL as NBBO_LULD_Indicator,
    NULL as Participant_Timestamp_Quotes,
    NULL as FINRA_ADF_Timestamp,
    NULL as FINRA_ADF_Market_Participant_Quote_Indicator,
    NULL as Security_Status_Indicator
FROM TRADESDB.trades2017view 
WHERE 
    Trade_Price >= 0 
    AND Trade_Volume >= 0 
    AND Trade_Reporting_Facility <> 'D'
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME)
UNION ALL 
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    NULL as Sale_Condition, 
    NULL as Trade_Volume, 
    NULL as Trade_Price, 
    NULL as Trade_Stop_Stock_Indicator, 
    NULL as Trade_Correction_Indicator, 
    NULL as Sequence_Number, 
    NULL as Trade_Id, 
    NULL as Source_of_Trade, 
    NULL as Trade_Reporting_Facility, 
    NULL as Participant_Timestamp, 
    NULL as Trade_Reporting_Facility_TRF_Timestamp, 
    NULL as Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    Bid_Price,
    Bid_Size,
    Offer_Price,
    Offer_Size,
    Quote_Condition,
    National_BBO_Indicator,
    FINRA_BBO_Indicator,
    FINRA_ADF_MPID_Indicator,
    Quote_Cancel_Correction,
    Source_Of_Quote,
    Retail_Interest_Indicator,
    Short_Sale_Restriction_Indicator,
    LULD_BBO_Indicator,
    SIP_Generated_Message_Identifier,
    NBBO_LULD_Indicator,
    Participant_Timestamp as Participant_Timestamp_Quotes,
    FINRA_ADF_Timestamp,
    FINRA_ADF_Market_Participant_Quote_Indicator,
    Security_Status_Indicator
FROM QUOTESDB.quotes2017view
WHERE 
    (Bid_Price >= 0 OR Offer_Price >= Bid_Price)
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME);



CREATE VIEW IF NOT EXISTS COMBINEDDB.combined2018view AS
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    Sale_Condition, 
    Trade_Volume, 
    Trade_Price, 
    Trade_Stop_Stock_Indicator, 
    Trade_Correction_Indicator, 
    Sequence_Number, 
    Trade_Id, 
    Source_of_Trade, 
    Trade_Reporting_Facility, 
    Participant_Timestamp, 
    Trade_Reporting_Facility_TRF_Timestamp, 
    Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    NULL as Bid_Price,
    NULL as Bid_Size,
    NULL as Offer_Price,
    NULL as Offer_Size,
    NULL as Quote_Condition,
    NULL as National_BBO_Indicator,
    NULL as FINRA_BBO_Indicator,
    NULL as FINRA_ADF_MPID_Indicator,
    NULL as Quote_Cancel_Correction,
    NULL as Source_Of_Quote,
    NULL as Retail_Interest_Indicator,
    NULL as Short_Sale_Restriction_Indicator,
    NULL as LULD_BBO_Indicator,
    NULL as SIP_Generated_Message_Identifier,
    NULL as NBBO_LULD_Indicator,
    NULL as Participant_Timestamp_Quotes,
    NULL as FINRA_ADF_Timestamp,
    NULL as FINRA_ADF_Market_Participant_Quote_Indicator,
    NULL as Security_Status_Indicator
FROM TRADESDB.trades2018view 
WHERE 
    Trade_Price >= 0 
    AND Trade_Volume >= 0 
    AND Trade_Reporting_Facility <> 'D'
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME)
UNION ALL 
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    NULL as Sale_Condition, 
    NULL as Trade_Volume, 
    NULL as Trade_Price, 
    NULL as Trade_Stop_Stock_Indicator, 
    NULL as Trade_Correction_Indicator, 
    NULL as Sequence_Number, 
    NULL as Trade_Id, 
    NULL as Source_of_Trade, 
    NULL as Trade_Reporting_Facility, 
    NULL as Participant_Timestamp, 
    NULL as Trade_Reporting_Facility_TRF_Timestamp, 
    NULL as Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    Bid_Price,
    Bid_Size,
    Offer_Price,
    Offer_Size,
    Quote_Condition,
    National_BBO_Indicator,
    FINRA_BBO_Indicator,
    FINRA_ADF_MPID_Indicator,
    Quote_Cancel_Correction,
    Source_Of_Quote,
    Retail_Interest_Indicator,
    Short_Sale_Restriction_Indicator,
    LULD_BBO_Indicator,
    SIP_Generated_Message_Identifier,
    NBBO_LULD_Indicator,
    Participant_Timestamp as Participant_Timestamp_Quotes,
    FINRA_ADF_Timestamp,
    FINRA_ADF_Market_Participant_Quote_Indicator,
    Security_Status_Indicator
FROM QUOTESDB.quotes2018view
WHERE 
    (Bid_Price >= 0 OR Offer_Price >= Bid_Price)
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME);


CREATE VIEW IF NOT EXISTS COMBINEDDB.combined2019view AS
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    Sale_Condition, 
    Trade_Volume, 
    Trade_Price, 
    Trade_Stop_Stock_Indicator, 
    Trade_Correction_Indicator, 
    Sequence_Number, 
    Trade_Id, 
    Source_of_Trade, 
    Trade_Reporting_Facility, 
    Participant_Timestamp, 
    Trade_Reporting_Facility_TRF_Timestamp, 
    Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    NULL as Bid_Price,
    NULL as Bid_Size,
    NULL as Offer_Price,
    NULL as Offer_Size,
    NULL as Quote_Condition,
    NULL as National_BBO_Indicator,
    NULL as FINRA_BBO_Indicator,
    NULL as FINRA_ADF_MPID_Indicator,
    NULL as Quote_Cancel_Correction,
    NULL as Source_Of_Quote,
    NULL as Retail_Interest_Indicator,
    NULL as Short_Sale_Restriction_Indicator,
    NULL as LULD_BBO_Indicator,
    NULL as SIP_Generated_Message_Identifier,
    NULL as NBBO_LULD_Indicator,
    NULL as Participant_Timestamp_Quotes,
    NULL as FINRA_ADF_Timestamp,
    NULL as FINRA_ADF_Market_Participant_Quote_Indicator,
    NULL as Security_Status_Indicator
FROM TRADESDB.trades2019view 
WHERE 
    Trade_Price >= 0 
    AND Trade_Volume >= 0 
    AND Trade_Reporting_Facility <> 'D'
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME)
UNION ALL 
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    NULL as Sale_Condition, 
    NULL as Trade_Volume, 
    NULL as Trade_Price, 
    NULL as Trade_Stop_Stock_Indicator, 
    NULL as Trade_Correction_Indicator, 
    NULL as Sequence_Number, 
    NULL as Trade_Id, 
    NULL as Source_of_Trade, 
    NULL as Trade_Reporting_Facility, 
    NULL as Participant_Timestamp, 
    NULL as Trade_Reporting_Facility_TRF_Timestamp, 
    NULL as Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    Bid_Price,
    Bid_Size,
    Offer_Price,
    Offer_Size,
    Quote_Condition,
    National_BBO_Indicator,
    FINRA_BBO_Indicator,
    FINRA_ADF_MPID_Indicator,
    Quote_Cancel_Correction,
    Source_Of_Quote,
    Retail_Interest_Indicator,
    Short_Sale_Restriction_Indicator,
    LULD_BBO_Indicator,
    SIP_Generated_Message_Identifier,
    NBBO_LULD_Indicator,
    Participant_Timestamp as Participant_Timestamp_Quotes,
    FINRA_ADF_Timestamp,
    FINRA_ADF_Market_Participant_Quote_Indicator,
    Security_Status_Indicator
FROM QUOTESDB.quotes2019view
WHERE 
    (Bid_Price >= 0 OR Offer_Price >= Bid_Price)
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME);


CREATE VIEW IF NOT EXISTS COMBINEDDB.combined2020view AS
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    Sale_Condition, 
    Trade_Volume, 
    Trade_Price, 
    Trade_Stop_Stock_Indicator, 
    Trade_Correction_Indicator, 
    Sequence_Number, 
    Trade_Id, 
    Source_of_Trade, 
    Trade_Reporting_Facility, 
    Participant_Timestamp, 
    Trade_Reporting_Facility_TRF_Timestamp, 
    Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    NULL as Bid_Price,
    NULL as Bid_Size,
    NULL as Offer_Price,
    NULL as Offer_Size,
    NULL as Quote_Condition,
    NULL as National_BBO_Indicator,
    NULL as FINRA_BBO_Indicator,
    NULL as FINRA_ADF_MPID_Indicator,
    NULL as Quote_Cancel_Correction,
    NULL as Source_Of_Quote,
    NULL as Retail_Interest_Indicator,
    NULL as Short_Sale_Restriction_Indicator,
    NULL as LULD_BBO_Indicator,
    NULL as SIP_Generated_Message_Identifier,
    NULL as NBBO_LULD_Indicator,
    NULL as Participant_Timestamp_Quotes,
    NULL as FINRA_ADF_Timestamp,
    NULL as FINRA_ADF_Market_Participant_Quote_Indicator,
    NULL as Security_Status_Indicator
FROM TRADESDB.trades2020view 
WHERE 
    Trade_Price >= 0 
    AND Trade_Volume >= 0 
    AND Trade_Reporting_Facility <> 'D'
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME)
UNION ALL 
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    NULL as Sale_Condition, 
    NULL as Trade_Volume, 
    NULL as Trade_Price, 
    NULL as Trade_Stop_Stock_Indicator, 
    NULL as Trade_Correction_Indicator, 
    NULL as Sequence_Number, 
    NULL as Trade_Id, 
    NULL as Source_of_Trade, 
    NULL as Trade_Reporting_Facility, 
    NULL as Participant_Timestamp, 
    NULL as Trade_Reporting_Facility_TRF_Timestamp, 
    NULL as Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    Bid_Price,
    Bid_Size,
    Offer_Price,
    Offer_Size,
    Quote_Condition,
    National_BBO_Indicator,
    FINRA_BBO_Indicator,
    FINRA_ADF_MPID_Indicator,
    Quote_Cancel_Correction,
    Source_Of_Quote,
    Retail_Interest_Indicator,
    Short_Sale_Restriction_Indicator,
    LULD_BBO_Indicator,
    SIP_Generated_Message_Identifier,
    NBBO_LULD_Indicator,
    Participant_Timestamp as Participant_Timestamp_Quotes,
    FINRA_ADF_Timestamp,
    FINRA_ADF_Market_Participant_Quote_Indicator,
    Security_Status_Indicator
FROM QUOTESDB.quotes2020view
WHERE 
    (Bid_Price >= 0 OR Offer_Price >= Bid_Price)
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME);


CREATE VIEW IF NOT EXISTS COMBINEDDB.combined2021view AS
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    Sale_Condition, 
    Trade_Volume, 
    Trade_Price, 
    Trade_Stop_Stock_Indicator, 
    Trade_Correction_Indicator, 
    Sequence_Number, 
    Trade_Id, 
    Source_of_Trade, 
    Trade_Reporting_Facility, 
    Participant_Timestamp, 
    Trade_Reporting_Facility_TRF_Timestamp, 
    Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    NULL as Bid_Price,
    NULL as Bid_Size,
    NULL as Offer_Price,
    NULL as Offer_Size,
    NULL as Quote_Condition,
    NULL as National_BBO_Indicator,
    NULL as FINRA_BBO_Indicator,
    NULL as FINRA_ADF_MPID_Indicator,
    NULL as Quote_Cancel_Correction,
    NULL as Source_Of_Quote,
    NULL as Retail_Interest_Indicator,
    NULL as Short_Sale_Restriction_Indicator,
    NULL as LULD_BBO_Indicator,
    NULL as SIP_Generated_Message_Identifier,
    NULL as NBBO_LULD_Indicator,
    NULL as Participant_Timestamp_Quotes,
    NULL as FINRA_ADF_Timestamp,
    NULL as FINRA_ADF_Market_Participant_Quote_Indicator,
    NULL as Security_Status_Indicator
FROM TRADESDB.trades2021view 
WHERE 
    Trade_Price >= 0 
    AND Trade_Volume >= 0 
    AND Trade_Reporting_Facility <> 'D'
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME)
UNION ALL 
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    NULL as Sale_Condition, 
    NULL as Trade_Volume, 
    NULL as Trade_Price, 
    NULL as Trade_Stop_Stock_Indicator, 
    NULL as Trade_Correction_Indicator, 
    NULL as Sequence_Number, 
    NULL as Trade_Id, 
    NULL as Source_of_Trade, 
    NULL as Trade_Reporting_Facility, 
    NULL as Participant_Timestamp, 
    NULL as Trade_Reporting_Facility_TRF_Timestamp, 
    NULL as Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    Bid_Price,
    Bid_Size,
    Offer_Price,
    Offer_Size,
    Quote_Condition,
    National_BBO_Indicator,
    FINRA_BBO_Indicator,
    FINRA_ADF_MPID_Indicator,
    Quote_Cancel_Correction,
    Source_Of_Quote,
    Retail_Interest_Indicator,
    Short_Sale_Restriction_Indicator,
    LULD_BBO_Indicator,
    SIP_Generated_Message_Identifier,
    NBBO_LULD_Indicator,
    Participant_Timestamp as Participant_Timestamp_Quotes,
    FINRA_ADF_Timestamp,
    FINRA_ADF_Market_Participant_Quote_Indicator,
    Security_Status_Indicator
FROM QUOTESDB.quotes2021view
WHERE 
    (Bid_Price >= 0 OR Offer_Price >= Bid_Price)
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME);


CREATE VIEW IF NOT EXISTS COMBINEDDB.combined2022view AS
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    Sale_Condition, 
    Trade_Volume, 
    Trade_Price, 
    Trade_Stop_Stock_Indicator, 
    Trade_Correction_Indicator, 
    Sequence_Number, 
    Trade_Id, 
    Source_of_Trade, 
    Trade_Reporting_Facility, 
    Participant_Timestamp, 
    Trade_Reporting_Facility_TRF_Timestamp, 
    Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    NULL as Bid_Price,
    NULL as Bid_Size,
    NULL as Offer_Price,
    NULL as Offer_Size,
    NULL as Quote_Condition,
    NULL as National_BBO_Indicator,
    NULL as FINRA_BBO_Indicator,
    NULL as FINRA_ADF_MPID_Indicator,
    NULL as Quote_Cancel_Correction,
    NULL as Source_Of_Quote,
    NULL as Retail_Interest_Indicator,
    NULL as Short_Sale_Restriction_Indicator,
    NULL as LULD_BBO_Indicator,
    NULL as SIP_Generated_Message_Identifier,
    NULL as NBBO_LULD_Indicator,
    NULL as Participant_Timestamp_Quotes,
    NULL as FINRA_ADF_Timestamp,
    NULL as FINRA_ADF_Market_Participant_Quote_Indicator,
    NULL as Security_Status_Indicator
FROM TRADESDB.trades2022view 
WHERE 
    Trade_Price >= 0 
    AND Trade_Volume >= 0 
    AND Trade_Reporting_Facility <> 'D'
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME)
UNION ALL 
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    NULL as Sale_Condition, 
    NULL as Trade_Volume, 
    NULL as Trade_Price, 
    NULL as Trade_Stop_Stock_Indicator, 
    NULL as Trade_Correction_Indicator, 
    NULL as Sequence_Number, 
    NULL as Trade_Id, 
    NULL as Source_of_Trade, 
    NULL as Trade_Reporting_Facility, 
    NULL as Participant_Timestamp, 
    NULL as Trade_Reporting_Facility_TRF_Timestamp, 
    NULL as Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    Bid_Price,
    Bid_Size,
    Offer_Price,
    Offer_Size,
    Quote_Condition,
    National_BBO_Indicator,
    FINRA_BBO_Indicator,
    FINRA_ADF_MPID_Indicator,
    Quote_Cancel_Correction,
    Source_Of_Quote,
    Retail_Interest_Indicator,
    Short_Sale_Restriction_Indicator,
    LULD_BBO_Indicator,
    SIP_Generated_Message_Identifier,
    NBBO_LULD_Indicator,
    Participant_Timestamp as Participant_Timestamp_Quotes,
    FINRA_ADF_Timestamp,
    FINRA_ADF_Market_Participant_Quote_Indicator,
    Security_Status_Indicator
FROM QUOTESDB.quotes2022view
WHERE 
    (Bid_Price >= 0 OR Offer_Price >= Bid_Price)
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME);


CREATE VIEW IF NOT EXISTS COMBINEDDB.combined2023view AS
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    Sale_Condition, 
    Trade_Volume, 
    Trade_Price, 
    Trade_Stop_Stock_Indicator, 
    Trade_Correction_Indicator, 
    Sequence_Number, 
    Trade_Id, 
    Source_of_Trade, 
    Trade_Reporting_Facility, 
    Participant_Timestamp, 
    Trade_Reporting_Facility_TRF_Timestamp, 
    Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    NULL as Bid_Price,
    NULL as Bid_Size,
    NULL as Offer_Price,
    NULL as Offer_Size,
    NULL as Quote_Condition,
    NULL as National_BBO_Indicator,
    NULL as FINRA_BBO_Indicator,
    NULL as FINRA_ADF_MPID_Indicator,
    NULL as Quote_Cancel_Correction,
    NULL as Source_Of_Quote,
    NULL as Retail_Interest_Indicator,
    NULL as Short_Sale_Restriction_Indicator,
    NULL as LULD_BBO_Indicator,
    NULL as SIP_Generated_Message_Identifier,
    NULL as NBBO_LULD_Indicator,
    NULL as Participant_Timestamp_Quotes,
    NULL as FINRA_ADF_Timestamp,
    NULL as FINRA_ADF_Market_Participant_Quote_Indicator,
    NULL as Security_Status_Indicator
FROM TRADESDB.trades2023view 
WHERE 
    Trade_Price >= 0 
    AND Trade_Volume >= 0 
    AND Trade_Reporting_Facility <> 'D'
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME)
UNION ALL 
SELECT 
    Time, 
    Exchange, 
    Symbol, 
    NULL as Sale_Condition, 
    NULL as Trade_Volume, 
    NULL as Trade_Price, 
    NULL as Trade_Stop_Stock_Indicator, 
    NULL as Trade_Correction_Indicator, 
    NULL as Sequence_Number, 
    NULL as Trade_Id, 
    NULL as Source_of_Trade, 
    NULL as Trade_Reporting_Facility, 
    NULL as Participant_Timestamp, 
    NULL as Trade_Reporting_Facility_TRF_Timestamp, 
    NULL as Trade_Through_Exempt_Indicator, 
    Date, 
    YearMonth,
    Bid_Price,
    Bid_Size,
    Offer_Price,
    Offer_Size,
    Quote_Condition,
    National_BBO_Indicator,
    FINRA_BBO_Indicator,
    FINRA_ADF_MPID_Indicator,
    Quote_Cancel_Correction,
    Source_Of_Quote,
    Retail_Interest_Indicator,
    Short_Sale_Restriction_Indicator,
    LULD_BBO_Indicator,
    SIP_Generated_Message_Identifier,
    NBBO_LULD_Indicator,
    Participant_Timestamp as Participant_Timestamp_Quotes,
    FINRA_ADF_Timestamp,
    FINRA_ADF_Market_Participant_Quote_Indicator,
    Security_Status_Indicator
FROM QUOTESDB.quotes2023view
WHERE 
    (Bid_Price >= 0 OR Offer_Price >= Bid_Price)
    AND CAST(Time AS TIME) >= CAST('09:45:00' AS TIME)
    AND CAST(Time AS TIME) <= CAST('15:45:00' AS TIME);
