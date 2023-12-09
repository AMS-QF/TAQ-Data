import pandas as pd
import numpy as np
from datetime import timedelta
from sortedcollections import OrderedSet
import sys
sys.path.insert(1, '../testData')


def convertParticipantTimestamp(pts, date):
    """
    Convert participant timestamps to a combined datetime index.

    Given participant timestamps (`pts`) and a date, this function converts the timestamps
    to a combined datetime index by adding the timestamp durations to the provided date.

    Parameters:
    ----------
    pts : numpy.ndarray
        Array of participant timestamps to convert. It should be an array of integers
        with 15 digits, representing hours, minutes, seconds, and microseconds.
    date : str or datetime.datetime
        Date in string format or as a `datetime` object to combine with the timestamps.

    Returns:
    -------
    pandas.DatetimeIndex
        Combined datetime index obtained by adding the timestamp durations to the provided date.

    """
    date = pd.to_datetime(date)
    pts = pd.to_datetime(
        pts.astype(str).str.zfill(15), format="%H%M%S%f")

    # convert datetime to index
    idx = date.apply(lambda x: x) + pts.apply(
        lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second, microseconds=x.microsecond))

    return idx


def generate_mox_identifier(participant_timestamps):
    """
    Generate MOX identifiers from participant timestamps.

    This function takes a list of participant timestamps and generates MOX identifiers for each timestamp.
    The MOX identifier represents a unique index based on the order of the timestamps.

    Parameters:
    ----------
    participant_timestamps : list-like
        Array of participant timestamps.

    Returns:
    -------
    list
        List of MOX identifiers corresponding to the input participant timestamps.

    """
    fl_participant_timestamps = [float(ts.timestamp() * 1000) for ts in participant_timestamps]
    time_mox_mapping = {ts: mox_idx for mox_idx, ts in enumerate(OrderedSet(fl_participant_timestamps))}
    mox_identifiers = [time_mox_mapping[t] for t in fl_participant_timestamps]

    return mox_identifiers


def generate_trade_side(trade_prices):
    """
    Classify trade side using tick test.

    This function classifies the trade side (trade direction) based on the tick test using the provided trade prices.
    It assigns a trade side category to each trade price based on the comparison with the previous price.

    Parameters:
    ----------
    trade_prices : list-like
        Array containing trade prices.

    Returns:
    -------
    list
        List of trade signs representing the trade side for each trade price.

    """
    trade_direction_dic = {
        "uptick": 1,
        "zero-uptick": 1,
        "downtick": -1,
        "zero-downtick": -1,
        "NaN": np.nan
    }
    pre_price, pre_cat = 0, 0
    trade_cats = []
    for p in trade_prices:
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
                elif pre_cat == "uptick":
                    trade_cats.append("zero-uptick")
                    pre_cat = "zero-uptick"
                else:
                    trade_cats.append(pre_cat)

            pre_price = p

    trade_signs = [trade_direction_dic[c] for c in trade_cats]

    return trade_signs




# --------------------------------Helper Function -----------------------------------------------

def generate_transaction_return(df, span, mode='calendar'):
    
    if mode == 'calendar':
        avgPrice = df[df['Is_Quote'] == False]['Participant_Timestamp_f'].apply(lambda t:
            df[df['Participant_Timestamp_f'].between(t, t+span, inclusive='neither')]['Trade_Price'].mean()
        )
    else:
        avgPrice = None
    return avgPrice / df['Trade_Price'] - 1

def feature_name_generator(feature_name: str, delta1: float, delta2: float = None) -> str:
    """
    Generate a standardized feature name based on the provided inputs.

    Parameters:
    ----------
    feature_name : str
        Base name of the feature.

    delta1 : float
        The primary delta value to be appended to the feature name.

    delta2 : float, optional (default is None)
        An additional delta value to be appended after delta1. 
        If not provided, only delta1 will be appended to the feature name.

    Returns:
    -------
    str
        The generated standardized feature name.

    Examples:
    --------
    >>> feature_name_generator("VolumeAll", 2)
    'VolumeAll_2'

    >>> feature_name_generator("VolumeAll", 2, 5)
    'VolumeAll_2_5'
    """

    if delta2 is not None:
        return f"{feature_name}_{delta1}_{delta2}"
    else:
        return f"{feature_name}_{delta1}"
    
    
# ---------------------- Calendar Mode -------------------------------------------------------
def generate_cal_VolumeAll(df, delta1, delta2):
    """
    Generate calendar VolumeAll values for trades and quotes in the given DataFrame.

    This function calculates the calendar VolumeAll values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified time deltas `delta1` and `delta2`.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    delta1 : pandas.Timedelta
        The first time delta (offset) to calculate the time window.
    delta2 : pandas.Timedelta
        The second time delta (offset) to calculate the time window.

    Returns:
    -------
    pandas.Series
        A pandas Series containing the calculated calendar VolumeAll values.
    """
    return df['Participant_Timestamp_f'].apply(lambda t: sum(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='right')]['Trade_Volume']))


def generate_cal_Lambda(df, delta1, delta2):
    """
    Calculates the Lambda value for each trade in the given dataframe based on price differences
    within a specified time window and the corresponding volume.

    Parameters:
    ----------
    df : pandas.DataFrame
        The dataframe containing trade and quote data.
    delta1 : float
        The start time for the time window.
    delta2 : float
        The end time for the time window.

    Returns:
    -------
    pandas.Series
        A pandas series containing the Lambda values for each trade.

    Raises:
    ------
    ValueError:
        If the required VolumeAll column (based on delta1, delta2) is not found in df.
    """
    
    # Calculate the maximum trade price for each trade within the specified time window
    p_max = df['Participant_Timestamp_f'].apply(
        lambda t: max(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='right')]['Trade_Price'], default=np.nan)
    )

    # Calculate the minimum trade price for each trade within the specified time window
    p_min = df['Participant_Timestamp_f'].apply(
        lambda t: min(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='right')]['Trade_Price'], default=np.nan)
    )

    # Calculate the price difference (p_max - p_min) for each trade
    p_diff = p_max - p_min
    
    try:
        # Generate the name of the volume column based on the given deltas
        volumeAll = df[feature_name_generator('VolumeAll', delta1, delta2)]

        # Calculate the Lambda value for each trade by dividing the price difference by the volume (volumeAll)
        return p_diff / volumeAll

    except KeyError:
        raise ValueError('VolumeAll not defined')



def generate_cal_LobImbalance(df, delta1, delta2):
    """
    Generate calendar LobImbalance values for trades in the given DataFrame.

    This function calculates the calendar LobImbalance values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified time deltas `delta1` and `delta2`.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    delta1 : pandas.Timedelta
        The first time delta (offset) to calculate the time window.
    delta2 : pandas.Timedelta
        The second time delta (offset) to calculate the time window.

    Returns:
    -------
    pandas.Series
        A pandas Series containing the calculated calendar LobImbalance values.

    """
    # Calculate LOB imbalance for each trade and store it in the 'Imbalance' column
    df['Imbalance'] = (df['Offer_Size'] - df['Bid_Size']) / (df['Offer_Size'] + df['Bid_Size'])
    
    # Calculate mean LOB imbalance within the specified time window for each trade timestamp
    return df['Participant_Timestamp_f'].apply(
        lambda t: df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='right')]['Imbalance'].mean()
    )


def generate_cal_TxnImbalance(df, delta1, delta2):
    """
    Calculates the Lambda value for each trade in the given dataframe based on price differences
    within a specified time window and the corresponding volume.

    Parameters:
    ----------
    df : pandas.DataFrame
        The dataframe containing trade and quote data.
    delta1 : float
        The start time for the time window.
    delta2 : float
        The end time for the time window.

    Returns:
    -------
    pandas.Series
        A pandas series containing the Lambda values for each trade.

    Raises:
    ------
    ValueError:
        If the required VolumeAll column (based on delta1, delta2) is not found in df.
    """
    # Calculate the transaction imbalance for each trade and store it in the 'Vt_Dir' column
    df['Vt_Dir'] = df['Trade_Volume'] * df['Trade_Sign']

    # Calculate the sum of transaction imbalances within the specified time window for each trade timestamp
    sum_Vt_Dir = df['Participant_Timestamp_f'].apply(
        lambda t: sum(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='right')]['Vt_Dir'])
    )
    
    try:
        # Generate the name of the volume column based on the given deltas
        volumeAll = df[feature_name_generator('VolumeAll', delta1, delta2)]

        # Calculate the Lambda value for each trade by dividing the price difference by the volume (volumeAll)
        return sum_Vt_Dir / volumeAll

    except KeyError:
        raise ValueError('VolumeAll not defined')

        
def generate_cal_PastReturn(df, delta1, delta2):
    """
    Generate calendar PastReturn values for trades in the given DataFrame.

    This function calculates the calendar PastReturn values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified time deltas `delta1` and `delta2`.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    delta1 : pandas.Timedelta
        The first time delta (offset) to calculate the time window.
    delta2 : pandas.Timedelta
        The second time delta (offset) to calculate the time window.

    Returns:
    -------
    pandas.Series
        A pandas Series containing the calculated calendar PastReturn values.

    """
    # Calculate the average trade price for each trade within the specified time window
    avg_return = df['Participant_Timestamp_f'].apply(
        lambda t: df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='right')]['Trade_Price'].mean())

    # Calculate the maximum trade price for each trade within the specified time window
    p_max = df['Participant_Timestamp_f'].apply(
        lambda t: max(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='right')]['Trade_Price'], default=np.nan))

    # Calculate the PastReturn value for each trade
    return 1 - avg_return / p_max



# ---------------------- Transaction Mode -------------------------------------------------------

def generate_trans_VolumeAll(df, delta1, delta2):
    """
    Generate the aggregated trade volume over a specified range of transactions.
    
    This function aggregates the trade volume over a range defined by `delta1` and `delta2`
    (i.e., from the `delta1`-th last transaction to the `delta2`-th last transaction).
    If a row is not associated with a valid quote, it gets assigned a NaN value.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data with at least 'Valid_Quotes', 'Is_Quote', and 'Trade_Volume' columns.
    delta1 : int
        Starting point (transaction offset) for the aggregation.
    delta2 : int
        Ending point (transaction offset) for the aggregation.
    
    Returns:
    -------
    list
        A list of aggregated volumes for each entry in the DataFrame.
    """
    
    volumeAll = []

    for i in df.index:
        data = df.iloc[i]
        
        # Check if the row is a trade or valid quotes
        if (not data['Is_Quote']) or data['Valid_Quotes']:
            numTrans = 0 if data['Valid_Quotes'] else 1 
            cumVol = 0 if data['Valid_Quotes'] else data['Trade_Volume']
            next = i-1

            # Loop backwards to aggregate volume over the specified transaction range
            while next >= 0 and numTrans < delta2:
                d = df.iloc[next]

                # Check if the row is a trade (not a quote)
                if not d['Is_Quote']:
                    numTrans += 1

                # If the transaction is within the specified range, add its volume
                if delta1 <= numTrans < delta2:
                    cumVol += d['Trade_Volume']

                next -= 1
            
            volumeAll.append(cumVol)
        else:
            volumeAll.append(np.nan)  # Append NaN for invalid quotes

    return volumeAll



def generate_trans_Lambda(df, delta1, delta2):
    """
    Generate the Lambda values over a specified range of transactions.
    
    This function calculates the Lambda value, defined as the price range (max price - min price) 
    over a range defined by `delta1` and `delta2` transactions, divided by the aggregate volume
    over the same range.
    
    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data with columns: 'Is_Quote', 'Valid_Quotes', 'Trade_Price'.
    delta1 : int
        Starting point (transaction offset) for calculating the price range.
    delta2 : int
        Ending point (transaction offset) for calculating the price range.
    
    Returns:
    -------
    list
        A list of Lambda values for each entry in the DataFrame.
    
    Raises:
    ------
    ValueError
        If 'VolumeAll' column is not found in the DataFrame.
    """
    
    p_diff = []  # List to store the difference between max and min prices over the transaction range

    for i in df.index:
        data = df.iloc[i]
        
        # Check if the current entry is not a quote or has valid quotes
        if (not data['Is_Quote']) or data['Valid_Quotes']:
            numTrans = 0 if data['Valid_Quotes'] else 1 
            pmax = -float("inf") if data['Valid_Quotes'] else data['Trade_Price']
            pmin = float("inf") if data['Valid_Quotes'] else data['Trade_Price']
            
            next = i-1
            # Loop backwards to find max and min prices over the specified transaction range
            while next >= 0 and numTrans < delta2:
                d = df.iloc[next]
                if not d['Is_Quote']:
                    numTrans += 1
                if delta1 <= numTrans < delta2:
                    pmax = max(pmax, d['Trade_Price'])
                    pmin = min(pmin, d['Trade_Price'])
                
                next -= 1
            p_diff.append(pmax - pmin)
        else:
            p_diff.append(np.nan)  # Append NaN if the current entry is a quote without valid quotes
    
    try:
        # Generate the name of the volume column based on the given deltas using the assumed feature_name_generator function
        volumeAll = df[feature_name_generator('VolumeAll', delta1, delta2)]
        
        # Calculate the Lambda value for each trade by dividing the price difference by the volume (volumeAll)
        return p_diff / volumeAll

    except KeyError:
        raise ValueError('VolumeAll column not found in DataFrame.')




def generate_trans_LobImbalance(df, delta1, delta2):
    """
    Generate transaction LOB imbalance values for trades in the given DataFrame.

    This function calculates the LOB imbalance values based on the difference 
    between the offer and bid sizes, normalized by their sum, for trades in 
    the provided DataFrame (`df`). The calculation window is based on the 
    specified time deltas `delta1` and `delta2`.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data with columns: Offer_Size, Bid_Size, 
        Is_Quote, and Participant_Timestamp_f.
    delta1 : int
        The first time delta (offset) to define the start of the computation window.
    delta2 : int
        The second time delta (offset) to define the end of the computation window.

    Returns:
    -------
    list
        A list containing the calculated LOB imbalance values.
    """
    
    # Calculate the LOB imbalance for each row
    df['Imbalance'] = (df['Offer_Size'] - df['Bid_Size']) / (df['Offer_Size'] + df['Bid_Size'])

    lobImbalance = []

    # Iterate over the DataFrame's indices
    for i in df.index:
        data = df.iloc[i]

        # Check if the current row is a trade or valid quote
        if (not data['Is_Quote']) or data['Valid_Quotes']:
            numTrans = 0 if data['Valid_Quotes'] else 1 
            imbalances = [data['Imbalance']] if data['Valid_Quotes'] else []
            next = i-1

            while next >= 0 and numTrans < delta2:
                d = df.iloc[next]
                
                # Check if a trade/transaction
                if not d['Is_Quote']:
                    numTrans += 1
                    if numTrans >= delta2:
                        break
                # Check if a quote (valid or invalid)
                else:
                    if delta1 <= numTrans < delta2:
                        imbalances.append(d['Imbalance'])
                
                next -= 1

            # Calculate the average LOB imbalance
            lobImbalance.append(np.nan if not imbalances else sum(imbalances) / len(imbalances))
        else:
            lobImbalance.append(np.nan)

    return lobImbalance


def generate_trans_TxnImbalance(df, delta1, delta2):
    """
    Generate transaction imbalance values based on the specified deltas.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data with columns: Trade_Volume, Trade_Sign, Is_Quote, and Valid_Quotes.
    delta1 : int
        The first transaction delta to define the start of the computation window.
    delta2 : int
        The second transaction delta to define the end of the computation window.

    Returns:
    -------
    list
        A list containing the calculated transaction imbalance values.
    """

    # Compute transaction direction for each trade
    df['Vt_Dir'] = df['Trade_Volume'] * df['Trade_Sign']
    sum_vt_dir = []

    for i in df.index:
        data = df.iloc[i]

        if (not data['Is_Quote']) or data['Valid_Quotes']:
            numTrans = 0 if data['Valid_Quotes'] else 1
            vt_dir = 0 if data['Valid_Quotes'] else data['Vt_Dir']
            next_idx = i - 1

            while next_idx >= 0 and numTrans < delta2:
                d = df.iloc[next_idx]

                if not d['Is_Quote']:
                    numTrans += 1

                if delta1 <= numTrans < delta2:
                    vt_dir += d['Vt_Dir']

                next_idx -= 1

            sum_vt_dir.append(vt_dir)
        else:
            sum_vt_dir.append(np.nan)

    try:
        volume_column_name = feature_name_generator('VolumeAll', delta1, delta2)
        volumeAll = df[volume_column_name]

        return sum_vt_dir / volumeAll
    except KeyError:
        raise ValueError('VolumeAll column not found in DataFrame.')


        
def generate_trans_PastReturn(df, delta1, delta2):
    """
    Calculate the past return based on the specified transaction deltas.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data with columns: Bid_Price, Offer_Price, Is_Quote, Trade_Price, and Valid_Quotes.
    delta1 : int
        The first transaction delta to define the start of the computation window.
    delta2 : int
        The second transaction delta to define the end of the computation window.

    Returns:
    -------
    list
        A list containing the calculated past return values.
    """
    
    # Calculate the mid-price for each row
    df['MidPrice'] = (df['Bid_Price'] + df['Offer_Price']) / 2
    past_ret = []

    for i in df.index:
        data = df.iloc[i]

        # If the current row is a trade or valid quote
        if (not data['Is_Quote']) or data['Valid_Quotes']:
            numTrans = 0 if data['Valid_Quotes'] else 1
            ret_values = [data['MidPrice']] if data['Valid_Quotes'] else []
            pmax = data['Trade_Price'] if not data['Valid_Quotes'] else -float("inf")
            next_idx = i - 1

            while next_idx >= 0 and numTrans < delta2:
                d = df.iloc[next_idx]

                if not d['Is_Quote']:
                    numTrans += 1
                    if numTrans < delta2:
                        pmax = max(pmax, d['Trade_Price'])
                else:
                    if delta1 <= numTrans < delta2:
                        ret_values.append(d['MidPrice'])

                next_idx -= 1

            if not ret_values:
                past_ret.append(np.nan)
            else:
                average_return = sum(ret_values) / len(ret_values)
                past_ret.append(1 - (average_return / pmax))
        else:
            past_ret.append(np.nan)

    return past_ret



# -------------------------------- Volume Mode ------------------------------------------------------

def generate_volume_span(span, trade_volumes):
    """
    Generate volume spans for the given trade volumes.

    This function calculates the volume spans for the provided trade volumes using a sliding window approach.
    A volume span represents the index (position) in the trade volumes list, from which the cumulative sum of volumes
    reaches or exceeds the specified `span` value.

    Parameters:
    ----------
    span : int
        The target sum of volumes to be reached.
    trade_volumes : list
        A list of trade volumes (integers) for which the volume spans need to be calculated.

    Returns:
    -------
    list
        A list containing the volume spans for the trade volumes.

    """
    volume_spans = []
    for idx, val in enumerate(trade_volumes):
        cur = max(idx - 1, 0)
        # Sliding window to find the volume span
        while cur >= 0:
            if cur == 0:
                volume_spans.append(0)
                break
            if sum(trade_volumes[cur:idx + 1]) >= span:
                volume_spans.append(cur + 1)
                break
            cur -= 1

    return volume_spans


def check_volume_span(df, delta1, delta2):
    """
    Ensure volume span indices exist in the DataFrame based on specified deltas.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing trade data, particularly the 'Trade_Volume' column.

    delta1 : float
        Defines the primary volume span.

    delta2 : float
        Defines the secondary volume span.

    Returns:
    -------
    tuple (str, str)
        The names of the primary and secondary volume span indices.
    """

    # Generate the volume span indices based on provided deltas
    spanIdx1 = feature_name_generator('VolumeSpan', delta1)
    spanIdx2 = feature_name_generator('VolumeSpan', delta2)

    # If the primary span index is not in the dataframe, compute and add it
    if spanIdx1 not in df:
        if delta1 == 0:
            df[spanIdx1] = df.index.copy()
        else:
            df[spanIdx1] = generate_volume_span(delta1, df['Trade_Volume'])

    # If the secondary span index is not in the dataframe, compute and add it
    if spanIdx2 not in df:
        df[spanIdx2] = generate_volume_span(delta2, df['Trade_Volume'])

    return spanIdx1, spanIdx2


def generate_vol_VolumeAll(df, delta1, delta2):
    """
    Compute the cumulative volume for the specified volume span indices in the DataFrame.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing trade data, specifically the 'Trade_Volume' column.

    delta1 : float
        Defines the primary volume span.

    delta2 : float
        Defines the secondary volume span.

    Returns:
    -------
    list[float]
        List containing the computed cumulative trade volume for each trade 
        within the defined spans.

    """

    # Ensure the required volume span indices are present in the DataFrame
    spanIdx1, spanIdx2 = check_volume_span(df, delta1, delta2)

    volumeAll = []
    for i in df.index:
        # Get the current and previous indices for the volume span
        cur, pre = df.iloc[i][spanIdx1], df.iloc[i][spanIdx2]
        
        # Calculate the cumulative trade volume for the span and append to the list
        volumeAll.append(df.iloc[pre:cur+1]['Trade_Volume'].sum())

    return volumeAll


def generate_vol_lambda(df, delta1, delta2):
    """
    Compute the Lambda values for a given trade DataFrame based on the specified volume spans.

    The Lambda value for each trade is calculated as the difference between the maximum and 
    minimum trade prices within the span defined by `delta1` and `delta2`, divided by the 
    volume for the same span.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing trade data with columns 'Is_Quote', 'Trade_Price', and 'Trade_Volume'.

    delta1 : float
        Defines the primary volume span.

    delta2 : float
        Defines the secondary volume span.

    Returns:
    -------
    list[float]
        List containing the Lambda values for each trade within the defined spans.

    """

    # Ensure the required volume span indices are present in the DataFrame
    spanIdx1, spanIdx2 = check_volume_span(df, delta1, delta2)

    # Fetch the VolumeAll column based on delta values or raise an error if not present
    try:
        volumeAll = df[feature_name_generator('VolumeAll', delta1, delta2)]
    except KeyError:
        raise ValueError('VolumeAll column not found in DataFrame.')
    
    vol_Lambda = []
    for i in df.index:
        data = df.iloc[i]

        # Skip calculating Lambda for invalid quotes and append NaN instead
        if data['Is_Quote'] and data['Valid_Quotes'] == False:
            vol_Lambda.append(np.nan)
            continue

        # Fetch the current and previous indices for the volume span
        cur, pre = data[spanIdx1], data[spanIdx2]

        # Calculate minimum and maximum trade prices within the span
        p_min, p_max = df.iloc[pre:cur + 1]['Trade_Price'].min(), df.iloc[pre:cur + 1]['Trade_Price'].max()

        # Compute the Lambda value and append to the list
        vol_Lambda.append((p_max - p_min) / volumeAll[i])
    
    return vol_Lambda


    
def generate_vol_lobImbalance(df, delta1, delta2):
    """
    Compute the Limit Order Book (LOB) Imbalance values based on volume spans for a given trade DataFrame.

    The LOB Imbalance for each trade is calculated as the difference between the offer size and the 
    bid size, normalized by the total of offer and bid sizes, averaged over the volume span defined 
    by `delta1` and `delta2`.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing trade data with columns 'Is_Quote', 'Offer_Size', and 'Bid_Size'.

    delta1 : float
        Defines the primary volume span.

    delta2 : float
        Defines the secondary volume span.

    Returns:
    -------
    list[float]
        List containing the LOB Imbalance values for each trade within the defined spans.

    """

    # Ensure the required volume span indices are present in the DataFrame
    spanIdx1, spanIdx2 = check_volume_span(df, delta1, delta2)

    # Calculate the LOB Imbalance for each entry in the DataFrame
    df['Imbalance'] = (df['Offer_Size'] - df['Bid_Size']) / (df['Offer_Size'] + df['Bid_Size'])

    vol_LobImbalance = []
    for i in df.index:
        data = df.iloc[i]

        # Skip calculating LOB Imbalance for quotes and append NaN instead
        if data['Is_Quote'] and data['Valid_Quotes'] == False:
            vol_LobImbalance.append(np.nan)
            continue
        
        # Fetch the current and previous indices for the volume span
        cur, pre = data[spanIdx1], data[spanIdx2]

        # Calculate and append the mean LOB Imbalance over the span
        vol_LobImbalance.append(df.iloc[pre:cur + 1]['Imbalance'].mean())

    return vol_LobImbalance



def generate_vol_txnImbalance(df, delta1, delta2):
    """
    Compute the transaction (txn) imbalance values based on volume spans for a given trade DataFrame.

    For each trade, the txn imbalance is calculated as the sum of the product of 'Trade_Volume' and 
    'Trade_Sign' over the volume span defined by `delta1` and `delta2`, normalized by the volume 
    aggregated over the same span.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing trade data with columns 'Is_Quote', 'Trade_Volume', and 'Trade_Sign'.

    delta1 : float
        Defines the primary volume span.

    delta2 : float
        Defines the secondary volume span.

    Returns:
    -------
    list[float]
        List containing the transaction imbalance values for each trade within the defined spans.
    """
    
    # Ensure the required volume span indices are present in the DataFrame
    spanIdx1, spanIdx2 = check_volume_span(df, delta1, delta2)
    
    # Fetch the volume aggregated over the defined spans
    try:
        volumeAll = df[feature_name_generator('VolumeAll', delta1, delta2)]
    except KeyError:
        raise ValueError('VolumeAll column not found in DataFrame.')

    # Calculate the txn imbalance for each entry in the DataFrame
    df['Vt_Dir'] = df['Trade_Volume'] * df['Trade_Sign']

    vol_TxnImbalance = []
    for i in df.index:
        data = df.iloc[i]

        # Skip calculating txn imbalance for quotes and append NaN instead
        if data['Is_Quote'] and data['Valid_Quotes'] == False:
            vol_TxnImbalance.append(np.nan)
            continue
        
        # Fetch the current and previous indices for the volume span
        cur, pre = data[spanIdx1], data[spanIdx2]

        # Calculate and append the txn imbalance over the span normalized by the volume
        sum_vt_dir = df.iloc[pre:cur + 1]['Vt_Dir'].sum()
        vol_TxnImbalance.append(sum_vt_dir / volumeAll[i])

    return vol_TxnImbalance


    
def generate_vol_pastReturn(df, delta1, delta2):
    """
    Calculate past return values based on volume spans for a given trade DataFrame.

    For each trade, the past return is calculated as the difference between 1 and the ratio of 
    the average trade price to the maximum trade price over the volume span defined by `delta1` and `delta2`.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing trade data with columns 'Is_Quote' and 'Trade_Price'.

    delta1 : float
        Defines the primary volume span.

    delta2 : float
        Defines the secondary volume span.

    Returns:
    -------
    list[float]
        List containing the past return values for each trade within the defined spans.

    """
    
    # Ensure the required volume span indices are present in the DataFrame
    spanIdx1, spanIdx2 = check_volume_span(df, delta1, delta2)
    
    vol_PastReturn = []
    for i in df.index:
        data = df.iloc[i]

        # Skip calculating past return for quotes and append NaN instead
        if data['Is_Quote'] and data['Valid_Quotes'] == False:
            vol_PastReturn.append(np.nan)
            continue
        
        # Fetch the current and previous indices for the volume span
        cur, pre = data[spanIdx1], data[spanIdx2]

        # Calculate and append the past return over the span
        p_max = df.iloc[pre:cur + 1]['Trade_Price'].max()
        p_avg = df.iloc[pre:cur + 1]['Trade_Price'].mean()
        vol_PastReturn.append(1 - p_avg / p_max)

    return vol_PastReturn


# --------------------------------PARENT GENERATOR/WRAPPER FUNCTION ----------------------------------------------

def parent_generator_ret_imb(df, deltas, mode='calendar'):
    """
    Generate features for a given DataFrame based on the specified mode and time deltas.
    
    Depending on the mode selected ('calendar', 'transaction', or 'volume'), the function maps 
    specific feature generator functions to the features and then computes them for each 
    specified time delta pair.
    
    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing trade data.
        
    deltas : list[tuple]
        List of delta pairs to compute features over.
        
    mode : str, optional (default='calendar')
        The mode based on which the features are computed. 
        It can be 'calendar', 'transaction', or 'volume'.
        
    Returns:
    -------
    pandas.DataFrame
        Original DataFrame appended with the generated features.

    Raises:
    ------
    ValueError
        If the mode provided does not match the expected values.
    """
    
    # Mapping of feature generation functions for calendar mode
    cal_feature_mapping = {
        'Lambda': generate_cal_Lambda,
        'LobImbalance': generate_cal_LobImbalance,
        'TxnImbalance': generate_cal_TxnImbalance,
        'PastReturn': generate_cal_PastReturn
    }
    
    # Mapping of feature generation functions for transaction mode
    trans_feature_mapping = {
        'Lambda': generate_trans_Lambda,
        'LobImbalance': generate_trans_LobImbalance,
        'TxnImbalance': generate_trans_TxnImbalance,
        'PastReturn': generate_trans_PastReturn
    }
    
    # Mapping of feature generation functions for volume mode
    vol_feature_mapping = {
        'Lambda': generate_vol_lambda,
        'LobImbalance': generate_vol_lobImbalance,
        'TxnImbalance': generate_vol_txnImbalance,
        'PastReturn': generate_vol_pastReturn
    }
    
    # Select the appropriate feature mapping based on mode
    if mode == 'calendar':
        feature_mapping = cal_feature_mapping
    elif mode == 'transaction':
        feature_mapping = trans_feature_mapping
    elif mode == 'volume':
        feature_mapping = vol_feature_mapping
    else:
        raise ValueError("Invalid mode provided. Expected 'calendar', 'transaction', or 'volume'.")
        
    feature_df = pd.DataFrame()
    
    # Generate features for each delta pair and append to feature DataFrame
    for f, f_function in feature_mapping.items():
        for delta in deltas:
            d1, d2 = delta[0], delta[1]
            f_name = feature_name_generator(f, d1, d2)
            feature_df[f_name] = f_function(df, d1, d2)
    
    # Concatenate the original DataFrame with the generated features
    return pd.concat([df, feature_df], axis=1)






if __name__ == "__main__":
    print('Test generator functions')
    # df = pd.read_csv("testData/PreparedData.csv")
    # generateVolumeAll(df, M = 'calendar')