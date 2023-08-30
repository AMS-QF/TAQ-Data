import pandas as pd
import numpy as np
from datetime import timedelta
from sortedcollections import OrderedSet
import time
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




# -------------------------------------------------------------------------------------------

def generate_transaction_return(df, span, mode='calendar'):
    
    if mode == 'calendar':
        avgPrice = df[df['Is_Quote'] == False]['Participant_Timestamp_f'].apply(lambda t:
            df[df['Participant_Timestamp_f'].between(t, t+span, inclusive='neither')]['Trade_Price'].mean()
        )
    else:
        avgPrice = None
    return avgPrice / df['Trade_Price'] - 1


def feature_name_generator(feature_name: str, delta1: float, delta2: float) -> str:
    return feature_name + '_' + str(delta1) + '_' + str(delta2)


# ---------------------- Calendar Mode -------------------------------------------------------
def generate_cal_VolumeAll(df, delta1, delta2):
    """
    Generate calendar VolumeAll values for trades in the given DataFrame.

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
    return df['Participant_Timestamp_f'].apply(lambda t: sum(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')]['Trade_Volume']))


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
    
    # Selecting only the rows where 'Is_Quote' is False i.e., trades
    trades_df = df[df['Is_Quote'] == False]

    # Calculate the maximum trade price for each trade within the specified time window
    p_max = trades_df['Participant_Timestamp_f'].apply(
        lambda t: max(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')]['Trade_Price'], default=np.nan)
    )

    # Calculate the minimum trade price for each trade within the specified time window
    p_min = trades_df['Participant_Timestamp_f'].apply(
        lambda t: min(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')]['Trade_Price'], default=np.nan)
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
        lambda t: df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')]['Imbalance'].mean()
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
        lambda t: sum(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')]['Vt_Dir'])
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
    avg_return = df[df['Is_Quote'] == False]['Participant_Timestamp_f'].apply(
        lambda t: df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')]['Trade_Price'].mean())

    # Calculate the maximum trade price for each trade within the specified time window
    p_max = df[df['Is_Quote'] == False]['Participant_Timestamp_f'].apply(
        lambda t: max(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')]['Trade_Price'], default=np.nan))

    # Calculate the PastReturn value for each trade
    return 1 - avg_return / p_max



# ---------------------- Transaction Mode -------------------------------------------------------

def generate_trans_VolumeAll(df, delta1, delta2):
    """
    Generate transaction VolumeAll values for trades in the given DataFrame.

    This function calculates the transaction VolumeAll values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified time deltas `delta1` and `delta2`.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    delta1 : pandas.Timedelta
        The first time delta (offset) to calculate the start of the rolling window.
    delta2 : pandas.Timedelta
        The second time delta (offset) to calculate the end of the rolling window.

    Returns:
    -------
    pandas.Series
        A pandas Series containing the calculated transaction VolumeAll values.
    """
    # Shift 'Trade_Volume' column by `delta1` time offset
    shifted_trade_volume = df[df['Is_Quote'] == False]['Trade_Volume'].shift(delta1)

    # Calculate the rolling sum of the shifted trade volumes within the window defined by `delta2-delta1`
    return shifted_trade_volume.rolling(delta2 - delta1).sum()


def generate_trans_Lambda(df, delta1, delta2):
    """
    Generate transaction Lambda values for trades in the given DataFrame.

    This function calculates the transaction Lambda values for trades based on the difference 
    between rolling maximum and minimum trade prices within a defined window and the corresponding volume.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    delta1 : pandas.Timedelta
        The first time delta (offset) to calculate the start of the rolling window.
    delta2 : pandas.Timedelta
        The second time delta (offset) to calculate the end of the rolling window.

    Returns:
    -------
    pandas.Series
        A pandas Series containing the calculated transaction Lambda values.

    Raises:
    ------
    ValueError:
        If the required VolumeAll column (based on delta1, delta2) is not found in df.
    """

    # Filter the DataFrame to select only trades, not quotes
    trades_df = df[df['Is_Quote'] == False]

    # Shift 'Trade_Price' column by `delta1` time offset
    shifted_trade_price = trades_df['Trade_Price'].shift(delta1)

    # Calculate the rolling maximum and minimum of the shifted trade prices within the window defined by `delta2-delta1`
    p_max = shifted_trade_price.rolling(delta2 - delta1).max()
    p_min = shifted_trade_price.rolling(delta2 - delta1).min()

    # Calculate the price difference (p_max - p_min) for each trade
    p_diff = p_max - p_min
    
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

    This function calculates the LOB imbalance values based on the difference between the offer 
    and bid sizes, normalized by their sum, for trades in the provided DataFrame (`df`). 
    The calculation window is based on the specified time deltas `delta1` and `delta2`.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data with columns: Offer_Size, Bid_Size, Is_Quote, 
        and Participant_Timestamp_f.
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

    # Container for the resulting LOB imbalance values
    lobImbalance = []

    # Iterate over the DataFrame's indices
    for i in df.index:

        # If the current row is a quote, append NaN to the results and continue
        if df.iloc[i]['Is_Quote']:
            lobImbalance.append(np.nan)
            continue

        # Define the start and end timestamps of the computation window
        t1 = df[df['Is_Quote'] == False].shift(delta1)['Participant_Timestamp_f'][i]
        t2 = df[df['Is_Quote'] == False].shift(delta2)['Participant_Timestamp_f'][i]

        # Calculate the mean LOB imbalance within the defined window
        val = df[df['Participant_Timestamp_f'].between(t2, t1, inclusive='right')]['Imbalance'].mean()
        
        # Append the calculated value to the results
        lobImbalance.append(val)

    return lobImbalance


def generate_trans_TxnImbalance(df, delta1, delta2):
    """
    Generate transaction TxnImbalance values for trades in the given DataFrame.

    This function calculates the transaction TxnImbalance values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified time deltas `delta1` and `delta2`.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    delta1 : pandas.Timedelta
        The first time delta (offset) to calculate the start of the rolling window.
    delta2 : pandas.Timedelta
        The second time delta (offset) to calculate the end of the rolling window.
  
    Returns:
    -------
    pandas.Series
        A pandas Series containing the calculated transaction TxnImbalance values.
    """
    # Calculate the transaction imbalance for each trade and store it in the 'Vt_Dir' column
    df['Vt_Dir'] = df['Trade_Volume'] * df['Trade_Sign']

    # Shift the 'Vt_Dir' column by `delta1` time offset
    shifted_vt_dir = df[df['Is_Quote'] == False]['Vt_Dir'].shift(delta1)

    # Calculate the rolling sum of the shifted transaction imbalances within the window defined by `delta2-delta1`
    vt_dir = shifted_vt_dir.rolling(delta2 - delta1).sum()
    
    try:
        # Generate the name of the volume column based on the given deltas using the assumed feature_name_generator function
        volumeAll = df[feature_name_generator('VolumeAll', delta1, delta2)]
        
        # Calculate the TxnImbalance value for each trade by dividing the price difference by the volume (volumeAll)
        return vt_dir / volumeAll 

    except KeyError:
        raise ValueError('VolumeAll column not found in DataFrame.')

        
def generate_trans_PastReturn(df, delta1, delta2):
    """
    Generate transaction PastReturn values for trades in the given DataFrame.

    This function calculates the transaction PastReturn values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified time deltas `delta1` and `delta2`.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    delta1 : pandas.Timedelta
        The first time delta (offset) to calculate the start of the rolling window.
    delta2 : pandas.Timedelta
        The second time delta (offset) to calculate the end of the rolling window.

    Returns:
    -------
    pandas.Series
        A pandas Series containing the calculated transaction PastReturn values.
    """
    # Shift 'Trade_Price' column by `delta1` time offset
    shifted_trade_price = df[df['Is_Quote'] == False]['Trade_Price'].shift(delta1)

    # Calculate the rolling mean of the shifted trade prices within the window defined by `delta2-delta1`
    avg_return = shifted_trade_price.rolling(delta2 - delta1).mean()

    # Calculate the rolling maximum of the shifted trade prices within the window defined by `delta2-delta1`
    p_max = shifted_trade_price.rolling(delta2 - delta1).max()

    # Calculate the PastReturn value for each trade by subtracting the rolling mean (`avg_return`) from `p_max`
    # and dividing the result by `p_max`
    return 1 - avg_return / p_max


# -------------------------------- Volume Mode -----------------------------------------------------------------

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



def generate_vol_lambda(df, preIdxCol, curIdxCol, volumeAll):
    """
    Generate volume lambda values for trades in the given DataFrame.

    This function calculates the volume lambda values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified columns representing the previous index, current index,
    and the total volume (trade volume) for each trade.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    preIdxCol : str
        Column name representing the previous index of the trade in the DataFrame.
    curIdxCol : str
        Column name representing the current index of the trade in the DataFrame.
    volumeAll : str
        Column containing the corresponding VolumeAll for each trade in the DataFrame.

    Returns:
    -------
    list
        A list containing the volume lambda values for trades.

    """
    vol_Lambda = []
    for i in df.index:
        if df.iloc[i]['Is_Quote']:
            vol_Lambda.append(np.nan)
            continue
        pre = df.iloc[i][preIdxCol]
        cur = i if not curIdxCol else df.iloc[i][curIdxCol]
        p_min, p_max = df.iloc[pre:cur + 1]['Trade_Price'].min(), df.iloc[pre:cur + 1]['Trade_Price'].max()
        vol_Lambda.append((p_max - p_min) / df.iloc[i][volumeAll])

    return vol_Lambda

    
    
def generate_vol_lobImbalance(df, preIdxCol, curIdxCol):
    """
    Generate volume LOB imbalance values for trades in the given DataFrame.

    This function calculates the volume LOB imbalance values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified columns representing the previous index and current index of each trade.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    preIdxCol : str
        Column name representing the previous index of the trade in the DataFrame.
    curIdxCol : str
        Column name representing the current index of the trade in the DataFrame.

    Returns:
    -------
    list
        A list containing the volume LOB imbalance values for trades.

    """
    vol_LobImbalance = []
    for i in df.index:
        pre = df.iloc[i][preIdxCol]
        cur = i if not curIdxCol else df.iloc[i][curIdxCol]
        vol_LobImbalance.append(df.iloc[pre:cur + 1]['Imbalance'].mean())

    return vol_LobImbalance


def generate_vol_txnImbalance(df, preIdxCol, curIdxCol, volumeAll):
    """
    Generate volume transaction imbalance values for trades in the given DataFrame.

    This function calculates the volume transaction imbalance values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified columns representing the previous index, current index,
    and the total volume (trade volume) for each trade.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    preIdxCol : str
        Column name representing the previous index of the trade in the DataFrame.
    curIdxCol : str
        Column name representing the current index of the trade in the DataFrame.
    volumeAll : str
        Column containing the corresponding VolumeAll for each trade in the DataFrame.

    Returns:
    -------
    list
        A list containing the volume transaction imbalance values for trades.

    """
    vol_TxnImbalance = []
    for i in df.index:
        if df.iloc[i]['Is_Quote']:
            vol_TxnImbalance.append(np.nan)
            continue
        pre = df.iloc[i][preIdxCol]
        cur = i if not curIdxCol else df.iloc[i][curIdxCol]
        sum_vt_dir = df.iloc[pre:cur + 1]['Vt_Dir'].sum()
        vol_TxnImbalance.append(sum_vt_dir / df.iloc[i][volumeAll])

    return vol_TxnImbalance


    
def generate_vol_pastReturn(df, preIdxCol, curIdxCol):
    """
    Generate volume past return values for trades in the given DataFrame.

    This function calculates the volume past return values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified columns representing the previous index and current index of each trade.

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    preIdxCol : str
        Column name representing the previous index of the trade in the DataFrame.
    curIdxCol : str
        Column name representing the current index of the trade in the DataFrame.

    Returns:
    -------
    list
        A list containing the volume past return values for trades.

    """
    vol_PastReturn = []
    for i in df.index:
        if df.iloc[i]['Is_Quote']:
            vol_PastReturn.append(np.nan)
            continue
        pre = df.iloc[i][preIdxCol]
        cur = i if not curIdxCol else df.iloc[i][curIdxCol]
        p_max = df.iloc[pre:cur + 1]['Trade_Price'].max()
        p_avg = df.iloc[pre:cur + 1]['Trade_Price'].mean()
        vol_PastReturn.append(1 - p_avg / p_max)

    return vol_PastReturn




if __name__ == "__main__":
    print('Test generator functions')
    df = pd.read_csv("testData/PreparedData.csv")
    # generateVolumeAll(df, M = 'calendar')
