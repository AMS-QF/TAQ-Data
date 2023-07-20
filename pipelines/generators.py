import pandas as pd
import numpy as np
from datetime import timedelta
from sortedcollections import OrderedSet
import time
import sys
sys.path.insert(1, '../testData')

# calendar mode
calendar_spreads = [(0, .1), (.1, .2)]
volumeAlls = ['VolumeAll_C_.0', 'VolumeAll_C_.1']
priceDiffs = ['PriceDiff_C_.0', 'PriceDiff_C_.1']
lambdas = ['Lambda_C_.0', 'Lambda_C_.1']
lobImbalance = ['LobImbalance_C_.0', 'LobImbalance_C_.1']
txnVtDirs = ['TxnVtDirs_C_.0', 'TxnVtDirs_C_.1']
txnImbalances = ['TxnImbalance_C_.0', 'TxnImbalance_C_.1']
avgReturns = ['AvgReturns_C_.0', 'AvgReturns_C_.1']
pMaxs = ['PMax_C_.0', 'PMax_C_.1']
pastReturns = ['PastReturn_C_.0', 'PastReturn_C_.1']

# transaction mode
transaction_spreads = [(2, 4),(4, 8)]
volumeAllsT = ['VolumeAll_T_2_4', 'VolumeAll_4_8']
priceDiffsT = ['PriceDiff_T_2_4', 'PriceDiff_T_4_8']
txnVtDirsT = ['TxnVtDirs_T_2_4', 'TxnVtDirs_T_4_8']
txnImbalancesT = ['TxnImbalance_T_2_4', 'TxnImbalance_T_4_8']
avgReturnsT = ['AvgReturns_T_2_4', 'AvgReturns_T_4_8']
pMaxsT = ['PMax_T_2_4', 'PMax_T_4_8']

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


# ---------------------- Calendar & Transaction Mode -------------------------------------------------------

def generateVolumeAll(df, M = 'calendar'):
    """
    Generate the VolumeAll feature for the dataframe.

    This function calculates the VolumeAll feature for trades data in the dataframe.
    The volumeAll calculation is based on the specified time mode (`M`).

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    M : str
        Time Mode. Default is 'calendar'.

    Returns:
    -------
    pandas.DataFrame
        DataFrame with the added volumeAll feature for all participants.

    """
    if M == 'calendar':
        for i in range(len(calendar_spreads)):
            delta1, delta2 = calendar_spreads[i][0], calendar_spreads[i][1]
            df[volumeAlls[i]] = df[df['Is_Quote'] == False]['Participant_Timestamp_f']\
                .apply(lambda t:
                       sum(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')]
                           ['Trade_Volume']))
    if M == 'transaction':
        for i in range(len(transaction_spreads)):       
            delta1, delta2 = transaction_spreads[i][0], transaction_spreads[i][1]
            df[volumeAllsT[i]] = df[df['Is_Quote'] == False]['Trade_Volume'].shift(delta1).rolling(delta2-delta1).sum()
    
    
    return df


def generatePriceDiff(df, M = 'calendar'):
    """
    Generate trade price differences (max-min) for all data in the given DataFrame.

    This function calculates the max and min price differences for all trade price in the dataframe.
    The price difference calculation is based on the specified mode (`M`).

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    M : str, optional
        Time Mode. Default is 'calendar'.

    Returns:
    -------
    pandas.DataFrame
        DataFrame with the added price difference column.

    """
    if M == 'calendar':
        for i in range(len(calendar_spreads)):
            delta1, delta2 = calendar_spreads[i][0], calendar_spreads[i][1]
            df[priceDiffs[i]] = df[df['Is_Quote'] == False]['Participant_Timestamp_f'].apply(
                lambda t: max(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')][
                                  'Trade_Price'], default=np.nan) -
                          min(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')][
                                  'Trade_Price'], default=np.nan)
            )
    
    if M == 'transaction':
        for i in range(len(transaction_spreads)):
            delta1, delta2 = transaction_spreads[i][0], transaction_spreads[i][1]
            df['maxPrix'] = df[df['Is_Quote'] == False]['Trade_Price'].shift(delta1).rolling(delta2-delta1).max()
            df['minPrix'] = df[df['Is_Quote'] == False]['Trade_Price'].shift(delta1).rolling(delta2-delta1).min()
            df[priceDiffsT[i]] = df['maxPrix'] - df['minPrix']


    return df



def generateLambda(df, M='calendar'):
    """
    Generate lambda values for the given DataFrame.

    This function calculates the lambda values for the trade data.
    The calculation is based on the specified method (`M`).

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    M : str, optional
        Method for lambda calculation. Default is 'calendar'.

    Returns:
    -------
    pandas.DataFrame
        DataFrame with the added lambda values.

    """
    if M == 'calendar':
        for i in range(len(calendar_spreads)):
            # Calculate lambda values using price differences and volume alls
            df[lambdas[i]] = df[priceDiffs[i]] / df[volumeAlls[i]]

    if M == 'transaction':
        for i in range(len(transaction_spreads)):
            # Calculate lambda values using transaction price differences and volume alls
            df[lambdasT[i]] = df[priceDiffsT[i]] / df[volumeAllsT[i]]

    return df 



def generateLobImbalance(df, M='calendar'):
    """
    Generate level of book (LOB) average imbalance values for quotes in the given DataFrame.

    This function calculates the level of book imbalance values for quotes in the provided DataFrame (`df`).
    The imbalance calculation is based on the specified method (`M`).

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    M : str, optional
        Method for imbalance calculation. Default is 'calendar'.

    Returns:
    -------
    pandas.DataFrame
        DataFrame with the added LOB imbalance values for quotes.

    """
    if M == 'calendar':
        for i in range(len(calendar_spreads)):
            delta1, delta2 = calendar_spreads[i][0], calendar_spreads[i][1]
            df[lobImbalance[i]] = df[df['Is_Quote']]['Participant_Timestamp_f'].apply(
                lambda t: df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')][
                    'Imbalance'].mean()
            )

    return df


def generateTxnVtDir(df, M = 'calendar'):
    """
    Generate transaction volume times trade direction values for trades in the given DataFrame.

    This function calculates the transaction volume * trade direction values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified method (`M`).

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    M : str, optional
        Method for transaction volume direction calculation. Default is 'calendar'.

    Returns:
    -------
    pandas.DataFrame
        DataFrame with the added transaction volume direction values for trades.

    """
    if M == 'calendar':
        for i in range(len(calendar_spreads)):
            delta1, delta2 = calendar_spreads[i][0], calendar_spreads[i][1]
            df[txnVtDirs[i]] = df[df['Is_Quote'] == False]['Participant_Timestamp_f'].apply(
                lambda t: sum(df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')][
                                  'Vt_Dir'])
            )
    if M == 'transaction':
        for i in range(len(transaction_spreads)):
            delta1, delta2 = transaction_spreads[i][0], transaction_spreads[i][1]
            df[txnVtDirsT[i]] = df[df['Is_Quote'] == False]['Vt_Dir'].shift(delta1).rolling(delta2-delta1).sum()

    return df


def generateTxnImbalance(df, M='calendar'):
    """
    Generate transaction imbalance values for the given DataFrame.

    This function calculates the transaction imbalance values for the provided DataFrame (`df`).
    The calculation is based on the specified method (`M`).

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    M : str, optional
        Method for transaction imbalance calculation. Default is 'calendar'.

    Returns:
    -------
    pandas.DataFrame
        DataFrame with the added transaction imbalance values.

    """
    if M == 'calendar':
        for i in range(len(calendar_spreads)):
            # Calculate transaction imbalance values using transaction volume directions and volume alls
            df[txnImbalances[i]] = df[txnVtDirs[i]] / df[volumeAlls[i]]

    if M == 'transaction':
        for i in range(len(transaction_spreads)):
            # Calculate transaction imbalance values using transaction volume directionTs and volume allsT
            df[txnImbalancesT[i]] = df[txnVtDirsT[i]] / df[volumeAllsT[i]]

    return df


def generateAvgReturn(df, M='calendar'):
    """
    Generate average return values for trades in the given DataFrame.

    This function calculates the average return values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified method (`M`).

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    M : str, optional
        Method for average return calculation. Default is 'calendar'.

    Returns:
    -------
    pandas.DataFrame
        DataFrame with the added average return values for trades.

    """
    if M == 'calendar':
        for i in range(len(calendar_spreads)):
            delta1, delta2 = calendar_spreads[i][0], calendar_spreads[i][1]
            df[avgReturns[i]] = df[df['Is_Quote'] == False]['Participant_Timestamp_f'].apply(
                lambda t: df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')][
                    'Trade_Price'].mean()
            )
    
    if M == 'transaction':
        for i in range(len(transaction_spreads)):
            delta1, delta2 = transaction_spreads[i][0], transaction_spreads[i][1]
            df[avgReturnsT[i]] = df[df['Is_Quote'] == False]['Trade_Price'].shift(delta1).rolling(delta2-delta1).mean()

    return df


def generatePriceMax(df, M='calendar'):
    """
    Generate maximum price values for trades in a given time frame.

    This function calculates the maximum price values for trades in the provided DataFrame (`df`).
    The calculation is based on the specified method (`M`).

    Parameters:
    ----------
    df : pandas.DataFrame
        DataFrame containing the data.
    M : str, optional
        Method for maximum price calculation. Default is 'calendar'.

    Returns:
    -------
    pandas.DataFrame
        DataFrame with the added maximum price values for trades.
    """
    if M == 'calendar':
        for i in range(len(calendar_spreads)):
            delta1, delta2 = calendar_spreads[i][0], calendar_spreads[i][1]
            df[pMaxs[i]] = df[df['Is_Quote'] == False]['Participant_Timestamp_f'].apply(
                lambda t: max(
                    df[df['Participant_Timestamp_f'].between(t - delta2, t - delta1, inclusive='neither')][
                        'Trade_Price'],
                    default=np.nan
                )
            )
            
    if M == 'transaction':
        for i in range(len(transaction_spreads)):
            delta1, delta2 = transaction_spreads[i][0], transaction_spreads[i][1]
            df[pMaxsT[i]] = df[df['Is_Quote'] == False]['Trade_Price'].shift(delta1).rolling(delta2-delta1).max()

    return df

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
