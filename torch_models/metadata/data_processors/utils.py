from __future__ import annotations

import pandas as pd
import numpy as np



def convert_participant_timestamps(date, pts) -> pd.Series:
    """
    convert participant timestamp 
    """
    date = pd.to_datetime(date)
    pts_str = np.char.zfill(np.array(pts).astype(str), 15)
    pts = pd.to_datetime(pts_str, format="%H%M%S%f")
    return date + pd.to_timedelta(
        pts.hour * 60 * 60 * 1e9 +   # Convert hours to nanoseconds
        pts.minute * 60 * 1e9 +     # Convert minutes to nanoseconds
        pts.second * 1e9 +          # Convert seconds to nanoseconds
        pts.microsecond * 1e3       # Convert microseconds to nanoseconds
    )


def drop_irreg_hours(df):
    """
    drop first 15 min and last 15 min trading
    """
    pts = df['Participant_Timestamp']
    mask = (pts.dt.time < pd.Timestamp("09:15:00").time()) | \
        (pts.dt.time > pd.Timestamp("15:45:00").time())
    drop_idx = df[mask].index
    new_df = df.drop(drop_idx)
    return new_df


def generate_trade_side(prices):
    """
    Classify trade direction using tick test.
    """
    trade_directions = [np.nan] #default value for the first

    for i in range(1, len(prices)):
        if prices[i] > prices[i-1]:
            trade_directions.append(1)
        elif prices[i] < prices[i-1]:
            trade_directions.append(-1)
        else:
            trade_directions.append(trade_directions[i-1])
    
    return trade_directions
