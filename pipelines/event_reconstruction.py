from typing import Dict, List, Union

import pandas as pd

### TO-DO: Reconstruct book events before feature generation


def reconstruct_book_events(input_files: Union[List[Dict], None] = None):
    """Reconstruct the book events from the cleaned data"""

    path_list = []
    # iterate through each day
    for day in input_files:

        print("Reconstructing book events for {}".format(day["trades"]))

        base_path = day["trades"].split("trades")[0]

        trades = pd.read_csv(day["trades"], index_col=0, low_memory=False)
        quotes = pd.read_csv(day["quotes"], index_col=0, low_memory=False)

        trades["participant_timestamp"] = trades.index
        quotes["participant_timestamp"] = quotes.index

        all_events = pd.concat([trades, quotes], axis=0)

        # sort by participant timestamp
        all_events = all_events.sort_values(by=["participant_timestamp"])

        # label mox identifier for each event

        # save to file
        reconstructed_path = base_path + "reconstructed.csv"

        all_events.to_csv(reconstructed_path)

        path_list.append(reconstructed_path)

    return path_list
