import argparse
from typing import Dict, List, Union

import pandas as pd


def reconstruct_book_events(input_files: Union[List[Dict], None] = None):
    """Reconstruct the book events from the cleaned data"""

    path_list = []
    # iterate through each day
    for day in input_files:

        print("Reconstructing book events for {}".format(day["trades"]))

        base_path = day["trades"].split("trades")[0]

        trades = pd.read_csv(day["trades"], index_col=0, low_memory=False, on_bad_lines="skip")
        quotes = pd.read_csv(day["quotes"], index_col=0, low_memory=False, on_bad_lines="skip")

        trades["index_to_sort"] = trades.index
        quotes["index_to_sort"] = quotes.index

        all_events = pd.concat([trades, quotes], axis=0)

        # sort by participant timestamp
        all_events = all_events.sort_values(by=["index_to_sort"])

        # label mox identifier for each event

        # save to file
        reconstructed_path = base_path + "reconstructed.csv"

        # save file names
        all_events = all_events.drop(columns=["index_to_sort"])
        all_events.index.name = "index"

        all_events.to_csv(reconstructed_path, index=True)

        path_list.append(reconstructed_path)

    return path_list


# python event_reconstruction.py --input_files ../data/raw_data/2020-01-02/AMZN_trades_cleaned.csv ../data/raw_data/2020-01-02/AMZN_quotes_cleaned.csv
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--input_files", type=str, nargs="+", required=True)

    args = parser.parse_args()

    input_files = args.input_files

    input_files = [{"trades": input_files[0], "quotes": input_files[1]}]

    reconstruct_book_events(input_files)
