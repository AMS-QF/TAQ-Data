import argparse
import os
from typing import List, Union

import pandas as pd

from feature_generation.generators import parent_generator
from feature_generation.list_features import list_features

PATH = "data/"


def generate_features(
    trade_features: List[str] = None, quote_features: List[str] = None, input_file=List[Union[str, None]]
):

    # If no features are specified, generate all features (raw and generated)
    if trade_features is None and quote_features is None:
        trade_features, quote_features = list_features(names_only=True)

    # get all files in raw_data
    all_files = os.listdir(f"{PATH}/raw_data")

    assert len(all_files) > 0, "No files in raw_data"

    # case where file is specified
    file_found = False
    if input_file is not None:
        for file in input_file:
            if file not in all_files:
                print(f"File {file} not in raw_data")
                continue

            file_found = True

        # if no file is found, return
        if not file_found:
            return

        # if file is found, set all_files to input_file
        all_files = input_file

    # for each file, generate features
    for file in all_files:
        # read in raw data file
        df = pd.read_csv(f"{PATH}/raw_data/{file}", low_memory=False)

        # generate trade features via parent_generator
        if trade_features:
            """Generate Trade Features"""

            features_to_generate = [feature for feature in trade_features if feature not in df.columns]

            print("Generating Features: {}".format(features_to_generate))

            df = pd.DataFrame(list(map(lambda x: parent_generator(df, x), features_to_generate))[-1])

            df.to_csv(f"{PATH}/features/{file}_features.csv", index=False)

        # generate quote features via parent_generator
        if quote_features:
            """Generate Quote Features"""

            features_to_generate = [feature for feature in quote_features if feature not in df.columns]

            print("Generating Features: {}".format(features_to_generate))

            df = pd.DataFrame(list(map(lambda x: parent_generator(df, x), features_to_generate))[-1])

            print(df)

            df.to_csv(f"{PATH}/features/{file}_features.csv", index=False)


# python scripts/feature_gen/generate_features.py
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Generate Features")
    parser.add_argument("--trade_features", nargs="+", help="Features to Generate")
    parser.add_argument("--quote_features", nargs="+", help="Features to Generate")
    parser.add_argument("--input_file", default=[None], help="Directory of Raw Data")

    args = parser.parse_args()

    # generate all features if no features are specified
    if args.trade_features is None and args.quote_features is None:
        generate_features(input_file=args.input_file)

    else:
        generate_features(args.trade_features, args.quote_features, input_file=args.input_file)
