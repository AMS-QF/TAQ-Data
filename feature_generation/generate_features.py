import argparse
from typing import List, Union

import pandas as pd

from feature_generation.generators import parent_generator
from feature_generation.list_features import list_features


def generate_features(
    trade_features: List[str] = None, quote_features: List[str] = None, input_file=List[Union[str, None]]
):

    # If no features are specified, generate all features (raw and generated)
    if trade_features is None and quote_features is None:
        trade_features, quote_features = list_features(names_only=True)

    # for each file, generate features
    for file in input_file:
        # read in raw data file
        df = pd.read_csv(f"{file}", low_memory=False)

        # remove .csv from file name
        if ".csv" in file:
            file = file.replace(".csv", "")

        # replace raw_data with features
        file = file.replace("data/raw_data/", "data/features/")

        if "cleaned" in file:
            file = file.replace("cleaned", "")

        # generate trade features via parent_generator
        if trade_features and "trades" in file:
            """Generate Trade Features"""

            features_to_generate = [feature for feature in trade_features if feature not in df.columns]

            print("Generating Features: {}".format(features_to_generate))

            df = pd.DataFrame(list(map(lambda x: parent_generator(df, x), features_to_generate))[-1])

            df.to_csv(f"{file}_features.csv", index=False)

        # generate quote features via parent_generator
        elif quote_features and "quotes" in file:
            """Generate Quote Features"""

            features_to_generate = [feature for feature in quote_features if feature not in df.columns]

            print("Generating Features: {}".format(features_to_generate))

            df = pd.DataFrame(list(map(lambda x: parent_generator(df, x), features_to_generate))[-1])

            df.to_csv(f"{file}_features.csv", index=False)

        else:
            print(f"File {file} is not a trades or quotes file or no features were specified.")


# python scripts/feature_gen/generate_features.py
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Generate Features")
    parser.add_argument("--trade_features", nargs="+", help="Features to Generate")
    parser.add_argument("--quote_features", nargs="+", help="Features to Generate")
    parser.add_argument("--input_file", default=[None], help="Directory of Raw Data")

    args = parser.parse_args()

    # generate all features if no features are specified
    if args.trade_features is None and args.quote_features is None:
        generate_features(input_file=[args.input_file])

    else:
        generate_features(args.trade_features, args.quote_features, input_file=[args.input_file])
