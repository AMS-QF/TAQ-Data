import argparse
import os
from typing import List, Union

import pandas as pd

from feature_generation.generators import parent_generator
from feature_generation.list_features import list_features


def generate_features(
    trade_features: List[str] = None, quote_features: List[str] = None, input_file=List[Union[str, None]]
):

    # input file can be trades or quotes or reconstructed events

    # If no features are specified, generate all features (raw and generated)
    if trade_features is None and quote_features is None:
        trade_features, quote_features = list_features(names_only=True)

    # for each file, generate features
    for file in input_file:
        # read in raw data file accounting for iter index
        df = pd.read_csv(f"{file}", low_memory=False, index_col="index")

        # remove .csv from file name
        if ".csv" in file:
            file = file.replace(".csv", "")

        # replace raw_data with features
        file = file.replace("data/raw_data/", "data/features/")

        if "cleaned" in file:
            file = file.replace("cleaned", "")

        # get directory name
        dir_name = file.split("/")[:-1]
        dir_name = "/".join(dir_name)

        is_exist = os.path.exists(dir_name)
        if not is_exist:
            os.makedirs(dir_name)

        trade_features_to_generate = [feature for feature in trade_features if feature not in df.columns]
        quote_features_to_generate = [feature for feature in quote_features if feature not in df.columns]

        features_to_generate = set(trade_features_to_generate + quote_features_to_generate)

        print("Generating Features: {}".format(features_to_generate))

        # generate features
        for feature in features_to_generate:
            df_copy, features_to_add = parent_generator(df, feature)

            # add features to dataframe
            for feature in features_to_add:
                df[feature] = df_copy[feature]

        # save to file
        df.index.name = "index"
        df.to_csv(f"{file}_features.csv", index=True)


# python generate_features.py  --input_file ../data/raw_data/2020-01-02/AMZN_reconstructed.csv
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
