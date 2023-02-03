import argparse

import pandas as pd

from data_preprocessing.preprocess import chunk_clean


def clean_data(input_file: str):

    if "TRADES" in input_file:
        df = pd.read_csv(input_file, low_memory=False)
        df = chunk_clean(df, False)

    elif "QUOTES" in input_file:
        df = pd.read_csv(input_file, low_memory=False)
        df = chunk_clean(df, True)

    else:
        print("Input file is not a trades or quotes file")


# python data_preprocessing/clean_data.py --input_file <input_file>
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-- input_file", type=str, default="AAPL.csv")

    args = parser.parse_args()

    clean_data(args.input_file)
