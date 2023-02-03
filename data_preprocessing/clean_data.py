import argparse

from data_preprocessing.preprocess import chunk_clean


def clean_data(input_file: str):

    if "trades" in input_file:
        print("Cleaning trades file: {}".format(input_file))

        clean_path = chunk_clean(input_file, False)

    elif "quotes" in input_file:
        print("Cleaning quotes file: {}".format(input_file))

        clean_path = chunk_clean(input_file, True)

    else:
        print("Input file is not a trades or quotes file")

        return

    return clean_path


# python data_preprocessing/clean_data.py --input_file <input_file>
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-- input_file", type=str, default="AAPL.csv")

    args = parser.parse_args()

    clean_data(args.input_file)
