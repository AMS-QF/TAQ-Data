import argparse
import importlib.util
from pathlib import Path

import pandas as pd

from data_preprocessing.get_data import get_trades


def run_jobs(symbol: str, start_date: str, end_date: str, row_limit=1000000):
    """Run all jobs for a given symbol and date range"""

    # Define the absolute path to the module
    module_path = Path("data_preprocessing/get_data.py")

    # Ensure the module file exists
    assert module_path.is_file(), f"File does not exist: {module_path}"

    # Use importlib to load the module
    spec = importlib.util.spec_from_file_location("get_data", module_path)
    get_data = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(get_data)

    # To-DO: create get-reference-data function

    # load data
    # get_ref(symbol, start_date, end_date, row_limit)
    get_trades(symbol, start_date, end_date, row_limit)
    # get_quotes(symbol, start_date, end_date, row_limit)

    # TO-DO - return paths of raw data to be cleaned

    # # clean data
    # ref_clean_path = clean_data.clean_data(ref_path)
    # trade_clean_path = clean_data.clean_data(trade_path)
    # quote_clean_path = clean_data.clean_data(quote_path)

    # all_clean_paths = []
    # for i, path in enumerate(trade_clean_path):
    #     all_clean_paths.append({"trades": trade_clean_path[i], "quotes": quote_clean_path[i], "ref": ref_clean_path[i]})

    # print(f"Files to be processed: {all_clean_paths}")
    # # reconstruct full book events

    # ## TO-DO: Reconstruct book events before feature generation
    # reconstructed_path = event_reconstruction.reconstruct_book_events(input_files=all_clean_paths)

    # # generate features
    # generate_features.generate_features(input_file=reconstructed_path)


# python run_jobs.py --symbol AAPL --start_date 2021-01-01 --end_date 2021-01-03
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default="AAPL")
    parser.add_argument("--start_date", type=str, default="2020-01-01")
    parser.add_argument("--end_date", type=str, default="2020-01-03")
    parser.add_argument("--row_limit", type=int, default=1000000)

    args = parser.parse_args()

    if args.symbol == "S&P500":

        symbol_list = pd.read_csv("data/sp500.txt", sep=" ")

        for symbol in symbol_list:
            print(f"Processing {symbol}...")
            try:
                run_jobs(symbol=symbol, start_date=args.start_date, end_date=args.end_date, row_limit=args.row_limit)
            except Exception as e:
                print(f"Error processing {symbol}: {e}")

    else:
        run_jobs(symbol=args.symbol, start_date=args.start_date, end_date=args.end_date, row_limit=args.row_limit)
