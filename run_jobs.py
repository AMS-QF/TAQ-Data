import argparse

import pandas as pd

from data_preprocessing import clean_data, load_data
from feature_generation import generate_features
from pipelines import event_reconstruction


def run_jobs(symbol: str, start_date: str, end_date: str):

    # connect to database
    conn = load_data.connect_to_db()

    # load data
    trade_path = load_data.get_trades(conn, symbol, start_date, end_date)
    quote_path = load_data.get_quotes(conn, symbol, start_date, end_date)

    # clean data

    trade_clean_path = clean_data.clean_data(trade_path)
    quote_clean_path = clean_data.clean_data(quote_path)

    if len(trade_clean_path) == 0 or len(quote_clean_path) == 0:
        print(f"Error: No data for {symbol} {start_date} {end_date}")
        return

    all_clean_paths = list(zip(sorted(trade_clean_path), sorted(quote_clean_path)))
    all_clean_paths = [{"trade": x[0], "quote": x[1]} for x in all_clean_paths]

    # reconstruct full book events

    ## TO-DO: Reconstruct book events before feature generation
    reconstructed_path = event_reconstruction.reconstruct_book_events(input_files=all_clean_paths)

    # generate features
    generate_features.generate_features(input_file=reconstructed_path)


# python run_jobs.py --symbol AAPL --start_date 2021-01-01 --end_date 2021-01-03
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default="AAPL")
    parser.add_argument("--start_date", type=str, default="2020-01-01")
    parser.add_argument("--end_date", type=str, default="2020-01-03")

    args = parser.parse_args()

    if args.symbol == "S&P500":

        symbol_list = pd.read_csv("data/sp500.txt", sep=" ")

        for symbol in symbol_list:
            print(f"Processing {symbol}...")
            try:
                run_jobs(symbol=symbol, start_date=args.start_date, end_date=args.end_date)
            except Exception as e:
                print(f"Error processing {symbol}: {e}")

    else:
        run_jobs(symbol=args.symbol, start_date=args.start_date, end_date=args.end_date)
