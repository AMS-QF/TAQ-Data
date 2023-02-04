import argparse

from data_preprocessing import clean_data, load_data
from feature_generation import generate_features


def run_jobs(exchange: str, symbol: str, start_date: str, end_date: str):

    # connect to database
    conn = load_data.connect_to_db()

    # load data
    result, trade_path = load_data.get_trades(conn, exchange, symbol, start_date, end_date)
    result, quote_path = load_data.get_quotes(conn, exchange, symbol, start_date, end_date)

    # clean data

    trade_clean_path = clean_data.clean_data(trade_path)
    quote_clean_path = clean_data.clean_data(quote_path)

    all_clean_paths = [trade_clean_path, quote_clean_path]
    all_clean_paths = set([item for sublist in all_clean_paths for item in sublist])

    print(all_clean_paths)

    # generate features
    generate_features.generate_features(input_file=all_clean_paths)


# python run_jobs.py --exchange N --symbol AAPL --start_date 2021-01-01 --end_date 2021-01-03
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--exchange", type=str, default="N")
    parser.add_argument("--symbol", type=str, default="AAPL")
    parser.add_argument("--start_date", type=str, default="2021-01-01")
    parser.add_argument("--end_date", type=str, default="2021-01-31")

    args = parser.parse_args()

    run_jobs(exchange=args.exchange, symbol=args.symbol, start_date=args.start_date, end_date=args.end_date)
