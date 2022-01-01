from scrape_utils import Scraper
import argparse
import pandas as pd

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="For scraping from sportsbookreview.com")

    parser.add_argument("start_date", type=str, help="Start date to scrape")
    parser.add_argument("end_date", type=str, help="Final date to scrape")
    parser.add_argument("-league", "-L", type=str, default='nba-basketball', 
        help="all games in given league will be scraped")

    parser.add_argument("-bet_types", "-B", nargs="+", type=str, 
        default=['pointspread', 'money-line'], help="Types of bets (optional, default is 'pointspread')")

    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    league = args.league
    bet_types = args.bet_types

    print("Scraping bet_types={} for league={} from {} to {}".format(bet_types, league, start_date, end_date))
    
    if pd.to_datetime(start_date) > pd.to_datetime(end_date):
        raise Exception("(start_date={}) > (end_date={})".format(start_date, end_date))

    valid_bet_types = ["pointspread", "money-line", "totals", "merged"]

    if not all([b in valid_bet_types for b in bet_types]):
        raise Exception("invalid bet type in {}. must be in {}".format(bet_types, valid_bet_types))

    S = Scraper(start_date, end_date, league, bet_types, max_tries=1, driver="firefox")
    league_df = S.run_scraper()
    S.close_driver()
    print(league_df.isnull().mean())
    print(league_df.head())
    path = "scraped_data/{league}_{start}_{end}.csv".format(
        league=league, start=start_date, end=end_date
    )
    league_df.to_csv(path, index=False)
