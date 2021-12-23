from scrape_utils import Scraper
import pandas as pd
# import sys

if __name__ == "__main__":
    ### idk one of these days i can give this thing some command line arguments
    # print(f"Arguments count: {len(sys.argv)}")
    # for i, arg in enumerate(sys.argv):
    #     print(f"Argument {i:>6}: {arg}")

    # league = "nfl-football"
    league = "nba-basketball"
    bet_types = ["pointspread", "money-line"]
    start_date = "2021-01-01"
    # end_date = "2010-01-07"
    end_date = "2021-12-20"

    print("Scraping {} for {} from {} to {}".format(bet_types, league, start_date, end_date))
    S = Scraper(start_date, end_date, league, bet_types, max_tries=8, driver="firefox")
    league_df = S.run_scraper()
    S.close_driver()
    print(league_df.isnull().mean())
    print(league_df.head())
    path = "scraped_data/{league}_{start}_{end}.csv".format(
        league=league, start=start_date, end=end_date
    )
    league_df.to_csv(path)

    