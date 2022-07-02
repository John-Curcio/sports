import pandas as pd
from wrangle.clean_bfo_data import clean_bfo 
from wrangle.clean_espn_data import EspnDataCleaner
from wrangle.clean_ufc_stats_data import UfcDataCleaner
from wrangle import join_datasets
from wrangle.simple_features import Preprocessor


# if __name__ == "__main__":
print("--- clean ufcstats data ---")
folder = "scrape/scraped_data/mma/ufcstats/"
totals_path = folder+"totals.csv"
strikes_path = folder+"strikes.csv"
# round_totals_path = folder+"round_totals.csv"
# round_strikes_path = folder+"round_strikes.csv"
events_path = folder+"event_data.csv"
desc_path = folder+"fight_descriptions.csv"
ufc_dc = UfcDataCleaner(totals_path, strikes_path, events_path, desc_path)
ufc_dc.parse_all()
ufc_dc.ufc_df.to_csv("data/ufc_stats_df.csv")

print("--- clean espn data ---")
folder = "scrape/scraped_data/mma/espn/"
bio_path = folder+"espn_bios_2022-05-20.csv"
stats_path = folder+"espn_stats_2022-05-20.csv"
match_path = folder+"espn_matches_2022-05-20.csv"

DC = EspnDataCleaner(stats_path, bio_path, match_path)
DC.parse_all()

DC.espn_df.to_csv("data/espn_data.csv", index=False)

print("--- clean bestfightodds data ---")
bfo_df = pd.read_csv("data/all_fighter_odds_2022-05-10.csv")
bfo_df = clean_bfo(bfo_df)
bfo_df.to_csv("data/bfo_fighter_odds.csv", index=False)

print("--- join em all ---")
join_datasets.main()

print("--- extract some simple features ---")
df = pd.read_csv("data/full_bfo_ufc_espn_data.csv")
df["Date"] = pd.to_datetime(df["Date"])
pp = Preprocessor(df)
pp.preprocess()
pp.pp_df.to_csv("data/full_bfo_ufc_espn_data_clean.csv", index=False)

print("--- done! ---")


