import pandas as pd
from wrangle.clean_bfo_data import clean_fighter_bfo #clean_all_bfo 
from wrangle.clean_espn_data import EspnDataCleaner
from wrangle.clean_ufc_stats_data import UfcDataCleaner
from wrangle import join_datasets
from wrangle.simple_features import Preprocessor
from db import base_db_interface


def clean_all():
    print("--- clean ufcstats data ---")
    ufc_dc = UfcDataCleaner()
    ufc_dc.parse_all()
    base_db_interface.write_replace(
        table_name="clean_ufc_data", 
        df=ufc_dc.ufc_df
    )

    print("--- clean espn data ---")
    espn_dc = EspnDataCleaner()
    espn_dc.parse_all()
    base_db_interface.write_replace(
        table_name="clean_espn_data",
        df=espn_dc.espn_df
    )

    print("--- clean bestfightodds data ---")
    bfo_fighter_odds_df = base_db_interface.read("bfo_fighter_odds")
    bfo_df = clean_fighter_bfo(bfo_fighter_odds_df)
    base_db_interface.write_replace(
        table_name="clean_bfo_data",
        df=bfo_df
    )

    print("--- find mapping ufcstats --> espn ---")
    join_datasets.find_ufc_espn_mapping()
    print("--- find mapping bfo --> ufc ---")
    join_datasets.find_bfo_ufc_mapping()

    print("--- join bfo, espn, and ufc cleaned datasets ---")
    bfo_df = base_db_interface.read("clean_bfo_data")
    espn_df = base_db_interface.read("clean_espn_data")
    ufc_df = base_db_interface.read("clean_ufc_data")
    join_datasets.join_bfo_espn_ufc(bfo_df, espn_df, ufc_df)
    join_datasets.final_clean_step()
    print("--- extract some simple features ---")
    # df = pd.read_csv("data/full_bfo_ufc_espn_data.csv")
    df = base_db_interface.read("clean_bfo_espn_ufc_data")
    df["Date"] = pd.to_datetime(df["Date"])

    pp = Preprocessor(df)
    pp.preprocess()
    base_db_interface.write_replace(
        table_name="bfo_espn_ufc_features",
        df=pp.pp_df
    )
    # pp.pp_df.to_csv("data/full_bfo_ufc_espn_data_clean.csv", index=False)

    print("--- done! ---")


if __name__ == "__main__":
    clean_all()