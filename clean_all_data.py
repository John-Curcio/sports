import pandas as pd
from wrangle.clean_bfo_data import clean_fighter_bfo #clean_all_bfo 
from wrangle.clean_espn_data import EspnDataCleaner
from wrangle.clean_ufc_stats_data import UfcDataCleaner
from wrangle import join_datasets
from wrangle.simple_features import Preprocessor
from db import base_db_interface


def clean_all():
    print("--- find mapping ufcstats --> espn ---")
    join_datasets.find_ufc_espn_mapping()
    print("--- find mapping bfo --> ufc ---")
    join_datasets.find_bfo_ufc_mapping()
    print("--- done! ---")


    print("--- clean ufcstats data ---")
    ufc_dc = UfcDataCleaner()
    ufc_dc.parse_all()
    base_db_interface.write_replace(
        table_name="ufc_stats_df", 
        df=ufc_dc.ufc_df
    )

    print("--- clean espn data ---")
    espn_dc = EspnDataCleaner()
    espn_dc.parse_all()
    base_db_interface.write_replace(
        table_name="espn_data",
        df=espn_dc.espn_df
    )

    # print("--- clean bestfightodds data ---")
    # bfo_fighter_odds_df = base_db_interface.read("bfo_fighter_odds")
    # bfo_df = clean_fighter_bfo(bfo_fighter_odds_df)
    # base_db_interface.write_replace(
    #     table_name="bfo_open_odds",
    #     df=bfo_df
    # )

    # print("--- extract some simple features ---")
    # df = pd.read_csv("data/full_bfo_ufc_espn_data.csv")
    # df["Date"] = pd.to_datetime(df["Date"])
    # pp = Preprocessor(df)
    # pp.preprocess()
    # pp.pp_df.to_csv("data/full_bfo_ufc_espn_data_clean.csv", index=False)

    # print("--- done! ---")


if __name__ == "__main__":
    clean_all()