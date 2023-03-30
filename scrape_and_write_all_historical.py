from scrape import scrape_ufcstats
from scrape import scrape_espn
from scrape import scrape_bfo
from db import base_db_interface
import time

if __name__ == "__main__":
    start = time.time()
    print("--- scrape bestfightodds data ---")
    scrape_bfo.main()
    print("--- scrape espn data ---")
    scrape_espn.main()
    print("--- scrape ufcstats data ---")
    scrape_ufcstats.main()
    print("--- done! ---")
    espn_df = base_db_interface.read("espn_matches")
    print("ESPN DF SHAPE: ", espn_df.shape)
    espn_bio_df = base_db_interface.read("espn_bio")
    print("ESPN BIO DF SHAPE: ", espn_bio_df.shape)

    bfo_df = base_db_interface.read("bfo_fighter_odds")
    print("BFO DF SHAPE: ", bfo_df.shape)
    ufc_df = base_db_interface.read("ufc_events")
    print("UFC EVENTS DF SHAPE: ", ufc_df.shape)
    ufc_df = base_db_interface.read("ufc_totals")
    print("UFC TOTALS DF SHAPE: ", ufc_df.shape)
    base_db_interface.close()
    end = time.time()
    print("total time, in seconds: ", end-start)
    print("total time, in minutes: ", (end-start)/60)
    print("total time, in hours: ", (end-start)/3600)