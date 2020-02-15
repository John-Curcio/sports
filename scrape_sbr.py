# Stupid script for grabbing data
import time
import os
from datetime import date

from scrape_sbr_utils import DateData

if __name__ == "__main__":
    # TODO: ought to set up some way for this to pick up where we left off
    for y in range(2009, 2005, -1):
        start_date = date(month=1, day=1, year=y)
        end_date = date(month=1, day=1, year=(y+1))
        DD = DateData()
        DD.grab_data_for_date_range(start_date=start_date, end_date=end_date)
        DD.to_csv(path=os.path.join("data", "team_formatting_preserved", ("sbr_data_%s.csv"%y)))
        print("dun with year", y)
5