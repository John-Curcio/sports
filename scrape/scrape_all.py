import numpy as np
from scrape_bfo import BfoOddsScraper
from scrape_espn import FighterSearchScraper
from scrape_ufcstats import UfcUrlScraper, FullUfcScraper

if __name__ == "__main__":
    print("scraping bestfightodds data")
    bfo = BfoOddsScraper(max_iters=np.inf)
    url_df = bfo.scrape_all_fighter_urls()
    bfo.scrape_all_opening_odds(url_df)
    bfo.scrape_all_closing_odds()
    print("done scraping bestfightodds")

    print("scraping espn data")
    start_letter, end_letter = "a", "z"
    foo = FighterSearchScraper(start_letter=start_letter, end_letter=end_letter)
    foo.run_scraper(bio=True, matches=True, stats=True)
    print(foo.stats_df.head())
    foo.stats_df.to_csv("scraped_data/mma/espn/{}_{}_stats_df.csv".format(start_letter, end_letter), index=False)
    foo.bio_df.to_csv("scraped_data/mma/espn/{}_{}_bio_df.csv".format(start_letter, end_letter), index=False)
    foo.matches_df.to_csv("scraped_data/mma/espn/{}_{}_matches_df.csv".format(start_letter, end_letter), index=False)
    print("done with fighters in letter range {}-{}".format(start_letter, end_letter))

    url_scraper = UfcUrlScraper()
    url_scraper.get_all_event_and_fight_urls()

    full_scraper = FullUfcScraper(
        fighter_urls=url_scraper.fighter_urls,
        event_urls=url_scraper.event_urls,
        fight_urls=url_scraper.fight_urls,
    ) 
    full_scraper.scrape_all()
    folder = "scraped_data/mma/ufcstats/"
    filename_dict = {
        "totals": full_scraper.totals_df,
        "strikes": full_scraper.strikes_df,
        "round_totals": full_scraper.round_totals_df,
        "round_strikes": full_scraper.round_strikes_df,
        "fight_descriptions": full_scraper.fight_description_df,
        "event_data": full_scraper.event_data,
        "fighter_data": full_scraper.fighter_data,
    }
    for filename, df in filename_dict.items():
        path = folder + filename + ".csv"
        df.to_csv(path, index=False)
