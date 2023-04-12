"""
This module contains classes and functions for scraping ESPN fighter 
stats from the wayback machine. 


When fighters leave the UFC for another promotion, their stats are
removed from the ESPN website. This module scrapes ESPN's fighter stats
from the wayback machine, which archives the ESPN website.
"""

import pandas as pd
import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}


def request_archived_stats_page(espn_url, last_ufc_fight_date):
    """
    Get fighter stats page from ESPN's wayback machine.
    Link to wayback machine API docs: https://archive.org/help/wayback_api.php
    :param espn_url: url of ESPN fighter stats page
    :param last_ufc_fight_date: datetime of last UFC fight
    """
    # first, find the archived page that is closest to the last UFC fight
    # date
    last_ufc_fight_date = last_ufc_fight_date.strftime("%Y%m%d")
    # add one day to the last UFC fight date, because the wayback machine
    # will return the closest archived page, and that might 
    query_timestamp = last_ufc_fight_date + pd.Timedelta(days=1) 
    wayback_url = "http://archive.org/wayback/available?url={}&timestamp={}"\
        .format(espn_url, last_ufc_fight_date + pd.Timedelta(days=1))
    r = requests.get(wayback_url, headers=headers)
    if r.status_code != 200:
        return None
    json = r.json()
    try:
        archived_url = json['archived_snapshots']['closest']['url']
    except KeyError:
        return None
    # now, get the archived page
    r = requests.get(archived_url, headers=headers)
    if r.status_code != 200:
        return None
    return r