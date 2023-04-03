"""
Scrapes sherdog.com for article content
"""

from scrape.base_scrape import BasePageScraper
from db import base_db_interface
import pandas as pd
import numpy as np
import string
from tqdm import tqdm
import re
from bs4 import BeautifulSoup
from db import base_db_interface
from multiprocessing import Pool


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

class SherdogUrlScraper(BasePageScraper):

    def get_page_urls(self):
        html = self.get_html()
        # get URLs to articles from html. These are of the form:
        # <a href="/news/news/Bodycam-Footage-of-Jon-Jones-Arrest-Released-Jones-Claims-He-Got-Stir-Crazy-172021" class="title">Bodycam Footage of Jon Jones’ Arrest Released, Jones Claims He ‘Got Stir Crazy’</a>
        soup = BeautifulSoup(html, 'html.parser')
        urls = soup.find_all('a', class_="title")
        urls = [url.get('href') for url in urls]
        urls = ["https://www.sherdog.com" + url for url in urls]
        return urls


class SherdogSearchScraper(object):
    """
    Scrapes sherdog.com for URLs to articles
    """

    def __init__(self, base_url="https://www.sherdog.com/tag/ufc/list/", 
                 start_page=1, end_page=1000, max_tries=10, sleep_time=5):
        self.start_page = start_page
        self.end_page = end_page
        self.max_tries = max_tries
        self.sleep_time = sleep_time
        self.base_url = base_url
        self.urls = []
        self.url_df = None

    def get_urls(self):
        """
        Gets all URLs to articles linked on pages on sherdog.com. 
        We iterate through sherdog.com pages, up to max_pages.
        Returns a pandas DataFrame of the URLs
        """
        # url_set = set()
        self.urls = []
        for i in tqdm(range(self.start_page, self.end_page)):
            url = self.base_url + str(i)
            scraper = SherdogUrlScraper(url, self.max_tries, self.sleep_time)
            curr_urls = scraper.get_page_urls()
            self.urls.extend(curr_urls)
            # url_set = url_set.union(set(urls))
        # self.urls = list(url_set)
        self.url_df = pd.DataFrame(self.urls, columns=["url"])
        return self.url_df
    
    def write_urls_to_db(self, table_name="sherdog_urls"):
        """
        Writes the URLs to the database, updating the table if it already 
        exists
        """
        if self.url_df is None:
            self.get_urls()
        cursor = base_db_interface._cursor
        # create table if it doesn't exist
        # 
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS {} (url text PRIMARY KEY UNIQUE)"\
            .format(table_name)
        )
        # write to table
        urls = set(self.url_df.url.values)
        cursor.executemany(
            'INSERT OR IGNORE INTO {} (url) VALUES (?)'.format(table_name), 
            [(url,) for url in urls]
        )
        base_db_interface._con.commit()
        # leave the connection open; this base_db_interface will be used by 
        # other classes
        return
    
def _scrape_single_article(url):
    scraper = BasePageScraper(url)
    scraper.get_html()
    return scraper

def scrape_all_articles(urls, processes=4):
    """
    Scrapes all articles in the given list of URLs
    """
    articles = []
    # use concurrency to speed up scraping
    with Pool(processes=processes) as p:
        articles = p.map(_scrape_single_article, urls)
        # articles = list(tqdm(p.imap(_scrape_single_article, urls, chunksize=100), 
        #                      total=len(urls)))
    # for url in tqdm(urls):
    #     scraper = BasePageScraper(url)
    #     articles.append(scraper.get_html())
    return articles

def write_articles_to_db(articles, table_name="sherdog_raw_html"):
    """
    Writes the articles to the database, updating the table if it already 
    exists
    """
    cursor = base_db_interface._cursor
    # create table if it doesn't exist
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS {} (url text PRIMARY KEY UNIQUE, \
        html text)"\
        .format(table_name)
    )
    
    # write to table
    cursor.executemany(
        'INSERT OR IGNORE INTO {} (url, html) \
        VALUES (?, ?)'.format(table_name), 
        [(article.url, article.raw_html) for article in articles]
    )
    base_db_interface._con.commit()
    # leave the connection open; this base_db_interface will be used by 
    # other classes
    return

def main():
    base_url = "https://www.sherdog.com/tag/ufc/list/"
    sher = SherdogSearchScraper(base_url=base_url, start_page=1, end_page=350)
    url_df = sher.get_urls()
    print(url_df.shape)
    sher.write_urls_to_db()

    # get all URLs from the database
    urls = base_db_interface.read("sherdog_urls")
    urls = urls.url.values
    print(urls.shape)
    articles = scrape_all_articles(urls, processes=8)
    write_articles_to_db(articles)
