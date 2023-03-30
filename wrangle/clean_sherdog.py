from db import base_db_interface
import html2text
from bs4 import BeautifulSoup
from multiprocessing import Pool
import pandas as pd
from tqdm import tqdm

def parse_article(url, raw_html):
    """
    Parses the article and returns a dictionary of the parsed data
    * url
    * title
    * date
    * body
    """
    soup = BeautifulSoup(raw_html, "html.parser")
    # get the title
    title = soup.find('div', class_="section_title")
    if title is None:
        title = soup.find('h1', itemprop="name")

    # get the article info
    article_info = soup.find('div', class_="article-info")

    # get the date
    date = article_info.find('span')

    # get the body
    h = html2text.HTML2Text()
    h.ignore_links = True
    h.ignore_images = True
    h.body_width = 0 # if left unspecified, text will wrap at 80 characters.
                     # I don't want it to wrap at all, the newline characters
                     # mess up the text
    body = soup.find('div', class_="content body_content")
    if body is None:
        # handle the case of videos, which have a different structure
        # eg https://www.sherdog.com/videos/highlightreels/UFC-Fight-Night-220-Highlight-Video-Mike-Malott-Taps-Yohan-Lainesse-19511
        body = soup.find('article').find('div', class_="content")
    body = h.handle(str(body))
    return {
        "url": url,
        "title": title.text,
        "date": date.text,
        "body": body
    }

def parse_all_articles(article_df, processes=4):
    """
    Parses all articles in the given DataFrame. Returns a DataFrame of the
    parsed data: url, title, date, body
    """
    articles = []
    # use concurrency to speed up parsing
    for url, raw_html in tqdm(zip(article_df.url, article_df.html), total=len(article_df)):
        articles.append(parse_article(url, raw_html))
    # with Pool(processes=processes) as p:
        # articles = p.starmap(parse_article, zip(article_df.url, article_df.html))
        # articles = list(tqdm(p.imap(_scrape_single_article, urls, chunksize=100), 
        #                      total=len(urls)))
    return pd.DataFrame(articles)

def write_all_articles_to_db(article_df, table_name="sherdog_parsed_articles"):
    """
    Writes the articles to the database, updating the table if it already 
    exists
    """
    cursor = base_db_interface._cursor
    # create table if it doesn't exist
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            url TEXT PRIMARY KEY,
            title TEXT,
            date TEXT,
            body TEXT
        )
        """
    )
    # insert the articles
    for url, title, date, body in tqdm(zip(article_df.url, 
                                           article_df.title, 
                                           article_df.date, 
                                           article_df.body), 
                                        total=len(article_df)):
        cursor.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} (url, title, date, body)
            VALUES (?, ?, ?, ?)
            """,
            (url, title, date, body)
        )
    base_db_interface._con.commit()
    return

def main():
    # get the articles from the database
    article_df = base_db_interface.read("sherdog_raw_html")
    # parse the articles
    article_df = parse_all_articles(article_df)
    # write the articles to the database
    write_all_articles_to_db(article_df)
    return