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

def parse_all_articles(article_df):
    """
    Parses all articles in the given DataFrame. Returns a DataFrame of the
    parsed data: url, title, date, body
    """
    articles = []
    # use concurrency to speed up parsing
    for url, raw_html in tqdm(zip(article_df.url, article_df.html), 
                              total=len(article_df)):
        articles.append(parse_article(url, raw_html))
    article_df = pd.DataFrame(articles)
    article_df["is_play_by_play"] = article_df["title"].str.lower()\
        .str.contains("play-by-play")
    return article_df

def write_parsed_articles_to_db(article_df, table_name="sherdog_parsed_articles"):
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
            is_play_by_play INTEGER,
            date TEXT,
            body TEXT
        )
        """
    )
    # insert the articles
    for url, title, is_play_by_play, date, body in tqdm(zip(article_df.url, 
                                                            article_df.title, 
                                                            article_df.is_play_by_play,
                                                            article_df.date, 
                                                            article_df.body), 
                                                        total=len(article_df)):
        cursor.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} (url, title, is_play_by_play, date, body)
            VALUES (?, ?, ?, ?, ?)
            """,
            (url, title, is_play_by_play, date, body)
        )
    base_db_interface._con.commit()
    return

def parse_play_by_play_articles(article_df):
    """
    Parses all play-by-play articles in the given DataFrame. Returns a DataFrame of the
    play-by-play articles, one row per each fight in the article. If the article
    is not a play-by-play article, it is ignored.
    Returns a DataFrame with the following columns:
    * url
    * fight_title (eg "## Joaquin Buckley (186) vs. Nassourdine Imavov (186)").
        None if the article is not a play-by-play article.
    * event_title (eg "UFC Fight Night: Lewis vs. Oleinik")
    * date (eg "February 15, 2020")
    * body (the text of the fight)
    """
    # split the play-by-play articles
    pbp_articles = []
    for _, row in tqdm(article_df.query("is_play_by_play").iterrows(), 
                       total=article_df["is_play_by_play"].sum()):
        # split the article into fights
        fights = row.body.split("\n## ")[1:] # remove text before the first fight
        pbp_articles.append(pd.DataFrame({
            "url": row.url,
            "event_title": row["title"],
            "date": row["date"],
            # "fight_title": row["title"],
            "body": fights
        }))
    pbp_articles = pd.concat(pbp_articles, ignore_index=True)
    pbp_articles["fight_title"] = pbp_articles["body"].str.split("\n").str[0]
    # return pbp_articles
    return pbp_articles

def write_play_by_play_articles_to_db(pbp_articles, table_name="sherdog_parsed_play_by_play_articles"):
    """
    Writes the play-by-play articles to the database, updating the table if it already 
    exists
    """
    cursor = base_db_interface._cursor
    # create table if it doesn't exist
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            url TEXT,
            fight_title TEXT,
            event_title TEXT,
            date TEXT,
            body TEXT
        )
        """
    )
    # insert the articles
    for _, row in tqdm(pbp_articles.iterrows(), total=len(pbp_articles)):
    # for url, fight_title, body in tqdm(zip(pbp_articles.url, 
    #                                         pbp_articles.fight_title, 
    #                                         pbp_articles.body), 
    #                                     total=len(pbp_articles)):

        cursor.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} 
            (url, fight_title, event_title, date, body)
            VALUES (?, ?, ?)
            """,
            (row["url"], row["fight_title"], row["event_title"], row["date"], row["body"])
        )
    base_db_interface._con.commit()
    return

def main():
    # get the articles from the database
    article_df = base_db_interface.read("sherdog_raw_html")
    # parse the articles
    article_df = parse_all_articles(article_df)
    # write the articles to the database
    write_parsed_articles_to_db(article_df)

    # parse the play-by-play articles
    pbp_articles = parse_play_by_play_articles(article_df)
    # write the play-by-play articles to the database
    write_play_by_play_articles_to_db(pbp_articles)
    return