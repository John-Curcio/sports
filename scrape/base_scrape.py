from abc import ABC, abstractmethod
from queue import Queue
import re
from bs4 import BeautifulSoup

from tqdm import tqdm
import pandas as pd
import numpy as np
import os

import requests
from io import StringIO 
from lxml import html

import boto3
import time

import sqlite3

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}


class EmptyResponse(Exception):
    pass


class BasePageScraper(ABC):
    """
    Base class for scraping a single page
    """
    
    def __init__(self, url, max_tries=10, sleep_time=5):
        self.url = url
        self.max_tries = max_tries
        self.sleep_time = sleep_time
        self.raw_html = None
        self.data = None
        
    def get_request(self):
        for i in range(self.max_tries):
            r = requests.get(self.url, headers=headers)
            r.close()
            if not r.text.startswith("Error "):
                # great, we got a legit response
                break
            # sleep for sleep_time
            print("{} gave us an Error 0, retrying in {} seconds".format(self.url, self.sleep_time))
            time.sleep(self.sleep_time)
        if r.text.startswith("Error "):
            raise EmptyResponse(self.url)
        return r
    
    def get_html(self):
        r = self.get_request()
        self.raw_html = r.text
        return self.raw_html
    
    def get_soup(self):
        raw_html = self.raw_html
        if raw_html is None:
            raw_html = self.get_html()
        return BeautifulSoup(raw_html, features="lxml")
    
    @abstractmethod
    def get_page_urls(self):
        # get set of urls mapping to other pages to scrape
        raise NotImplemented
        
    # @abstractmethod
    def get_page_data(self):
        # get all the data we could want on this page
        raise NotImplemented


class BaseBfs(ABC):
    
    def __init__(self, root_urls, max_depth=3, verbose=True):
        self.root_urls = root_urls
        self.max_depth = max_depth
        self.verbose = verbose
        self.urls_seen = set()
        self.failed_urls = set()
        
    @abstractmethod
    def get_neighbor_urls(self, url:str) -> set:
        # just initialize an instance of BasePageScraper and call the get_page_urls method
        # there's gotta be a cleaner OOP-y way to do this but idgaf rn
        raise NotImplemented
        
    def crawl_urls(self):
        # get the urls of all pages. scrape their contents later
        frontier = Queue()
        for root_url in self.root_urls:
            frontier.put(root_url)
        curr_depth = 0
        curr_width = frontier.qsize()
        while ((curr_depth < self.max_depth) and frontier.qsize() > 0):
            url = frontier.get()
            if url in self.urls_seen:
                # might have duplicates in the queue - two pages are both neighbors of the same page
                continue
            self.urls_seen.add(url)
            if self.verbose:
                print("depth={}, width={}, |frontier|={}, crawling page {}"\
                      .format(curr_depth, curr_width, frontier.qsize(), url))
            try:
                neighbor_urls = self.get_neighbor_urls(url)
                neighbor_urls -= self.urls_seen
                for neighbor_url in neighbor_urls:
                    frontier.put(neighbor_url)
            except EmptyResponse:
                if self.verbose:
                    print("oy, emptyresponse from {}".format(url))
                self.failed_urls.add(url)
            except ValueError:
                if self.verbose:
                    print("oy, valueerror from {}".format(url))
                self.failed_urls.add(url)
            curr_width -= 1
            if curr_width <= 0:
                curr_depth += 1
                curr_width = frontier.qsize()
        return self.urls_seen

    
class DbInterface(object):
    """
    Provide an interface to read from and write to a sqlite database
    """

    def __init__(self, db_name="deleteme.db"):
        self.db_name = db_name 
        self._con = None
        self.connect()

    def connect(self):
        self._con = sqlite3.connect(self.db_name)

    def close(self):
        self._con.close()
        
    def read(self, table_name):
        """
        read all rows from table_name
        """
        return pd.read_sql(f"select * from {table_name}", con=self._con)
    
    def write_replace(self, table_name, df:pd.DataFrame):
        """
        write df to table_name, replacing any existing table
        """
        return df.to_sql(
            table_name, 
            con=self._con, 
            if_exists="replace", 
            index=False
        )
    
    def write_update(self, table_name, df:pd.DataFrame):
        """
        Write df to table_name, updating according to index.
        * If a row in df has the same index as a row in table_name, then 
        table_name's row will be updated with the values in df's row
        * Else, df's row will be inserted into table_name
        """
        # I should probably set the index of df appropriately before passing in df
        try:
            # if the table doesn't already exist, create it from scratch
            return df.to_sql(
                table_name, 
                con=self._con, 
                if_exists="fail", 
                index=False
            )
        except ValueError:
            # if the table already exists, append new rows
            # I do this in a hacky way: I read all the data into memory, 
            # then concat old data with new data. then i check the result 
            # for duplicates and drop them, favoring new data. Then I 
            # overwrite all the data in the original table. 
            df_old = self.read(table_name)
            df_new = pd.concat([df_old, df])
            # in the case of duplicate rows, use the result from df, which is more recent
            df_new = df_new.loc[~df_new.index.duplicated(keep="last")]
            return df_new.to_sql(
                table_name, 
                con=self._con, 
                if_exists="replace",
                index=False
            )

base_db_interface = DbInterface(db_name="mma.db")