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

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import FirefoxOptions

opts = FirefoxOptions()
opts.add_argument("--headless")
driver = webdriver.Firefox(options=opts)


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
        # driver.get(self.url)
        # return driver.page_source
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
    
    # @abstractmethod
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

