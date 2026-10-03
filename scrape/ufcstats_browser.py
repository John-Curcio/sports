"""Local Firefox acquisition and durable historical HTML checkpoints.

Neither Firefox nor a database is opened when importing this module.
"""
import logging
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlsplit, urlunsplit

from bs4 import BeautifulSoup

LOG = logging.getLogger(__name__)
MISSING_STATS = 'Round-by-round stats not currently available'


class CollectionError(RuntimeError):
    pass


def canonical_url(url):
    parts = urlsplit(url)
    if parts.hostname != 'ufcstats.com' or parts.scheme not in ('http', 'https'):
        raise CollectionError('Unexpected UFCstats URL: ' + str(url))
    # Existing consumers store HTTP URLs as identities, even after HTTPS redirects.
    return urlunsplit(('http', 'ufcstats.com', parts.path, parts.query, ''))


def page_ready(html, kind):
    """Require content for this page type, not a generic navigation success."""
    soup = BeautifulSoup(html, 'lxml')
    text = soup.get_text(' ', strip=True)
    if any(marker in text.lower() for marker in
           ('checking your browser', 'access denied', 'service unavailable')):
        return False
    if kind == 'directory':
        return bool(soup.select_one('table tbody') and
                    soup.select_one('a[href*="/fighter-details/"]'))
    if kind == 'fighter':
        bio = soup.select_one('ul.b-list__box-list')
        return bool(bio and 'Height:' in bio.text and 'DOB:' in bio.text and
                    soup.select_one('table tbody'))
    if kind in ('event', 'upcoming_event'):
        metadata = soup.select('.b-list__box-list-item')
        return bool(len(metadata) == 2 and 'Date:' in metadata[0].text and
                    'Location:' in metadata[1].text and soup.select_one('table tbody'))
    if kind == 'fight':
        description = soup.select_one('.b-fight-details__fight')
        return bool(description and 'Method:' in description.text and
                    len(soup.select('a.b-fight-details__person-link')) == 2 and
                    (MISSING_STATS in text or len(soup.select('table')) == 4))
    if kind == 'upcoming':
        table = soup.select_one('table')
        return bool(table and table.select_one('tbody') and
                    'date' in table.get_text(' ', strip=True).lower() and
                    'name' in table.get_text(' ', strip=True).lower())
    raise ValueError('Unknown UFCstats page kind: ' + kind)


class FirefoxClient:
    """One lazy browser per crawl. Cache commits precede the next navigation.

    Pass a new checkpoint path to start a new historical snapshot. ``refresh``
    bypasses prior pages, but reuses pages fetched during this client lifetime.
    Upcoming pages always bypass historical storage.
    """

    def __init__(self, checkpoint_path='.cache/ufcstats/checkpoint.sqlite',
                 page_timeout=45, wait_timeout=30, retries=2, pace=1,
                 firefox_binary=None, geckodriver=None, refresh=False,
                 driver_factory=None):
        if page_timeout <= 0 or wait_timeout <= 0 or retries < 0 or pace < 0:
            raise ValueError('Timeouts must be positive; retries and pace nonnegative')
        self.checkpoint_path = checkpoint_path
        self.page_timeout, self.wait_timeout = page_timeout, wait_timeout
        self.retries, self.pace, self.refresh = retries, pace, refresh
        self.firefox_binary, self.geckodriver = firefox_binary, geckodriver
        self.driver_factory = driver_factory
        self.driver = self.con = None
        self.fetched = set()
        self.last_navigation = None
        self.requests = self.cache_hits = self.browser_starts = 0

    def _checkpoint(self):
        if self.con is None:
            if self.checkpoint_path != ':memory:':
                Path(self.checkpoint_path).parent.mkdir(parents=True, exist_ok=True)
            self.con = sqlite3.connect(self.checkpoint_path)
            self.con.execute('CREATE TABLE IF NOT EXISTS ufcstats_pages '
                             '(url TEXT PRIMARY KEY, kind TEXT NOT NULL, '
                             'html TEXT NOT NULL, fetched_at REAL NOT NULL)')
            self.con.commit()
        return self.con

    def _browser(self):
        if self.driver is None:
            if self.driver_factory:
                self.driver = self.driver_factory()
            else:
                from selenium import webdriver
                from selenium.webdriver.firefox.service import Service
                options = webdriver.FirefoxOptions()
                options.add_argument('-headless')
                if self.firefox_binary:
                    options.binary_location = self.firefox_binary
                service = Service(executable_path=self.geckodriver) if self.geckodriver else Service()
                self.driver = webdriver.Firefox(options=options, service=service)
            self.browser_starts += 1
            self.driver.set_page_load_timeout(self.page_timeout)
        return self.driver

    def _stop_browser(self):
        driver, self.driver = self.driver, None
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                LOG.warning('Firefox shutdown failed', exc_info=True)

    def fetch(self, url, kind):
        url = canonical_url(url)
        cacheable = kind not in ('upcoming', 'upcoming_event')
        if cacheable:
            row = self._checkpoint().execute(
                'SELECT html FROM ufcstats_pages WHERE url=? AND kind=?', (url, kind)).fetchone()
            if row and (not self.refresh or url in self.fetched) and page_ready(row[0], kind):
                self.cache_hits += 1
                return row[0]
        # Imports stay lazy so cached/offline parsing does not need Selenium.
        from selenium.common.exceptions import WebDriverException
        from selenium.webdriver.support.ui import WebDriverWait
        error = None
        for attempt in range(self.retries + 1):
            try:
                driver = self._browser()
                if self.last_navigation is not None:
                    time.sleep(max(0, self.pace - (time.monotonic() - self.last_navigation)))
                self.last_navigation = time.monotonic()
                self.requests += 1
                driver.get(url)
                def ready_html(browser):
                    source = browser.page_source
                    return source if page_ready(source, kind) else False
                html = WebDriverWait(driver, self.wait_timeout).until(ready_html)
                if canonical_url(driver.current_url) != url:
                    raise CollectionError('Unexpected redirect from ' + url + ' to ' + driver.current_url)
                if cacheable:
                    with self._checkpoint():
                        self.con.execute('INSERT OR REPLACE INTO ufcstats_pages VALUES (?, ?, ?, ?)',
                                         (url, kind, html, time.time()))
                    self.fetched.add(url)
                return html
            except (WebDriverException, CollectionError) as exc:
                error = exc
                self._stop_browser()
                if attempt < self.retries:
                    LOG.warning('Retrying %s after attempt %s: %s', url, attempt + 1, exc)
                    time.sleep(self.pace * (attempt + 1))
        raise CollectionError(f'{url}: acquisition failed after {self.retries + 1} attempts: {error}') from error

    def close(self):
        self._stop_browser()
        if self.con is not None:
            self.con.close()
            self.con = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
