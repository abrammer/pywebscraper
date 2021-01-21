"""
Simple utility to replicate a remote html file listing or ftp site
Server modified times are copied over and directoriy structure is retained
Can run as a service in which it won't repoll header information if no filenames have changed

Examples:
    python scrape.py --config ncep_gens_config
    python scrape.py --url https://ftp.ncep.noaa.gov/data/nccf/com/ens_tracker/prod/ --output ./


TODO:
    Make installable and into a simple importable module

Author: A. Brammer  (CIRA, 2019)
"""

import argparse
import concurrent.futures
import functools
import gzip
import logging
import os
import pathlib
import re
import socket
import subprocess
import sys
import time
import zlib
from datetime import datetime
from html.entities import name2codepoint
from html.parser import HTMLParser
from logging.handlers import TimedRotatingFileHandler
from urllib.parse import urljoin, urlsplit

import requests
import yaml
from dateutil.parser import parse as parsedate
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

MAX_CONNECTION_ERRS = 2
MAX_RETRY_TIMES = 5
BACKOFF_FACTOR = 1
TIMEOUT_SECS = 2
SERVICE_REFRESH_MINS = 5

def log_namer(name):
    return name + ".gz"


def log_rotator(source, dest):
    with open(source, "rb") as sf:
        data = sf.read()
        compressed = zlib.compress(data, 9)
        with open(dest, "wb") as df:
            df.write(compressed)
    os.remove(source)


def _create_https_session():
    """
    Creates a session

    Returns
    -------
    session : requests.sessions.Session
        http(s) session with retry flags
    """
    retry = Retry(total=MAX_RETRY_TIMES, connect=MAX_CONNECTION_ERRS, backoff_factor=BACKOFF_FACTOR)
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.auth = ('wmo', 'essential')
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.request = functools.partial(session.request, timeout=TIMEOUT_SECS)
    return session


class websync(HTMLParser):
    def __init__(self,
                 url,
                 download_location='./',
                 regex_exclude=None,
                 regex_include=None,
                 recursive=True,
                 no_parents=False,
                 update_existing=True,
                 session=None):
        self.return_links = []
        self.base_url = url
        self.download_location = download_location
        self.exclude_match = regex_exclude
        self.include_match = regex_include
        self.recursive = recursive
        self.no_parents = no_parents
        self.session = session or _create_https_session()
        logging.info("websync init'd")
        super().__init__()

    @property
    def exclude_match(self):
        return self.__exclude_match

    @exclude_match.setter
    def exclude_match(self, value):
        if value is not None:
            self.__exclude_match = re.compile(value)
        else:
            self.__exclude_match = None

    @property
    def include_match(self):
        return self.__include_match

    @include_match.setter
    def include_match(self, value):
        if value is not None:
            self.__include_match = re.compile(value)
        else:
            self.__include_match = None

    def handle_starttag(self, tag, attrs):
        ''' Called by parent HTMLParser on finding a tag
            This only processes a tags.
        '''
        if tag == 'a':
            self.find_download_links(attrs)

    def find_download_links(self, attrs):
        ''' Process a tags to find approprate links to follow
            if self.recursive then this will follow links down

        Appends downloadble links to self.return_links
        '''
        for attr in attrs:
            if ((self.exclude_match is not None)
                    and self.exclude_match.match(attr[1])):
                logging.debug(f'{attr[1]} matched -- excluded')
                continue
            if attr[0] == 'href' and not (attr[1].startswith('/')
                                          or attr[1].startswith('?')
                                          or attr[1].startswith('http')):
                if attr[1].endswith('/') or attr[1].endswith('html'):
                    logging.debug(f'checking on {attr[1]}')
                    if self.recursive:
                        sub_url = urljoin(self.base_url, attr[1].strip())
                        parser = websync(sub_url,
                                         regex_exclude=self.exclude_match,
                                         regex_include=self.include_match,
                                         session=self.session)
                        parser.ls()
                        self.return_links += parser.return_links
                else:
                    if ((self.include_match is None)
                            or self.include_match.match(attr[1])):
                        self.return_links.append(
                            urljoin(self.base_url, attr[1].strip()))

    def ls(self, recursive=True):
        ''' Start the recursive or shallow search for files
        '''
        logging.info(f"ls called {self.base_url}")
        self.recursive = recursive
        try:
            req = self.session.get(self.base_url)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            return
        logging.info(f'{self.base_url} -- {req.status_code}')
        if req.status_code == 200:
            self.feed(req.text)

    def cp(self, link: str):
        ''' Copy file if it doesn't exist or if the remote modified time is newer than
        the current local version.
        '''
        logging.debug(f'Start copy on {link}')
        logging.debug(f'no parents: {self.no_parents}')
        if self.no_parents:
            local_link = link.replace(self.base_url, '')
            split_result = urlsplit(local_link)
        else:
            split_result = urlsplit(link)

        logging.debug(split_result)
        topdir = split_result.netloc.replace('.', '_')
        outpath = pathlib.Path(self.download_location, topdir,
                               *split_result.path.split('/'))
        logging.debug(f'Checking {link} against {outpath}')
        if not outpath.is_file():
            try:
                websync.sync_files(link, outpath)
            except RuntimeError:
                return None
            logging.debug(f"downloaded new file to {outpath}")
            return outpath
        elif self.update_existing:
            req = self.session.head(link)
            url_date = parsedate(req.headers['Last-Modified']).timestamp()
            if url_date > outpath.stat().st_mtime:
                websync.sync_files(link, outpath)
                logging.debug(f"downloaded newer file to {outpath}")
                return outpath
        return None

    @staticmethod
    def sync_files(remote_url: str, local_path: pathlib.Path):
        ''' Sync remote url with local path, copy over modified time
        '''
        logging.info(f'Downloading: {remote_url}')
        try:
            session = _create_https_session()
            req = session.get(remote_url)
        except requests.exceptions.ContentDecodingError:
            logging.error(f"failed to decode {remote_url}")
            raise RuntimeError
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            return RuntimeError
        if req.status_code == 200:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            if local_path.suffix.endswith('gz'):
                with gzip.open(local_path, 'wb') as f:
                    f.write(req.content)
            else:
                with open(local_path, 'wb') as f:
                    f.write(req.content)
            modified_time = parsedate(req.headers['Last-Modified']).timestamp()
            access_time = datetime.utcnow().timestamp()
            os.utime(local_path, (access_time, modified_time))
        else:
            raise RuntimeError(f'{req.status_code} returned')


class scraper:
    def __init__(self,):
        pass

    @staticmethod
    def scrape(url:str, callback_func=None, **kwargs):
        ''' Build list of potential files then farm out the download to multiple threads
        '''
        if not url.endswith('/'):
            url = f'{url}/'
        logging.info(url)
        logging.info(kwargs)
        parser = websync(url, **kwargs)
        logging.info(parser)
        parser.ls(recursive=True)
        logging.info(f'Checking on {len(parser.return_links)} files')
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for link in parser.return_links:
                futures.append(executor.submit(parser.cp, link))

            for x in concurrent.futures.as_completed(futures):
                if x.result() is not None:
                    logging.info(x.result())
                    if callback_func is not None:
                        try:
                            callback_func(x.result())
                        except Exception:
                            logging.exception(f"{x.result()} not parsed correctly")

        logging.info(f'Finished syncing w/ {url}')

    @staticmethod
    def keep_scraping(url:str, callback_func=None, **kwargs):
        ''' Build list of potential files then farm out the download to multiple threads
        '''
        if not url.endswith('/'):
            url = f'{url}/'
        most_recent_files = []
        logging.info(callback_func)
        while True:
            parser = websync(url, **kwargs)
            parser.ls(recursive=True)
            if parser.return_links != most_recent_files:
                logging.info(f'Checking on {len(parser.return_links)} files')
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    futures = []
                    for link in parser.return_links:
                        futures.append(executor.submit(parser.cp, link))

                    for x in concurrent.futures.as_completed(futures):
                        if x.result() is not None:
                            logging.info(callback_func)
                            if callback_func is not None:
                                logging.info(f'calling {callback_func}')
                                try:
                                    callback_func(x.result())
                                except Exception:
                                    logging.exception(f"{url} not parsed correctly")
                            logging.info(x.result())

                most_recent_files = parser.return_links.copy()
            logging.info(f'Sleeping on sync w/ {url}')
            time.sleep(SERVICE_REFRESH_MINS*60)


def command_line_interface():
    parser = argparse.ArgumentParser(description='Scrape remote weblisting')
    parser.add_argument('--url', type=str, help='url to scrape')
    parser.add_argument('--output', default='./')
    parser.add_argument('--config', type=str)
    parser.add_argument('--service', type=bool, default=False)
    parser.add_argument('--logfile', type=str, default=None)
    parser.add_argument('--debug', type=bool, default=False)
    args = parser.parse_args()

    FORMAT = '%(levelname)-8s | %(asctime)-15s | %(pathname)-15s +%(lineno)-4d |  %(message)s'
    logFormatter = logging.Formatter(FORMAT)
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if args.logfile is not None:
        logHandler = TimedRotatingFileHandler(args.logfile,
                                       when="d",
                                       interval=5,
                                       backupCount=5)
        logHandler.rotator = log_rotator
        logHandler.namer = log_namer
        logHandler.setFormatter( logFormatter )
        logHandler.setLevel(logging.INFO)
        logger.addHandler(logHandler)

    if args.debug or args.debug == "True":
        logger.setLevel(logging.DEBUG)

    if (args.url is None) and (args.config is None):
        raise RuntimeError('Requires either --url or --config ')

    if args.config is not None:
        with open(args.config) as f:
            config = yaml.safe_load(f)
    else:
        config = {'scraped_site': {'url': args.url,
                                   'download_location': args.output},
                                   }
    config['service'] = args.service
    logging.info(f'Running with config {config}')
    return config


def main(mp=-1):
    config = command_line_interface()
    run_as_service = config.pop('service', False)
    if run_as_service:
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.bind(f"\0{'_'.join(config.keys())}")
        except OSError:
            logging.debug('Already Running')
            return
        func = scraper.keep_scraping
    else:
        func = scraper.scrape

    if mp == -1: mp = None
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
            jobs = []
            for site, data in config.items():
                logging.info(f'Starting to scrape {site}')
                jobs.append(executor.submit(func, **data))

            for future in concurrent.futures.as_completed(jobs):
                logging.info(future.exception())
                logging.info("completed future")
    except KeyboardInterrupt:
        pass
    logging.info('Done with loop')


if __name__ == "__main__":
    main()
