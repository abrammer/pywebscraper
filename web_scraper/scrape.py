"""
Simple utility to replicate a remote html file listing or ftp site
Server modified times are copied over and directoriy structure is retained

Examples:
    python scrape.py --config ncep_gens_config
    python scrape.py --url https://ftp.ncep.noaa.gov/data/nccf/com/ens_tracker/prod/ --output ./


TODO:
    Build in some memory so it can be run as a service and limit header polling

Author: A. Brammer  (CIRA, 2019)
"""

import argparse
import concurrent.futures
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import zlib
import time
import pathlib
import re
import sys
import subprocess
import socket
from datetime import datetime
from html.entities import name2codepoint
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

import requests
import yaml
from dateutil.parser import parse as parsedate


def log_namer(name):
    return name + ".gz"


def log_rotator(source, dest):
    with open(source, "rb") as sf:
        data = sf.read()
        compressed = zlib.compress(data, 9)
        with open(dest, "wb") as df:
            df.write(compressed)
    os.remove(source)


class websync(HTMLParser):
    def __init__(self,
                 url,
                 download_location='./',
                 regex_exclude=None,
                 regex_include=None,
                 recursive=True,
                 no_parents=False):
        self.return_links = []
        self.base_url = url
        self.download_location = download_location
        self.exclude_match = regex_exclude
        self.include_match = regex_include
        self.recursive = recursive
        self.no_parents = no_parents
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
                                         regex_include=self.include_match)
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
        self.recursive = recursive
        req = requests.get(self.base_url)
        logging.debug(f'{self.base_url} -- {req.status_code}')
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
            websync.sync_files(link, outpath)
            return outpath
        else:
            req = requests.head(link)
            url_date = parsedate(req.headers['Last-Modified']).timestamp()
            if url_date > outpath.stat().st_mtime:
                websync.sync_files(link, outpath)
                return outpath
        return None

    @staticmethod
    def sync_files(remote_url: str, local_path: pathlib.Path):
        ''' Sync remote url with local path, copy over modified time
        '''
        logging.info(f'Downloading: {remote_url}')
        req = requests.get(remote_url)
        if req.status_code == 200:
            local_path.parent.mkdir(parents=True, exist_ok=True)
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
    def scrape(url:str, **kwargs): 
        ''' Build list of potential files then farm out the download to multiple threads
        '''
        if not url.endswith('/'):
            url = f'{url}/'
        parser = websync(url, **kwargs)
        parser.ls(recursive=True)
        logging.info(f'Checking on {len(parser.return_links)} files')
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for link in parser.return_links:
                futures.append(executor.submit(parser.cp, link))

            for x in concurrent.futures.as_completed(futures):
                if x.result() is not None:
                    logging.info(x.result())

        logging.info(f'Finished syncing w/ {url}')
    
    @staticmethod
    def keep_scraping(url:str, **kwargs):
        ''' Build list of potential files then farm out the download to multiple threads
        '''
        if not url.endswith('/'):
            url = f'{url}/'
        most_recent_files = []
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
                            logging.info(x.result())

                most_recent_files = parser.return_links.copy()
            logging.info(f'Sleeping on sync w/ {url}')
            time.sleep(5*60)


def command_line_interface():
    parser = argparse.ArgumentParser(description='Scrape remote weblisting')
    parser.add_argument('--url', type=str, help='url to scrape')
    parser.add_argument('--output', default='./')
    parser.add_argument('--config', type=str)
    parser.add_argument('--service', type=bool, default=False)
    parser.add_argument('--logfile', type=str, default=None)
    args = parser.parse_args()

    FORMAT = '%(levelname)-8s | %(asctime)-15s | %(pathname)-15s +%(lineno)-4d |  %(message)s'    
    logFormatter = logging.Formatter(FORMAT)
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger()
    if args.logfile is not None:
        logHandler = TimedRotatingFileHandler(args.logfile,
                                       when="d",
                                       interval=5,
                                       backupCount=5)
        logHandler.rotator = log_rotator
        logHandler.namer = log_namer
        logHandler.setFormatter( logFormatter )
        logger.addHandler(logHandler)
    logger.setLevel(logging.INFO)
    
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
            return
        func = scraper.keep_scraping
    else:
        func = scraper.scrape

    if mp == -1: mp = None
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
            for site, data in config.items():
                logging.info(f'Starting to scrape {site}')
                executor.submit(func, **data)
    except KeyboardInterrupt:
        pass
    logging.info('Done with loop')


if __name__ == "__main__":
    main()
