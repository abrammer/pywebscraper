import argparse
import concurrent.futures
import itertools
import logging
import os
import pathlib
from collections.abc import Iterable
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlsplit

import yaml
import numpy as np
import requests
from dateutil.parser import parse as parsedate


def generate_links(link_format, **kwargs):
    keys = kwargs.keys()
    vals = [[val] if not isinstance(val, Iterable) else val for val in kwargs.values() ]
    for instance in itertools.product(*vals):
        yield link_format.format(**dict(zip(keys, instance)))


def sync_files(remote_url: str, local_path: pathlib.Path):
    ''' Sync remote url with local path, copy over modified time
    '''
    req = requests.get(remote_url)
    if req.status_code == 200:
        logging.info(f'Downloading: {remote_url} to {local_path}')
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, 'wb') as f:
            f.write(req.content)
        modified_time = parsedate(req.headers['Last-Modified']).timestamp()
        access_time = datetime.utcnow().timestamp()
        os.utime(local_path, (access_time, modified_time))
    elif req.status_code == 404:
        raise FileNotFoundError
    else:
        raise RuntimeError(f'{req.status_code} returned')


def cp(link: str, output_path: pathlib.Path):
    ''' Copy file if it doesn't exist or if the remote modified time is newer than
    the current local version. 
    '''
    split_result = urlsplit(link)
    if output_path.is_file():
        try:
            req = requests.head(link)
        except FileNotFoundError:
            return False
        url_date = parsedate(req.headers['Last-Modified']).timestamp()
        if url_date <= output_path.stat().st_mtime:
            logging.info(f'{link} was already downloaded')
            return True
        logging.info(f'{output_path} exists but will be replaced')
    try:
        sync_files(link, output_path)
        return True
    except FileNotFoundError:
            return False

def _init_logger():
    FORMAT = '%(levelname)-8s | %(asctime)-15s | %(pathname)-15s +%(lineno)-4d |  %(message)s'
    logging.basicConfig(format=FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    logging.getLogger().setLevel(logging.INFO)


def _parse_input():
    parser = argparse.ArgumentParser(description='Download files quikcly with multiple threads')
    parser.add_argument('--config', type=str)
    parser.add_argument('--runtime', type=int,)
    
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)
    
    remote_format = config['remote_format']
    local_format = config['local_format']

    fhr_min = config.get('fhr_min', 0)
    fhr_max = config.get('fhr_max', 120) + 0.1
    fhr_interval = config.get('fhr_int', 3)
    fhrs = np.arange(fhr_min, fhr_max, fhr_interval)

    runtime = args.runtime or 0
    if runtime <= 120:
        runtime = datetime.utcnow() - timedelta(hours=runtime)
    else:
        runtime = parsedate(runtime)
    floor_hour = runtime.hour - (runtime.hour%6)
    runtime = runtime.replace(hour=floor_hour, minute=0, second=0, microsecond=0)

    topdir = config.get('download_location', './')
    ens_min = config.get('ens_min', 1)
    ens_max = config.get('ens_max', 1)+1
    ens_range = range(ens_min, ens_max)

    remote_links = [link for link in generate_links(remote_format, date=runtime, ens=ens_range, fhr=fhrs)]
    local_paths =  [pathlib.Path(topdir, link) for link in generate_links(local_format, date=runtime, ens=ens_range, fhr=fhrs)]
    return remote_links, local_paths


def main():
    _init_logger()
    remote_links, local_paths = _parse_input()

    with concurrent.futures.ThreadPoolExecutor(25) as executor:
        for local, remote in zip(local_paths, remote_links):
            executor.submit(cp, link=remote, output_path=local)

if __name__ == "__main__":
        main()
