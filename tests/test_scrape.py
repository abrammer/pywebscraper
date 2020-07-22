import os
import pathlib
import sys
import tempfile
from datetime import datetime, timedelta
from unittest import mock

import pytest
import requests
import requests_mock
import yaml

try:
    import tests.context
except ModuleNotFoundError:
    import context
from web_scraper import scrape


@mock.patch('web_scraper.scrape.scraper.scrape', return_value=None)
def test_main(_scrape):
    with pytest.raises(RuntimeError):
        test_args = ['call.py']
        sys.argv = test_args
        scrape.main(mp=1)

    test_args = ['call.py', '--url', 'url', '--output', 'outputdir']
    sys.argv = test_args
    scrape.main(mp=1)
    assert _scrape.call_count == 1
    assert _scrape.call_args[0] == ()
    assert _scrape.call_args[1] == {'download_location': 'outputdir', 'url': 'url' }


@mock.patch('web_scraper.scrape.scraper.scrape', return_value=None)
def test_main_config(_scrape):
    config = {
        'test': {
            'url': 'http://none.invalid',
            'regex_exclude': '(.*2014.*)'
        }
    }
    with tempfile.NamedTemporaryFile() as tmpfile:
        with open(tmpfile.name, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

        test_args = ['call.py', '--config', tmpfile.name]
        sys.argv = test_args
        scrape.main(mp=1)
        assert _scrape.call_count == 1
        assert _scrape.call_args[0] == ()
        assert _scrape.call_args[1] == {
            'url': config['test']['url'],
            'regex_exclude': config['test']['regex_exclude'],
        }


@mock.patch('web_scraper.scrape.websync.cp', return_value=None)
def test_webls(_cp):
    return_text1 = b"""
    <a href="18060818EP0218.DAT">18060818EP0218.DAT</a>
    <a href="1806072011EP02.AMSR2.INTENSITY_ETA.DAT">1806072011EP02.AMSR2..&gt;</a>
    <a href="subdir/">subdir/</a>
    """
    return_text2 = b"""
    <a href="subdir1.DAT">ubdir1.DAT.DAT</a>
    """
    with requests_mock.Mocker() as m:
        test_url = 'http://none.invalid'
        m.get(test_url, status_code=200, content=return_text1)
        m.get(f'{test_url}/subdir/', status_code=200, content=return_text2)

        websyncer = scrape.websync(test_url, )
        websyncer.ls()
        assert len(websyncer.return_links) == 3
        assert websyncer.return_links == [
            f'{test_url}/{a}' for a in
            ['18060818EP0218.DAT', '1806072011EP02.AMSR2.INTENSITY_ETA.DAT']
        ] + [f'{test_url}/subdir/subdir1.DAT']

        scrape.scraper.scrape(test_url)
        assert _cp.call_count == 3


@mock.patch('web_scraper.scrape.websync.cp', return_value=None)
def test_webls_exclude(_cp):
    return_text1 = b"""
    <a href="18060818EP0214.DAT">_18060818EP0214.DAT</a>
    <a href="18060818EP0218.DAT">_18060818EP0218.DAT</a>
    <a href="1806072011EP02.AMSR2.INTENSITY_ETA.DAT">_1806072011EP02.AMSR2..&gt;</a>
    <a href="subdir/">subdir/</a>
    """
    return_text2 = b"""
    <a href="subdir1.DAT">_subdir1.DAT</a>
    """

    with requests_mock.Mocker() as m:
        test_url = 'http://none.invalid'
        m.get(test_url, status_code=200, content=return_text1)
        m.get(f'{test_url}/subdir/', status_code=200, content=return_text2)

        websyncer = scrape.websync(test_url, regex_exclude='(.*214.*)')
        websyncer.ls()
        assert len(websyncer.return_links) == 3
        assert websyncer.return_links == [
            f'{test_url}/{a}' for a in
            ['18060818EP0218.DAT', '1806072011EP02.AMSR2.INTENSITY_ETA.DAT']
        ] + [f'{test_url}/subdir/subdir1.DAT']

        # test above again passing through scrape this time
        # how many times does cp get hit
        scrape.scraper.scrape(test_url, regex_exclude='(.*214.*)')
        assert _cp.call_count == 3


@mock.patch('web_scraper.scrape.websync.cp', return_value=None)
def test_webls_include(_cp):
    return_text1 = b"""
    <a href="18060818EP0214.DAT">18060818EP0214.DAT</a>
    <a href="18060818EP0218.DAT">18060818EP0218.DAT</a>
    <a href="1806072011EP02.AMSR2.INTENSITY_ETA.DAT">1806072011EP02.AMSR2..&gt;</a>
    <a href="subdir/">subdir/</a>
    """
    return_text2 = b"""
    <a href="subdir1.DAT">ubdir1.DAT.DAT</a>
    """

    with requests_mock.Mocker() as m:
        test_url = 'http://none.invalid'
        m.get(test_url, status_code=200, content=return_text1)
        m.get(f'{test_url}/subdir/', status_code=200, content=return_text2)

        websyncer = scrape.websync(test_url, regex_include='(.*AMSR2.*)')
        websyncer.ls()
        assert len(websyncer.return_links) == 1
        assert websyncer.return_links == [
            f'{test_url}/{a}' for a in
            [ '1806072011EP02.AMSR2.INTENSITY_ETA.DAT']]

        # test above again passing through scrape this time
        # how many times does cp get hit
        scrape.scraper.scrape(test_url, regex_include='(.*214.*)')
        assert _cp.call_count == 1



def test_sync_file_download():
    with requests_mock.Mocker() as m:
        test_url = 'http://none.invalid'
        mod_time = datetime(2018, 1, 1, 12, 00)
        ret_content = b'test_content'
        m.get(test_url,
              status_code=200,
              headers={'Last-Modified': mod_time.isoformat()},
              content=ret_content)
        with tempfile.NamedTemporaryFile() as localfile:
            localpath = pathlib.Path(localfile.name)
            assert localpath.read_text() != 'test_content'
            scrape.websync.sync_files(test_url, localpath)
            assert localpath.stat().st_mtime == mod_time.timestamp()
            assert localpath.read_text() == ret_content.decode()


def test_sync_file_non200():
    with requests_mock.Mocker() as m:
        test_url = 'http://none.invalid'
        m.get(
            test_url,
            status_code=404,
        )
        with pytest.raises(RuntimeError):
            with tempfile.NamedTemporaryFile() as localfile:
                localpath = pathlib.Path(localfile.name)
                scrape.websync.sync_files(test_url, localpath)


def test_cp_download_nofile():
    with requests_mock.Mocker() as m:
        with tempfile.TemporaryDirectory() as tempdir:
            base_url = 'http://none.invalid/'
            test_url = f'{base_url}/somefile'
            mod_time = datetime(2018, 1, 1, 12, 00)
            ret_content = b'test_content'
            m.get(test_url,
                  status_code=200,
                  headers={'Last-Modified': mod_time.isoformat()},
                  content=ret_content)
            websyncer = scrape.websync(base_url, tempdir)
            websyncer.cp(test_url)

            localpath = pathlib.Path(tempdir, 'none_invalid', 'somefile')
            assert localpath.read_text() == 'test_content'
            assert localpath.stat().st_mtime == mod_time.timestamp()


def test_cp_nodownload():
    with requests_mock.Mocker() as m:
        with tempfile.TemporaryDirectory() as tempdir:
            base_url = 'http://none.invalid/'
            test_url = f'{base_url}/somefile'
            mod_time = datetime(2018, 1, 1, 12, 00) - timedelta(days=1)
            m.get(  # will raise an error if a get request is attempted
                test_url,
                status_code=404,
            )
            m.head(
                test_url,
                status_code=200,
                headers={'Last-Modified': mod_time.isoformat()},
            )

            localpath = pathlib.Path(tempdir, 'none_invalid', 'somefile')
            localpath.parent.mkdir(parents=True, exist_ok=True)
            localpath.write_text('old_content')

            websyncer = scrape.websync(base_url, tempdir)
            websyncer.cp(test_url)

            assert localpath.read_text() == 'old_content'
            assert localpath.stat().st_mtime != mod_time.timestamp()


def test_cp_download_newer_file():
    with requests_mock.Mocker() as m:
        with tempfile.TemporaryDirectory() as tempdir:
            base_url = 'http://none.invalid/'
            test_url = f'{base_url}/somefile'
            mod_time = datetime(2018, 1, 1, 12, 00)
            ret_content = b'test_content'
            m.get(test_url,
                  status_code=200,
                  headers={'Last-Modified': mod_time.isoformat()},
                  content=ret_content)
            m.head(
                test_url,
                status_code=200,
                headers={'Last-Modified': mod_time.isoformat()},
            )

            localpath = pathlib.Path(tempdir, 'none_invalid', 'somefile')
            localpath.parent.mkdir(parents=True, exist_ok=True)
            localpath.write_text('old_content')
            old_time = (mod_time - timedelta(days=1)).timestamp()
            os.utime(localpath, (old_time, old_time))

            websyncer = scrape.websync(base_url, tempdir)
            websyncer.cp(test_url)

            assert localpath.read_text() == ret_content.decode()
            assert localpath.stat().st_mtime == mod_time.timestamp()


if __name__ == "__main__":
    pytest.main()
