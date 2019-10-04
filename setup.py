from setuptools import setup, find_packages

setup(
    name="pywebscraper",
    description="Python Utility to Scrape a https site",
    url="",
    version="0.0.1",
    license="",
    packages=['web_scraper'],
    install_requires = [
        "requests",
        "pyyaml",
        "python-dateutil",
    ],
)
