"""
Load sample data.
"""
import pandas
import pooch
import os

from pathlib import Path

project_dir = Path(__file__).resolve().parents[2]
cache_path =  project_dir / "data" / "cache"

GOODBOY = pooch.create(
    # Use the default cache folder for the OS
    path=cache_path,#pooch.os_cache("fars"),
    # The remote data is on the FARS FTP Server
    #base_url="ftp://ftp.nhtsa.dot.gov/FARS/",
    base_url="https://www.nhtsa.gov/filebrowser/download/",
    # The registry specifies the files that can be fetched from the local storage
    registry=None,
)
GOODBOY.load_registry(os.path.join(os.path.dirname(__file__), "registry.txt"))


def fetch_all():
    """
    Download the entire FARS dataset, to cache folder.
    """
    # The file will be downloaded automatically the first time this is run.
    unpack = pooch.Unzip()

    fnames = GOODBOY.registry_files
    for fname in fnames:
        GOODBOY.fetch(fname, processor=unpack)
    print(fnames)


def fetch_subset(start_yr, end_yr):
    """
    Download a subset of the FARS dataset. 
    """
    for yr in range(start_yr, end_yr+1):
        fetch_single(yr)


def fetch_single(year):
    """
    Load the FARS data for a given year.
    """
    #fname = f'{year}/National/FARS{year}NationalCSV.zip'
    fname = f'{year}'
    unzipped = GOODBOY.fetch(fname, processor=pooch.Unzip())
