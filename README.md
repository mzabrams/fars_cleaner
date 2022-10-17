![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/mzabrams/fars-cleaner)
![PyPI](https://img.shields.io/pypi/v/fars-cleaner)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![DOI](https://zenodo.org/badge/252038452.svg)](https://zenodo.org/badge/latestdoi/252038452)

[![status](https://joss.theoj.org/papers/2ca54c6935611fe3cb0303c49a354c51/status.svg)](https://joss.theoj.org/papers/2ca54c6935611fe3cb0303c49a354c51)

# FARS Cleaner `fars-cleaner`

`fars-cleaner` is a Python library for downloading and pre-processing data 
from the Fatality Analysis Reporting System, collected annually by NHTSA since
 1975. 

## Installation

The preferred installation method is through `conda`.
```bash
conda install -c conda-forge fars-cleaner
```
You can also install with [pip](https://pip.pypa.io/en/stable/).

```bash
pip install fars-cleaner
```

## Usage

### Downloading FARS data
The `FARSFetcher` class provides an interface to download and unzip selected years from the NHTSA FARS FTP server. 
The class uses `pooch` to download and unzip the selected files. By default, files are unzipped to your OS's cache directory.

```python
from fars_cleaner import FARSFetcher

# Prepare for FARS file download, using the OS cache directory. 
fetcher = FARSFetcher()
```
Suggested usage is to download files to a data directory in your current project directory. 
Passing `project_dir` will download files to `project_dir/data/fars` by default. This behavior can be 
overridden by setting `cache_path` as well. Setting `cache_path` alone provides a direct path to the directory
you want to download files into.
```python
from pathlib import Path
from fars_cleaner import FARSFetcher

SOME_PATH = Path("/YOUR/PROJECT/PATH") 
# Prepare to download to /YOUR/PROJECT/PATH/data/fars
# This is the recommended usage.
fetcher = FARSFetcher(project_dir=SOME_PATH)

# Prepare to download to /YOUR/PROJECT/PATH/fars
cache_path = "fars"
fetcher = FARSFetcher(project_dir=SOME_PATH, cache_path=cache_path)

cache_path = Path("/SOME/TARGET/DIRECTORY")
# Prepare to download directly to a specific directory.
fetcher = FARSFetcher(cache_path=cache_path)
```

Files can be downloaded in their entirety (data from 1975-2018), as a single year, or across a specified year range.
Downloading all of the data can be quite time consuming. The download will simultaneously unzip the folders, and delete 
the zip files. Each zipped file will be unzipped and saved in a folder `{YEAR}.unzip`
```python
# Fetch all data
fetcher.fetch_all()

# Fetch a single year
fetcher.fetch_single(1984)

# Fetch data in a year range (inclusive).
fetcher.fetch_subset(1999, 2007)
```

### Processing FARS data
Calling `load_pipeline` will allow for full loading and pre-processing of the FARS data requested by the user.
```python
from fars_cleaner import FARSFetcher, load_pipeline

fetcher = FARSFetcher(project_dir=SOME_PATH)
vehicles, accidents, people = load_pipeline(fetcher=fetcher,
                                            first_run=True,
                                            target_folder=SOME_PATH)
```

Calling `load_basic` allows for simple loading of the FARS data for a single year, with no preprocessing. Files must
be prefetched using a `FARSFetcher` or similar method. A `mapper` dictionary must be provided to identify what, if 
any, columns require renaming. 

```python
from fars_cleaner.data_loader import load_basic

vehicles, accidents, people = load_basic(year=1975, data_dir=SOME_PATH, mapping=mappings)
```

## Requirements
Downloading and processing the full FARS dataset currently runs out of memory on Windows machines with only 16GB RAM. It is recommended to have at least 32GB RAM on Windows systems. macOS and Linux run with no issues on 16GB systems.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change. See [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

## License
[BSD-3 Clause](https://choosealicense.com/licenses/bsd-3-clause/)
