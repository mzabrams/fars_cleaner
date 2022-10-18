from fars_cleaner import FARSFetcher
import pandas as pd

from hypothesis import given
from hypothesis import strategies as st
from hypothesis.extra.pandas import column, data_frames

import os, pooch, shutil


@pytest.mark.skip(reason="Should pass on local machine, fails on GitHub runners")
def test_init_fetcher_defaults():
    fetcher = FARSFetcher()
    assert fetcher.get_show_progress() == True
    assert fetcher.get_data_path() == pooch.os_cache("fars")


@pytest.mark.skip(reason="Should pass on local machine, fails on GitHub runners")
def test_fetch_single():
    fetcher = FARSFetcher()
    result = fetcher.fetch_single(1975)
    from pathlib import Path
    target_cache = fetcher.get_data_path() / "1975.unzip"
    assert os.path.exists(target_cache)
    assert os.path.exists(target_cache / "ACCIDENT.CSV")
    assert os.path.exists(target_cache / "VEHICLE.CSV")
    assert os.path.exists(target_cache / "PERSON.CSV")
    shutil.rmtree(target_cache)
    os.remove(fetcher.get_data_path() / "1975.zip")


@pytest.mark.skip(reason="Should pass on local machine, fails on GitHub runners")
def test_fetch_subset():
    fetcher = FARSFetcher()
    result = fetcher.fetch_subset(2000, 2001)
    from pathlib import Path
    target_cache1 = fetcher.get_data_path() / "2000.unzip"
    assert os.path.exists(target_cache1)
    assert os.path.exists(target_cache1 / "ACCIDENT.CSV")
    assert os.path.exists(target_cache1 / "VEHICLE.CSV")
    assert os.path.exists(target_cache1 / "PERSON.CSV")
    shutil.rmtree(target_cache1)
    os.remove(fetcher.get_data_path() / "2000.zip")

    target_cache2 = fetcher.get_data_path() / "2001.unzip"
    assert os.path.exists(target_cache2)
    assert os.path.exists(target_cache2 / "ACCIDENT.CSV")
    assert os.path.exists(target_cache2 / "VEHICLE.CSV")
    assert os.path.exists(target_cache2 / "PERSON.CSV")
    shutil.rmtree(target_cache2)
    os.remove(fetcher.get_data_path() / "2001.zip")


def test_fetch_all():
    fetcher = FARSFetcher()
    result = fetcher.fetch_all()
    from pathlib import Path
    for year in range(1975, 2021):
        zip_path = fetcher.get_data_path() / f"{year}.zip"
        unzip_path = fetcher.get_data_path() / f"{year}.unzip"
        accident_path = "ACCIDENT.CSV"
        vehicle_path = "VEHICLE.CSV"
        person_path = "PERSON.CSV"
        assert os.path.exists(zip_path)
        assert os.path.exists(unzip_path)
        unzipped_files = [x.upper() for x in os.listdir(unzip_path)]

        assert accident_path in unzipped_files
        assert person_path in unzipped_files
        assert vehicle_path in unzipped_files
        shutil.rmtree(unzip_path)
        os.remove(zip_path)
        assert not os.path.exists(zip_path)
        assert not os.path.exists(unzip_path)
    assert os.path.exists(fetcher.get_data_path() / "mapping.dict")
    os.remove(fetcher.get_data_path() / "mapping.dict")
