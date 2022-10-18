from fars_cleaner import FARSFetcher, load_pipeline
import pytest

import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client


def test_load_pipeline_no_dask_client_error():
    # Do not include client
    fetcher = FARSFetcher()
    with pytest.raises(ValueError) as errinfo:
        vehicles, accidents, people = load_pipeline(fetcher=fetcher,
                                                    first_run=True,
                                                    target_folder=None,
                                                    use_dask=True,)
    assert "Must provide a client" in str(errinfo.value)


def test_load_pipeline_improper_paths_error():
    with pytest.raises(ValueError) as errinfo:
        vehicles, accidents, people = load_pipeline(fetcher=None,
                                                    first_run=True,
                                                    target_folder=None,
                                                    start_year=1975,
                                                    end_year=1975)
    assert "Must provide one of" in str(errinfo.value)


@pytest.mark.skip(reason="Should pass on local machine, fails on GitHub runners")
def test_full_loader():
    client = Client()
    fetcher = FARSFetcher()
    target_cache = fetcher.get_data_path()

    vehicles, accidents, people = load_pipeline(fetcher=fetcher,
                                                first_run=True,
                                                target_folder=None,
                                                use_dask=True,
                                                start_year = 1975,
                                                end_year = 1975,
                                                client=client)

