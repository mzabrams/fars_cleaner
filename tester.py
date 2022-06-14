from fars_cleaner import FARSFetcher, load_pipeline
from pathlib import Path

dl_path = Path(".")

fetcher = FARSFetcher(project_dir=dl_path)

#v, a, p = load_pipeline(start_year=1975, end_year=1981,fetcher=fetcher,debug=2)

#v, a, p = load_pipeline(start_year=1980, end_year=1990,fetcher=fetcher,debug=2)

#v, a, p = load_pipeline(start_year=1975, end_year=2018,fetcher=fetcher,debug=2)

v, a, p = load_pipeline(start_year=1975, end_year=2020,fetcher=fetcher,debug=2)
