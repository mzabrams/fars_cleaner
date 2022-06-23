"""
Load sample data.
"""
import pooch
import os

from pathlib import Path

class FARSFetcher:
    def __init__(self,
                 cache_path=None,
                 registry=None,
                 project_dir=None,
                 check_hash=True,
                 show_progress=True,
                 ):
        """Class to download FARS data from the NHTSA FTP repository.

        Note that on first run, this will take a long time to fully download the data, as the repository is large.
        Expect first run to take 5-10+ minutes, depending on your setup.

        Parameters
        ----------
        cache_path: `os.path` or path-like, or str, optional
            The path to save the downloaded FARS files to.
            Default is `pooch.os_cache("fars")`, the default cache path as defined by the OS. See `pooch` and
            `appdirs` documentations.
            If `str`, and `project_dir` is not `None`, files will be downloaded to `project_dir/cache_path`
        registry:
            Path to registry file. Defaults to path for packaged `registry.txt` file. Override at your own risk.
        project_dir:
            Top level directory for your current project. If a path is provided, and `cache_path` is left as default,
            files will be downloaded to `project_dir/data/fars`. If `cache_path` is not the default, files will be
            downloaded to `project_dir/cache_path`.
        check_hash: bool
            Flag to enforce pooch download behavior. Defaults to True. When False, force download of FARS resources
            regardless of hash mismatch against the local registry version. Useful for when the FARS
            database is updated before the registry can be modified. Should normally be left to default (False).
        show_progress: bool
            Use pooch built-in feature to show progress bars during download. Default True.
        """
        if project_dir:
            self.project_dir = project_dir
            if cache_path:
                self.cache_path = Path(project_dir) / cache_path
            else:
                self.cache_path = Path(project_dir) / "data" / "fars"
            self.project_dir.mkdir(parents=True, exist_ok=True)
            self.cache_path.mkdir(parents=True, exist_ok=True)
        else:
            self.project_dir = None
            if cache_path:
                self.cache_path = Path(cache_path)
                self.cache_path.mkdir(parents=True, exist_ok=True)
            else:
                self.cache_path = pooch.os_cache("fars")

        if registry:
            self.registry = Path(registry)
        else:
            self.registry = os.path.join(os.path.dirname(__file__), "registry.txt")

        self.check_hash = check_hash
        self.show_progress = show_progress

        self.GOODBOY = pooch.create(
            path=self.cache_path,
            base_url="https://www.nhtsa.gov/filebrowser/download/",
            registry=None,
            allow_updates=self.check_hash,
        )

        self.GOODBOY.load_registry(self.registry)

    def fetch_all(self):
        """
        Download the entire FARS dataset, to cache folder.
        """
        # The file will be downloaded automatically the first time this is run.

        fnames = self.GOODBOY.registry_files
        unzipped = {}
        
        for fname in fnames:

            if self.GOODBOY.is_available(fname):
                if "dict" in fname:
                    self.GOODBOY.fetch(fname, progressbar=self.show_progress)
                else:
                    unpack = pooch.Unzip(extract_dir=f"./{fname[:-4]}.unzip")
                    unzipped[fname] = self.GOODBOY.fetch(fname, processor=unpack, progressbar=self.show_progress)
            else:
                raise FileNotFoundError("File could not be found in FARS FTP directory.")
        return unzipped

    def fetch_subset(self, start_yr, end_yr):
        """
        Download a subset of the FARS dataset.
        """
        unzipped = {}
        for yr in range(start_yr, end_yr + 1):
            unzipped[yr] = self.fetch_single(yr)
        return unzipped

    def fetch_single(self, year):
        """
        Load the FARS data for a given year.
        """
        # fname = f'{year}/National/FARS{year}NationalCSV.zip'
        fname = f'{year}.zip'
        if self.GOODBOY.is_available(fname):
            unpack = pooch.Unzip(extract_dir=f"./{fname[:-4]}.unzip")
            unzipped = self.GOODBOY.fetch(fname, processor=unpack, progressbar=self.show_progress)
        else:
            raise FileNotFoundError(f"{fname}: File could not be found in FARS FTP directory.")

        return {year: unzipped}

    def fetch_mappers(self):
        """
        Loads the mappings for each variable from a pickled dictionary.

        Returns
        -------
        Path to the mapper file
        """
        return self.GOODBOY.fetch("mapping.dict")

    def get_data_path(self):
        return self.cache_path
    
    def get_show_progress(self):
        return self.show_progress
