# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 11:04:59 2020

@author: Mitchell Abrams
"""

# import duecredit
import pandas as pd
import dask.dataframe as dd
import janitor
import os

import pickle

from pathlib import Path

from thefuzz import process as fuzzyprocess

from .builder import *
from .builder import get_renaming
import fars_cleaner.extra_info as ei

from fars_cleaner.fars_utils import createPerID

from fars_cleaner import FARSFetcher


def load_pipeline(
        start_year=1975,
        end_year=2020,
        first_run=True,
        target_folder=None,
        load_from=None,
        use_dask=False,
        client=None,
        fetcher: FARSFetcher = None,
        debug=0,
        **kwargs):
    """Load pipeline to load and process FARS data between `start_year` and `end_year.`

    Loads FARS data from NHTSA database and pre-processes in an opinionated manner.
    FARS data are preprocessed according to a preset mapping file.


    Parameters
    ----------
    start_year : int, optional
        Year to start analysis. The default is 1975.
    end_year : int, optional
        Year to end analysis. The default is 2020.
    first_run : bool, optional
        Flag to determine whether to process and write-out the required files, or whether the data can be loaded from
        disk pre-processed. The default is True.
    target_folder : Path-like, optional
        Path to target folder to save out pickled data after processing. If None, defaults to the cache directory
        specified by the FARSFetcher.
    load_from : Path-like, optional
        Path to source folder to load processed data from. If `first_run` is true, this parameter is ignored.
    use_dask : bool, optional
        Flag to determine whether to parallelize using Dask. Supported, but not recommended at this time.
        The default is False.
    client : Dask Distributed Client, optional. Required if use_dask is True.
        When provided and use_dask is False, use_dask is set to True.
    fetcher : FARSFetcher, optional
        FARSFetcher instance to facilitate file download and cache management. Overrides `load_from` value.
    debug : int, optional
        Set verbosity level.
        0 = No verbose output.
        1 = Print basic progress messages.
        2 = Print messages along with progress bars (provided by tqdm). NOT implemented yet.

    Returns
    -------
    (pandas.DataFrame, pandas.DataFrame, pandas.DataFrame)
        Returns three DataFrames: vehicles, accidents, and people.

    Raises
    ------
    ValueError
        If `use_dask` is True and no client provided.
        If `fetcher` and `load_from` are both not provided.

    """

    if use_dask and client is None:
        raise ValueError("Must provide a client to use Dask.")

    if client is not None and not use_dask:
        use_dask = True

    if fetcher is None:
        if load_from is None:
            raise ValueError("Must provide one of fetcher or load_from to access data.")
        else:
            data_dir = Path(load_from)
    else:
        data_dir = fetcher.get_data_path()

    if target_folder is None:
        target_folder = data_dir

    if debug > 0:
        print("Loading mappings...")

    if fetcher:
        mapper_file = Path(fetcher.fetch_mappers())
    else:
        mapper_file = data_dir / "mapping.dict"

    if mapper_file.exists():
        with open(mapper_file, 'rb') as f:
            mappers = pickle.load(f)
    else:
        raise FileNotFoundError("Mapping dictionary not found.")

    if debug > 0:
        print("Mappings loaded.")
        print("Loading data...")

    if fetcher:
        fetcher.fetch_subset(start_year, end_year)
        if debug > 0:
            print("Fetched originals, loading to pandas....")

    if first_run:

        lazy_people = []
        lazy_vehicles = []
        lazy_accidents = []

        for year in range(start_year, end_year + 1):
            if debug > 1:
                print(year)
            vehicle, person, accident = load_basic(year,
                                                   use_dask=use_dask,
                                                   data_dir=data_dir,
                                                   mapping=mappers,
                                                   client=client)
            accident['YEAR'] = year
            vehicle['YEAR'] = year
            person['YEAR'] = year

            lazy_people.append(person)
            lazy_vehicles.append(vehicle)
            lazy_accidents.append(accident)
            if debug > 1:
                #print('dense: acc {:0.2f} bytes, per {:0.2f} bytes, veh {:0.2f} bytes'.format(accident.memory_usage(), person.memory_usage(deep=True), vehicle.memory_usage(deep=True)))
                print(accident.info())
                print(vehicle.info())
                print(person.info())
            del accident
            del person
            del vehicle

        if use_dask:
            people = dd.concat(lazy_people)
            vehicles = dd.concat(lazy_vehicles)
            accidents = dd.concat(lazy_accidents)
            people = people.compute()
            vehicles = vehicles.compute()
            accidents = accidents.compute()
        else:
            people = pd.concat(lazy_people)
            vehicles = pd.concat(lazy_vehicles)
            accidents = pd.concat(lazy_accidents)

        if debug > 0:
            print("Data loaded.")
            print("Processing People.")

        per = process_people(people, mappers)

        if debug > 0:
            print("Processing Accidents.")

        acc = process_accidents(accidents, mappers)

        if debug > 0:
            print("Processing Vehicles.")

        veh = process_vehicles(vehicles, mappers)

        save_pkl(target_folder, per, veh, acc)
    else:

        load_path = Path(__file__).resolve().parents[2] / "data" / "processed" / load_from

        veh = pd.read_pickle(load_path / "vehicles.pkl.xz")
        acc = pd.read_pickle(load_path / "accidents.pkl.xz")
        per = pd.read_pickle(load_path / "people.pkl.xz")

        if start_year > 1975 or end_year < 2018:
            veh = veh.query(f"YEAR >= {start_year} and YEAR <= {end_year}").reset_index()
            acc = veh.query(f"YEAR >= {start_year} and YEAR <= {end_year}").reset_index()
            per = per.query(f"YEAR >= {start_year} and YEAR <= {end_year}").reset_index()

    if debug > 0: print("Done")

    return veh, acc, per


def process_accidents(accidents, mappers):
    """Accident file processor, using predefined mapping dictionary.

    Accepts the unprocessed data from the concatenated ACCIDENTS.csv files. Applies appropriate mappings
    based on the year of each record, and returns a fully preprocessed DataFrame. Several specific mappings
    are applied from `fars_cleaner.extra_info`.

    Parameters
    ----------
    accidents : pandas.DataFrame
        Unprocessed DataFrame with accident-level data
    mappers : dict
        Dictionary containing mapping structure of selected years for all relevant variables in the accident table

    Returns
    -------
    pandas.DataFrame
        Processed accidents DataFrame
    """
    func_coalesce = []
    if 'ROAD_FNC' in accidents.columns:
        func_coalesce.append('ROAD_FNC')
    if 'FUNC_SYS' in accidents.columns:
        func_coalesce.append('FUNC_SYS')
    if len(func_coalesce) == 2:
        acc = accidents.coalesce(*func_coalesce, target_column_name='FUNCTION')
    elif len(func_coalesce) == 1:
        acc = accidents.rename({func_coalesce[0]: 'FUNCTION'})
    else:
        acc = accidents
    if 'FUNCTION' in acc.columns:
        acc = (acc
               .assign(FUNCTIONAL_CLASS=lambda x: ei.functional_class(x),
                       RURAL_OR_URBAN=lambda x: ei.land_use(x),
                       INTERSTATE=lambda x: ei.interstate(x), ))
    acc = (
        acc
            .assign(TIME_OF_DAY=lambda x: ei.time_of_day(x),
                    DAY_OF_WEEK=lambda x: ei.day_of_week(x),
                    COLLISION_TYPE=lambda x: ei.collision_type(x),
                    TRAFFICWAY=lambda x: ei.trafficway(x),
                    FIPS=lambda x: ei.get_fips(x),
                    )
            .groupby(['YEAR'])
            .apply(mapping, mappers=mappers['Accident'])
            .find_replace(match='exact',
                          CF1={45: pd.NA,
                               46: pd.NA,
                               60: pd.NA},
                          CF2={45: pd.NA,
                               46: pd.NA,
                               60: pd.NA},
                          CF3={45: pd.NA,
                               46: pd.NA,
                               60: pd.NA},

                          )
            # .droplevel(0)
            .remove_columns(column_names=['COUNTY'])
            .remove_empty()
    )
    if 'latitude' in acc.columns:
        acc = (
            acc
                .coalesce('LATITUDE', 'latitude')
                .coalesce('LONGITUD', 'longitud')
                .remove_columns(column_names=[
                                "latitude",
                                "longitud"]
            )
        )

    return acc


def fix_mod_year(df):
    df['MOD_YEAR'] = df['MOD_YEAR'].mask((df['MOD_YEAR'] < 99),
                                         df['MOD_YEAR'].add(1900))
    return df


def process_vehicles(vehicles, mappers):
    """Vehicle file processor, using predefined mapping dictionary.

    Accepts the unprocessed data from the concatenated VEHICLES.csv files. Applies appropriate mappings
    based on the year of each record, and returns a fully preprocessed DataFrame. Several specific mappings
    are applied from `fars_cleaner.extra_info`.

    Parameters
    ----------
    vehicles : pandas.DataFrame
        Unprocessed DataFrame with vehicle-level data
    mappers : dict
        Dictionary containing mapping structure of selected years for all relevant variables in the vehicle table

    Returns
    -------
    pandas.DataFrame
        Processed vehicles DataFrame
    """
    vehicles.reset_index(drop=True, inplace=True)

    veh = (
        vehicles
            .update_where(
            conditions=(vehicles['YEAR'] <= 1997) & (vehicles['MOD_YEAR'] == 99),
            target_column_name='MOD_YEAR',
            target_val=9999)
            .then(fix_mod_year)
            .assign(PASSENGER_CAR=lambda x: ei.is_passenger_car(x),
                    LIGHT_TRUCK_OR_VAN=lambda x:
                    ei.is_light_truck_or_van(x),
                    LARGE_TRUCK=lambda x: ei.is_large_truck(x),
                    MOTORCYCLE=lambda x: ei.is_motorcycle(x),
                    BUS=lambda x: ei.is_bus(x),
                    OTHER_UNKNOWN_VEHICLE=lambda x:
                    ei.is_other_or_unknown(x),
                    PASSENGER_VEHICLE=lambda x:
                    ei.is_passenger_vehicle(x),
                    UTILITY_VEHICLE=lambda x:
                    ei.is_utility_vehicle(x),
                    PICKUP=lambda x: ei.is_pickup(x),
                    VAN=lambda x: ei.is_van(x),
                    MEDIUM_TRUCK=lambda x: ei.is_medium_truck(x),
                    HEAVY_TRUCK=lambda x: ei.is_heavy_truck(x),
                    COMBINATION_TRUCK=lambda x:
                    ei.is_combination_truck(x),
                    SINGLE_UNIT_TRUCK=lambda x:
                    ei.is_single_unit_truck(x))
            .groupby(['YEAR'])
            .apply(mapping, mappers=mappers['Vehicle'])
            # .droplevel(0)
            .remove_empty()
    )

    if ('VEH_SC1' in veh.columns) and ('VEH_CF1' in veh.columns):
        veh = (
            veh
                # .coalesce(['VEH_CF1', 'VEH_SC1'])
                .coalesce('VEH_SC1', 'VEH_CF1', target_column_name='VEH_CF1')
                .coalesce('VEH_SC2', 'VEH_CF2', target_column_name='VEH_CF2')
                .coalesce('DR_SF1', 'DR_CF1', target_column_name='DR_CF1')
                .coalesce('DR_SF2', 'DR_CF2', target_column_name='DR_CF2')
                .coalesce('DR_SF3', 'DR_CF3', target_column_name='DR_CF3')
                .coalesce('DR_SF4', 'DR_CF4', target_column_name='DR_CF4')
                .remove_columns(column_names=[
                "VEH_SC1", "VEH_SC2",
                "DR_SF1", "DR_SF2", "DR_SF3", "DR_SF4",
            ]
            )
        )
    elif ('VEH_CF1' not in veh.columns):
        veh = (
            veh
                .rename_columns({
                'VEH_SC1': 'VEH_CF1',
                'VEH_SC2': 'VEH_CF2',
                'DR_SF1': 'DR_CF1',
                'DR_SF2': 'DR_CF2',
                'DR_SF3': 'DR_CF3',
                'DR_SF4': 'DR_CF4',
            }))

    return veh.encode_categorical(list(
        {'STATE', 'HIT_RUN', 'MAKE', 'BODY_TYP', 'ROLLOVER', 'J_KNIFE', 'TOW_VEH', 'SPEC_USE', 'EMER_USE', 'IMPACT1',
         'IMPACT2', 'DEFORMED', 'IMPACTS', 'TOWED', 'FIRE_EXP', 'VEH_CF1', 'VEH_CF2', 'M_HARM', 'WGTCD_TR', 'FUELCODE',
         'DR_PRES', 'DR_DRINK', 'L_STATE', 'L_STATUS', 'L_RESTRI', 'DR_TRAIN', 'VIOL_CHG', 'DR_CF1', 'DR_CF2', 'DR_CF3',
         'VINA_MOD', 'HAZ_CARG', 'VEH_MAN', 'L_COMPL', 'VIN_BT'} & set(veh.columns)))


def process_people(people, mappers):
    """Person file processor, using predefined mapping dictionary.

    Accepts the unprocessed data from the concatenated PERSON.csv files. Applies appropriate mappings
    based on the year of each record, and returns a fully preprocessed DataFrame. Several specific mappings
    are applied from `fars_cleaner.extra_info`.

    Parameters
    ----------
    people : pandas.DataFrame
        Unprocessed DataFrame with person-level data
    mappers : dict
        Dictionary containing mapping structure of selected years for all relevant variables in the person table

    Returns
    -------
    pandas.DataFrame
        Processed people DataFrame
    """
    if ('MAN_REST' in people.columns) and ('REST_USE' in people.columns):
        people = (
            people.coalesce('MAN_REST', 'REST_USE', target_column_name='REST_USE')
            .remove_columns(column_names=['MAN_REST'])
        )
    elif ('MAN_REST' in people.columns):
        people = (
            people.rename_columns({'MAN_REST':'REST_USE'})
        )

    per = (
        people
            .remove_empty()
            .assign(PERSON_TYPE=lambda x: ei.person_type(x),
                    RESTRAINTS=lambda x: ei.restraint_use(x),
                    AIR_BAG_DEPLOYMENT=lambda x: ei.air_bag_deployed(x),
                    DEAD=lambda x: x['INJ_SEV'] == 4,
                    HELMETED=lambda x: ei.helmet_use(x),
                    )
            .groupby(['YEAR'])
            .apply(mapping, mappers=mappers['Person'])
        # .droplevel(0)
    )

    if ('P_SF1' in per.columns) and ('P_CF1' in per.columns):
        per = (
            per
                .coalesce('P_SF1', 'P_CF1', target_column_name='P_CF1')
                .coalesce('P_SF2', 'P_CF2', target_column_name='P_CF2')
                .coalesce('P_SF3', 'P_CF3', target_column_name='P_CF3')
                .remove_columns(column_names=
                                ["P_SF1", "P_SF2", "P_SF3"]
                                )
        )
    elif 'P_CF1' not in per.columns:
        per = (
            per
                .rename_columns({
                'P_SF1': 'P_CF1',
                'P_SF2': 'P_CF2',
                'P_SF3': 'P_CF3',
            }))
    per = per.encode_categorical(list(
        {'STATE', 'SEX', 'PER_TYP', 'SEAT_POS', 'P_CF1', 'P_CF2', 'P_CF3'} & set(per.columns)))

    return createPerID(per, None)


def save_pkl(fname, people, vehicles, accidents):
    pkl_path = Path(__file__).resolve().parents[2] / "data" / "processed" / fname
    pkl_path.mkdir(parents=True, exist_ok=True)
    print("Saving accidents.pkl.xz ...")
    accidents.to_pickle(pkl_path / "accidents.pkl.xz")
    print("Saving people.pkl.xz ...")
    people.to_pickle(pkl_path / "people.pkl.xz")
    print('Saving vehicles.pkl.xz ...')
    vehicles.to_pickle(pkl_path / "vehicles.pkl.xz")


def decode_categorical(df):
    new_df = df.copy()
    for col in df.columns:
        if df[col].dtype.name == 'category':
            new_df[col] = new_df[col].astype(str)
    return new_df


def fix_mod_year(df):
    df['MOD_YEAR'] = df['MOD_YEAR'].mask((df['MOD_YEAR'] < 99),
                                         df['MOD_YEAR'].add(1900))
    return df


def mapping(group, mappers):
    yr = group.name
    cur_mappers = year_mapper(mappers, yr)
    if 'CARBUR' in cur_mappers.keys():
        cur_mappers['CARBUR'] = {str(k): v for k, v in cur_mappers['CARBUR'].items()}
        cur_mappers['FUELCODE'] = {str(k): v for k, v in cur_mappers['FUELCODE'].items()}
        cur_mappers['CYLINDER'] = {str(k): v for k, v in cur_mappers['CYLINDER'].items()}
        if 2011 <= yr <= 2012:
            cur_mappers['MCYCL_CY'] = {2: 'Two-Stroke', 4: 'Four-Stroke'}
            # {k:v for k,v in cur_mappers['MCYCL_CY'].items()}
        cur_mappers['TON_RAT'] = {str(k): v for k, v in cur_mappers['TON_RAT'].items()}
        cur_mappers['VIN_REST'] = {str(k): v for k, v in cur_mappers['VIN_REST'].items()}
        cur_mappers['VIN_BT'] = {str(k): v for k, v in cur_mappers['VIN_BT'].items()}
        cur_mappers['VINTYPE'] = {str(k): v for k, v in cur_mappers['VINTYPE'].items()}

    return group.replace(cur_mappers)


def load_basic(year, use_dask=False, data_dir=None, mapping=None, client=None):
    """Basic load pipeline to load FARS data for `year`

    Assumes FARS data for a given year is already downloaded and unzipped in `data_dir`, and
    requires a mapping dictionary to appropriately rename fields within the relevant dataframes.


    Parameters
    ----------
    year : int
        Year of data to load
    data_dir : Path-like
        Path where the data is stored. Must contain the folder "YYYY.unzip", where YYYY=`year`
    use_dask : bool, optional
        Flag to determine whether to parallelize using Dask. Supported, but not recommended at this time.
        The default is False.
    client : Dask Distributed Client, optional. Required if use_dask is True.
    mapping: dict
        mapping dictionary, as generated by fars_cleaner.builder.load_sheets()

    Returns
    -------
    (pandas.DataFrame, pandas.DataFrame, pandas.DataFrame)
        Returns three DataFrames: vehicles, accidents, and people.

    Raises
    ------
    ValueError
        If `use_dask` is True and no client provided.

    """

    if use_dask and client is None:
        raise ValueError("Must provide a client to use Dask.")

    cur_year = data_dir / f"{year}.unzip"


    cur_dir_files = os.listdir(cur_year)
    #veh_suggestions = fuzzyfinder("VEHICLE.csv", cur_dir_files)
    #per_suggestions = fuzzyfinder("PERSON.csv", cur_dir_files)
    #acc_suggestions = fuzzyfinder("ACCIDENT.csv", cur_dir_files)
    veh_suggestions = fuzzyprocess.extractOne("VEHICLE.csv", cur_dir_files)
    per_suggestions = fuzzyprocess.extractOne("PERSON.csv", cur_dir_files)
    acc_suggestions = fuzzyprocess.extractOne("ACCIDENT.csv", cur_dir_files)
    vehicle_fname = veh_suggestions[0]
    person_fname = per_suggestions[0]
    accident_fname = acc_suggestions[0]
    vehicle_file = cur_year / vehicle_fname
    person_file = cur_year / person_fname
    accident_file = cur_year / accident_fname
    print(vehicle_fname)

    acc_cols = get_renaming(mapping['Accident'], year)
    per_cols = get_renaming(mapping['Person'], year)
    veh_cols = get_renaming(mapping['Vehicle'], year)
    skip_veh = ['VE_FORMS', 'COUNTY', 'MONTH', 'DAY', 'HOUR', 'MINUTE',
                'ROAD_FNC', 'HARM_EV', 'MAN_COLL', 'SCH_BUS']
    skip_per = skip_veh + ['SCH_BUS', 'MAKE', 'BODY_TYP', 'MOD_YEAR', 'ROLLOVER',
                           'TOW_VEH', 'SPEC_USE', 'EMER_USE', 'IMPACT1', 'IMPACT2',
                           'IMPACTS', 'FIRE_EXP', 'WGTCD_TR', 'MAK_MOD', 'VIN_WGT',
                           'WHLBS_SH', 'WHLBS_LG', 'MCYCL_DS', 'VINA_MOD', 'SER_TR',
                           'VIN_BT', 'CERT_NO', 'VINTYPE', 'VINMAKE', 'VINMODYR',
                           'VIN_LNGT', 'FUELCODE', 'CARBUR', 'CYLINDER', 'DISPLACE',
                           'MCYCL_CY', 'TIRE_SZE', 'TON_RAT', 'TRK_WT', 'TRKWTVAR',
                           'MCYCL_WT', 'VIN_REST', 'WHLDRWHL', 'RUR_URB', 'FUNC_SYS',
                           'VPICMAKE', 'VPICMODEL', 'VPICBODYCLASS', 'ICFINALBODY']
    namedskip = []
    for skipper in skip_per:
        toap = f"{skipper}NAME"
        if toap not in skip_per:
            namedskip.append(toap)
    skip_per.extend(namedskip)
    namedskip = []
    for skipper in skip_veh:
        toap = f"{skipper}NAME"
        if toap not in skip_veh:
            namedskip.append(toap)
    skip_veh.extend(namedskip)
    if use_dask:

        veh_df = dd.from_pandas(pd.read_csv(vehicle_file,
                                            # encoding='ansi',
                                            encoding='cp1252',
                                            low_memory=False,
                                            usecols=lambda x: x not in skip_veh,
                                            dtype={'DEATHS': 'float64',
                                                   'OCUPANTS': 'float64'}),
                                npartitions=1).rename(columns=veh_cols)
        per_df = dd.from_pandas(pd.read_csv(person_file, encoding='cp1252',
                                            usecols=lambda x: x not in skip_per,
                                            low_memory=False),
                                npartitions=1).rename(columns=per_cols)

        acc_df = dd.from_pandas(pd.read_csv(accident_file, encoding='cp1252',
                                            low_memory=False),
                                npartitions=1).rename(columns=acc_cols)

        acc_df = dd.read_csv(accident_file, encoding='cp1252',
                             low_memory=False,
                             assume_missing=True).rename(columns=acc_cols)
    else:
        veh_df = pd.read_csv(vehicle_file,
                             # encoding='ansi',
                             encoding='cp1252',
                             low_memory=True,
                             usecols=lambda x: x not in skip_veh,
                             dtype={'DEATHS': 'float64',
                                    'OCUPANTS': 'float64',
                                    'MCARR_I1': 'str', 'MCARR_I2': 'str', 'MCARR_ID': 'str',
                                    'VIN_1': 'str', 'VIN_2': 'str', 'VIN_3': 'str',
                                    'VIN_4': 'str', 'VIN_5': 'str', 'VIN_5': 'str',
                                    'VIN_7': 'str', 'VIN_8': 'str', 'VIN_9': 'str',
                                    'VIN_10': 'str', 'VIN_11': 'str', 'VIN_12': 'str',
                                    'CYLINDER': 'str', 'TRLR1VIN': 'str', 'TRLR2VIN': 'str',
                                    'TRLR3VIN': 'str'
                                    }
                             ).rename(columns=veh_cols)
        per_df = pd.read_csv(person_file, encoding='cp1252',
                             usecols=lambda x: x not in skip_per and not x.endswith("NAME"),
                             low_memory=True).rename(columns=per_cols)
        acc_df = pd.read_csv(accident_file, encoding='cp1252',
                             usecols=lambda x: not x.endswith("NAME"),
                             low_memory=True).rename(columns=acc_cols)

    if not use_dask:
        acc_intCols = acc_df.select_dtypes('integer').columns
        acc_floatCols = acc_df.select_dtypes('float').columns
        acc_df[acc_intCols] = acc_df[acc_intCols].apply(pd.to_numeric, downcast='integer')
        acc_df[acc_floatCols] = acc_df[acc_floatCols].apply(pd.to_numeric, downcast='float')

        veh_intCols = veh_df.select_dtypes('integer').columns
        veh_floatCols = veh_df.select_dtypes('float').columns
        veh_df[veh_intCols] = veh_df[veh_intCols].apply(pd.to_numeric, downcast='integer')
        veh_df[veh_floatCols] = veh_df[veh_floatCols].apply(pd.to_numeric, downcast='float')

        per_intCols = per_df.select_dtypes('integer').columns
        per_floatCols = per_df.select_dtypes('float').columns
        per_df[per_intCols] = per_df[per_intCols].apply(pd.to_numeric, downcast='integer')
        per_df[per_floatCols] = per_df[per_floatCols].apply(pd.to_numeric, downcast='float')
	
    return veh_df, per_df, acc_df
