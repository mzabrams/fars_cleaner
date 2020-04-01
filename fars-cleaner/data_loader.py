# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 11:04:59 2020

@author: Mitchell Abrams
"""


import pandas as pd
import dask.dataframe as dd
import janitor

import pickle

from pathlib import Path

from builder import *
from builder import get_renaming
import extra_info as ei

from fars_utils import createPerID

import datasets as ds


def load_pipeline(
        start_year=1975,
        end_year=2018,
        first_run=True,
        target_folder=None,
        load_from=None,
        use_dask=False,
        client=None,
        **kwargs):
    """


    Parameters
    ----------
    start_year : int, optional
        Year to start analysis. The default is 1975.
    end_year : int, optional
        Year to end analysis. The default is 2018.
    first_run : bool, optional
        Flag to determine whether to process and write-out the required files, or whether the data can be loaded from
        disk pre-processed. The default is True.
    use_dask : bool, optional
        Flag to determine whether to parallelize using Dask. Supported, but not recommended at this time.
        The default is False.
    client : Dask Distributed Client, optional. Required if use_dask is True.

    Returns
    -------


    """

    data_dir = Path(__file__).resolve().parents[2] / "data" / "cache"

    if target_folder:
        fpath = Path(__file__).resolve().parents[2] / "data" / "processed" / target_folder
    else:
        target_folder = "test"
        fpath = Path(__file__).resolve().parents[2] / "data" / "processed" / "test"

    print("Loading mappings...")

    mapper_file = Path(__file__).parent.resolve() / "mapping.dict"

    if mapper_file.exists():
        with open(mapper_file, 'rb') as f:
            mappers = pickle.load(f)
    else:
        mappers = load_sheets(['Accident', 'Vehicle', 'Person'])
        with open(mapper_file, 'wb') as f:
            pickle.dump(mappers, f)

    print("Mappings loaded.")
    print("Loading data...")

    ds.fetch_subset(start_year, end_year)

    if first_run:

        lazy_people = []
        lazy_vehicles = []
        lazy_accidents = []

        for year in range(start_year, end_year + 1):
            vehicle, person, accident = load_basic(year,
                                                   use_dask=use_dask,
                                                   data_dir=data_dir,
                                                   mapping=mappers)
            accident['YEAR'] = year
            vehicle['YEAR'] = year
            person['YEAR'] = year

            if use_dask:
                lazy_people.append(person)
                lazy_vehicles.append(vehicle)
                lazy_accidents.append(accident)
            else:
                lazy_people.append(person)
                lazy_vehicles.append(vehicle)
                lazy_accidents.append(accident)

        if use_dask:
            people = dd.concat(lazy_people)
            vehicles = dd.concat(lazy_vehicles)
            accidents = dd.concat(lazy_accidents)
            people.visualize(filename='people.svg')
            vehicles.visualize(filename='vehicles.svg')
            accidents.visualize(filename='accidents.svg')
            people = people.compute()
            vehicles = vehicles.compute()
            accidents = accidents.compute()
        else:
            people = pd.concat(lazy_people)
            vehicles = pd.concat(lazy_vehicles)
            accidents = pd.concat(lazy_accidents)

        print("Data loaded.")
        print("Processing People.")

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
                .droplevel(0)
                .coalesce(['P_CF1', 'P_SF1'])
                .coalesce(['P_CF2', 'P_SF2'])
                .coalesce(['P_CF3', 'P_SF3'])

        )

        per = createPerID(per, None)

        print("Processing Accidents.")

        acc = (
            accidents
                .remove_empty()
                .coalesce(['ROAD_FNC', 'FUNC_SYS'], 'FUNCTION', delete_columns=False)
                .assign(TIME_OF_DAY=lambda x: ei.time_of_day(x),
                        DAY_OF_WEEK=lambda x: ei.day_of_week(x),
                        COLLISION_TYPE=lambda x: ei.collision_type(x),
                        TRAFFICWAY=lambda x: ei.trafficway(x),
                        FUNCTIONAL_CLASS=lambda x: ei.functional_class(x),
                        RURAL_OR_URBAN=lambda x: ei.land_use(x),
                        INTERSTATE=lambda x: ei.interstate(x),
                        FIPS=lambda x: ei.get_fips(x),
                        )
                .remove_columns(['FUNCTION', 'COUNTY'])
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
                .droplevel(0)
                .coalesce(['LATITUDE', 'latitude'])
                .coalesce(['LONGITUD', 'longitud'])
        )

        print("Processing Vehicles.")

        vehicles.reset_index(drop=True, inplace=True)

        veh = (
            vehicles
                .remove_empty()
                .update_where(
                conditions=(vehicles['YEAR'] <= 1997) & (vehicles['MOD_YEAR'] == 99),
                target_column_name='MOD_YEAR',
                target_val=9999
            )
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
                .droplevel(0)
                .coalesce(['VEH_CF1', 'VEH_SC1'])
                .coalesce(['VEH_CF2', 'VEH_SC2'])
                .coalesce(['DR_CF1', 'DR_SF1'])
                .coalesce(['DR_CF2', 'DR_SF2'])
                .coalesce(['DR_CF3', 'DR_SF3'])
                .coalesce(['DR_CF4', 'DR_SF4'])
                .encode_categorical(['STATE', 'HIT_RUN', 'MAKE', 'BODY_TYP',
                                     'ROLLOVER', 'J_KNIFE', 'TOW_VEH', 'SPEC_USE', 'EMER_USE', 'IMPACT1',
                                     'IMPACT2', 'DEFORMED', 'IMPACTS', 'TOWED', 'FIRE_EXP', 'VEH_CF1',
                                     'VEH_CF2', 'M_HARM', 'WGTCD_TR', 'FUELCODE', 'DR_PRES', 'DR_DRINK',
                                     'L_STATE', 'L_STATUS', 'L_RESTRI', 'DR_TRAIN', 'VIOL_CHG',
                                     'DR_CF1', 'DR_CF2', 'DR_CF3', 'VINA_MOD', 'HAZ_CARG', 'VEH_MAN',
                                     'L_COMPL', 'VIN_BT'])
        )

        save_pkl(target_folder, per, veh, acc)
    else:
        if not load_from:
            load_from = "full"

        load_path = Path(__file__).resolve().parents[2] / "data" / "processed" / load_from

        veh = pd.read_pickle(load_path / "vehicles.pkl.xz")
        acc = pd.read_pickle(load_path / "accidents.pkl.xz")
        per = pd.read_pickle(load_path / "people.pkl.xz")
        if start_year > 1975 or end_year < 2018:
            veh = veh.query(f"YEAR >= {start_year} and YEAR <= {end_year}").reset_index()
            acc = veh.query(f"YEAR >= {start_year} and YEAR <= {end_year}").reset_index()
            per = per.query(f"YEAR >= {start_year} and YEAR <= {end_year}").reset_index()

    print("Done")
    return veh, acc, per


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


def load_basic(year, use_dask=True, data_dir=None, mapping=None):
    cur_year = data_dir / f"{year}.unzip"
    vehicle_file = cur_year / "VEHICLE.csv"
    person_file = cur_year / "PERSON.csv"
    accident_file = cur_year / "ACCIDENT.csv"
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
                           'MCYCL_WT', 'VIN_REST', 'WHLDRWHL', 'RUR_URB', 'FUNC_SYS']

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
                             low_memory=False,
                             usecols=lambda x: x not in skip_veh,
                             dtype={'DEATHS': 'float64',
                                    'OCUPANTS': 'float64'}
                             ).rename(columns=veh_cols)
        per_df = pd.read_csv(person_file, encoding='cp1252',
                             usecols=lambda x: x not in skip_per,
                             low_memory=False).rename(columns=per_cols)

        acc_df = pd.read_csv(accident_file, encoding='cp1252',
                             low_memory=False).rename(columns=acc_cols)

    return veh_df, per_df, acc_df
