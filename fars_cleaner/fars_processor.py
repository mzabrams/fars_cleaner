# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 11:04:59 2020

@author: Mitchell Abrams
"""


import pandas as pd
import janitor
import datetime

import pickle

from pathlib import Path

#from builder import *
from fars_cleaner.builder import get_renaming
import fars_cleaner.extra_info as ei

from fars_cleaner.fars_utils import createPerID

from fars_cleaner import FARSFetcher

class FARSProcessor:
    def __init__(self,
                 start_year=1975,
                 end_year=2018,
                 fetcher: FARSFetcher = None,
                 ):
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
            Flag to determine whether to parallelize using Dask. Not implemented at this time.
            The default is False.
        client : Dask Distributed Client, optional. Required if use_dask is True. Not implemented at this time.

        Returns
        -------


        """
        self.NOW = datetime.datetime.now()
        self.start_year = start_year
        self.end_year = end_year

        if fetcher is None:
            self.fetcher = FARSFetcher()
        else:
            self.fetcher = fetcher
            
        fetcher.fetch_subset(self.start_year, self.end_year)

        self.data_dir = self.fetcher.get_data_path()
        with open(fetcher.fetch_mappers(), 'rb') as f:
            self.mappers = pickle.load(f)
        self.load_paths = self.fetcher.fetch_subset(self.start_year, self.end_year)

        people = []
        vehicles = []
        accidents = []

        for year in range(start_year, end_year + 1):
            vehicle = self.load_vehicles(year)
            person = self.load_people(year)
            accident = self.load_accidents(year)

            accident['YEAR'] = year
            vehicle['YEAR'] = year
            person['YEAR'] = year

            people.append(person)
            vehicles.append(vehicle)
            accidents.append(accident)
        #self.accidents = pd.concat(accidents)
        print("accidents")
        self.accidents = self.process_accidents(pd.concat(accidents))
        print("people")
        self.people = self.process_people(pd.concat(people))
        print("vehicles")
        self.vehicles = self.process_vehicles(pd.concat(vehicles))

    def year_mapper(self, mappers, year):
        # Take mapper for the data file
        # Iterate through: for each sub-dictionary that was implemented before
        # and discontinued after this year
        cur_mapper = {code: {} for code in mappers.keys()}
        for code, detail in mappers.items():
            discon = detail['discontinued']
            if not discon:
                discon = self.NOW.year

            if detail['implemented'] <= year <= discon:
                # CONDITIONAL FOR RENAMING VARIABLES THAT CHANGED NAMES
                if detail['df_name']:
                    code = detail['df_name']
                if detail['lookup']:
                    cur_mapper[code] = detail['mappers'][year]

        return cur_mapper

    def mapping(self, group, mappers):
        yr = group.name
        cur_mappers = self.year_mapper(mappers, yr)
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

    def process_accidents(self, accidents):

        acc = (
            accidents
                .assign(TIME_OF_DAY=lambda x: ei.time_of_day(x),
                        DAY_OF_WEEK=lambda x: ei.day_of_week(x),
                        COLLISION_TYPE=lambda x: ei.collision_type(x),
                        TRAFFICWAY=lambda x: ei.trafficway(x),
                        FUNCTIONAL_CLASS=lambda x: ei.functional_class(x),
                        RURAL_OR_URBAN=lambda x: ei.land_use(x),
                        INTERSTATE=lambda x: ei.interstate(x),
                        FIPS=lambda x: ei.get_fips(x),
                        )
                .groupby(['YEAR'])
                .apply(self.mapping, mappers=self.mappers['Accident'])
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
                .remove_empty()
        )
        if 'latitude' in acc.columns:
            acc = (
                acc
                .coalesce(['LATITUDE', 'latitude'])
                .coalesce(['LONGITUD', 'longitud'])
            )

        return acc

    def fix_mod_year(self, df):
        df['MOD_YEAR'] = df['MOD_YEAR'].mask((df['MOD_YEAR'] < 99),
                                             df['MOD_YEAR'].add(1900))
        return df

    def process_vehicles(self, vehicles):
        vehicles.reset_index(drop=True, inplace=True)

        veh = (
            vehicles
                .update_where(
                conditions=(vehicles['YEAR'] <= 1997) & (vehicles['MOD_YEAR'] == 99),
                target_column_name='MOD_YEAR',
                target_val=9999)
                .then(self.fix_mod_year)
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
                .apply(self.mapping, mappers=self.mappers['Vehicle'])
                .droplevel(0)
                .remove_empty()
        )

        if ('VEH_SC1' in veh.columns) and ('VEH_CF1' in veh.columns):
            veh = (
                veh
                    .coalesce(['VEH_CF1', 'VEH_SC1'])
                    .coalesce(['VEH_CF2', 'VEH_SC2'])
                    .coalesce(['DR_CF1', 'DR_SF1'])
                    .coalesce(['DR_CF2', 'DR_SF2'])
                    .coalesce(['DR_CF3', 'DR_SF3'])
                    .coalesce(['DR_CF4', 'DR_SF4'])
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

        return veh.encode_categorical(list(set(['STATE', 'HIT_RUN', 'MAKE', 'BODY_TYP',
                                     'ROLLOVER', 'J_KNIFE', 'TOW_VEH', 'SPEC_USE', 'EMER_USE', 'IMPACT1',
                                     'IMPACT2', 'DEFORMED', 'IMPACTS', 'TOWED', 'FIRE_EXP', 'VEH_CF1',
                                     'VEH_CF2', 'M_HARM', 'WGTCD_TR', 'FUELCODE', 'DR_PRES', 'DR_DRINK',
                                     'L_STATE', 'L_STATUS', 'L_RESTRI', 'DR_TRAIN', 'VIOL_CHG',
                                     'DR_CF1', 'DR_CF2', 'DR_CF3', 'VINA_MOD', 'HAZ_CARG', 'VEH_MAN',
                                     'L_COMPL', 'VIN_BT']) & set(veh.columns)))

    def process_people(self, people):
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
                .apply(self.mapping, mappers=self.mappers['Person'])
                .droplevel(0)
        )
        if ('P_SF1' in per.columns) and ('P_CF1' in per.columns):
            per = (
                per
                .coalesce(['P_CF1', 'P_SF1'])
                .coalesce(['P_CF2', 'P_SF2'])
                .coalesce(['P_CF3', 'P_SF3'])
            )
        elif ('P_CF1' not in per.columns):
            per = (
                per
                .rename_columns({
                    'P_SF1': 'P_CF1',
                    'P_SF2': 'P_CF2',
                    'P_SF3': 'P_CF3',
                }))

        return createPerID(per, None)

    def get_renaming(self, map_with):
        """Get original to final column namings."""
        renamers = {}
        for code, attr in self.mappers[map_with].items():
            renamers[code] = attr['df_name']
        return renamers

    def load_accidents(self, year):
        accident_file = self.data_dir / f"{year}.unzip" / "ACCIDENT.csv"
        acc_cols = self.get_renaming('Accident')

        acc_df = pd.read_csv(accident_file, encoding='cp1252',
                             low_memory=False).rename(columns=acc_cols)

        return acc_df

    def load_vehicles(self, year):
        vehicle_file = self.data_dir / f"{year}.unzip" / "VEHICLE.csv"
        veh_cols = self.get_renaming('Vehicle')
        skip_cols = ['VE_FORMS', 'COUNTY', 'MONTH', 'DAY', 'HOUR', 'MINUTE',
                     'ROAD_FNC', 'HARM_EV', 'MAN_COLL', 'SCH_BUS']

        veh_df = pd.read_csv(vehicle_file,
                             # encoding='ansi',
                             encoding='cp1252',
                             low_memory=False,
                             usecols=lambda x: x not in skip_cols,
                             dtype={'DEATHS': 'float64',
                                    'OCUPANTS': 'float64'}
                             ).rename(columns=veh_cols)

        return veh_df

    def load_people(self, year):
        person_file = self.data_dir / f"{year}.unzip" / "PERSON.csv"
        per_cols = self.get_renaming('Person')
        skip_cols = ['VE_FORMS', 'COUNTY', 'MONTH', 'DAY', 'HOUR', 'MINUTE',
                     'ROAD_FNC', 'HARM_EV', 'MAN_COLL', 'SCH_BUS', 'SCH_BUS',
                     'MAKE', 'BODY_TYP', 'MOD_YEAR', 'ROLLOVER',
                     'TOW_VEH', 'SPEC_USE', 'EMER_USE', 'IMPACT1', 'IMPACT2',
                     'IMPACTS', 'FIRE_EXP', 'WGTCD_TR', 'MAK_MOD', 'VIN_WGT',
                     'WHLBS_SH', 'WHLBS_LG', 'MCYCL_DS', 'VINA_MOD', 'SER_TR',
                     'VIN_BT', 'CERT_NO', 'VINTYPE', 'VINMAKE', 'VINMODYR',
                     'VIN_LNGT', 'FUELCODE', 'CARBUR', 'CYLINDER', 'DISPLACE',
                     'MCYCL_CY', 'TIRE_SZE', 'TON_RAT', 'TRK_WT', 'TRKWTVAR',
                     'MCYCL_WT', 'VIN_REST', 'WHLDRWHL', 'RUR_URB', 'FUNC_SYS']

        per_df = pd.read_csv(person_file, encoding='cp1252',
                             usecols=lambda x: x not in skip_cols,
                             low_memory=False).rename(columns=per_cols)
        return per_df