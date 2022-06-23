# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 13:24:59 2020

@author: Mitchell Abrams

Consolidates additional processing as in Appendix D of the FARS Analytical
User Manual.
"""
import pandas as pd
import numpy as np

import janitor
from datetime import datetime


def get_fips(df):
    """Convert State/County codes to FIPS locations."""
    state = df['STATE'].apply(lambda x: str(x).zfill(2))
    county = df['COUNTY'].apply(lambda x: str(x).zfill(3))

    fips = state + county

    return fips.where(df['COUNTY'].between(1, 996))


def time_of_day(df):
    """Return daytime/nighttime using NHTSA convention."""
    hr = df['HOUR']
    conditions = [
        (hr.between(6, 17)),
        (hr.between(0, 5) | (hr.between(18, 24))),
        (hr == 99)]
    choices = ['Daytime', 'Nighttime', 'Unknown']
    return np.select(conditions, choices, default='ERROR')


def day_of_week(df):
    """Return weekday/weekend using NHTSA convention."""
    hr = df['HOUR']
    day = df['DAY_WEEK']

    conditions = [
        (((day == 2) & hr.between(6, 23)) | day.isin([3, 4, 5]) |
         ((day == 6) & (hr.between(0, 17) | (hr == 24)))),
        (((day == 6) & hr.between(18, 23)) | day.isin([1, 7]) |
         ((day == 2) & (hr.between(0, 5) | (hr == 24)))),
        ((day == 9) | ((hr == 99) & day.isin([2, 6])))]
    choices = ['Weekday', 'Weekend', 'Unknown']
    return np.select(conditions, choices, default='ERROR')


def collision_type(df):
    """Return manner of collision using NHTSA convention."""
    coll = df['MAN_COLL']
    yr = df['YEAR']

    conditions = [
         (coll == 0), (coll == 1), (coll == 2),
         (((coll == 4) & yr.between(1975, 2001)) |
          (coll.between(3, 6) & (yr.between(2002, 2009))) |
          ((coll == 6) & (yr >= 2010))),
         (((coll == 7) & yr.between(1975, 1977)) |
          (coll.isin([5, 6]) & yr.between(1978, 2001)) |
          (coll.isin([7, 8]) & (yr >= 2002))),
         (((coll == 3) & yr.between(1975, 2001)) |
          (coll.between(9, 11) & (yr >= 2002))),
         (((coll == 9) & yr.between(1975, 2001)) |
          (coll.isin([98, 99]) & (yr >= 2002)))]
    choices = ["Not Collision", "Rear-End", "Head-On", "Angle", "Sideswipe",
               "Other", "Unknown"]

    return np.select(conditions, choices, default='ERROR')


def trafficway(df):
    """Return relative position to trafficway using NHTSA convention."""
    road = df['REL_ROAD']
    yr = df['YEAR']

    conditions = [
        ((road == 1) | ((yr >= 2001) & (road == 11))),
        (road == 2),
        (road == 3),
        (road.isin([4, 5, 6, 7, 8, 10])),
        (road.isin([9, 98, 99]))]
    choices = ["On roadway", "Off roadway/shoulder", "Off roadway/median",
               "Off roadway/other", "Unknown"]

    return np.select(conditions, choices, default='ERROR')


def functional_class(df):
    """Return roadway function class using NHTSA convention."""
    yr = df['YEAR']
    # if before 1981, return series of pd.NA
    func = df['FUNCTION']

    conditions = [
        (func.isin([1, 11])),
        ((~(yr.between(1987, 2014)) & (func == 2)) |
         ((yr.between(1987, 2014) & (func == 12)))),
        ((~(yr.between(1987, 2014)) & (func == 3)) |
         (yr.between(1987, 2014) & (func.isin([2, 13])))),
        ((~(yr.between(1987, 2014)) & (func == 4)) |
         (yr.between(1987, 2014) & (func.isin([3, 14])))),
        ((yr.between(1981, 1986) & func.isin([5, 6, 7])) |
         (yr.between(1987, 2014) & func.isin([4, 5, 15])) |
         ((yr >= 2015) & func.isin([5, 6]))),
        ((yr.between(1981, 1986) & (func == 8)) |
         (yr.between(1987, 2014) & func.isin([6, 16])) |
         ((yr >= 2015) & (func == 7))),
        ((yr.between(1981, 1986) & (func == 9)) |
         (yr.between(1987, 2014) & func.isin([9, 19, 99])) |
         ((yr >= 2015) & func.isin([96, 98, 99])))]
    choices = ["Interstate, principal arterial",
               "Freeway and expressway, principal arterial",
               "Principal arterial, other",
               "Minor arterial",
               "Collector",
               "Local",
               "Unknown"]
    return np.select(conditions, choices, default=np.nan)


def land_use(df):
    """Return rural/urban land use using NHTSA convention."""
    yr = df['YEAR']

    func1 = df['LAND_USE']
    func2 = df['FUNC_SYS']
    if 'RUR_URB' in df.columns:
        func3 = df['RUR_URB']
    else:
        func3 = pd.Series(0, index=np.arange(len(func1)))

    conditions = [
        ((yr.between(1981, 1986) & (func1 == 2)) |
         (yr.between(1987, 2014) & ((func2 == 9) | func2.between(1, 6))) |
         ((yr >= 2015) & (func3 == 1))),
        ((yr.between(1981, 1986) & (func1 == 1)) |
         (yr.between(1987, 2014) & ((func2 == 19) | func2.between(11, 16))) |
         ((yr >= 2015) & (func3 == 2))),
        ((yr.between(1981, 1986) & (func1 == 9)) |
         (yr.between(1987, 2014) & (func2 == 99)) |
         ((yr >= 2015) & func3.isin([6, 8, 9])))]
    choices = ["Rural", "Urban", "Unknown"]
    return np.select(conditions, choices, default='ERROR')


def interstate(df):
    """Return whether road is an interstate using NHTSA convention."""
    yr = df['YEAR']
    func = df['ROUTE'].where(yr.between(1975, 1980)).add(
        df['ROAD_FNC'].where(yr.between(1981, 2014)), fill_value=0).add(
        df['FUNC_SYS'].where((yr >= 2015)), fill_value=0)

    conditions = [
        ((func == 1) | (func == 11)),
        (func.between(2, 8) | func.between(12, 16)),
        func.isin([9, 19, 96, 98, 99])]
    choices = ["Interstate", "Non Interstate", "Unknown"]
    return np.select(conditions, choices, default='ERROR')


def is_passenger_car(df):
    """Return if vehicle is a passenger car, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']

    return ((yr.between(1975, 1981) & body.between(1, 9)) |
            (yr.between(1982, 1990) & (body.between(1, 11) | (body == 67))) |
            ((1991 <= yr) & (body.between(1, 11) | (body == 17))))


def is_light_truck_or_van(df):
    """Return if vehicle is a light truck or van, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']
    tow_veh = df['TOW_VEH']

    return ((yr.between(1975, 1981) & (body.isin([43, 50, 51, 52]) |
                                       ((body == 60) & (tow_veh == 0)))) |
            (yr.between(1982, 1990) & (body.isin([12, 40, 41, 48, 49, 50, 51,
                                                  53, 54, 55, 56, 58, 59, 68,
                                                  69]) |
                                       ((body == 79) &
                                        (tow_veh.isin([0, 9]))))) |
            ((1991 <= yr) & (body.isin([14, 15, 16, 19, 20, 21, 22, 24, 25]) |
                             body.between(28, 41) | body.between(45, 49) |
                             ((body == 79) & (tow_veh.isin([0, 9]))))))


def is_large_truck(df):
    """Return if vehicle is a large truck, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']
    tow_veh = df['TOW_VEH']
    return ((yr.between(1975, 1981) & (body.between(53, 59) |
                                       ((body == 60) & (tow_veh == 1)))) |
            (yr.between(1982, 1990) & (body.isin([70, 71, 72, 74,
                                                  75, 76, 78]) |
                                       ((body == 79) &
                                        tow_veh.between(1, 5)))) |
            ((1991 <= yr) & (body.isin([60, 61, 62, 63, 64,
                                        66, 67, 71, 72, 78]) |
                             ((body == 79) & tow_veh.between(1, 4)))))


def is_motorcycle(df):
    """Return if vehicle is a motorcycle, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']

    return ((yr.between(1975, 1981) & body.between(15, 18)) |
            (yr.between(1982, 1990) & body.between(20, 29)) |
            ((1991 <= yr) & body.between(80, 89)))


def is_bus(df):
    """Return if vehicle is a bus, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']

    return ((yr.between(1975, 1981) & body.between(25, 29)) |
            (yr.between(1982, 1990) & body.between(30, 39)) |
            ((1991 <= yr) & body.between(50, 59)))


def is_other_or_unknown(df):
    """Return if vehicle is other/unknown, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']
    tow_veh = df['TOW_VEH']

    return ((yr.between(1975, 1981) & (body.between(35, 42) |
                                       body.isin([44, 45, 99]))) |
            (yr.between(1982, 1990) & (body.isin([13, 14, 42, 52, 73, 77,
                                                  80, 81, 82, 83, 88, 89,
                                                  90, 99]))) |
            ((1991 <= yr) & (body.isin([12, 13, 23, 42, 65, 73, 90,
                                        91, 92, 93, 94, 95, 96, 97,
                                        99, 98]) |
                             ((body == 79) & tow_veh.isin([5, 6])))))


def is_passenger_vehicle(df):
    """Return if vehicle is a passenger vehicle, per NHTSA convention."""
    return is_passenger_car(df) & is_light_truck_or_van(df)


def is_utility_vehicle(df):
    """Return if vehicle is a utility vehicle, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']

    return ((yr.between(1975, 1981) & (body == 43)) |
            (yr.between(1982, 1990) & body.isin([12, 56, 68])) |
            ((1991 <= yr) & body.isin([14, 15, 16, 19])))


def is_pickup(df):
    """Return if vehicle is a pickup truck, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']

    return ((yr.between(1975, 1981) & (body == 50)) |
            (yr.between(1982, 1990) & body.isin([50, 51])) |
            ((1991 <= yr) & body.between(30, 39)))


def is_van(df):
    """Return if vehicle is a van, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']

    return ((yr.between(1975, 1981) & (body == 51)) |
            (yr.between(1982, 1990) & body.isin([40, 41, 48, 49])) |
            ((1991 <= yr) & body.isin([20, 21, 22, 24, 25, 28, 29])))


def is_medium_truck(df):
    """Return if vehicle is a medium truck, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']

    return ((yr.between(1975, 1981) & body.isin([53, 54, 56])) |
            (yr.between(1982, 1990) & body.isin([70, 71, 75, 78])) |
            ((1991 <= yr) & body.isin([60, 61, 62, 64, 67, 71])))


def is_heavy_truck(df):
    """Return if vehicle is a heavy truck, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']
    tow_veh = df['TOW_VEH']

    return ((yr.between(1975, 1981) & (body.isin([55, 57, 58, 59]) |
                                       ((body == 60) & (tow_veh == 1)))) |
            (yr.between(1982, 1990) & (body.isin([72, 74, 76]) |
                                       ((body == 79) &
                                        tow_veh.between(1, 5)))) |
            ((1991 <= yr) & (body.isin([63, 66, 72, 78]) |
                             ((body == 79) & tow_veh.between(1, 4)))))


def is_combination_truck(df):
    """Return if vehicle is a combination truck, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']
    tow_veh = df['TOW_VEH']

    return ((yr.between(1975, 1981) & (body.isin([57, 58, 59]) |
                                       (body.isin([53, 54, 55, 56, 60]) &
                                        (tow_veh == 1)))) |
            (yr.between(1982, 1990) & ((body == 74) |
                                        (body.isin([70, 71, 72, 75,
                                                    76, 78, 79]) &
                                         tow_veh.between(1, 5)))) |
            ((1991 <= yr) & ((body == 66) |
                             (body.isin([60, 61, 62, 63, 64,
                                         71, 72, 78, 79]) &
                             tow_veh.between(1, 4)))))


def is_single_unit_truck(df):
    """Return if vehicle is a single unit truck, per NHTSA convention."""
    yr = df['YEAR']
    body = df['BODY_TYP']
    tow_veh = df['TOW_VEH']

    return ((yr.between(1975, 1981) & body.isin([53, 54, 55, 56, 60]) &
             (tow_veh == 0)) |
            (yr.between(1982, 1990) & body.isin([70, 71, 72, 75, 76, 78, 79]) &
             tow_veh.isin([0, 9])) |
            ((1991 <= yr) & body.isin([60, 61, 62, 63, 64,
                                       67, 71, 72, 78, 79]) &
             tow_veh.isin([0, 5, 6, 9])))


def impact_area(df, i_type):
    """Return impact area classification, per NHTSA convention."""
    yr = df['YEAR']
    if i_type == 1:
        impact = df['IMPACT1']
    else:
        impact = df['IMPACT2']

    conditions = [
        (impact == 0),
        impact.isin([1, 11, 12]),
        impact.isin([2, 3, 4, 81, 82, 83]),
        impact.isin([8, 9, 10, 61, 62, 63]),
        impact.isin([5, 6, 7]),
        impact.isin([13, 14, 15, 16, 18, 19, 20]),
        impact.isin([98, 99])]
    choices = ['Non-Collision', 'Front', 'Right Side', 'Left Side',
               'Rear', 'Other', 'Unknown']
    return np.select(conditions, choices, default='ERROR')


def license_status(df):
    """Return license status, per NHTSA convention."""
    yr = df['YEAR']
    l_status = df['L_STATUS']
    conditions = [
        ((yr.between(1975, 1981) & l_status.isin([0, 3, 7])) |
         (yr.between(1982, 1986) & l_status.isin([0, 2, 7, 8])) |
         (yr.between(1987, 1992) & l_status.isin([5, 6, 7, 8])) |
         (yr.between(1993, 2003) & l_status.isin([6, 7, 8])) |
         ((2004 <= yr) & (l_status == 6))),
        ((yr.between(1975, 1981) & l_status.isin([1, 2, 4, 5, 6])) |
         (yr.between(1982, 1986) & l_status.isin([1, 3, 4, 5, 6])) |
         ((1987 <= yr) & l_status.isin([0, 1, 2, 3, 4]))),
        ((yr.between(1975, 2010) & (l_status == 9)) |
         ((2011 <= yr) & l_status.isin([7, 9])))]
    choices = ["Valid", "Invalid", "Unknown"]
    return np.select(conditions, choices, default='ERROR')


def license_compliance(df):
    """Return license compliance, per NHTSA convention."""
    yr = df['YEAR']
    if 1981 <= yr <= 1986:
        compl = df['L_CL_VEH']
    elif 1987 <= yr:
        compl = df['L_COMPL']

    conditions = [
        ((yr.between(1982, 1986) & compl.isin([0, 2, 4])) |
         ((1987 <= yr) & compl.isin([1, 3]))),
        ((yr.between(1982, 1986) & compl.isin([1, 3, 5])) |
         ((1987 <= yr) & compl.isin([0, 2]))),
        ((yr.between(1982, 1986) & compl == 9) |
         ((1987 <= yr) & compl.isin([9, 6, 7, 8])))]
    choices = ["Valid", "Invalid", "Unknown"]
    return np.select(conditions, choices, default='ERROR')


def person_type(df):
    """Return person classification, per NHTSA convention."""
    yr = df['YEAR']
    per_typ = df['PER_TYP']

    conditions = [
        (per_typ == 1),
        (per_typ.isin([2, 9])),
        ((yr.between(1975, 1981) & per_typ == 5) |
         ((yr <= 1982) & per_typ.isin([3, 4]))),
        ((yr.between(1975, 1981) & (per_typ == 3)) |
         ((yr <= 1982) & (per_typ == 5))),
        ((yr.between(1975, 1981) & (per_typ == 4)) |
         ((yr <= 1982) & per_typ.isin([6, 7]))),
        (per_typ.isin([8, 10, 19])),
        (per_typ.isin([99, 88]))]
    choices = ["Driver", "Passenger",  "Other non-occupant", "Pedestrian",
               "Pedalcyclist", "Other/Unknown non-occupant",
               "Unknown person type"]
    return np.select(conditions, choices, default='ERROR')


def restraint_use(df):
    """Return restraint usage, per NHTSA convention."""
    yr = df['YEAR']
    rest = df['REST_USE']

    conditions = [
        rest.isin([0, 5, 6, 7, 15, 16, 17, 19, 20]),
        rest.isin([1, 2, 3, 4, 8, 10, 11, 12, 97]),
        rest.isin([13, 14]),
        rest.isin([9, 29, 98, 99])]
    choices = ["Not Used", "Used", "Used Improperly", "Unknown"]
    return np.select(conditions, choices, default='ERROR')


def helmet_use(df):
    """Return helmet usage, per NHTSA convention."""
    yr = df['YEAR']
    helmet = df['REST_USE']
    if 'REST_MIS' in df.columns:
        rest_mis = df['REST_MIS']
    else:
        rest_mis = pd.Series(pd.NA, index=df.index)

    conditions = [
        ((helmet.isin([0, 1, 2, 3, 4, 6, 10, 11, 12, 13, 14, 15, 17])) |
         ((rest_mis == 1) & (helmet.isin([5, 16, 8, 19, 97])))),
        (((yr <= 2009) & (helmet.isin([5, 8]))) |
         ((yr >= 2010) & (rest_mis == 0) & (helmet.isin([5, 16, 19, 8, 97])))),
        (helmet.isin([9, 29, 98, 99]))]
    choices = ["Not Helmeted", "Helmeted", "Unknown"]
    return np.select(conditions, choices, default='ERROR')

def air_bag_deployed(df):
    """Return air bag deployment as True/False, or NA if unknown"""
    yr = df['YEAR']
    if 'AUT_REST' in df.columns:
        air_bag = df['AUT_REST'].where(yr.between(1975, 1990))
    else:
        air_bag = pd.Series()
    if 'AIR_BAG' in df.columns:
        air_bag = air_bag.add(df['AIR_BAG'].where((yr >= 1991)), fill_value=0)

    conditions = [
        (((yr <= 1997) & (air_bag == 3)) |
         ((yr >= 1998) & (air_bag.isin([1, 2, 3, 7, 8, 9])))),
        (((yr <= 1997) & (air_bag == 4)) |
         ((yr >= 1998) & (air_bag.isin([20, 28, 30, 31, 32])))),
        (((yr <= 1997) & (air_bag == 0)) |
         ((yr >= 1998) & (air_bag.isin([0, 97])))),
        (((yr <= 1997) & (air_bag == 9)) |
         ((yr >= 1998) & (air_bag.isin([99, 98]))))]
    choices = ["Deployed", "Not Deployed", "Non-Motorist", "Unknown"]
    return np.select(conditions, choices, default=np.nan)
