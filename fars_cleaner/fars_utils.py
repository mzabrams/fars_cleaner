'''
Utility scripts for analyzing the FARS dataset
Mitchell Abrams, 2019

Includes functions and data structures which may be useful when exploring the FARS dataset. Provides dicts
mapping various FARS codes to human-readable values
across multiple years.

'''
import pandas as pd
import numpy as np
import requests, json


def get_vin_info(vin, s = requests.Session()):
    url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValuesExtended/'
    fields = {'format': 'json', 'data': vin}
    resp = s.post(url, data=fields)
    result = resp.json().get('Results')
    return {vin: dict(result)}


def standard_error(R, a, b, c, d, sigma_u = 0.05):
    return R*np.sqrt(sigma_u**2 + 1/a + 1/b + 1/c + 1/d)
    # We take sigma_u = intrinsic uncertainty, .05


def createVehID(df, yr):
    x = df

    x['VEH_ID'] = x.apply((lambda a: int("{0}{1}{2:03}".format(str(yr)[-2:], int(a.ST_CASE), int(a.VEH_NO)))), axis=1)
    return x.copy()


def createPerID(df, yr):
    x = df.copy()
    if yr is None:
        x['PER_ID'] = x.apply(
            (lambda a: int("{0}{1}{2:03}{3:03}".format(str(a.YEAR)[-2:], int(a.ST_CASE), int(a.VEH_NO), int(a.PER_NO)))),
            axis=1)
    else:
        x['PER_ID'] = x.apply(
            (lambda a: int("{0}{1}{2:03}{3:03}".format(str(yr)[-2:], int(a.ST_CASE), int(a.VEH_NO), int(a.PER_NO)))),
            axis=1)
    return x


def createCaseID(df, yr):
    x = df
    x['ID'] = x.apply((lambda a: int("{0}{1}".format(str(yr)[-2:], int(a.ST_CASE)))), axis=1)#, meta=(None, 'int64'))
    return x.copy()
