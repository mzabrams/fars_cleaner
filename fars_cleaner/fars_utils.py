'''
Utility scripts for analyzing the FARS dataset
Mitchell Abrams, 2019

Includes functions and data structures which may be useful when exploring the FARS dataset. Provides dicts
mapping various FARS codes to human-readable values
across multiple years.

'''
import pandas as pd
import numpy as np
import requests





def get_vin_info(vin, s = requests.Session()):
    url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValuesExtended/'
    fields = {'format': 'json', 'data': vin}
    resp = s.post(url, data=fields)
    result = resp.json().get('Results')

    return {vin: dict(result)}

def standard_error(R, a, b, c, d, sigma_u = 0.05):
    return R*np.sqrt(sigma_u**2 + 1/a + 1/b + 1/c + 1/d)
    # We take sigma_u = intrinsic uncertainty, .05

def get_rr_contrib_grouped(df):
    soln = []
    for name, group in df:
        for drAge in group['AGE_S'].unique():
            cur = group[(group['AGE_S'] == drAge)]#.persist()
            A, B, C, D = get_rr_contrib(cur)

            if (A > 0) and (B>0) and (C > 0) and (D > 0):
                r1 = A/B
                r2 = C/D
                R = r1/r2
                serr = standard_error(R, A, B, C, D)
                soln.append(list(name) + [drAge, A, B, C, D, r1, r2, R, serr])
    return soln

def get_rr_contrib(df):
    a = df.loc[(df['SEX_S'] == 2) & (df['DEAD_S'])]#.persist()
    b = df.loc[(df['SEX_S'] == 2) & (df['DEAD_C'])]#.persist()
    c = df.loc[(df['SEX_S'] == 1) & (df['DEAD_S'])]#.persist()
    d = df.loc[(df['SEX_S'] == 1) & (df['DEAD_C'])]#.persist()


    A = a['PER_ID_S'].nunique()
    B = b['PER_ID_C'].nunique()
    C = c['PER_ID_S'].nunique()
    D = d['PER_ID_C'].nunique()
    return A, B, C, D



def createVehID(df, yr):
    x = df

    x['VEH_ID'] = x.apply((lambda a: int("{0}{1}{2:03}".format(str(yr)[-2:], int(a.ST_CASE), int(a.VEH_NO)))), axis=1, meta=(None, 'int64'))
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
            axis=1, meta=(None, 'int64'))
    return x

def createCaseID(df, yr):
    x = df
    x['ID'] = x.apply((lambda a: int("{0}{1}".format(str(yr)[-2:], int(a.ST_CASE)))), axis=1, meta=(None, 'int64'))
    return x.copy()


def getWeightedAvg(group, includeTotal = False, includeCounts=False):
    working = group
    working = working.assign(w = (working['R']/working['DeltaR'])**2)
    R_bar = np.exp(np.sum(working['w']*(np.log(working['R'])/working['w'].sum())))
    #delta_R_bar = np.std(working['R'])
    delta_R_bar = R_bar / np.sqrt(np.sum((working['R']/working['DeltaR'])**2))

    if includeTotal:
        ttl = (working['A'] + working['B'] + working['C'] + working['D'])
        return pd.Series({'R': R_bar, 'DeltaR': delta_R_bar, 'Total': ttl})
    elif includeCounts:
        return pd.Series({
            'A': working['A'].sum(),
            'B': working['B'].sum(),
            'C': working['C'].sum(),
            'D': working['D'].sum(),
            'R': R_bar,
            'DeltaR': delta_R_bar})
    else:
        return pd.Series({'R': R_bar, 'DeltaR': delta_R_bar})

def newWeightedAvg(group):
    working = group
    working['Total'] = working['A'] + working['B'] + working['C'] + working['D']
    R_bar = np.average(working['R'], weights=working['Total'])
    delta_R = np.std(working['R'])
    return pd.Series({'R': R_bar, 'DeltaR': delta_R})