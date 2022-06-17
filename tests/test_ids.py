import fars_cleaner.fars_utils as futil
import pandas as pd

from hypothesis import given
from hypothesis import strategies as st
from hypothesis.extra.pandas import column, data_frames


@given(year=st.integers(min_value=1975, max_value=2019),
       vehicle_numbers=st.integers(min_value=0,max_value=99),
       st_case=st.integers(min_value=10000, max_value=569999))
def test_vehicle_id(year, vehicle_numbers, st_case):
    output = int("{0}{1}{2:03}".format(str(year)[-2:], int(st_case), int(vehicle_numbers)))
    df_in = pd.DataFrame({'ST_CASE': [st_case], 'VEH_NO': [vehicle_numbers]})
    dummy = futil.createVehID(df_in, year)['VEH_ID'][0]
    assert dummy == output


@given(year=st.integers(min_value=1975, max_value=2019),
       vehicle_numbers=st.integers(min_value=0,max_value=99),
       person_numbers=st.integers(min_value=0,max_value=99),
       st_case=st.integers(min_value=10000, max_value=569999))
def test_person_id(year, vehicle_numbers, person_numbers, st_case):
    output = int("{0}{1}{2:03}{3:03}".format(str(year)[-2:], int(st_case), int(vehicle_numbers), int(person_numbers)))
    df_in = pd.DataFrame({'ST_CASE': [st_case],
                          'VEH_NO': [vehicle_numbers],
                          'PER_NO': [person_numbers],
                          'YEAR': [year]})
    dummy = futil.createPerID(df_in, year)['PER_ID'][0]
    dummy2 = futil.createPerID(df_in, None)['PER_ID'][0]
    assert dummy == output
    assert dummy2 == output


@given(year=st.integers(min_value=1975, max_value=2019),
       st_case=st.integers(min_value=10000, max_value=569999))
def test_case_id(year, st_case):
    output = int("{0}{1}".format(str(year)[-2:], int(st_case)))
    df_in = pd.DataFrame({'ST_CASE': [st_case],
                          'YEAR': [year]})
    dummy = futil.createCaseID(df_in, year)['ID'][0]
    assert dummy == output