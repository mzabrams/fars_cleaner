from fars_cleaner import FARSProcessor
import fars_cleaner.extra_info as ei
import pandas as pd

from hypothesis import given
from hypothesis import strategies as st
from hypothesis.extra.pandas import column, data_frames

@given(
    data_frames(
    columns=[
        column(
            name='HOUR',
            elements=st.one_of(
                st.integers(min_value=0, max_value=5),
                st.integers(min_value=18, max_value=24),
            )
        , unique=True),
    ])
)
def test_nighttime(data):
    tod_dummies = ei.time_of_day(data)
    for d in tod_dummies:
        assert d == 'Nighttime'

@given(
    data_frames(
    columns=[
        column(
            name='HOUR',
            elements=st.integers(min_value=6, max_value=17),
            unique=True),
    ])
)
def test_daytime(data):
    tod_dummies = ei.time_of_day(data)
    for d in tod_dummies:
        assert d == 'Daytime'

@given(
    data_frames(
    columns=[
        column(
            name='HOUR',
            elements=st.just(99),
            unique=True),
    ])
)
def test_tod_unknown(data):
    tod_dummies = ei.time_of_day(data)
    for d in tod_dummies:
        assert d == 'Unknown'
