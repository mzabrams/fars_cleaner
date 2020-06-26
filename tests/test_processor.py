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

@given(
    data_frames(
    columns=[
        column(
            name='STATE',
            elements=st.integers(min_value=0, max_value=56),
            unique=False),
        column(
            name='COUNTY',
            elements=st.integers(min_value=0, max_value=999),
            unique=False),
    ])
)
def test_fips(data):
    dummies = ei.get_fips(data)
    for (d, (ind, o)) in zip(dummies, data.iterrows()):
        if 1 <= o['COUNTY'] <= 996:
            assert d == str(o['STATE']).zfill(2) + str(o['COUNTY']).zfill(3)
        else:
            assert pd.isna(d)


@given(st.data())
def test_weekday(data):
    # Weekday, day=3,4,5
    data1 = data.draw(data_frames(
        columns=[
            column(
                name='HOUR',
                elements=st.one_of(st.integers(min_value=0, max_value=24),
                                   st.just(99)),
                unique=False),
            column(
                name='DAY_WEEK',
                elements=st.integers(min_value=3, max_value=5),
                unique=False)
        ]))
    # Weekday, day=6, hr=0-17, 24
    data2 = data.draw(data_frames(
        columns=[
            column(
                name='HOUR',
                elements=st.one_of(st.integers(min_value=0, max_value=17),
                                   st.just(24)),
                unique=False),
            column(
                name='DAY_WEEK',
                elements=st.just(6),
                unique=False)
        ]))
    # Weekday, day=2, hr=6-23
    data3 = data.draw(data_frames(
        columns=[
            column(
                name='HOUR',
                elements=st.integers(min_value=6, max_value=23),
                unique=False),
            column(
                name='DAY_WEEK',
                elements=st.just(2),
                unique=False)
        ]))
    for data in [data1, data2, data3]:
        for d in ei.day_of_week(data):
            assert d == 'Weekday'

@given(st.data())
def test_weekend(data):
    # Weekend, day=1,7
    data1 = data.draw(data_frames(
        columns=[
            column(
                name='HOUR',
                elements=st.one_of(st.integers(min_value=0, max_value=24),
                                   st.just(99)),
                unique=False),
            column(
                name='DAY_WEEK',
                elements=st.one_of(st.just(1), st.just(7)),
                unique=False)
        ]))
    # Weekend, day=6, hr=18-23
    data2 = data.draw(data_frames(
        columns=[
            column(
                name='HOUR',
                elements=st.integers(min_value=18, max_value=23),
                unique=False),
            column(
                name='DAY_WEEK',
                elements=st.just(6),
                unique=False)
        ]))
    # Weekday, day=2, hr=0-5, 24
    data3 = data.draw(data_frames(
        columns=[
            column(
                name='HOUR',
                elements=st.one_of(
                    st.integers(min_value=0, max_value=5),
                    st.just(24)),
                unique=False),
            column(
                name='DAY_WEEK',
                elements=st.just(2),
                unique=False)
        ]))
    for data in [data1, data2, data3]:
        for d in ei.day_of_week(data):
            assert d == 'Weekend'

@given(st.data())
def test_dayofweek_unknown(data):
    data1 = data.draw(data_frames(
        columns=[
            column(
                name='HOUR',
                elements=st.integers(min_value=0, max_value=24),
                unique=False),
            column(
                name='DAY_WEEK',
                elements=st.just(9),
                unique=False)
        ]))
    data2 = data.draw(data_frames(
        columns=[
            column(
                name='HOUR',
                elements=st.just(99),
                unique=False),
            column(
                name='DAY_WEEK',
                elements=st.one_of(st.just(2), st.just(6)),
                unique=False)
        ]))
    for data in [data1, data2]:
        for d in ei.day_of_week(data):
            assert d == 'Unknown'

def test_