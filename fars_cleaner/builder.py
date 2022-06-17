# -*- coding: utf-8 -*-
"""
Created on Mon Jan 27 10:07:15 2020

@author: Mitchell Abrams

"""
import datetime

import pandas as pd
#import extra_info as ei

from pathlib import Path
import pickle

from time import sleep

NOW = datetime.datetime.now()

def get_renaming(mappers, year):
    """Get original to final column namings."""
    renamers = {}
    for code, attr in mappers.items():
        renamers[code] = attr['df_name']
    return renamers


def year_mapper(mappers, year):
    # Take mapper for the data file
    # Iterate through: for each sub-dictionary that was implemented before
    # and discontinued after this year
    cur_mapper = {code: {} for code in mappers.keys()}
    for code, detail in mappers.items():
        discon = detail['discontinued']
        if not discon:
            discon = NOW.year

        if detail['implemented'] <= year <= discon:
            # CONDITIONAL FOR RENAMING VARIABLES THAT CHANGED NAMES
            if detail['df_name']:
                code = detail['df_name']
            if detail['lookup']:
                cur_mapper[code] = detail['mappers'][year]

    return cur_mapper


def load_sheets(t_list=None,
                table_folder=Path(__file__).parent.resolve() / "lookup_tables"):
    """Load mapping for given sheets.

    Loads a lsit of .xlsx files from disk as named in `t_list`. Generates a
    nested dict through which individual codes can be accessed and decoded to
    a human-readable format.

    Parameters
    ----------
    table_folder: Path-like
        Path to folder with lookup tables. Defaults to "lookup_tables"
        folder in current directory.
    t_list : list, optional
        List of code sheets to load and map. The default is:
            ['Accident', 'Vehicle', ' Person', 'Vehnit'].

    Returns
    -------
    mappers : multi-level nested dict
        `mappers` is a nested dictionary. Level hierarachy is:
            File_Name:
                Field_Name:
                    `description`: str
                        Short description of field.
                    `discontinued`: float64 or bool
                        Last year variable appears in data set.
                        False if not yet discontinued.
                    `implemented`: float64
                        First year variable appears in data set
                    `mappers`: dict
                        YEAR: dict (keys are int)
                            key:value pairs by year
                    `df_name`: str
                        Variable name to use in dataframe

    See Also
    --------
    lookup: Generate dictionary mapping for single code in a single year.

    """
    if t_list is None:
        t_list = ['Accident', 'Vehicle', 'Person', 'Vehnit']

    mappers = {}

    template = {'description': None,
                'rename': False,
                'implemented': 1975,
                'discontinued': False,
                'dtype': None,
                'lookup': False,
                'mappers': None,}
    #table_folder = Path(__file__).parent.resolve()
    for table in t_list:

        target_table = table_folder / f"{table}.xlsx"

        df = pd.read_excel(target_table, sheet_name=None)

        df['Summary']['Year_Discontinued'].fillna(int(NOW.year), inplace=True)
        df['Summary']['Years_Skipped'].fillna(-1, inplace=True)
        df['Summary'].set_index('Code', inplace=True)

        mapper = {code: template.copy()
                  for code in df['Summary'].index.values}

        summary = df['Summary']

        for code in mapper.keys():

            impl = summary.at[code, 'Year_Implemented']
            discon = summary.at[code, 'Year_Discontinued']
            df_name = summary.at[code, 'Rename']

            mapper[code] = {'description': summary.at[code, 'Description'],
                            'use_name': summary.at[code, 'Use_Sheet'],
                            'implemented': impl,
                            'discontinued': discon if discon != 2020 else False,
                            'lookup': bool(summary.at[code, 'Lookup']),
                            'df_name': df_name if not pd.isna(df_name) else code
                            }
            lookup_me = mapper[code]['lookup']

            if lookup_me:
                mapper[code]['mappers'] = {yr: {} for
                                           yr in range(impl, int(discon)+1)}
                if not pd.isna(summary.at[code, 'Coded_In']):
                    secondary_target = table_folder / "{}.xlsx".format(
                        summary.at[code, 'Coded_In'])
                    target_sheet = pd.read_excel(
                        secondary_target,
                        summary.at[code, 'Use_Sheet'])
                else:
                    target_sheet = df[mapper[code]['use_name']]
                #print(code)
                for year in range(impl, int(discon)+1):
                    if impl <= year <= discon:# and discon >= year:
                        mapper[code]['mappers'][year] = dict(
                            lookup(target_sheet, year))
            else:
                mapper[code]['mappers'] = None

        mappers[table] = mapper.copy()

    return mappers


def lookup(code_table, year):
    """Generate a dictionary mappping for the key-value pairing for one code.

    Filters a dataframe to contain only code mappings valid during the
    specified `year`. Returns the dictionary representation of these code
    mappings (code:decoded_value format). Typically, encoded values are int
    and decoded values are str.

    Parameters
    ----------
    code_table : pd.DataFrame
        Dataframe with all key-value mappings for all years for a single code.
    year : int
        The year to pull mappings for.

    Returns
    -------
    dict
        Returns a dictionary with the key:value mapping for `year`.

    See Also
    --------
    load_sheets: loads the data sheets and passes to this for mapping
    """
    cur_lookup = code_table.fillna({'Year_Implemented': 1975,
                                    'Year_Discontinued': 2020})
    cur_lookup = cur_lookup.query("Year_Implemented <= {year} and "
                                  "Year_Discontinued >= {year}".format(year=year))
    return pd.Series(cur_lookup['DEF'].values, index=cur_lookup['ID'].values).to_dict()


if __name__ == '__main__':

    m = load_sheets()
    # for year in range(1975, 2019):
    #     year_mapper(m['Accident'], year)
    #     year_mapper(m['Person'], year)
    #     year_mapper(m['Vehicle'], year)
    #     year_mapper(m['Vehnit'], year)

