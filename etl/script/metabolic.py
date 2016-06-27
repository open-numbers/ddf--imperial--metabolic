# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os
import xlrd

from ddf_utils.str import to_concept_id
from ddf_utils.index import create_index_file


# configuration of file path
source = '../source/sbp_bmi_tc_fpg_agestandardized_12911.xls'
out_dir = '../../'


def get_all_data(source):
    """read data from all tab and return a dict of dataframes"""
    all_data = {}

    xls = xlrd.open_workbook(source, on_demand=True)

    for i in xls.sheet_names():
        df = pd.read_excel(source, sheetname=i)
        df = df[['Country', 'Year', 'Female', 'Male']].drop(0)
        all_data[i] = df

    return all_data


def extract_concepts(data):
    all_concepts = [x for x in data.keys()]
    all_concept_ids = [to_concept_id(x) for x in all_concepts]

    concepts = pd.DataFrame([], columns=['concept', 'name', 'concept_type'])
    concepts['concept'] = ['name', 'year', 'sex', 'country', *all_concept_ids]
    concepts['name'] = ['Name', 'Year', 'Sex', 'Country', *all_concepts]

    concepts['concept_type'] = 'measure'
    concepts.iloc[0]['concept_type'] = 'string'
    concepts.iloc[1]['concept_type'] = 'time'
    concepts.iloc[2]['concept_type'] = 'entity_domain'
    concepts.iloc[3]['concept_type'] = 'entity_domain'

    return concepts


def extract_entities_country(data):
    all_country = []

    for df in data.values():
        country = df[['Country']].drop_duplicates().copy()
        country['Country'] = country['Country'].str.strip()
        all_country.append(country)

    country_df = pd.concat(all_country).drop_duplicates()
    country_df['country'] = country_df['Country'].map(to_concept_id)
    country_df.columns = ['name', 'country']

    return country_df


def extract_entities_sex():
    sex = pd.DataFrame([['male', 'Male'], ['female', 'Female']], columns=['sex', 'name'])

    return sex


def extract_datapoints(data):
    for k, df in data.items():
        df.columns = list(map(to_concept_id, df.columns))
        df['country'] = df['country'].map(to_concept_id)

        df = df.set_index(['country', 'year'])

        df = df.stack().reset_index()
        df['year'] = df['year'].map(int)

        df.columns = ['country', 'year', 'sex', to_concept_id(k)]

        yield to_concept_id(k), df


if __name__ == '__main__':
    print('reading source files...')
    data = get_all_data(source)

    print('creating concept files...')
    concepts = extract_concepts(data)
    path = os.path.join(out_dir, 'ddf--concepts.csv')
    concepts.to_csv(path, index=False)

    print('creating entities files...')
    country = extract_entities_country(data)
    path = os.path.join(out_dir, 'ddf--entities--country.csv')
    country.to_csv(path, index=False)

    sex = extract_entities_sex()
    path = os.path.join(out_dir, 'ddf--entities--sex.csv')
    sex.to_csv(path, index=False)

    print('creating datapoints files...')
    for k, df in extract_datapoints(data):
        path = os.path.join(out_dir, 'ddf--datapoints--{}--by--country--sex--year.csv'.format(k))
        df.to_csv(path, index=False)

    print('creating index file...')
    create_index_file(out_dir)

    print('Done.')
