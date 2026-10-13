from datetime import datetime
import json
import os
import requests
from typing import Tuple

import pandas as pd
import numpy as np

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models import Param

import clickhouse_connect as cc

# =====================================
# Import credentials
# =====================================
from dotenv import load_dotenv
load_dotenv()

token = os.environ.get('iucn_token')
url = os.environ.get('iucn_api_url')

host = os.environ.get('clickhouse_server')
username = os.environ.get('clickhouse_username')
password = os.environ.get('clickhouse_password')
port = os.environ.get('clickhouse_port')

# logging
import logging
logger = logging.getLogger(__name__)

def parse_levels(reference: json) -> dict:
    """
    Creates nested dictionary with three-level hierarchy.
    >>> data = [
        {'description': {'en': 'Agriculture & aquaculture'}, 'code': '2'},
        {'description': {'en': 'Annual & perennial non-timber crops'}, 'code': '2_1'},
        {'description': {'en': 'Shifting agriculture'}, 'code': '2_1_1'}
    ]
    >>> parse_levels(data)

    {
    'lvl1': {
        '2': {
            'name': 'Agriculture & aquaculture',
            'lvl2': {
                '1': {
                    'name': 'Annual & perennial non-timber crops',
                    'lvl3': {
                        '1': {'name': 'Shifting agriculture'},
                        '2': {'name': 'Small-holder farming'}
                        }
                    }
                }
            },
        '3': {
            'name': 'Energy production & mining',
            'lvl2': {
                '1': {'name': 'Oil & gas drilling', 'lvl3': {}},
                '2': {'name': 'Mining & quarrying', 'lvl3': {}},
                '3': {'name': 'Renewable energy', 'lvl3': {}}
                }
            }
        }
    }
    """
    levels = {'lvl1': {}}
    # Collect only first-level values
    for r in reference:
        parts = r['code'].split('_')
        if len(parts) == 1:
            levels['lvl1'][parts[0]] = {'name': r['description']['en'], 'lvl2': {}}
    
    # Collect only second-level values
    for r in reference:
        parts = r['code'].split('_')
        if len(parts) == 2:
            lvl1_key = parts[0]
            levels['lvl1'][lvl1_key]['lvl2'][parts[1]] = {'name': r['description']['en'], 'lvl3': {}}
    
    # Collect only third-level values
    for r in reference:
        parts = r['code'].split('_')
        if len(parts) == 3:
            lvl1_key = parts[0]
            lvl2_key = parts[1]
            levels['lvl1'][lvl1_key]['lvl2'][lvl2_key]['lvl3'][parts[2]] = {'name': r['description']['en']}
    return levels

def decode_enum(code: str, levels: dict) -> tuple:
    """
    Expand two- and three-level values.

    >>> decode_enum(threats_levels, '7_2_11')
    ('Natural system modifications', 'Dams & water management/use', 'Dams (size unknown)')
    """
    parts = code.split('_')
    lvl1 = levels['lvl1'][parts[0]]['name']

    # if value consists of two parts
    if len(parts)>=2:
        lvl2 = levels['lvl1'][parts[0]]['lvl2'][parts[1]]['name']
    else:
        lvl2 = None
        
    # if value consists of three parts
    if len(parts)==3:
        lvl3 = levels['lvl1'][parts[0]]['lvl2'][parts[1]]['lvl3'][parts[2]]['name']
    else:
        lvl3 = None
    return (lvl1, lvl2, lvl3)

@task
def check__data(data: json, json_key: str, columns: list) -> bool:
    """
    Check if the file structure contains all necessary columns and is not empty
    """
    condition = \
        json_key in data and \
        len(data[json_key]) > 0 and \
        all([k in data[json_key][0].keys() for k in columns])
    if not condition:
        logger.error('Invalid file structure for %s!', json_key)
        raise AirflowSkipException

dag_doc = """
# IUCN reference data: API to Clickhouse
DAG for extracting reference data on:
- Red List categories
- threats
- stresses
- habitats

## Red List categories
Categories are simply extracted and loaded to database.

## Threats and Stresses
Threats and stresses are extracted and parsed by levels so that all second-level threats/stresses are assigned to their correspondding first level threats/stresses, same with third-level threats/stresses.

# Habitats 
# Habitats are assigned levels and mapped to the three systems with a dictionary created manually.
"""

# =============================================
# Red List categories
# =============================================
dag_doc = """
# IUCN reference data on Red List categories: API to Clickhouse
DAG extracts data on Red List categories from endpoint https://api.iucnredlist.org/api/v4/red_list_categories.
Transformation includes assigning a rank to each category of versions 2.3 and 3.1. 
"""
@dag(
    dag_id="iucn_load_red_list_categories_mapping",
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
    params={
        "table_name": Param(
            default='red_list_categories_mapping', 
            type="string", 
            title="Table name",
            description="Name of the table in database",
        ),
    },
    render_template_as_native_obj=True,
    schedule=None, # only manual launch
    start_date=datetime(2026, 10, 1, 23, 0),
    catchup=False,
    tags=['IUCN', 'API', 'ClickHouse', 'references', 'replace'],
    doc_md=dag_doc
)
def iucn_load_red_list_categories_mapping():
    @task
    def extract__red_list_categories(token: str, url: str, params=None) -> json: 
        """
        Get dictionary of Red List categories
        """
        res = requests.get(
                url + 'red_list_categories/',
                headers={
                    'Authorization': token, 
                    'accept':'*/*'
                }
        )
        res.raise_for_status()
        logger.info('Obtained data for %s', params['table_name'])
        return res.json()

    @task
    def transform__red_list_categories(data: json, params=None) -> pd.DataFrame:
        """
        Parse JSON, convert to dataframe and add column where rank is added to the full category name (column description).
        """
        mapping = {
            'NE': '0. ',
            'NA': '0. ',
            'DD': '0. ',
            'LC': '1. ',
            'NT': '2. ',
            'VU': '3. ',
            'EN': '4. ',
            'CR': '5. ',
            'RE': '6. ',
            'EW': '7. ',
            'EX': '8. '
        }
        def assign_order(row: pd.Series) -> str:
            """
            Adds number in front of full description of the category.
            """
            if row['version'] == '3.1' and mapping.get(row['code']):
                return mapping[row['code']] + row['description']

        data = data['red_list_categories']
        df = pd.DataFrame.from_dict(data)
        # extract value from nested dict
        df['description'] = df['description'].apply(lambda x: x['en'])
        # apply mapping
        df['category_sorted'] = df.apply(assign_order, axis=1)
        logger.info('Transformed data for %s', params['table_name'])
        return df

    @task
    def load__red_list_categories(df: pd.DataFrame, host: str, port: str, username: str, password: str, params=None) -> None:
        """
        Load dataframe to database with replacement. Create table beforehand if id does not exist.
        """
        table_name = params['table_name']
        if len(df) == 0:
            raise AirflowFailException("Empty dataframe! Aborting...")

        # logger.info('Trying to connect to database: %s:%s', host, port)
        with cc.get_client(host=host, port=port, username=username, password=password) as client:
            # truncate and create if necessary
            client.command(f'TRUNCATE TABLE IF EXISTS dict.{table_name}')
            ddl = f"""CREATE TABLE IF NOT EXISTS dict.{table_name} (
                version         Nullable(String),
                description     String,
                code            String,
                category_sorted Nullable(String),
                `CREATE_DTTM`   DateTime default now()
            )"""
            client.command(ddl)

            # load
            query_result = client.insert_df(
                table=table_name,
                df=df,
                column_names=list(df.columns),
            )
        logger.info('Table %s: %s', table_name, query_result.summary)

    # sequence
    json_ = extract__red_list_categories(token, url)

    check_ = check__data.override(
        task_id="check__red_list_categories",
    )(
        data=json_, 
        json_key='red_list_categories', 
        columns=['code', 'description', 'version']
    )
    df_ = transform__red_list_categories(data=json_)
    check_ >> df_

    load__red_list_categories(df=df_, host=host, port=port, username=username, password=password)
    
    
# =============================================
# Threats
# =============================================
dag_doc = """
# IUCN reference data on Threats: API to Clickhouse
DAG extracts data on Threats from endpoint https://api.iucnredlist.org/api/v4/threats.
Transformation includes extending 2nd- and 3rd-level threats with 1st- and 2nd level threats.
"""
@dag(
    dag_id="iucn_load_threats__dict",
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
    params={
        "table_name": Param(
            default='threats__dict', 
            type="string", 
            title="Table name",
            description="Name of the table in database",
        ),
    },
    render_template_as_native_obj=True,
    schedule=None, # only manual launch
    start_date=datetime(2026, 10, 1, 23, 0),
    catchup=False,
    tags=['IUCN', 'API', 'ClickHouse', 'references', 'replace'],
    doc_md=dag_doc
)
def iucn_load_threats__dict():
    @task
    def extract__threats(token: str, url: str, params=None) -> json:
        """
        Get dictionary of Red List categories
        """
        res = requests.get(
            url + 'threats/', 
            headers={
                'Authorization': token, 
                'accept':'*/*'
            }
        )
        res.raise_for_status()
        logger.info('Obtained data for %s', params['table_name'])
        return res.json()

    @task
    def transform__threats(data: json, params=None) -> pd.DataFrame:
        """
        Parse JSON, convert to dataframe and add levels.
        """
        data = data['threats']
        threats_levels = parse_levels(data)
        df = pd.DataFrame.from_dict(data)
        # add levels
        df['description'] = df['description'].apply(lambda x: x['en'])
        df[['threat_lvl1', 'threat_lvl2', 'threat_lvl3']] = df['code'].apply(decode_enum, levels=threats_levels).apply(pd.Series)
        
        # check for duplicated codes
        if len(df[df.duplicated(subset='code')]):
            logger.warning('Duplicated threat codes found!')

        # check for completeness of the 1st-level threats and codes
        if df['threat_lvl1'].isna().sum():
            logger.warning('1st-level threats missing!')

        if df['code'].isna().sum():
            logger.warning('Threat codes missing!')

        logger.info('Transformed data for %s', params['table_name'])
        return df
        
    @task
    def load__threats(df: pd.DataFrame, host: str, port: str, username: str, password: str, params=None) -> None:
        """
        Load dataframe to database with replacement. Create table beforehand if id does not exist.
        """
        table_name = params['table_name']
        if len(df) == 0:
            raise AirflowFailException("Empty dataframe! Aborting...")

        with cc.get_client(host=host, port=port, username=username, password=password) as client:
            # truncate and create if necessary
            client.command(f'TRUNCATE TABLE IF EXISTS dict.{table_name}')
            ddl = f"""CREATE TABLE IF NOT EXISTS dict.{table_name} (
                description      Nullable(String),
                code             String,
                threat_lvl1      Nullable(String),
                threat_lvl2      Nullable(String),
                threat_lvl3      Nullable(String),
                `CREATE_DTTM`    DateTime default now()
            )
            PRIMARY KEY (code)"""
            client.command(ddl)
            
            # load
            query_result = client.insert_df(
                table=table_name,
                df=df,
                column_names=list(df.columns),
            )
        logger.info('Table %s: %s', table_name, query_result.summary)

    # sequence
    json_ = extract__threats(token, url)

    check_ = check__data.override(
        task_id="check__threats",
    )(
        data=json_, 
        json_key='threats', 
        columns=['description', 'code']
    )
    df_ = transform__threats(data=json_)
    check_ >> df_

    load__threats(df=df_, host=host, port=port, username=username, password=password)


# =============================================
# Stresses
# =============================================
dag_doc = """
# IUCN reference data on Stresses: API to Clickhouse
DAG extracts data on Stresses from endpoint https://api.iucnredlist.org/api/v4/stresses.
Transformation includes extending 2nd- and 3rd-level threats with 1st- and 2nd level threats.
"""
@dag(
    dag_id="iucn_load_stresses__dict",
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
    params={
        "table_name": Param(
            default='stresses__dict', 
            type="string", 
            title="Table name",
            description="Name of the table in database",
        ),
    },
    render_template_as_native_obj=True,
    schedule=None, # only manual launch
    start_date=datetime(2026, 10, 1, 23, 0),
    catchup=False,
    tags=['IUCN', 'API', 'ClickHouse', 'references', 'replace'],
    doc_md=dag_doc
)
def iucn_load_stresses__dict():
    @task
    def extract__stresses(token: str, url: str, params=None) -> json:
        """
        Get dictionary of Red List categories
        """
        res = requests.get(
            url + 'stresses/', 
            headers={
                'Authorization': token, 
                'accept':'*/*'
            }
        )
        res.raise_for_status()
        logger.info('Obtained data for %s', params['table_name'])
        return res.json()

    @task
    def transform__stresses(data: json, params) -> pd.DataFrame:
        """
        Parse JSON, convert to dataframe and add levels.
        """
        data = data['stresses']
        stress_levels = parse_levels(data)
        df = pd.DataFrame.from_dict(data)
        # add levels
        df['description'] = df['description'].apply(lambda x: x['en'])
        df[['stress_lvl1', 'stress_lvl2', 'stress_lvl3']] = df['code'].apply(decode_enum, levels=stress_levels).apply(pd.Series)
        
        # check for duplicated codes
        if len(df[df.duplicated(subset='code')]):
            logger.warning('Duplicated stress codes found!')

        # check for completeness of the 1st-level stresses and codes
        if df['stress_lvl1'].isna().sum():
            logger.warning('1st-level stresses missing!')

        if df['code'].isna().sum():
            logger.warning('Stress codes missing!')

        logger.info('Transformed data for %s', params['table_name'])
        return df

    @task
    def load__stresses(df: pd.DataFrame, host: str, port: str, username: str, password: str, params=None) -> None:
        """
        Load dataframe to database with replacement. Create table beforehand if id does not exist.
        """
        table_name = params['table_name']
        if len(df) == 0:
            raise AirflowFailException("Empty dataframe! Aborting...")
            
        with cc.get_client(host=host, port=port, username=username, password=password) as client:
            # truncate and create if necessary
            client.command(f'TRUNCATE TABLE IF EXISTS dict.{table_name}')
            ddl = f"""CREATE TABLE IF NOT EXISTS dict.{table_name} (
                description      String,
                code             String,
                stress_lvl1      Nullable(String),
                stress_lvl2      Nullable(String),
                stress_lvl3      Nullable(String),
                `CREATE_DTTM`    DateTime default now()
            )
            PRIMARY KEY (code)"""
            client.command(ddl)
            
            # load
            query_result = client.insert_df(
                table=table_name,
                df=df,
                column_names=list(df.columns),
            )
        logger.info('Table %s: %s', table_name, query_result.summary)

    # sequence
    json_ = extract__stresses(token, url)

    check_ = check__data.override(
        task_id="check__stresses",
    )(
        data=json_, 
        json_key='stresses', 
        columns=['description', 'code']
    )
    df_ = transform__stresses(data=json_)
    check_ >> df_

    load__stresses(df=df_, host=host, port=port, username=username, password=password)

# ==============================================
# Habitats and systems mapping
# ==============================================
dag_doc = """
# IUCN reference data on Habitats: API to Clickhouse
DAG extracts data on Threats from endpoint https://api.iucnredlist.org/api/v4/habitats.
Transformation includes mapping habitats to systems.
"""
@dag(
    dag_id="iucn_load_habitat_system_mapping",
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
    params={
        "table_name": Param(
            default='habitat_system_mapping', 
            type="string", 
            title="Table name",
            description="Name of the table in database",
        ),
    },
    render_template_as_native_obj=True,
    schedule=None, # only manual launch
    start_date=datetime(2026, 10, 1, 23, 0),
    catchup=False,
    tags=['IUCN', 'API', 'ClickHouse', 'references', 'replace'],
    doc_md=dag_doc
)
def iucn_load_habitat_system_mapping():
    @task
    def extract__habitats(token: str, url: str, params=None) -> json:
        """
        Get dictionary of Habitats
        """
        res = requests.get(
            url + 'habitats', 
            headers={
                'Authorization': token, 
                'accept':'*/*'
            }
        )
        res.raise_for_status()
        logger.info('Obtained data for %s', params['table_name'])
        return res.json()
    
    @task
    def transform__habitats(data: json, params=None) -> pd.DataFrame:
        """
        Parse JSON, convert to dataframe and add levels.
        """
        data = data['habitats']
        df = pd.DataFrame.from_dict(data)

        df['description'] = df['description'].apply(lambda x: x['en'])

        # split two-part values
        df[['habitat_lvl1', 'habitat_lvl2']] = df['description'].apply(lambda x: x.split(' - ')).apply(pd.Series)

        # assign 3rd level value
        three_part_habitats = df[(df['code'].str.contains(r'\d+_\d+_\d+'))]['code']
        df['habitat_lvl3'] = np.where(
            df['code'].isin(three_part_habitats), 
            df['description'],
            np.nan
        )

        # expand 3rd-level value with 1st and 2nd levels
        def foo(row):
            if row['habitat_lvl3']:
                lvl12 = '_'.join(row['code'].split('_')[:2])
                match = df[df['code']==lvl12].iloc[0]
                return (match['habitat_lvl1'], match['habitat_lvl2'])
            return (row['habitat_lvl1'], row['habitat_lvl2'])
        df[['habitat_lvl1', 'habitat_lvl2']] = df.apply(foo, axis=1).apply(pd.Series)

        # check if level assignment is correct
        for _, row in df.iterrows():
            n_parts = len(row['code'].split('_'))
            if n_parts == 1:
                if not (row['habitat_lvl1'] is not np.nan and row['habitat_lvl2'] is np.nan and row['habitat_lvl3'] is np.nan):
                    logger.warning('Incorrect %s-level assignment!', 1)
            elif n_parts == 2:
                if not (row['habitat_lvl1'] is not np.nan and row['habitat_lvl2'] is not np.nan and row['habitat_lvl3'] is np.nan):
                    logger.warning('Incorrect %s-level assignment!', 2)
            elif n_parts == 3:
                if not (row['habitat_lvl1'] is not np.nan and row['habitat_lvl2'] is not np.nan and row['habitat_lvl3'] is not np.nan):
                    logger.warning('Incorrect %s-level assignment!', 3)

        # rename columns
        df.rename(columns={'description': 'habitat', 'code': 'habitat_code'}, inplace=True)

        # map 1st-level habitats to systems
        mapping = {
            'Marine Neritic': 'Marine',
            'Marine Oceanic': 'Marine',
            'Marine Deep Benthic': 'Marine',
            'Marine Intertidal': 'Marine',
            'Marine Coastal/Supratidal': 'Marine',
            'Artificial/Aquatic & Marine': 'Marine',

            'Wetlands (inland)': 'Freshwater (=Inland waters)',

            'Forest': 'Terrestrial',
            'Savanna': 'Terrestrial',
            'Shrubland': 'Terrestrial',
            'Grassland': 'Terrestrial',
            'Rocky areas (eg. inland cliffs, mountain peaks)': 'Terrestrial',
            'Caves and Subterranean Habitats (non-aquatic)': 'Terrestrial',
            'Desert': 'Terrestrial',
            'Artificial/Terrestrial': 'Terrestrial',
            'Introduced vegetation': 'Terrestrial',
            # 'Other',
            # 'Unknown'
        }
        df['system'] = df['habitat_lvl1'].apply(lambda x: mapping.get(x))

        # check for duplicated codes
        if len(df[df.duplicated(subset='habitat_code')]):
            logger.warning('Duplicated habitat_code found!')

        # check for completeness of the 1st-level habitats and codes
        if df['habitat_lvl1'].isna().sum():
            logger.warning('1st-level habitats missing!')

        if df['habitat_code'].isna().sum():
            logger.warning('Habitat codes missing!')

        logger.info('Transformed data for %s', params['table_name'])
        return df

    @task
    def load__habitats_dict(df: pd.DataFrame, host: str, port: str, username: str, password: str, params=None) -> None:
        """
        Load dataframe to database with replacement. Create table beforehand if id does not exist.
        """
        table_name = params['table_name']
        if len(df) == 0:
            raise AirflowFailException("Empty dataframe! Aborting...")
            
        # truncate and create if necessary
        logger.info('Trying to connect to database: %s:%s', host, port)
        with cc.get_client(host=host, port=port, username=username, password=password) as client:
            client.command(f'TRUNCATE TABLE IF EXISTS dict.{table_name}')
            ddl = f"""CREATE TABLE IF NOT EXISTS  dict.{table_name}(
                habitat         String,
                habitat_code    String,
                habitat_lvl1    String,
                habitat_lvl2    Nullable(String),
                habitat_lvl3    Nullable(String),
                system          Nullable(String),
                `CREATE_DTTM`   DateTime default now()
            )
            PRIMARY KEY (habitat_code)"""
            client.command(ddl)
            
            # load
            query_result = client.insert_df(
                table=table_name,
                df=df,
                column_names=list(df.columns),
            )
        logger.info('Table %s: %s', table_name, query_result.summary)


    json_ = extract__habitats(token, url)

    check_ = check__data.override(
        task_id="check__habitats",
        )(
            data=json_,
            json_key='habitats',
            columns=['description', 'code'],
        )
    df_ = transform__habitats(data=json_)
    check_ >> df_

    load__habitats_dict(df=df_, host=host, port=port, username=username, password=password)
    
iucn_load_red_list_categories_mapping()
iucn_load_threats__dict()
iucn_load_stresses__dict()
iucn_load_habitat_system_mapping()