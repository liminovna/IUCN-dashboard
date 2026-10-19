from datetime import datetime
import json
import os
import requests

import pandas as pd
import numpy as np

from airflow.sdk import get_current_context,  dag, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.models import Param
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

# =====================================
# Import credentials
# =====================================
from dotenv import load_dotenv
load_dotenv()

token = os.environ.get('iucn_token')
url = os.environ.get('iucn_api_url')

hook = ClickHouseHook(clickhouse_conn_id="clickhouse")

# logging
import logging
logger = logging.getLogger(__name__)

from data_quality import check_data_quality
check_data_quality_task = task(check_data_quality)

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
        Get dictionary of Red List categories. Abort DAG if no data
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
        res = res.json()

        # System critical data quality check
        main_key = 'red_list_categories'
        if main_key not in res:
            logger.error('Main key `%s` missing!', main_key)
            raise AirflowFailException
        if len(res[main_key]) == 0:
            logger.error('Empty response!')
            raise AirflowFailException
        if not all(c in res[main_key][0].keys() for c in ['version', 'description', 'code']) or 'en' not in res[main_key][0]['description']:
            logger.error('Unexpected data structure')
            raise AirflowFailException
        return res

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
        df.drop(df[df['version'].isna()].index)
        logger.info('Transformed data for %s', params['table_name'])

        # System critical data quality checks
        if len(df) == 0:
            logger.error('Empty dataframe!')
            raise AirflowFailException

        required_columns = set(['version', 'description', 'code', 'category_sorted'])
        if set(df.columns) != required_columns:
            logger.error('Unexpected data structure!')
            raise AirflowFailException

        context = get_current_context()
        ti = context["ti"]
        
        return {
            "data": df,
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        }

    # ===============================
    # Data quality check on dataframe
    # - row count
    # - CR: not empty on description
    # - CR: unique on (version, code)
    # - not empty version
    # - not empty code
    # - number of values in category_sorted
    # ===============================

    @task
    def load__red_list_categories(upstream_result: dict, params=None) -> None:
        """
        Load dataframe to database with replacement. Create table beforehand if id does not exist.
        """
        df = upstream_result['data']
        table_name = params['table_name']

        text_cols = df.select_dtypes(str).columns
        df[text_cols] = df[text_cols].fillna('empty')

        with hook.get_client() as client:
            # create if necessary and truncate
            ddl = f"""CREATE TABLE IF NOT EXISTS dict.{table_name} (
                version         LowCardinality(String) default 'empty',
                description     String,
                code            LowCardinality(String) default 'empty',
                category_sorted Nullable(String),
                `CREATE_DTTM`   DateTime default now()
            )
            ORDER BY (version, code)"""
            client.command(ddl)
            client.command(f'TRUNCATE TABLE dict.{table_name}')

            # load
            query_result = client.insert_df(
                table=f'dict.{table_name}',
                df=df,
                column_names=list(df.columns),
            )

        logger.info('Table %s: %s', table_name, query_result.summary)

        context = get_current_context()
        ti = context["ti"]

        return {
            "db_table": f'dict.{table_name}',
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        }

    # ===============================
    # Data quality check inside the database
    # - CR: table not empty
    # ===============================

    # sequence
    json_ = extract__red_list_categories(token, url)
    upstream_df_ = transform__red_list_categories(data=json_)

    transform_dq_check_ = check_data_quality_task.override(
        task_id="dq__transform__red_list_categories",
    )(
        upstream_result=upstream_df_, 
        checks=[
            {
                'check_name': 'record_count',
                'critical': False,

            },
            {
                'check_name': 'completeness',
                'check_subset': 'description',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'check_subset': ('version', 'code'),
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'version',
                'critical': False,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'code',
                'critical': False,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'category_sorted',
                'critical': False,
            }
        ]
    )

    upstream_result_db_ = load__red_list_categories(upstream_result=upstream_df_)
    transform_dq_check_ >> upstream_result_db_

    load_dq_check_ = check_data_quality_task.override(
        task_id="dq__load__red_list_categories",
    )(
        upstream_result=upstream_result_db_, 
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,

            }
        ]
    )
    upstream_result_db_ >> load_dq_check_
    
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
        res = res.json()

        # System critical data quality check
        main_key = 'threats'
        if main_key not in res:
            logger.error('Main key `%s` missing!', main_key)
            raise AirflowFailException
        if len(res[main_key]) == 0:
            logger.error('Empty response!')
            raise AirflowFailException
        if not all(c in res[main_key][0].keys() for c in ['description', 'code']) or 'en' not in res[main_key][0]['description']:
            logger.error('Unexpected data structure')
            raise AirflowFailException
        return res

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

        # System critical data quality check
        if len(df) == 0:
            logger.error('Empty dataframe!')
            raise AirflowFailException

        required_columns = set(['description', 'code', 'threat_lvl1', 'threat_lvl2', 'threat_lvl3'])
        if set(df.columns) != required_columns:
            logger.error('Unexpected data structure')
            raise AirflowFailException

        context = get_current_context()
        ti = context["ti"]
        
        return {
            "data": df,
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        }

    # ===============================
    # Data quality check on dataframe
    # - row count
    # - CR: not empty on description
    # - CR: not empty on code
    # - CR: not empty on threat_lvl1
    # - CR: unique on (code)
    # - number of values in threat_lvl2
    # - number of values in threat_lvl3
    # ===============================
        
    @task
    def load__threats(upstream_result: dict, params=None) -> None:
        """
        Load dataframe to database with replacement. Create table beforehand if id does not exist.
        """
        df = upstream_result['data']
        table_name = params['table_name']
        if len(df) == 0:
            raise AirflowFailException("Empty dataframe! Aborting...")

        with hook.get_client() as client:
            # create if necessary and truncate
            ddl = f"""CREATE TABLE IF NOT EXISTS dict.{table_name} (
                description      String,
                code             String,
                threat_lvl1      LowCardinality(String) default 'empty',
                threat_lvl2      Nullable(String),
                threat_lvl3      Nullable(String),
                `CREATE_DTTM`    DateTime default now()
            )
            ORDER BY (code, threat_lvl1)"""
            client.command(ddl)
            client.command(f'TRUNCATE TABLE dict.{table_name}')
            
            # load
            query_result = client.insert_df(
                table=f'dict.{table_name}',
                df=df,
                column_names=list(df.columns),
            )
        logger.info('Table %s: %s', table_name, query_result.summary)

        context = get_current_context()
        ti = context["ti"]

        return {
            "db_table": f'dict.{table_name}',
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        }

    # ===============================
    # Data quality check inside the database
    # - CR: table not empty
    # ===============================

    # sequence
    json_ = extract__threats(token, url)
    upstream_df_ = transform__threats(data=json_)

    transform_dq_check_ = check_data_quality_task.override(
        task_id="dq__transform__threats",
    )(
        upstream_result=upstream_df_, 
        checks=[
            {
                'check_name': 'record_count',
                'critical': False,

            },
            {
                'check_name': 'completeness',
                'check_subset': 'description',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'check_subset': ('code'),
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'code',
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'threat_lvl1',
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'threat_lvl2',
                'critical': False,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'threat_lvl3',
                'critical': False,
            },
        ]
    )

    upstream_result_db_ = load__threats(upstream_result=upstream_df_)
    transform_dq_check_ >> upstream_result_db_

    load_dq_check_ = check_data_quality_task.override(
        task_id="dq__load__threats",
    )(
        upstream_result=upstream_result_db_, 
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,

            }
        ]
    )
    upstream_result_db_ >> load_dq_check_

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
        res = res.json()

        # System critical data quality check
        main_key = 'stresses'
        if main_key not in res:
            logger.error('Main key `%s` missing!', main_key)
            raise AirflowFailException
        if len(res[main_key]) == 0:
            logger.error('Empty response!')
            raise AirflowFailException
        if not all(c in res[main_key][0].keys() for c in ['description', 'code']) or 'en' not in res[main_key][0]['description']:
            logger.error('Unexpected data structure')
            raise AirflowFailException
        return res

    @task
    def transform__stresses(data: json, params=None) -> pd.DataFrame:
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

        # System critical data quality check
        if len(df) == 0:
            logger.error('Empty dataframe!')
            raise AirflowFailException

        required_columns = set(['description', 'code', 'stress_lvl1', 'stress_lvl2', 'stress_lvl3'])
        if set(df.columns) != required_columns:
            logger.error('Unexpected data structure')
            raise AirflowFailException

        context = get_current_context()
        ti = context["ti"]
        
        return {
            "data": df,
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        }

    # ===============================
    # Data quality check on dataframe
    # - row count
    # - CR: not empty on description
    # - CR: not empty on code
    # - CR: not empty on stress_lvl1
    # - CR: unique on code
    # - number of values in stress_lvl2, stress_lvl3
    # ===============================

    @task
    def load__stresses(upstream_result: dict, params=None) -> None:
        """
        Load dataframe to database with replacement. Create table beforehand if id does not exist.
        """
        df = upstream_result['data']
        table_name = params['table_name']
        if len(df) == 0:
            raise AirflowFailException("Empty dataframe! Aborting...")
            
        with hook.get_client() as client:
            # create if necessary and truncate
            ddl = f"""CREATE TABLE IF NOT EXISTS dict.{table_name} (
                description      String,
                code             String,
                stress_lvl1      LowCardinality(String) default 'empty',
                stress_lvl2      Nullable(String),
                stress_lvl3      Nullable(String),
                `CREATE_DTTM`    DateTime default now()
            )
            ORDER BY (code)"""
            client.command(ddl)
            client.command(f'TRUNCATE TABLE dict.{table_name}')
            
            # load
            query_result = client.insert_df(
                table=f'dict.{table_name}',
                df=df,
                column_names=list(df.columns),
            )
        logger.info('Table %s: %s', table_name, query_result.summary)

        context = get_current_context()
        ti = context["ti"]

        return {
            "db_table": f'dict.{table_name}',
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        }

    # ===============================
    # Data quality check inside the database
    # - CR: table not empty
    # ===============================

    # sequence
    json_ = extract__stresses(token, url)
    upstream_df_ = transform__stresses(data=json_)

    transform_dq_check_ = check_data_quality_task.override(
        task_id="dq__transform__stresses",
    )(
        upstream_result=upstream_df_, 
        checks=[
            {
                'check_name': 'record_count',
                'critical': False,

            },
            {
                'check_name': 'completeness',
                'check_subset': 'description',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'check_subset': ('code'),
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'code',
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'stress_lvl1',
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'stress_lvl2',
                'critical': False,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'stress_lvl3',
                'critical': False,
            },
        ]
    )

    upstream_result_db_ = load__stresses(upstream_result=upstream_df_)
    transform_dq_check_ >> upstream_result_db_

    load_dq_check_ = check_data_quality_task.override(
        task_id="dq__load__stresses",
    )(
        upstream_result=upstream_result_db_, 
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,

            }
        ]
    )
    upstream_result_db_ >> load_dq_check_

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
        res = res.json()

        # System critical data quality check
        main_key = 'habitats'
        if main_key not in res:
            logger.error('Main key `%s` missing!', main_key)
            raise AirflowFailException
        if len(res[main_key]) == 0:
            logger.error('Empty response!')
            raise AirflowFailException
        if not all(c in res[main_key][0].keys() for c in ['description', 'code']) or 'en' not in res[main_key][0]['description']:
            logger.error('Unexpected data structure')
            raise AirflowFailException
        return res
    
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
        def expand_habitat(row):
            if row['habitat_lvl3']:
                lvl12 = '_'.join(row['code'].split('_')[:2])
                match = df[df['code']==lvl12].iloc[0]
                return (match['habitat_lvl1'], match['habitat_lvl2'])
            return (row['habitat_lvl1'], row['habitat_lvl2'])
        df[['habitat_lvl1', 'habitat_lvl2']] = df.apply(expand_habitat, axis=1).apply(pd.Series)

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

        logger.info('Transformed data for %s', params['table_name'])

        # System critical data quality check
        if len(df) == 0:
            logger.error('Empty dataframe!')
            raise AirflowFailException

        required_columns = set(['habitat', 'habitat_code', 'habitat_lvl1', 'habitat_lvl2', 'habitat_lvl3', 'system'])
        if set(df.columns) != required_columns:
            logger.error('Unexpected data structure')
            raise AirflowFailException

        context = get_current_context()
        ti = context["ti"]
        
        return {
            "data": df,
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        }

    # ===============================
    # Data quality check on dataframe
    # - row number
    # - CR: unique on habitat
    # - CR: not empty on habitat
    # - CR: unique on habitat_code
    # - CR: not empty on habitat_code
    # - CR: unique on habitat_lvl1
    # - CR: not empty on habitat_lvl1
    # - CR: unique on habitat_lvl1, habitat_lvl2, habitat_lvl3
    # - number of values in habitat_lvl2, habitat_lvl3, system
    # ===============================

    @task
    def load__habitats(upstream_result: dict, params=None) -> None:
        """
        Load dataframe to database with replacement. Create table beforehand if id does not exist.
        """
        df = upstream_result['data']
        table_name = params['table_name']
        if len(df) == 0:
            raise AirflowFailException("Empty dataframe! Aborting...")
            
        with hook.get_client() as client:
            # create if necessary and truncate
            ddl = f"""CREATE TABLE IF NOT EXISTS  dict.{table_name}(
                habitat         String,
                habitat_code    String,
                habitat_lvl1    LowCardinality(String) default 'empty',
                habitat_lvl2    Nullable(String),
                habitat_lvl3    Nullable(String),
                system          Nullable(String),
                `CREATE_DTTM`   DateTime default now()
            )
            ORDER BY (habitat_code)"""
            client.command(ddl)
            client.command(f'TRUNCATE TABLE dict.{table_name}')
            
            # load
            query_result = client.insert_df(
                table=f'dict.{table_name}',
                df=df,
                column_names=list(df.columns),
            )
        logger.info('Table %s: %s', table_name, query_result.summary)

        context = get_current_context()
        ti = context["ti"]

        return {
            "db_table": f'dict.{table_name}',
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        }
    # ===============================
    # Data quality check inside the database
    # - CR: table is not empty
    # ===============================

    json_ = extract__habitats(token, url)
    upstream_df_ = transform__habitats(data=json_)

    transform_dq_check_ = check_data_quality_task.override(
        task_id="dq__transform__habitats",
    )(
        upstream_result=upstream_df_, 
        checks=[
            {
                'check_name': 'record_count',
                'critical': False,

            },
            {
                'check_name': 'completeness',
                'check_subset': 'habitat',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'check_subset': ('habitat'),
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'habitat_code',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'check_subset': ('habitat_code'),
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'habitat_lvl1',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'check_subset': ('habitat_lvl1', 'habitat_lvl2', 'habitat_lvl3'),
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'habitat_lvl1',
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'habitat_lvl2',
                'critical': False,
            },
            {
                'check_name': 'completeness',
                'check_subset': 'habitat_lvl3',
                'critical': False,
            },
        ]
    )

    upstream_result_db_ = load__habitats(upstream_result=upstream_df_)
    transform_dq_check_ >> upstream_result_db_

    load_dq_check_ = check_data_quality_task.override(
        task_id="dq__load__habitats",
    )(
        upstream_result=upstream_result_db_,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,

            }
        ]
    )
    upstream_result_db_ >> load_dq_check_

iucn_load_red_list_categories_mapping()
iucn_load_threats__dict()
iucn_load_stresses__dict()
iucn_load_habitat_system_mapping()