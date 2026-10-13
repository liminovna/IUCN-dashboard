from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import shutil
import time

import pandas as pd

from airflow.models import Param
from airflow.decorators import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException

import clickhouse_connect as cc
from clickhouse_connect.driver.exceptions import DatabaseError

from extract_func import get_assessment_list_by_year, get_assessment 
from transform_func import parse_assessment
import normalization_func
import load_func

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

# =====================================
# Create paths

# """
# % project structure
# iucn_dashboard (cwd)
# |_ dags
# | |_ ...
# |_ data
#   |_{%Y%m%d}
#     |_ 1_raw
#     | |_ 1_1_countries
#     | |_ 1_2_assessments
#     |_ 2_stage
#     |_ 3_detail
# """
# =====================================

# current date
cur_date = datetime.now()
cur_date_str = cur_date.strftime('%Y%m%d')

# folders for raw data, stage and detail
DATA_DIR = Path(f'/opt/airflow/data/{cur_date_str}')
DATA_RAW_COUNTRIES = DATA_DIR / Path('1_raw', '1_1_countries')
DATA_RAW_ASSESSMENTS = DATA_DIR / Path('1_raw', '1_2_assessments')
DATA_STAGE = DATA_DIR / '2_stage'
DATA_DETAIL = DATA_DIR / '3_detail'

# logging
import logging
logger = logging.getLogger(__name__)
logger.info(f"Data will be saved to {DATA_DIR}")

dag_doc = """
# IUCN data: API to Clickhouse
DAG for extracting raw data on all the assessments for the specified years (current year by default). 

## Steps
1. Call API to fetch a list of assessments of species inhabiting Russia published in current year. Results in a collection of JSON files stored in DATA_RAW_COUNTRIES
2. Call API to fetch detailed data on the relevant assessments. Results in a collection of JSON files stored in DATA_RAW_ASSESSMENTS
3. Parse JSONs obtained in Step 2, split into multiple tables, load to Clickhouse 

## Data quality

The DAG validates the raw payload, checks nulls, uniqueness, validity of categorical values.
"""

@dag(
    dag_id="iucn_main",
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
    params={
        "current_year_only": Param(
            default=True, 
            type="boolean", 
            title="Fetch only current year publications",
            description="If `False` fetch all assessments published since 2020. If `True` only assessments published in the current year will be fetched.",
        ),
        "hard_replace": Param(
            default=False, 
            type="boolean", 
            title="Hard reload",
            description="If `False` (default) check if folder with today's date exists and if it does, skip all the tasks. If `True` delete today's folder and run the whole DAG.",
        ),
    },
    render_template_as_native_obj=True,
    schedule=None, #timedelta(days=1),
    start_date=datetime(2026, 10, 1, 23, 0),
    catchup=False,
    tags=['IUCN', 'API', 'ClickHouse', 'main', 'incremental'],
    doc_md=dag_doc
)
def iucn_main():
    @task
    def check_todays_progress(data_dir: str, params=None):
        """
        Abort the DAG if today's progress already exists and hard reload is not required. 
        If today's progress exists and hard reload is required delete progress and run DAG from scratch.
        """
        data_dir = Path(data_dir)
        if data_dir.exists:
            if params['hard_replace'] is False and os.listdir(data_dir):
                logger.warning('Today\'s progress already exists! Aborting DAG')
                raise AirflowSkipException
            elif params['hard_replace'] is True:
                shutil.rmtree(data_dir)
                logger.warning('Today\'s progress has been deleted')
        return True
            
    @task
    def check__references(host: str, port: str, username: str, password: str) -> None:
        """
        Check if reference tables exist and not empty. If not, abort DAG
        """
        empty_tables = []
        nonexistent_tables = []

        ref_tables = [
            'red_list_categories_mapping',
            'habitat_system_mapping',
            'threats__dict',
            'stresses__dict',
        ]
        with cc.get_client(host=host, port=port, username=username, password=password) as client:
            for t in ref_tables:
                table_exists_ = client.command(f"EXISTS TABLE {t}")
                if not table_exists_:
                    nonexistent_tables.append(t)
                    logger.error('Table %s is does not exist!', t)

                else:
                    nrows = client.query(f'select count() from {t}').result_rows[0][0]
                    if nrows == 0:
                        empty_tables.append(t)
                        logger.error('Table %s is empty!', t)

        if empty_tables or nonexistent_tables:
            raise AirflowFailException          

    # =====================================
    # Extract
    # =====================================
    @task
    def extract__raw_assessment_lists(token: str, url: str, output_dir: Path, params=None) -> dict():
        """
        Collect lists of relevant assessments via API, method `country`.
        Save to output_dir.

        returns:
        Path where all the JSONs are stored. Next we will read them
        """
        current_year_only = params['current_year_only']
        cur_year = cur_date.year
        years = [cur_year] if current_year_only is True else list(range(2020, cur_year))

        # # if the output directory already exists and not empty skip to the next step
        # if output_dir.exists() and os.listdir(output_dir):
        #     logger.warning('This command ran today. See results in %s', output_dir)
        #     n_assessments_total = 1

        if False:
            pass
        
        else:
            output_dir.mkdir(parents=True, exist_ok=False)

            logger.info('Collecting data on years: %s', years)

            # iterate over the required years of publication 
            for year in years:
                logger.info('starting year %s', year)

                page_n = 1
                n_assessments_total = 0

                while True:
                    n_assessments = 0
                    try:
                        # call API
                        assessments_lst = get_assessment_list_by_year(year, token, url, page_n)
                    # if any error stop the whole DAG
                    except Exception as e:
                        logger.error('Error on year=%s, page=%s: %s', year, page_n, e)
                        raise

                    # if invalid parameters or access token
                    if 'error' in assessments_lst:
                        logger.error('Error on year=%s, page=%s: %s', year, page_n, assessments_lst["error"])
                        raise

                    # if there is no data on the current page skip to next year
                    if not assessments_lst.get('assessments', []):
                        # logger.info('No assessments found on page %s, skipping...', page_n)
                        break

                    # update assessment counts
                    n_assessments = len(assessments_lst['assessments'])
                    n_assessments_total += n_assessments

                    # save to file
                    json_path = output_dir / f'{year}_{page_n}.json'
                    with open(json_path, 'w+', encoding='utf-8') as f:
                        json.dump(
                            {
                                'extracted_at': datetime.utcnow().isoformat(),
                                'source': url,
                                'record_count': n_assessments,
                                'data': assessments_lst,
                            },
                            f, 
                            ensure_ascii=False, 
                            indent=4
                        )
                        logger.info('Saved %s to %s', n_assessments, json_path)

                    page_n += 1
                    time.sleep(3)

        logger.info('Fetched %s assessments', n_assessments_total)
        return {'path': str(output_dir), 'n_assessments_total': n_assessments_total}

    @task
    def check__countries(upstream_result: dict) -> bool:
        """
        If no assessments found skip the remaining tasks. 
        """
        if upstream_result['n_assessments_total'] == 0:
            raise AirflowFailException

    @task
    def extract__raw_assessments(upstream_result: dict, token: str, url: str, output_dir: str) -> str:
        """
        Extract detailed data only on new assessments.
        For every assessment_id, check whether it is already in the database. If there has not been new assessments, stop the DAG. 
        Otherwise extract and save only the increment.
        """
        input_dir = Path(upstream_result['path'])
        # # if the output directory already exists and not empty skip to the next step
        # if output_dir.exists() and os.listdir(output_dir):
        #     logger.warning('This command ran today. See results in %s', output_dir)
        #     return str(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)
        api_error_count = 0
        new_assessments = 0

        # get unique assessment_ids published over the current year
        with cc.get_client(host=host, port=port, username=username, password=password) as client:
            if client.command("EXISTS TABLE assessments"):
                cur_year_assessments = set(client.query_df(f'select assessment_id from assessments where year_published = {cur_date.year}')['assessment_id'].to_list())
            else:
                cur_year_assessments = set()
        files = os.listdir(input_dir)
        for file in files:
            with open(input_dir / file, 'r', encoding='utf-8') as f:
                try:
                    assessments = json.load(f)['data']['assessments']
                except KeyError:
                    # skip to next file if the current file has unexpected structure
                    logger.error('Unexpected file structure: %s', file)
                    continue

            for asmnt in assessments:
                # if for some reason there is no assessment_id, skip to next element (i.e. assessment) in file
                if not asmnt.get('assessment_id'):
                    logger.error('Could not find assessment_id in file %s', file)
                    continue

                res = None
                try:
                    assessment_id = asmnt['assessment_id']
                    # if assessment is new call API for detailed information
                    if assessment_id not in cur_year_assessments:
                        res = get_assessment(assessment_id, token, url)
                except Exception as e:
                    logger.error('Error on assessment_id=%s: %s', assessment_id, e)
                    api_error_count += 1

                # once 5 concequtive errors occured raise exception
                if api_error_count == 5:
                    logger.error('5 consecutive API calls failed! Aborting....')
                    raise AirflowFailException

                else:
                    # save JSON
                    with open(output_dir / f'{assessment_id}.json', 'w+', encoding='utf-8') as f:
                        json.dump(
                            {
                                'extracted_at': datetime.utcnow().isoformat(),
                                'data': res
                            },
                            f, 
                            ensure_ascii=False, 
                            indent=4
                        )
                        logger.info('Data saved to %s.json', assessment_id)
                        new_assessments += 1

                    # if success reset error count
                    api_error_count = 0

                # wait 3 seconds as to not get banned
                time.sleep(3)
        if new_assessments == 0:
            raise AirflowSkipException("No new assessments to process")
        else:
            logger.info('New assessments found: %s', new_assessments)
        return str(output_dir)

    # =====================================
    # Transform
    # =====================================
    @task
    def parse__raw_data(input_dir: str, output_dir: str, output_filename: str='parsed.json') -> str:
        """
        Parse JSON files and merge into one DataFrame. Check quality of the data.

        returns:
        Path to the merged dataframe
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / output_filename
        parsed_data = pd.DataFrame()

        files = os.listdir(input_dir)
        for f in files:
            cur_data = None
            with open(input_dir / f, 'r', encoding='utf-8') as infile:
                data = json.load(infile) #['data']
            try:
                cur_data = parse_assessment(data)
            except Exception as e:
                logger.error('Could not parse %s! %s', f, e)
            
            parsed_data = pd.concat([parsed_data, cur_data], ignore_index=True)
        
        # check basic data stats
        logger.warning('New raw data stats:\n%s', parsed_data.isna().sum())

        logger.info('Saving dataframe of size %s to %s', parsed_data.shape, output_path)

        parsed_data.to_json(output_path)

        return str(output_path)

    # =====================================
    # Split data into separate tables
    # =====================================
    @task
    def normalize__data(input_path: str, output_dir: str) -> str:
        """
        Normalize dataframe: split data into separate dataframes. Additionally, perform basic quality checks and apply transfromations to data

        params:
        input_path: path to JSON file
        output_dir: path to directory where all the separated tables will be stored
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        data = pd.read_json(input_path)

        # functions from normalization_func
        func_names = (
            'save__assessments',
            'save__conservation_actions',
            'save__conservation_actions_in_place',
            'save__habitats',
            'save__locations',
            'save__scopes',
            'save__stresses',
            'save__systems',
            'save__taxonomy',
            'save__threats',
            'save__use_and_trade',
        )
        for func_name in func_names:
            func = getattr(normalization_func, func_name)
            func(data, output_dir)
            logging.info('%s ran successfully', func_name)

        return str(output_dir)     

    # =====================================
    # Load
    # =====================================
    @task
    def load_to_db(input_path: str, host: str, port: str, username: str, password: str) -> None:
        """
        Load data from files by running functions from module `load_func`
        """
        input_path = Path(input_path)
        with cc.get_client(host=host, port=port, username=username, password=password) as client:

            func_names = (
                'load__assessments',
                'load__conservation_actions_db',
                'load__conservation_actions_in_place_db',
                'load__habitats',
                'load__locations',
                'load__systems',
                'load__taxonomy',
                'load__threats',
                'load__stresses',
                'load__use_and_trade',
                'load__scopes',
            )
            for func_name in func_names:
                func = getattr(load_func, func_name)
                func(client, input_path)
                logging.info('%s ran successfully', func_name)

        
    # =====================================
    # Task sequence
    # =====================================
    if_start = check_todays_progress(data_dir=DATA_DIR)
    references_ok = check__references(host, port, username, password)
    raw_assessments_countries_result = extract__raw_assessment_lists(token=token, url=url, output_dir=DATA_RAW_COUNTRIES)
    countries_ok = check__countries(raw_assessments_countries_result)
    raw_assessments = extract__raw_assessments(upstream_result=raw_assessments_countries_result, token=token, url=url, output_dir=DATA_RAW_ASSESSMENTS)
    parsed = parse__raw_data(input_dir=raw_assessments, output_dir=DATA_STAGE)
    normalized = normalize__data(input_path=parsed, output_dir=DATA_DETAIL)
    loaded = load_to_db(input_path=normalized, host=host, port=port, username=username, password=password)

    if_start >> references_ok >> raw_assessments_countries_result >> countries_ok >> raw_assessments

    # TODO: quality checks


iucn_main()