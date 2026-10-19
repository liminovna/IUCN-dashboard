from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import shutil
import time

import pandas as pd

from airflow.models import Param
from airflow.utils.task_group import TaskGroup
from airflow.sdk import get_current_context, dag, task, task_group
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk.exceptions import AirflowSkipException, AirflowFailException
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

from extract_func import get_assessment_list_by_year, get_assessment 
from transform_func import parse_assessment
import normalization_func
import load_func
from data_quality import check_data_quality

# =====================================
# Import credentials
# =====================================
from dotenv import load_dotenv
load_dotenv()

token = os.environ.get('iucn_token')
url = os.environ.get('iucn_api_url')

hook = ClickHouseHook(clickhouse_conn_id="clickhouse")

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
1. Check if today's progress already loaded. If true and parameter `forced` is false or the dag has not ran today, skip the rest of the steps. Go to step 2.
2. Update (replace) reference tables by calling a separate DAG.
3. 
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
            description="If `True` (default), only assessments published in the current year will be fetched. If `False`, fetch all assessments published since 2020.",
        ),
        "forced": Param(
            default=False, 
            type="boolean", 
            title="Forced update",
            description="If `False` (default) check if folder with today's date exists and if it does, skip all the tasks. If `True` delete folder with today's raw data and run the whole DAG.",
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
        Abort the DAG if today's progress already exists and forced update is not required`. 
        If today's progress exists and hard reload is required delete progress and run DAG from scratch.
        """
        data_dir = Path(data_dir)
        # if today's progress exists
        if data_dir.exists():
            logger.info('Path exists: %s', str(data_dir))
            if params['forced'] is False and os.listdir(data_dir):
                logger.warning('Today\'s progress already exists! Aborting DAG')
                raise AirflowSkipException
            elif params['forced'] is True:
                shutil.rmtree(data_dir)
                logger.warning('Today\'s progress has been deleted')
        return True

    @task
    def check_data_quality_table():
        """
        Check if table for data quality checks exists. If not -- create
        """
        with hook.get_client() as client:
            query = """
            CREATE TABLE IF NOT EXISTS data_quality.main
            (
                `airflow__dag_id`               String,
                `airflow__run_id`               String,
                `airflow__source_task_id`       String,
                `airflow__source_start_date`    DateTime,
                `airflow__source_end_date`      Nullable(DateTime),
                `check`                         String,
                `check_subset`                  Nullable(String),
                `critical`                      Bool,
                `status`                        String,
                `comment`                       Nullable(String),
                `factual_value_str`             Nullable(String),
                `factual_value_int`             Nullable(UInt64),
            );
            """
            client.command(query)
        logger.info('Table data_quality.main in place')

    # ====================================
    # Update reference tables
    # ==================================== 

    with TaskGroup("ref_tables__taskgroup", tooltip="Update reference tables") as ref_tables:
        trigger__red_list_categories_mapping = TriggerDagRunOperator(
            task_id="trigger__red_list_categories_mapping",
            trigger_dag_id="iucn_load_red_list_categories_mapping",
            # wait_for_completion=True,
            deferrable=True, # When it has nothing to do but wait, an operator can suspend itself and free up the worker for other processes by deferring.
        )

        trigger__habitat_system_mapping = TriggerDagRunOperator(
            task_id="trigger__habitat_system_mapping",
            trigger_dag_id="iucn_load_habitat_system_mapping",
            # wait_for_completion=True,
            deferrable=True,
        )

        trigger__threats__dict = TriggerDagRunOperator(
            task_id="trigger__threats__dict",
            trigger_dag_id="iucn_load_threats__dict",
            # wait_for_completion=True,
            deferrable=True,
        )

        trigger__stresses__dict = TriggerDagRunOperator(
            task_id="trigger__stresses__dict",
            trigger_dag_id="iucn_load_stresses__dict",
            # wait_for_completion=True,
            deferrable=True,
        )

    # =====================================
    # Extract raw and save locally
    # =====================================
    @task
    def extract__raw_assessment_lists(token: str, url: str, output_dir: Path, params=None) -> dict():
        """
        Collect lists of relevant assessments for selected years via API, method `country`.
        Save to output_dir.

        returns:
        Path where all the JSONs are stored. Next we will read them
        """
        current_year_only = params['current_year_only']
        cur_year = cur_date.year
        years = [cur_year] if current_year_only is True else list(range(2020, cur_year))

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

                # System critical error: parameters or access token
                if 'error' in assessments_lst:
                    logger.error('Error on year=%s, page=%s: %s', year, page_n, assessments_lst["error"])
                    raise AirflowFailException

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

        # System critical error: no records found
        if n_assessments_total == 0:
            logging.error('No assessments found!')
            raise AirflowFailException

        logger.info('Fetched %s assessments', n_assessments_total)
        
        # =====================
        # Data quality check
        # - CR: number of records (n_assessments_total)
        # =====================
        # write number of records to data quality check table
        context = get_current_context()
        ti = context["ti"]
        
        prepared_check = [{
            "check": "record_count",
            "critical": True,
            "status": 'ok',
            "factual_value_int": n_assessments_total
        }]
        check_data_quality(
            upstream_result={
                "prepared_check": prepared_check,
                "metadata": {
                    'dag_id': context["dag"].dag_id,
                    "run_id": context["dag_run"].run_id,
                    "upstream_task_id": ti.task_id,
                    "upstream_started_at": ti.start_date,
                    "upstream_ended_at": datetime.now(),
                },
            }
        )
        return str(output_dir)

    @task
    def extract__raw_assessments(input_dir: str, token: str, url: str, output_dir: str) -> str:
        """
        Extract detailed data only on new assessments.
        For every assessment_id, check whether it is already in the database. If there has not been new assessments, stop the DAG. 
        Otherwise extract and save only the increment.
        """
        input_dir = Path(input_dir)

        output_dir.mkdir(parents=True, exist_ok=True)
        api_error_count = 0
        new_assessments = 0

        # get unique assessment_ids published over the current year
        with hook.get_client() as client:
            if client.command("EXISTS TABLE detail.assessments"):
                cur_year_assessments = set(client.query_df(f'select assessment_id from detail.assessments where year_published = {cur_date.year}')['assessment_id'].to_list())
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
                    # if the assessment is new, call API for detailed information
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

        # =====================
        # Data quality check
        # - CR: number of new records (new_assessments)
        # =====================
        # write number of new records to data quality check table
        context = get_current_context()
        ti = context["ti"]
        
        prepared_check = [{
            "check": "record_count",
            "critical": True,
            "status": 'ok',
            "factual_value_int": new_assessments
        }]
        check_data_quality(
            upstream_result={
                "prepared_check": prepared_check,
                "metadata": {
                    'dag_id': context["dag"].dag_id,
                    "run_id": context["dag_run"].run_id,
                    "upstream_task_id": ti.task_id,
                    "upstream_started_at": ti.start_date,
                    "upstream_ended_at": datetime.now(),
                },
            }
        )

        return str(output_dir)

    # =====================================
    # Transform raw data
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
                data = json.load(infile)['data'] #
            try:
                cur_data = parse_assessment(data)
            except Exception as e:
                logger.error('Could not parse %s! %s', f, e)
            
            parsed_data = pd.concat([parsed_data, cur_data], ignore_index=True)

        logger.info('Saving dataframe of size %s to %s', parsed_data.shape, output_path)

        parsed_data.to_json(output_path)

        # ===============================
        # Data quality check inside the database
        # - CR: table not empty
        # - CR: completeness on assessment_id
        # - CR: uniqueness on assessment_id
        # - CR: completeness on scientific_name
        # - expected year_published: 2020-2026
        # - completeness on latest
        # - completeness on population_trend
        # - completeness on population_trend_code
        # - completeness on red_list_category_version
        # - completeness on red_list_category
        # - completeness on red_list_category_code
        # TODO: referential integrity on (population_trend, population_trend_code)
        # TODO: referential integrity on (red_list_category_version, red_list_category, red_list_category_code)
        # ===============================
        context = get_current_context()
        ti = context["ti"]

        upstream_result = {
            "input_path": str(output_path), # expected by `normalize_data`
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        }

        check_data_quality(
            upstream_result=upstream_result,
            checks=[
                {
                    'check_name': 'record_count',
                    'critical': True,
                },
                {
                    'check_name': 'completeness',
                    'critical': True,
                    'check_subset': 'assessment_id'
                },
                {
                    'check_name': 'uniqueness',
                    'critical': True,
                    'check_subset': 'assessment_id'
                },
                {
                    'check_name': 'completeness',
                    'critical': True,
                    'check_subset': 'scientific_name'
                },
                {
                    'check_name': 'completeness',
                    'critical': False,
                    'check_subset': 'latest'
                },
                {
                    'check_name': 'completeness',
                    'critical': False,
                    'check_subset': 'population_trend'
                },
                {
                    'check_name': 'completeness',
                    'critical': False,
                    'check_subset': 'population_trend_code'
                },
                {
                    'check_name': 'completeness',
                    'critical': False,
                    'check_subset': 'red_list_category_version'
                },
                {
                    'check_name': 'completeness',
                    'critical': False,
                    'check_subset': 'red_list_category'
                },
                {
                    'check_name': 'completeness',
                    'critical': False,
                    'check_subset': 'red_list_category_code'
                },
                {
                    'check_name': 'expected_values',
                    'critical': False,
                    'check_subset': 'year_published',
                    'expected_values': list(range(2020, datetime.now().year+1))
                },
            ] 
        )
        return str(output_path)

    # =====================================
    # Split data into separate tables
    # =====================================
    @task_group(group_id="normalize_data", tooltip="Split raw dataframe")
    def normalize_data(input_path: str, output_dir: str) -> str:
        """
        Normalize dataframe: split data into separate dataframes. Additionally, perform basic quality checks and apply transfromations to data

        params:
        input_path: path to JSON file
        output_dir: path to directory where all the separated tables will be stored
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        nf_1 = normalization_func.normalize__assessments(input_path, output_dir) # data checks inside the functions
        nf_2 = normalization_func.normalize__conservation_actions(input_path, output_dir)
        nf_3 = normalization_func.normalize__conservation_actions_in_place(input_path, output_dir)
        nf_4 = normalization_func.normalize__habitats(input_path, output_dir)
        nf_5 = normalization_func.normalize__locations(input_path, output_dir)
        nf_6 = normalization_func.normalize__scopes(input_path, output_dir)
        nf_7 = normalization_func.normalize__stresses(input_path, output_dir)
        nf_8 = normalization_func.normalize__systems(input_path, output_dir)
        nf_9 = normalization_func.normalize__taxonomy(input_path, output_dir)
        nf_10 = normalization_func.normalize__threats(input_path, output_dir)
        nf_11 = normalization_func.normalize__use_and_trade(input_path, output_dir)

        @task
        def _get_output(output: str) -> str:
            return output
        output_dir_ = _get_output(str(output_dir))

        [nf_1,nf_2,nf_3,nf_4,nf_5,nf_6,nf_7,nf_8,nf_9,nf_10,nf_11,] >> output_dir_

        return output_dir_

    # =====================================
    # Load
    # =====================================
    @task_group(group_id="load_to_db", tooltip="Upload data to database")
    def load_to_db(input_dir: str) -> None:
        """
        Load data to database from files by running functions from module `load_func`
        """
        # input_dir = Path(input_dir)

        load_func.load__assessments(input_dir)
        load_func.load__conservation_actions(input_dir)
        load_func.load__conservation_actions_in_place(input_dir)
        load_func.load__habitats(input_dir)
        load_func.load__locations(input_dir)
        load_func.load__systems(input_dir)
        load_func.load__taxonomy(input_dir)
        load_func.load__threats(input_dir)
        load_func.load__stresses(input_dir)
        load_func.load__use_and_trade(input_dir)
        load_func.load__scopes(input_dir)


    # =====================================
    # Task sequence
    # =====================================
    if_start = check_todays_progress(data_dir=DATA_DIR)
    dq_table = check_data_quality_table()
    # Task group ref_tables (see above)
    raw_assessments_countries_result = extract__raw_assessment_lists(token=token, url=url, output_dir=DATA_RAW_COUNTRIES)
    raw_assessments = extract__raw_assessments(input_dir=raw_assessments_countries_result, token=token, url=url, output_dir=DATA_RAW_ASSESSMENTS)
    parsed = parse__raw_data(input_dir=raw_assessments, output_dir=DATA_STAGE)
    normalized = normalize_data(input_path=parsed, output_dir=DATA_DETAIL)
    loaded = load_to_db(input_dir=normalized)

    if_start >> dq_table >> ref_tables >> raw_assessments_countries_result
    parsed >> normalized >> loaded

iucn_main()