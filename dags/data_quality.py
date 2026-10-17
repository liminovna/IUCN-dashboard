import pandas as pd

from airflow.sdk import task
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
hook = ClickHouseHook(clickhouse_conn_id="clickhouse")

# logging
import logging
logger = logging.getLogger(__name__)

def check_data_quality(upstream_result:dict, checks: dict = None):
    """
    `upstream_result` may be a dataframe or a string, if we need to check data quality of a table in the database. 
    In this case the string is the name of the table (with schema) subject to the checks.
    Example:
    check_data_quality(
        upstream_result={
            "data": df,
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        },
        checks=[
            {
                'check_name': 'record_count',
                'critical': False,

            }
        ]
    )

    If data is large, provide path to the saved file in `input_path`: 

    There is capability to write to the db a data quality check prepared beforehand. In this case provide `prepared_check` instead of `data` in upstream_result.
    Example:
    check_data_quality(
        upstream_result={
            "prepared_check": [{
                "check": "record_count",
                "critical": True,
                "status": 'ok',
                "factual_value_int": n_assessments_total
            }],
            "metadata": {
                'dag_id': context["dag"].dag_id,
                "run_id": context["dag_run"].run_id,
                "upstream_task_id": ti.task_id,
                "upstream_started_at": ti.start_date,
                "upstream_ended_at": datetime.now(),
            },
        }
    )
    """
    logger.info('Collecting meta info...')

    airflow_data = {
        "airflow__dag_id": upstream_result['metadata']['dag_id'],
        "airflow__run_id": upstream_result['metadata']['run_id'],
        "airflow__source_task_id": upstream_result['metadata']['upstream_task_id'],
        "airflow__source_start_date": upstream_result['metadata']['upstream_started_at'],
        "airflow__source_end_date": upstream_result['metadata']['upstream_ended_at'],
    }

    logger.info('Initializing class instance...')
    
    if 'prepared_check' in upstream_result: # if no data check result prepared beforehand available 
        df = pd.DataFrame(upstream_result['prepared_check'])
    else:
        if 'input_path' in upstream_result: # if path to file with data provided
            data_path = upstream_result['input_path']
            if data_path.endswith('json'):
                data = pd.read_json(data_path)
        elif 'db_table' in upstream_result:
            with hook.get_client() as client:
                data = client.query_df(f"select * from {upstream_result['db_table']}")
        elif 'data' in upstream_result:
            data = upstream_result['data']

        data_check_report = dataQualityReport(airflow_data=airflow_data, data=data, checks=checks)

        if data_check_report.critical_errors:
            logger.error('Data did not pass data quality check!')
            # raise AirflowFailException

        if data_check_report.noncritical_errors:
            logger.warning('Data quality checks passed with non-critical errors')

        df = data_check_report.data_quality_checks_df
        

    for c in airflow_data.keys():
        df[c] = airflow_data[c]

    logger.info('Writing to db...')

    # write to database
    with hook.get_client() as client:
        client.insert_df(
            table='data_quality.main',
            df=df,
            column_names=list(df.columns),
        )

class dataQualityReport:
    def __init__(self, airflow_data, data, checks: list):
        """
        Expected `checks` structure:
        [
            {'check_name': 'uniqueness', 'critical': True, 'check_subset': ['col1', 'col2']},
            {'check_name': 'completeness', 'critical': False, 'check_subset': 'col1'},
            {'check_name': 'completeness', 'critical': False, 'check_subset': 'col2'},
        ]
        """
        self.airflow_data = airflow_data

        self.critical_errors = False
        self.noncritical_errors = False

        assert isinstance(data, dict) or isinstance(data, pd.DataFrame), 'Unsupported data type of data!'
        self.data = data

        rows = []
        for criterion in checks:
            rows.append(self.check_criterion_(criterion))

        self.data_quality_checks_df = pd.DataFrame(rows)
        
    def check_criterion_(self, criterion_params: dict):
        """
        Run data quality check. Form a template and update it with the output obtained during the data check.
        If any critical data checks failed, continue returning templates for the later data checks, but do not run the checks themselves.
        """
        # template
        check_subset_ = criterion_params.get('check_subset')
        row_data = {
            'check': criterion_params['check_name'], 
            'factual_value_int': None, # output, optional
            'factual_value_str': None, # output, optional
            'check_subset': str(check_subset_), # optional  
            'status': None, # output
            'critical': criterion_params['critical'], 
            'comment': None # output, optional
        }
        # if not self.critical_errors:
        if True:
            try:
                check_function = getattr(self, criterion_params['check_name'])
                try:
                    check_result = check_function(**criterion_params)

                    row_data['status'] = check_result['status']
                    row_data['factual_value_int'] = check_result.get('factual_value_int')
                    row_data['factual_value_str'] = check_result.get('factual_value_str')
                    row_data['comment'] = check_result.get('comment')
                    
                except Exception as e:
                    row_data['status'] = 'error'
                    row_data['comment'] = str(e)
                    # return row_data
            except AttributeError:
                row_data['status'] = 'unsupported'
                row_data['comment'] = 'Unsupported criterion!'
                # return row_data
            
            if row_data['status'] != 'ok':
                if criterion_params['critical'] == True:
                    self.critical_errors = True
                elif criterion_params['critical'] == False:
                    self.noncritical_errors = True
        # else:
        #     row_data['comment'] = 'Could not run due to previous critical errors!'

        return row_data

    def record_count(self, **criterion_params):
        """
        Counts records or rows. If data is an API response JSON, look inside `main_json_key`

        Supported data types for data:
        - dict (JSON): pass dct[main_key]
        - pandas DataFrame

        Example:
        {'check': 'record_count', 'critical': True}
        """
        count = len(self.data)
        
        return {'status': 'ok' if count > 0 else 'failed', 'factual_value_int': count}

    def required_cols_present(self, **criterion_params):
        """
        Checks for the columns specified by `expected_values`. If data is an API response JSON, look inside `main_json_key`.

        Supported data types for data:
        - dict (JSON): pass dct[main_key]
        - pandas DataFrame

        Required parameters:
        - `check_subset`

        Example:
        {'check': 'required_cols_present', 'critical': True, 'check_subset': ['description', 'code']}
        """
        if isinstance(self.data, pd.DataFrame):
            fact_columns = self.data.columns

        if isinstance(self.data, dict):
            fact_columns = self.data[0].keys()

        condition = all([c in fact_columns for c in criterion_params['check_subset']])
        return {'status': 'ok' if condition else 'failed', 'factual_value_str': str(list(fact_columns))}

    def uniqueness(self, **criterion_params):
        """
        Check for uniqueness in the primary key. 
        To check multiple columns separately, pass parameters for the checks in separate dictionaries

        Supported data types for data:
        - pandas DataFrame

        - `check_subset`: list

        Example:
        {'check': uniqueness, 'critical': True, 'check_subset': ['description', 'code']}
        """
        if not isinstance(self.data, pd.DataFrame):
            raise Exception('Unsupported data type!')
        if not 'check_subset' in criterion_params:
            raise Exception('Please pass parameter `check_subset` to run uniqueness')

        duplicated_count = len(self.data[self.data.duplicated(subset=criterion_params['check_subset'])])
        if duplicated_count == 0:
            return {'status': 'ok'}
        return {'status': 'failed', 'comment': f' Found {duplicated_count} duplicated rows'}
        
    def completeness(self, **criterion_params):
        """
        Check for missing values in a column specified in `check_subset`.

        Supported data types for data:
        - pandas DataFrame

        - `check_subset`: str

        Example:
        {'check': completeness, 'critical': True, 'check_subset': 'description'}
        """
        if not isinstance(self.data, pd.DataFrame):
            raise Exception('Unsupported data type!')
        if not 'check_subset' in criterion_params:
            raise Exception('Please pass parameter `check_subset` to run completeness')
        if not isinstance(criterion_params['check_subset'], str):
            raise Exception('`check_subset` must be a string!')

        missing_count = self.data[criterion_params['check_subset']].isna().sum()
        if missing_count == 0:
            return {'status': 'ok'}
        return {'status': 'failed', 'comment': f' Found {missing_count} ({round(missing_count/len(self.data)*100, 2)}%) missing values'}

    def expected_values(self, **criterion_params):
        """
        Check for missing values in a column specified in `check_subset`.

        Supported data types for data:
        - pandas DataFrame

        - `expected_values`: list[str]

        Example:
        {'check': 'expected_values', 'critical': True, 'check_subset': 'red_list_category_version', 'expected_values': ["2.3", "3.1", "Earlier Version"]}
        """
        if not isinstance(self.data, pd.DataFrame):
            raise Exception('Unsupported data type!')
        if not 'check_subset' in criterion_params:
            raise Exception('Please pass parameter check_subset to run expacted_values')
        if not 'expected_values' in criterion_params:
            raise Exception('Please pass parameter expected_values to run expacted_values')
        if not isinstance(criterion_params['check_subset'], str):
            raise Exception('`check_subset` must be a string!')
        if not isinstance(criterion_params['expected_values'], list):
            raise Exception('`expected_values` must be a list!')

        actual_values = set(self.data[criterion_params['check_subset']])
        condition = actual_values.issubset(set(criterion_params['expected_values']))

        return {
            'status': 'ok' if condition else 'failed', 
            'factual_value_str': str(actual_values), 
            'comment': f'Expected {criterion_params["expected_values"]}'
        }
