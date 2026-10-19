from datetime import datetime
import os
from pathlib import Path

from data_quality import check_data_quality

import pandas as pd

from airflow.sdk import get_current_context, dag, task
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

# =====================================
# Import credentials
# =====================================
from dotenv import load_dotenv
load_dotenv()

token = os.environ.get('iucn_token')
url = os.environ.get('iucn_api_url')

hook = ClickHouseHook(clickhouse_conn_id="clickhouse")

# =====================================
# Initialize logger
# =====================================
import logging
logger = logging.getLogger(__name__)

def split_sql_statements(sql_text: str) -> list[str]:
    return [
        statement.strip()
        for statement in sql_text.split(";")
        if statement.strip()
    ]

SQL_DIR = Path(f'/opt/airflow/dags/sql')

# =============================================
# DAG
# =============================================
dag_doc = """
# IUCN datamarts: Clickhouse to Clickhouse
DAG runs SQL queries and updates datamarts.
"""
@dag(
    dag_id="iucn_update__datamarts",
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
    render_template_as_native_obj=True,
    schedule=None, # only manual launch
    start_date=datetime(2026, 10, 1, 23, 0),
    catchup=False,
    tags=['IUCN', 'API', 'ClickHouse', 'datamart', 'replace', 'increment'],
    doc_md=dag_doc
)
def iucn_update__datamarts():
    @task
    def update_datamart__overview(sql_path: list):
        """
        Create datamart if it does not exist and run sql script from sql/*.sql
        """
        table_name = 'datamart.overview__datamart'
        sql_commands = Path(*sql_path).read_text(encoding="utf-8")
        with hook.get_client() as client:
            statements = split_sql_statements(sql_commands)

            for statement in statements:
                if not statement.strip():
                    continue

                # execute
                query_result = client.command(statement)
                logger.info('%s', query_result.summary)

            # ===============================
            # Data quality check inside the database
            # - CR: table not empty
            # - CR: completeness on assessment_id
            # - CR: completeness on scientific_name
            # - CR: completeness on kingdom_name
            # - CR: completeness on population_trend
            # - CR: completeness on red_list_category
            # - CR: completeness on red_list_category_code
            # - CR: completeness on system
            # ===============================
            context = get_current_context()
            ti = context["ti"]

            upstream_result = {
                "db_table": table_name,
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
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'kingdom_name'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'scientific_name'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'population_trend'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'red_list_category'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'red_list_category_code'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'category_sorted'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'system'
                    }
                ] 
            )

    @task
    def update_datamart__countries(sql_path: list):
        """
        Create datamart if it does not exist and run sql script from sql/*.sql
        """
        table_name = 'datamart.countries__datamart'
        sql_commands = Path(*sql_path).read_text(encoding="utf-8")
        with hook.get_client() as client:
            statements = split_sql_statements(sql_commands)

            for statement in statements:
                if not statement.strip():
                    continue

                # execute
                query_result = client.command(statement)
                logger.info('%s', query_result.summary)

            # ===============================
            # Data quality check inside the database
            # - CR: table not empty
            # - CR: completeness on assessment_id
            # - CR: completeness on code
            # - CR: completeness on country
            # - CR: uniqueness on (assessment_id, code)
            # ===============================
            context = get_current_context()
            ti = context["ti"]

            upstream_result = {
                "db_table": table_name,
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
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'code'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'country'
                    },
                    {
                        'check_name': 'uniqueness',
                        'critical': True,
                        'check_subset': ('assessment_id', 'code')
                    }
                ] 
            )

    @task
    def update_datamart__species(sql_path: list):
        """
        Create datamart if it does not exist and run sql script from sql/*.sql
        """
        table_name = 'datamart.species__datamart'
        sql_commands = Path(*sql_path).read_text(encoding="utf-8")
        with hook.get_client() as client:
            statements = split_sql_statements(sql_commands)

            for statement in statements:
                if not statement.strip():
                    continue

                # execute
                query_result = client.command(statement)
                logger.info('%s', query_result.summary)

            # ===============================
            # Data quality check inside the database
            # - CR: table not empty
            # - CR: completeness on assessment_id
            # - CR: uniqueness on assessment_id
            # - CR: completeness on scientific_name
            # - CR: completeness on kingdom_name
            # - CR: completeness on phylum_name
            # - CR: completeness on class_name
            # - CR: completeness on order_name
            # - CR: completeness on family_name
            # - CR: completeness on genus_name
            # - CR: completeness on species_name
            # - CR: completeness on population_trend
            # - CR: completeness on red_list_category
            # - CR: completeness on red_list_category_code
            # - completeness on url
            # ===============================
            context = get_current_context()
            ti = context["ti"]

            upstream_result = {
                "db_table": table_name,
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
                        'check_subset': 'kingdom_name'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'phylum_name'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'class_name'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'order_name'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'family_name'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'genus_name'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'species_name'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'scientific_name'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'population_trend'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'red_list_category'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'red_list_category_code'
                    }
                ] 
            )

    @task
    def update_datamart__changes(sql_path: list):
        """
        Create datamart if it does not exist and run sql script from sql/*.sql
        """
        table_name = 'datamart.changes__datamart'
        sql_commands = Path(*sql_path).read_text(encoding="utf-8")
        with hook.get_client() as client:
            statements = split_sql_statements(sql_commands)

            for statement in statements:
                if not statement.strip():
                    continue

                # execute
                query_result = client.command(statement)
                logger.info('%s', query_result.summary)

            # ===============================
            # Data quality check inside the database
            # - CR: table not empty
            # - CR: completeness on assessment_id
            # - CR: completeness on n_assessments
            # - CR: completeness on assessment_rank
            # ===============================
            context = get_current_context()
            ti = context["ti"]

            upstream_result = {
                "db_table": table_name,
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
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'n_assessments'
                    },
                    {
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'assessment_rank'
                    }
                ] 
            )

    @task
    def update_datamart__threats(sql_path: list):
        """
        Create datamart if it does not exist and run sql script from sql/*.sql
        """
        table_name = 'datamart.threats__datamart'
        sql_commands = Path(*sql_path).read_text(encoding="utf-8")
        with hook.get_client() as client:
            statements = split_sql_statements(sql_commands)

            for statement in statements:
                if not statement.strip():
                    continue

                # execute
                query_result = client.command(statement)
                logger.info('%s', query_result.summary)

            # ===============================
            # Data quality check inside the database
            # - CR: table not empty
            # - CR: completeness on assessment_id
            # - CR: completeness on threat
            # - CR: uniqueness on (assessment_id, threat)
            # ===============================
            context = get_current_context()
            ti = context["ti"]

            upstream_result = {
                "db_table": table_name,
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
                        'check_name': 'completeness',
                        'critical': True,
                        'check_subset': 'threat'
                    },
                    {
                        'check_name': 'uniqueness',
                        'critical': True,
                        'check_subset': ('assessment_id', 'threat', 'habitat')
                    }
                ] 
            )

    update_datamart__overview(sql_path=[SQL_DIR, 'overview__datamart.sql'])
    update_datamart__countries(sql_path=[SQL_DIR, 'countries__datamart.sql'])
    update_datamart__species(sql_path=[SQL_DIR, 'species__datamart.sql'])
    update_datamart__changes(sql_path=[SQL_DIR, 'changes__datamart.sql'])
    update_datamart__threats(sql_path=[SQL_DIR, 'threats__datamart.sql'])

iucn_update__datamarts()