from datetime import datetime
import json
import os
from pathlib import Path
from typing_extensions import Literal

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
    def update_table(host: str, port: str, username: str, password: str, sql_path: list):
        """
        Create datamart if it does not exist and run sql script from sql/*.sql

        TODO: integrate clickhouse provider
        """
        sql_commands = Path(*sql_path).read_text(encoding="utf-8")
        with cc.get_client(host=host, port=port, username=username, password=password) as client:
            statements = split_sql_statements(sql_commands)

            for statement in statements:
                if not statement.strip():
                    continue

                # execute
                query_result = client.command(statement)
                logger.info('%s', query_result.summary)

    @task
    def check_data_quality(data: json, params):
        """
        TODO: Run data quality checks and write to data_quality.main

        columns:
            - airflow__run_id: int
            - airflow__run_type: string
            - airflow__task_id: int
            - airflow__dag_id: int
            - airflow__start_date: datetime
            - airflow__end_date: datetime
            - airflow__status string

            - details: json
                - uniqueness
                - completeness
                - unexpected values (?)
                - referencial integrity
                - freshness (last update datetime)
        """
        raise NotImplemented

    # sequence
    update_table.override(
        task_id="load_overview__datamart",
    )(
        host=host, port=port, username=username, password=password,
        sql_path=[SQL_DIR, 'overview__datamart.sql']
    )
    # check_data_quality

    update_table.override(
        task_id="load_countries__datamart",
    )(
        host=host, port=port, username=username, password=password,
        sql_path=[SQL_DIR, 'countries__datamart.sql']
    )
    # check_data_quality

    update_table.override(
        task_id="load_species__datamart",
    )(
        host=host, port=port, username=username, password=password,
        sql_path=[SQL_DIR, 'species__datamart.sql']
    )
    # check_data_quality

    update_table.override(
        task_id="load_changes__datamart",
    )(
        host=host, port=port, username=username, password=password,
        sql_path=[SQL_DIR, 'changes__datamart.sql']
    )
    # check_data_quality

    update_table.override(
        task_id="load_threats__datamart",
    )(
        host=host, port=port, username=username, password=password,
        sql_path=[SQL_DIR, 'threats__datamart.sql']
    )
    # check_data_quality

iucn_update__datamarts()