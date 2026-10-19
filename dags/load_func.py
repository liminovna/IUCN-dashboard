from datetime import datetime
import logging
logger = logging.getLogger(__name__)
from pathlib import Path

import pandas as pd

from airflow.sdk import get_current_context, dag, task
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

from data_quality import check_data_quality
hook = ClickHouseHook(clickhouse_conn_id="clickhouse")

@task
def load__assessments(input_dir, schema_name='detail', table_name='assessments'):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    df['assessment_date'] = pd.to_datetime(df['assessment_date'], utc=True)

    # assessments_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    assessments_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        assessment_id                    UInt64,
        assessment_date                  Nullable(dateTime),
        year_published                   UInt16,
        latest                           Bool,
        possibly_extinct                 Bool,
        possibly_extinct_in_the_wild     Bool,
        sis_taxon_id                     UInt64,
        criteria                         String,
        url                              String,
        sis_id                           UInt64,
        scientific_name                  String,
        population_size                  LowCardinality(String) default 'empty',
        population_trend                 LowCardinality(String) default 'empty',
        population_trend_code            Float32 default 0.0,
        red_list_category_version        Float32 default 0.0,
        red_list_category                LowCardinality(String) default 'empty',
        red_list_category_code           LowCardinality(String) default 'empty',
        `CREATE_DTTM`                    DateTime default now()
    )
    ORDER BY (latest, red_list_category_code, year_published, scientific_name, sis_taxon_id, assessment_id)"""
    with hook.get_client() as client:
        client.command(assessments_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=list(df.columns),
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

@task
def load__taxonomy(input_dir, schema_name='detail', table_name='taxonomy'):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    # taxonomy_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    taxonomy_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        sis_taxon_id          UInt64,
        kingdom_name          LowCardinality(String) default 'empty',
        phylum_name           LowCardinality(String) default 'empty',
        class_name            LowCardinality(String) default 'empty',
        order_name            LowCardinality(String) default 'empty',
        family_name           LowCardinality(String) default 'empty',
        genus_name            LowCardinality(String) default 'empty',
        species_name          LowCardinality(String) default 'empty',
        scientific_name       String,
        `CREATE_DTTM`         DateTime default now()
    )
    order by (sis_taxon_id, scientific_name)"""
    with hook.get_client() as client:
        client.command(taxonomy_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=list(df.columns),
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

@task
def load__conservation_actions_in_place(input_dir, schema_name='detail', table_name='conservation_actions_in_place'):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    # ca_in_place_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    ca_in_place_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        assessment_id                 UInt64,
        conservation_action_name      LowCardinality(String) default 'empty',
        feature                       LowCardinality(String) default 'empty',
        value                         LowCardinality(String) default 'empty',
        `CREATE_DTTM`                 DateTime default now()
    )
    order by (assessment_id)"""
    with hook.get_client() as client:
        client.command(ca_in_place_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=list(df.columns),
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

@task
def load__conservation_actions(input_dir, schema_name='detail', table_name="conservation_actions"):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return
        
    # ca_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    ca_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        assessment_id   UInt64,
        code            LowCardinality(String) default 'empty',
        description     LowCardinality(String) default 'empty',
        `CREATE_DTTM`   DateTime default now()
    )
    order by (code, description, assessment_id)"""
    with hook.get_client() as client:
        client.command(ca_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=df.columns,
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

@task
def load__locations(input_dir, schema_name='detail', table_name="locations"):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    # locations_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    locations_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        assessment_id    UInt64,
        origin           LowCardinality(String) default 'empty',
        presence         LowCardinality(String) default 'empty',
        seasonality      Array(String),
        country          String,
        code             LowCardinality(String) default 'empty',
        `CREATE_DTTM`    DateTime default now()
    )
    ORDER BY (country, assessment_id)"""
    with hook.get_client() as client:
        client.command(locations_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=df.columns,
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

@task
def load__habitats(input_dir, schema_name='detail', table_name='habitats'):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    # habitat_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    habitat_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        assessment_id    UInt64,
        habitat          LowCardinality(String) default 'empty',
        code             LowCardinality(String) default 'empty',
        `CREATE_DTTM`    DateTime default now()
    )
    ORDER BY (code, habitat, assessment_id)"""
    with hook.get_client() as client:
        client.command(habitat_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=list(df.columns),
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

@task
def load__use_and_trade(input_dir, schema_name='detail', table_name='use_and_trade'):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    df[df.duplicated(subset=['assessment_id', 'use_and_trade'], keep=False)]

    # uat_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    uat_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        assessment_id    UInt64,
        international    Bool default False,
        national         Bool default False,
        other            Bool default False,
        subsistence      Bool default False,
        use_and_trade    LowCardinality(String) default 'empty',
        code             UInt16,
        `CREATE_DTTM`    DateTime default now()
    )
    ORDER BY (code, use_and_trade, assessment_id)"""
    with hook.get_client() as client:
        client.command(uat_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=list(df.columns),
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

@task
def load__threats(input_dir, schema_name='detail', table_name='threats'):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return
    with hook.get_client() as client:
        levels = client.query_df('select code, threat_lvl1, threat_lvl2, threat_lvl3 from dict.threats__dict')

    df['timing'] = df['timing'].fillna('Unknown')
    df = pd.merge(left=df, right=levels, on=('code'), how='left')

    # threats_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    threats_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        assessment_id    UInt64,
        timing           String,
        threat           LowCardinality(String) default 'empty',
        code             String,
        threat_lvl1      LowCardinality(String) default 'empty',
        threat_lvl2      LowCardinality(String) default 'empty',
        threat_lvl3      LowCardinality(String) default 'empty',
        `CREATE_DTTM`    DateTime default now()
    )
    ORDER BY (code, threat_lvl1, threat, assessment_id)"""
    with hook.get_client() as client:
        query_result = client.command(threats_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=list(df.columns),
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

@task
def load__stresses(input_dir, schema_name='detail', table_name='stresses'):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return
    with hook.get_client() as client:
        levels = client.query_df('select code, stress_lvl1, stress_lvl2, stress_lvl3 from dict.stresses__dict')

    df = pd.merge(left=df, right=levels, on=('code'), how='left')

    # stresses_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    stresses_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        assessment_id    UInt64,
        stress           LowCardinality(String) default 'empty',
        code             LowCardinality(String) default 'empty',
        stress_lvl1      LowCardinality(String) default 'empty',
        stress_lvl2      LowCardinality(String) default 'empty',
        stress_lvl3      LowCardinality(String) default 'empty',
        `CREATE_DTTM`    DateTime default now()
    )
    ORDER BY (stress_lvl1, stress, code, assessment_id)"""
    with hook.get_client() as client:
        query_result = client.command(stresses_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=list(df.columns),
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

@task
def load__systems(input_dir, schema_name='detail', table_name='systems'):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    df['code'] = df['code'].astype(str)

    # systems_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    systems_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        assessment_id    UInt64,
        system           LowCardinality(String) default 'empty',
        code             LowCardinality(String) default 'empty',
        `CREATE_DTTM`    DateTime default now()
    )
    ORDER BY (code, system, assessment_id)"""
    with hook.get_client() as client:
        query_result = client.command(systems_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=list(df.columns),
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

@task
def load__scopes(input_dir, schema_name='detail', table_name='scopes'):
    input_dir = Path(input_dir)
    input_path = input_dir / f'{table_name}_db.parquet'
    df = pd.read_parquet(input_path)
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return
        
    df.drop_duplicates(inplace=True)

    # scopes_ddl = f"""DROP TABLE IF EXISTS {schema_name}.{table_name}"""
    scopes_ddl = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        assessment_id    UInt64,
        scope            LowCardinality(String) default 'empty',
        code             LowCardinality(String) default 'empty',
        `CREATE_DTTM`    DateTime default now()
    )
    ORDER BY (code, scope, assessment_id)"""
    with hook.get_client() as client:
        query_result = client.command(scopes_ddl)

        # delete rows that have been added today
        query_result = client.command(f'delete from {schema_name}.{table_name} where `CREATE_DTTM`::date = current_date')
        logging.info('%s', query_result.summary)

        client.insert_df(
            table=f'{schema_name}.{table_name}',
            df=df,
            column_names=list(df.columns),
        )

    # ===================
    # Data quality checks
    # - CR: number of rows
    # ===================
    context = get_current_context()
    ti = context["ti"]
    dq_data = {
        "input_dir": input_dir,
        "metadata": {
            'dag_id': context["dag"].dag_id,
            "run_id": context["dag_run"].run_id,
            "upstream_task_id": ti.task_id,
            "upstream_started_at": ti.start_date,
            "upstream_ended_at": datetime.now(),
        },
    }
    dq_data.update({'db_table': f'{schema_name}.{table_name}'})
    check_data_quality(
        upstream_result=dq_data,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

