from datetime import datetime
import logging
logger = logging.getLogger(__name__)

import pandas as pd

def load__assessments(client, input_dir, table_name='assessments'):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    df['assessment_date'] = pd.to_datetime(df['assessment_date'], utc=True)

    # assessments_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    assessments_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        assessment_id                    UInt64,
        assessment_date                  Nullable(dateTime),
        year_published                   UInt16,
        latest                           Nullable(Bool),
        possibly_extinct                 Nullable(Bool),
        possibly_extinct_in_the_wild     Nullable(Bool),
        sis_taxon_id                     Nullable(UInt64),
        criteria                         Nullable(String),
        url                              Nullable(String),
        sis_id                           Nullable(UInt64),
        scientific_name                  String,
        population_size                  Nullable(String),
        population_trend                 Nullable(String),
        population_trend_code            Nullable(Float32),
        red_list_category_version        Nullable(Float32),
        red_list_category                Nullable(String),
        red_list_category_code           Nullable(String),
        `CREATE_DTTM`                    DateTime default now()
    )
    PRIMARY KEY (assessment_id)"""
    client.command(assessments_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=list(df.columns),
    )

def load__taxonomy(client, input_dir, table_name='taxonomy'):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    # taxonomy_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    taxonomy_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        sis_taxon_id          UInt64,
        kingdom_name          Nullable(String),
        phylum_name           Nullable(String),
        class_name            Nullable(String),
        order_name            Nullable(String),
        family_name           Nullable(String),
        genus_name            Nullable(String),
        species_name          Nullable(String),
        scientific_name       String,
        `CREATE_DTTM`         DateTime default now()
    )
    PRIMARY KEY (scientific_name)"""
    client.command(taxonomy_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=list(df.columns),
    )

def load__conservation_actions_in_place(client, input_dir, table_name='conservation_actions_in_place'):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    # ca_in_place_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    ca_in_place_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        assessment_id                 UInt64,
        conservation_action_name      Nullable(String),
        feature                       Nullable(String),
        value                         Nullable(String),
        `CREATE_DTTM`                 DateTime default now()
    )"""
    client.command(ca_in_place_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=list(df.columns),
    )

def load__conservation_actions(client, input_dir, table_name="conservation_actions"):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return
        
    # ca_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    ca_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        assessment_id   UInt64,
        code            String,
        description     String,
        `CREATE_DTTM`   DateTime default now()
    )
    PRIMARY KEY (assessment_id, description)"""
    client.command(ca_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=df.columns,
    )

def load__locations(client, input_dir, table_name="locations"):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    # locations_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    locations_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        assessment_id    UInt64,
        origin           Nullable(String),
        presence         Nullable(String),
        seasonality      Array(String),
        country          String,
        code             Nullable(String),
        `CREATE_DTTM`    DateTime default now()
    )
    PRIMARY KEY (assessment_id, country)"""
    client.command(locations_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=df.columns,
    )

def load__habitats(client, input_dir, table_name='habitats'):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    # habitat_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    habitat_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        assessment_id    UInt64,
        habitat          String,
        code             String,
        `CREATE_DTTM`    DateTime default now()
    )
    PRIMARY KEY (assessment_id, habitat)"""
    client.command(habitat_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=list(df.columns),
    )

def load__use_and_trade(client, input_dir, table_name='use_and_trade'):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    df[df.duplicated(subset=['assessment_id', 'use_and_trade'], keep=False)]

    # uat_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    uat_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        assessment_id    UInt64,
        international    Nullable(Bool),
        national         Nullable(Bool),
        other            Nullable(Bool),
        subsistence      Nullable(Bool),
        use_and_trade    String,
        code             UInt16,
        `CREATE_DTTM`    DateTime default now()
    )
    PRIMARY KEY (assessment_id, use_and_trade)"""
    client.command(uat_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=list(df.columns),
    )

def load__threats(client, input_dir: str, table_name='threats'):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    levels = client.query_df('select code, threat_lvl1, threat_lvl2, threat_lvl3 from threats__dict')

    df['timing'] = df['timing'].fillna('Unknown')
    df = pd.merge(left=df, right=levels, on=('code'), how='left')

    # threats_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    threats_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        assessment_id    UInt64,
        timing           String,
        threat           Nullable(String),
        code             String,
        threat_lvl1      Nullable(String),
        threat_lvl2      Nullable(String),
        threat_lvl3      Nullable(String),
        `CREATE_DTTM`    DateTime default now()
    )
    PRIMARY KEY (assessment_id, timing, code)"""
    query_result = client.command(threats_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=list(df.columns),
    )

def load__stresses(client, input_dir, table_name='stresses'):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    levels = client.query_df('select code, stress_lvl1, stress_lvl2, stress_lvl3 from stresses__dict')

    df = pd.merge(left=df, right=levels, on=('code'), how='left')

    # stresses_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    stresses_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        assessment_id    UInt64,
        stress           String,
        code             String,
        stress_lvl1      Nullable(String),
        stress_lvl2      Nullable(String),
        stress_lvl3      Nullable(String),
        `CREATE_DTTM`    DateTime default now()
    )
    PRIMARY KEY (assessment_id, code)"""
    query_result = client.command(stresses_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=list(df.columns),
    )

def load__systems(client, input_dir, table_name='systems'):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return

    df['code'] = df['code'].astype(str)

    # systems_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    systems_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        assessment_id    UInt64,
        system           String,
        code             String,
        `CREATE_DTTM`    DateTime default now()
    )
    PRIMARY KEY (assessment_id, system)"""
    query_result = client.command(systems_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=list(df.columns),
    )

def load__scopes(client, input_dir, table_name='scopes'):
    df = pd.read_parquet(input_dir / f'{table_name}_db.parquet')
    if df.empty:
        logging.warning('Empty dataframe %s', table_name)
        return
        
    df.drop_duplicates(inplace=True)

    # scopes_ddl = f"""DROP TABLE IF EXISTS detail.{table_name}"""
    scopes_ddl = f"""CREATE TABLE IF NOT EXISTS detail.{table_name} (
        assessment_id    UInt64,
        scope            String,
        code             String,
        `CREATE_DTTM`    DateTime default now()
    )
    PRIMARY KEY (assessment_id, scope)"""
    query_result = client.command(scopes_ddl)

    # delete rows that have been added today
    query_result = client.command(f'delete from detail.{table_name} where `CREATE_DTTM`::date = current_date')
    logging.info('%s', query_result.summary)

    client.insert_df(
        table=table_name,
        df=df,
        column_names=list(df.columns),
    )

