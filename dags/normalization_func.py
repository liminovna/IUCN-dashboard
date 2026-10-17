from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
import numpy as np

from airflow.sdk import get_current_context, task

import logging
logger = logging.getLogger(__name__)

from data_quality import check_data_quality

@task
def normalize__taxonomy(input_path: str, output_dir: Path) -> None:
    """
    Save taxonomy info on all unique species.
    Before saving, 
    - drop fully duplicated rows
    - check for multiple unique sis_taxon_id per scientific_name and vice versa
    - get columns with missing values
    """
    cols = [
        'sis_taxon_id',
        'kingdom_name',
        'phylum_name',
        'class_name',
        'order_name',
        'family_name',
        'genus_name',
        'species_name',
        'scientific_name'
    ]
    tax = pd.read_json(input_path)[cols].drop_duplicates() # since original data is denormalized (multiple assessments per one species) there has to be duplicates
    
    logger.warning('Checking quality of table %s', sys._getframe().f_code.co_name)

    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': tax,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - uniquness on (sis_taxon_id, scientific_name)
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'critical': False,
                'check_subset': ('sis_taxon_id', 'scientific_name')
            },
        ]
    )

    # save file
    tax.to_parquet(output_dir / 'taxonomy_db.parquet')

@task
def normalize__assessments(input_path: str, output_dir: Path) -> None:
    """
    Save info on all assessments.
    Before saving, 
    - drop fully duplicated rows
    - check for multiple unique sis_taxon_id per scientific_name and vice versa
    - get columns with missing values
    """
    # value determines whether to check the column for completeness
    cols = [
        'assessment_id',
        'assessment_date',
        'year_published',
        'latest',
        'possibly_extinct',
        'possibly_extinct_in_the_wild',
        'sis_taxon_id',
        'criteria',
        'url',
        'sis_id',
        'scientific_name',
        'population_size',
        'population_trend',
        'population_trend_code',
        'red_list_category_version',
        'red_list_category',
        'red_list_category_code'
    ]
    assessments = pd.read_json(input_path)[cols]

    # ===================
    # Transformations
    # ===================
    assessments['assessment_date'] = pd.to_datetime(assessments['assessment_date'], utc=True, errors='coerce')

    assessments.drop_duplicates(inplace=True)
    assessments['population_trend'] = assessments['population_trend'].fillna('Unknown')
    assessments['red_list_category_code'] = assessments['red_list_category_code'].fillna('NA')

    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': assessments,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - completeness on assessment_date
    # - completeness on population_trend
    # - completeness on red_list_category_code
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            },
            {
                'check_name': 'completeness',
                'critical': False,
                'check_subset': 'assessment_date'
            },
            {
                'check_name': 'completeness',
                'critical': False,
                'check_subset': 'population_trend'
            },
            {
                'check_name': 'completeness',
                'critical': False,
                'check_subset': 'red_list_category_code'
            },
        ]
    )

    assessments.to_parquet(output_dir / 'assessments_db.parquet')

@task
def normalize__conservation_actions_in_place(input_path: str, output_dir: Path) -> None:
    """
    Save info on all conservation_actions_in_place.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    # ===================
    # Transformations
    # ===================
    conservation_actions = pd.read_json(input_path)[['assessment_id', 'conservation_actions_in_place']]

    conservation_actions = conservation_actions.explode(['conservation_actions_in_place']).reset_index(drop=True)
    conservation_actions.dropna(subset=['conservation_actions_in_place'], inplace=True)

    conservation_actions = conservation_actions.join(conservation_actions['conservation_actions_in_place'].apply(pd.Series))

    conservation_actions = conservation_actions.explode('actions').reset_index(drop=True)
    conservation_actions.rename(columns={'name': 'conservation_action_name'}, inplace=True)

    conservation_actions = conservation_actions.join(conservation_actions['actions'].apply(pd.Series))

    conservation_actions.drop(columns=['conservation_actions_in_place', 'actions'], inplace=True)

    conservation_actions.rename(columns={'name': 'feature'}, inplace=True)

    conservation_actions.drop_duplicates(inplace=True)

    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': conservation_actions,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - uniquenesss on (assessment_id, conservation_action_name, feature)
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'critical': False,
                'check_subset': ('assessment_id', 'conservation_action_name', 'feature')
            }
        ]
    )

    conservation_actions.to_parquet(output_dir / 'conservation_actions_in_place_db.parquet')

@task
def normalize__conservation_actions(input_path: str, output_dir: Path) -> None:
    """
    Save info on all conservation_actions.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    # ===================
    # Transformations
    # ===================
    conservation_actions_2 = pd.read_json(input_path)[['assessment_id', 'conservation_actions']]

    conservation_actions_2 = conservation_actions_2.explode('conservation_actions')
    conservation_actions_2.dropna(subset=['conservation_actions'], inplace=True)
    conservation_actions_2.reset_index(drop=True, inplace=True)

    conservation_actions_2 = conservation_actions_2.join(conservation_actions_2['conservation_actions'].apply(pd.Series))

    conservation_actions_2['description'] = conservation_actions_2['description'].apply(pd.Series)
    conservation_actions_2.drop(columns=['conservation_actions', 'note'], inplace=True)

    conservation_actions_2.drop_duplicates(inplace=True)

    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': conservation_actions_2,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - uniquenesss on (assessment_id, conservation_action_name, feature)
    # - completeness on description
    # - completeness on code
    # TODO: referential integrity on (code, description)
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'critical': False,
                'check_subset': ('assessment_id', 'code', 'description')
            },
            {
                'check_name': 'completeness',
                'critical': False,
                'check_subset': 'code'
            },
            {
                'check_name': 'completeness',
                'critical': False,
                'check_subset': 'description'
            }
        ]
    )

    conservation_actions_2.to_parquet(output_dir / 'conservation_actions_db.parquet')

@task
def normalize__locations(input_path: str, output_dir: Path) -> None:
    """
    Save mapping assessment - country.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    # ===================
    # Transformations
    # ===================
    locations = pd.read_json(input_path)[['assessment_id', 'locations']]
    locations = locations.explode('locations')

    # # delete rows where location code is compound
    # https://en.wikipedia.org/wiki/List_of_codes_used_in_the_World_Geographical_Scheme_for_Recording_Plant_Distributions
    # pairs = [
    #     ('RS', 'YUG-SE'), # Serbia 
    #     ('AO', 'ANG-OO'), # Angola
    #     ('AM', 'TCS-AR'), # Armenia
    #     ('GE', 'GEO-OO'), # Georgia (U.S. state)
    #     ('AS', 'SAM-AS'), # American Samoa
    # ]
    # # locate rows with duplicated ('assessment_id', 'country') and delete the one with the compound code
    # locations.drop(
    #     locations[(locations.duplicated(['assessment_id', 'country'], keep=False)) & (locations['code'].isin([p[1] for p in pairs]))].index,
    #     inplace=True
    # )

    locations = locations.join(locations['locations'].apply(pd.Series))
    
    locations.drop(columns=['locations'], inplace=True)
    locations.drop_duplicates(subset=['assessment_id', 'origin', 'presence', 'country', 'code'], inplace=True)

    # fill empty values
    locations['seasonality'] = locations['seasonality'].apply(lambda x: ['Seasonal Occurrence Uncertain'] if x is np.nan else x)

    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': locations,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - TODO: referential integrity on (code, country)
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            }
        ]
    )

    locations.to_parquet(output_dir / 'locations_db.parquet')

@task
def normalize__habitats(input_path: str, output_dir: Path) -> None:
    """
    Save mapping assessment - habitats.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    habitats = pd.read_json(input_path)[['assessment_id','habitats']]

    # ===================
    # Transformations
    # ===================
    habitats = habitats.explode('habitats').reset_index(drop=True)

    habitats = habitats.join(habitats['habitats'].apply(pd.Series))

    habitats.drop(columns=['habitats', 'season'], inplace=True)
    habitats.drop_duplicates(subset=['assessment_id', 'habitat'], inplace=True)

    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': habitats,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - CR: uniqueness on (assessment_id, habitat)
    # - TODO: referential integrity on (code, habitat)
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'critical': False,
                'check_subset': ('assessment_id', 'habitat')
            },
        ]
    )

    habitats.to_parquet(output_dir / 'habitats_db.parquet')

@task
def normalize__use_and_trade(input_path: str, output_dir: Path) -> None:
    """
    Save mapping assessment - use_and_trade.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    use_and_trade = pd.read_json(input_path)[['assessment_id','use_and_trade']]

    # ===================
    # Transformations
    # ===================

    use_and_trade = use_and_trade.explode('use_and_trade').reset_index(drop=True)
    use_and_trade = use_and_trade.dropna(subset=['use_and_trade']).reset_index(drop=True)

    use_and_trade = use_and_trade.join(use_and_trade['use_and_trade'].apply(pd.Series))

    use_and_trade.drop(columns=['use_and_trade'], inplace=True)

    use_and_trade['description'] = use_and_trade['description'].apply(lambda x: x['en'])
    use_and_trade.rename(columns={'description': 'use_and_trade'}, inplace=True)

    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': use_and_trade,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - CR: uniqueness on (assessment_id, code)
    # - TODO: referential integrity on (use_and_trade, code)
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'critical': False,
                'check_subset': ('assessment_id', 'code')
            },
        ]
    )

    use_and_trade.to_parquet(output_dir / 'use_and_trade_db.parquet')

@task
def normalize__threats(input_path: str, output_dir: Path) -> None:
    """
    Save mapping assessment - threats.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    threats = pd.read_json(input_path)[['assessment_id','threats']]

    # ===================
    # Transformations
    # ===================

    threats = threats.explode('threats').reset_index(drop=True)
    threats = threats.dropna(subset=['threats']).reset_index(drop=True)

    threats = threats.join(threats['threats'].apply(pd.Series))
    
    threats.drop(columns=['threats'], inplace=True)
    threats.drop_duplicates(inplace=True)

    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': threats,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - CR: uniqueness on (assessment_id, code)
    # - CR: completeness on code
    # - completeness on timing
    # - TODO: referential integrity on (threat, code)
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'critical': True,
                'check_subset': ('assessment_id', 'code')
            },
            {
                'check_name': 'completeness',
                'critical': True,
                'check_subset': 'code'
            },
            {
                'check_name': 'completeness',
                'critical': False,
                'check_subset': 'timing'
            },
        ]
    )
    threats.to_parquet(output_dir / 'threats_db.parquet')

@task
def normalize__stresses(input_path: str, output_dir: Path) -> None:
    """
    Save mapping assessment - stresses.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    stresses = pd.read_json(input_path)[['assessment_id','stresses']]

    # ===================
    # Transformations
    # ===================

    stresses = stresses.explode('stresses').reset_index(drop=True)
    stresses = stresses.dropna(subset=['stresses']).reset_index(drop=True)

    stresses = stresses.join(stresses['stresses'].apply(pd.Series))

    stresses.drop(columns=['stresses'], inplace=True)
    stresses.drop_duplicates(inplace=True)

    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': stresses,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - CR: uniqueness on (assessment_id, code)
    # - CR: completeness on code
    # - TODO: referential integrity on (stress, code)
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'critical': True,
                'check_subset': ('assessment_id', 'code')
            },
            {
                'check_name': 'completeness',
                'critical': True,
                'check_subset': 'code'
            }
        ]
    )
    stresses.to_parquet(output_dir / 'stresses_db.parquet')

@task
def normalize__systems(input_path: str, output_dir: Path) -> None:
    """
    Save mapping assessment - systems.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    systems = pd.read_json(input_path)[['assessment_id','systems']]

    # ===================
    # Transformations
    # ===================
    systems = systems.explode('systems').reset_index(drop=True)
    systems = systems.dropna(subset=['systems']).reset_index(drop=True)

    systems = systems.join(systems['systems'].apply(pd.Series))
    systems.drop(columns=['systems'], inplace=True)
    systems.drop_duplicates(inplace=True)

    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': systems,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - CR: uniqueness on (assessment_id, code)
    # - CR: completeness on code
    # - TODO: referential integrity on (system, code)
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'critical': True,
                'check_subset': ('assessment_id', 'code')
            },
            {
                'check_name': 'completeness',
                'critical': True,
                'check_subset': 'code'
            }
        ]
    )
    systems.to_parquet(output_dir / 'systems_db.parquet')

@task
def normalize__scopes(input_path: str, output_dir: Path) -> None:
    """
    Save mapping assessment - scopes.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    scopes = pd.read_json(input_path)[['assessment_id','scopes']]

    # ===================
    # Transformations
    # ===================

    scopes = scopes.explode('scopes').reset_index(drop=True)
    scopes = scopes.dropna(subset=['scopes']).reset_index(drop=True)

    scopes = scopes.join(scopes['scopes'].apply(pd.Series))
    scopes.drop(columns=['scopes'], inplace=True)
    scopes.drop_duplicates(inplace=True)
    
    # ====================================
    # Prepare data on current task context
    # ====================================
    context = get_current_context()
    ti = context["ti"]
    upstream_result = {
        'data': scopes,
        'metadata': {
            'dag_id': context["dag"].dag_id,
            'run_id': context["dag_run"].run_id,
            'upstream_task_id': ti.task_id,
            'upstream_started_at': ti.start_date,
            'upstream_ended_at': datetime.now(),
        },
    }

    # ===================
    # Data quality checks
    # - CR: number of rows
    # - CR: uniqueness on (assessment_id, code)
    # - CR: completeness on code
    # - TODO: referential integrity on (scope, code)
    # ===================
    check_data_quality(
        upstream_result=upstream_result,
        checks=[
            {
                'check_name': 'record_count',
                'critical': True,
            },
            {
                'check_name': 'uniqueness',
                'critical': True,
                'check_subset': ('assessment_id', 'code')
            },
            {
                'check_name': 'completeness',
                'critical': True,
                'check_subset': 'code'
            }
        ]
    )
    scopes.to_parquet(output_dir / 'scopes_db.parquet')
