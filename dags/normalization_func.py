import os
from pathlib import Path
import sys

import pandas as pd
import numpy as np

import logging
logger = logging.getLogger(__name__)

def save__taxonomy(data: pd.DataFrame, output_dir: Path) -> None:
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
    tax = data[cols].drop_duplicates() # since original data is denormalized (multiple assessments per one species) there has to be duplicates
    
    # ===================
    # Data quality checks
    # ===================

    logger.warning('Checking quality of table %s', sys._getframe().f_code.co_name)

    # check one scientific_name -> multiple sis_taxon_id
    counts_ = tax.groupby('scientific_name')['sis_taxon_id'].nunique()
    check_ = counts_[counts_>1].index.to_list()
    if len(check_):
        logger.warning('scientific_name with >1 sis_taxon_id: %s', check_)

    # check one sis_taxon_id -> multiple scientific_name
    counts_ = tax.groupby('sis_taxon_id')['scientific_name'].nunique()
    check_ = counts_[counts_>1].index.to_list()
    if len(check_):
        logger.warning('sis_taxon_id with >1 scientific_name: %s', check_)

    # check completeness
    counts_ = tax.isna().sum()
    check_ = counts_[counts_>1].index.to_list()
    if len(check_):
        logger.warning('Data missing partially or completely missing in %s', check_)

    # save file
    logger.info('%s', tax.isna().sum())
    tax.to_parquet(output_dir / 'taxonomy_db.parquet')

def save__assessments(data: pd.DataFrame, output_dir: Path) -> None:
    """
    Save info on all assessments.
    Before saving, 
    - drop fully duplicated rows
    - check for multiple unique sis_taxon_id per scientific_name and vice versa
    - get columns with missing values
    """
    # value determines whether to check the column for completeness
    cols = {
        'assessment_id': True,
        'assessment_date': True,
        'year_published': True,
        'latest': True,
        'possibly_extinct': False,
        'possibly_extinct_in_the_wild': False,
        'sis_taxon_id': True,
        'criteria': False,
        'url': True,
        'sis_id': True,
        'scientific_name': True,
        'population_size': False,
        'population_trend': True,
        'population_trend_code': True,
        'red_list_category_version': True,
        'red_list_category': True,
        'red_list_category_code': True
    }
    assessments = data[cols.keys()]
    data['assessment_date'] = pd.to_datetime(data['assessment_date'], utc=True, errors='coerce')

    # ===================
    # Data quality checks
    # ===================
    logger.warning('Checking quality of table %s', sys._getframe().f_code.co_name)

    # check for non-unique assessment_id
    counts_ = assessments['assessment_id'].value_counts()
    check_ = counts_[counts_>1].index.to_list()
    if len(check_):
        logger.warning('Non-unique assessments found: %s', check_)

    # check completeness
    counts_ = assessments[[col for col, flag in cols.items() if flag]].isna().sum()
    check_ = counts_[counts_>1].index.to_list()
    if len(check_):
        logger.warning('Data missing partially or completely missing in %s', check_)

    # check unexpected values in population_trend* and red_list_category*
    valid_values = ['Increasing', 'Decreasing', 'Stable', 'Unknown']
    assessments[assessments['population_trend'].isin(valid_values)]
    # TODO: red_list_category

    # TODO: referential integrity for red_list_category - red_list_category_code - red_list_category_version

    # ===================
    # Transformations
    # ===================
    assessments.drop_duplicates(inplace=True)
    assessments['population_trend'] = assessments['population_trend'].fillna('Unknown')
    assessments['red_list_category_code'] = assessments['red_list_category_code'].fillna('NA')

    logger.info('%s', assessments.isna().sum())
    assessments.to_parquet(output_dir / 'assessments_db.parquet')

def save__conservation_actions_in_place(data: pd.DataFrame, output_dir: Path) -> None:
    """
    Save info on all conservation_actions_in_place.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    conservation_actions = data[['assessment_id', 'conservation_actions_in_place']]

    conservation_actions = conservation_actions.explode(['conservation_actions_in_place']).reset_index(drop=True)
    conservation_actions.dropna(subset=['conservation_actions_in_place'], inplace=True)

    conservation_actions = conservation_actions.join(conservation_actions['conservation_actions_in_place'].apply(pd.Series))

    conservation_actions = conservation_actions.explode('actions').reset_index(drop=True)
    conservation_actions.rename(columns={'name': 'conservation_action_name'}, inplace=True)

    conservation_actions = conservation_actions.join(conservation_actions['actions'].apply(pd.Series))

    conservation_actions.drop(columns=['conservation_actions_in_place', 'actions'], inplace=True)

    conservation_actions.rename(columns={'name': 'feature'}, inplace=True)

    conservation_actions.drop_duplicates(inplace=True)

    logger.info('%s', conservation_actions.isna().sum())
    conservation_actions.to_parquet(output_dir / 'conservation_actions_in_place_db.parquet')


def save__conservation_actions(data: pd.DataFrame, output_dir: Path) -> None:
    """
    Save info on all conservation_actions.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    conservation_actions_2 = data[['assessment_id', 'conservation_actions']]

    conservation_actions_2 = conservation_actions_2.explode('conservation_actions')
    conservation_actions_2.dropna(subset=['conservation_actions'], inplace=True)
    conservation_actions_2.reset_index(drop=True, inplace=True)

    conservation_actions_2 = conservation_actions_2.join(conservation_actions_2['conservation_actions'].apply(pd.Series))

    conservation_actions_2['description'] = conservation_actions_2['description'].apply(pd.Series)
    conservation_actions_2.drop(columns=['conservation_actions', 'note'], inplace=True)

    conservation_actions_2.drop_duplicates(inplace=True)

    logger.info('%s', conservation_actions_2.isna().sum())
    conservation_actions_2.to_parquet(output_dir / 'conservation_actions_db.parquet')

def save__locations(data: pd.DataFrame, output_dir: Path) -> None:
    """
    Save mapping assessment - country.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    locations = data[['assessment_id', 'locations']]
    locations = locations.explode('locations')

    # ===================
    # Data quality checks
    # ===================

    # check for empty values
    check_ = locations[locations['locations'].apply(lambda x: x == [])]
    if len(check_):
        logger.warning('Data missing partially or completely missing for locations')

    # ===================
    # Transformations
    # ===================

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
    # TODO: check data quality

    # fill empty values
    locations['seasonality'] = locations['seasonality'].apply(lambda x: ['Seasonal Occurrence Uncertain'] if x is np.nan else x)

    logger.info('%s', locations.isna().sum())
    locations.to_parquet(output_dir / 'locations_db.parquet')

def save__habitats(data: pd.DataFrame, output_dir: Path) -> None:
    """
    Save mapping assessment - habitats.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    habitats = data[['assessment_id','habitats']]
    # ===================
    # Data quality checks
    # ===================
    
    # check for empty values
    check_ = habitats[habitats['habitats'].apply(lambda x: x == [])]
    if len(check_):
        logger.warning('Data missing partially or completely missing for habitats')

    # ===================
    # Transformations
    # ===================
    habitats = habitats.explode('habitats').reset_index(drop=True)

    habitats = habitats.join(habitats['habitats'].apply(pd.Series))

    habitats.drop(columns=['habitats', 'season'], inplace=True)
    habitats.drop_duplicates(subset=['assessment_id', 'habitat'], inplace=True)
    # TODO: check data quality

    logger.info('%s', habitats.isna().sum())
    habitats.to_parquet(output_dir / 'habitats_db.parquet')

def save__use_and_trade(data: pd.DataFrame, output_dir: Path) -> None:
    """
    Save mapping assessment - use_and_trade.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    use_and_trade = data[['assessment_id','use_and_trade']]
    
    # ===================
    # Data quality checks
    # ===================
    
    # check for empty values
    check_ = use_and_trade[use_and_trade['use_and_trade'].apply(lambda x: x == [])]
    if len(check_):
        logger.warning('Data missing partially or completely missing for use_and_trade')

    # ===================
    # Transformations
    # ===================

    use_and_trade = use_and_trade.explode('use_and_trade').reset_index(drop=True)
    use_and_trade = use_and_trade.dropna(subset=['use_and_trade']).reset_index(drop=True)

    use_and_trade = use_and_trade.join(use_and_trade['use_and_trade'].apply(pd.Series))

    use_and_trade.drop(columns=['use_and_trade'], inplace=True)

    use_and_trade['description'] = use_and_trade['description'].apply(lambda x: x['en'])
    use_and_trade.rename(columns={'description': 'use_and_trade'}, inplace=True)

    logger.info('%s', use_and_trade.isna().sum())
    use_and_trade.to_parquet(output_dir / 'use_and_trade_db.parquet')

def save__threats(data: pd.DataFrame, output_dir: Path) -> None:
    """
    Save mapping assessment - threats.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    threats = data[['assessment_id','threats']]

    # ===================
    # Data quality checks
    # ===================
    
    # check for empty values
    check_ = threats[threats['threats'].apply(lambda x: x == [])]
    if len(check_):
        logger.warning('Data missing partially or completely missing for threats')

    # ===================
    # Transformations
    # ===================

    threats = threats.explode('threats').reset_index(drop=True)
    threats = threats.dropna(subset=['threats']).reset_index(drop=True)

    threats = threats.join(threats['threats'].apply(pd.Series))
    
    threats.drop(columns=['threats'], inplace=True)
    threats.drop_duplicates(inplace=True)
    
    logger.info('%s', threats.isna().sum())
    threats.to_parquet(output_dir / 'threats_db.parquet')

def save__stresses(data: pd.DataFrame, output_dir: Path) -> None:
    """
    Save mapping assessment - stresses.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    stresses = data[['assessment_id','stresses']]

    # ===================
    # Data quality checks
    # ===================
    
    # check for empty values
    check_ = stresses[stresses['stresses'].apply(lambda x: x == [])]
    if len(check_):
        logger.warning('Data missing partially or completely missing for stresses')

    # ===================
    # Transformations
    # ===================

    stresses = stresses.explode('stresses').reset_index(drop=True)
    stresses = stresses.dropna(subset=['stresses']).reset_index(drop=True)

    stresses = stresses.join(stresses['stresses'].apply(pd.Series))

    stresses.drop(columns=['stresses'], inplace=True)
    stresses.drop_duplicates(inplace=True)

    logger.info('%s', stresses.isna().sum())
    stresses.to_parquet(output_dir / 'stresses_db.parquet')

def save__systems(data: pd.DataFrame, output_dir: Path) -> None:
    """
    Save mapping assessment - systems.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    systems = data[['assessment_id','systems']]
    # ===================
    # Data quality checks
    # ===================
    
    # check for empty values
    check_ = systems[systems['systems'].apply(lambda x: x == [])]
    if len(check_):
        logger.warning('Data missing partially or completely missing for systems')

    # ===================
    # Transformations
    # ===================
    systems = systems.explode('systems').reset_index(drop=True)
    systems = systems.dropna(subset=['systems']).reset_index(drop=True)

    systems = systems.join(systems['systems'].apply(pd.Series))
    systems.drop(columns=['systems'], inplace=True)
    systems.drop_duplicates(inplace=True)

    logger.info('%s', systems.isna().sum())
    systems.to_parquet(output_dir / 'systems_db.parquet')

def save__scopes(data: pd.DataFrame, output_dir: Path) -> None:
    """
    Save mapping assessment - scopes.
    Originally, value is stored as json, so the point of transformation is to unnest it.
    """
    scopes = data[['assessment_id','scopes']]
    # ===================
    # Data quality checks
    # ===================
    
    # check for empty values
    check_ = scopes[scopes['scopes'].apply(lambda x: x == [])]
    if len(check_):
        logger.warning('Data missing partially or completely missing for scopes')

    # ===================
    # Transformations
    # ===================

    scopes = scopes.explode('scopes').reset_index(drop=True)
    scopes = scopes.dropna(subset=['scopes']).reset_index(drop=True)

    scopes = scopes.join(scopes['scopes'].apply(pd.Series))
    scopes.drop(columns=['scopes'], inplace=True)
    scopes.drop_duplicates(inplace=True)
    
    logger.info('%s', scopes.isna().sum())
    scopes.to_parquet(output_dir / 'scopes_db.parquet')
