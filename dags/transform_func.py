import json
import pandas as pd

# =====================================
# Parse raw JSON files into a table
# =====================================

def parse_assessment(assessment_json: json) -> pd.DataFrame: 
    """
    Extract all the necessary parameters from a given assessment and form a Dataframe
    """
    assessment = {   
        'assessment_id': assessment_json['assessment_id'],
        'assessment_date': assessment_json['assessment_date'],
        'year_published': assessment_json['year_published'],
        'latest': assessment_json['latest'],
        'possibly_extinct': assessment_json['possibly_extinct'],
        'possibly_extinct_in_the_wild': assessment_json['possibly_extinct_in_the_wild'],
        'sis_taxon_id': assessment_json['sis_taxon_id'],
        'criteria': assessment_json['criteria'],
        'url': assessment_json['url'],
        'sis_id': assessment_json['taxon']['sis_id'],
        'kingdom_name': assessment_json['taxon']['kingdom_name'],
        'phylum_name': assessment_json['taxon']['phylum_name'],
        'class_name': assessment_json['taxon']['class_name'],
        'order_name': assessment_json['taxon']['order_name'],
        'family_name': assessment_json['taxon']['family_name'],
        'genus_name': assessment_json['taxon']['genus_name'],
        'species_name': assessment_json['taxon']['species_name'],
        'scientific_name': assessment_json['taxon']['scientific_name'],
        'population_trend': assessment_json['population_trend']['description']['en'] if assessment_json['population_trend'] else None,
        'population_trend_code': assessment_json['population_trend']['code'] if assessment_json['population_trend'] else None,
        'red_list_category_version': assessment_json['red_list_category']['version'],
        'red_list_category': assessment_json['red_list_category']['description']['en'],
        'red_list_category_code': assessment_json['red_list_category']['code'],
        'conservation_actions_in_place': assessment_json['supplementary_info']['conservation_actions_in_place'],
        'conservation_actions': assessment_json['conservation_actions'],
        'population_size': assessment_json['supplementary_info']['population_size'],
        'locations': [
            {
                'origin': l['origin'], 
                'presence': l['presence'], 
                'seasonality': l['seasonality'], 
                'country': l['description']['en'], 
                'code': l['code']
            }
            for l in assessment_json['locations']
        ],
        'habitats': [        
            {
                'season': h['season'], 
                'habitat': h['description']['en'], 
                'code': h['code']
            }
            for h in assessment_json['habitats']
        ],
        'use_and_trade': assessment_json['use_and_trade'],
        'threats': [        
            {
                'timing': t['timing'], 
                'threat': t['description']['en'], 
                'code': t['code']
            }
            for t in assessment_json['threats']
        ],
        'stresses': [        
            {
                'stress': t['description']['en'], 
                'code': t['code']
            }
            for t in assessment_json['stresses']
        ],
        'systems': [        
            {
                'system': t['description']['en'], 
                'code': t['code']
            }
            for t in assessment_json['systems']
        ],
        'scopes': [        
            {
                'scope': t['description']['en'], 
                'code': t['code']
            }
            for t in assessment_json['scopes']
        ],
    
    }

    return pd.DataFrame([assessment])


