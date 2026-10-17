import json
import requests

def get_assessment_list_by_year(year: list, token: str, url: str, page_n: int, country_code: str='AQ') -> json: # AQ
    """
    Collect lists of relevant assessments

    params:
    year: year of publication. Typically, current year
    token: IUCN API access token
    url: IUCN API base address
    page_n: page number to be retrieved
    country_code: RU by default. Set to AQ (Antarcrica) for testing.
    """
    res = requests.get(
        url + f'countries/{country_code}?year_published={year}&page={page_n}',
        headers={
            'Authorization': token, 
            'accept':'*/*'
        }
    )
    res.raise_for_status()
    return res.json()

def get_assessment(id: int, token: str, url: str) -> json: 
    """
    Get detailed data on the assessment specified by `id`. 
    """
    res = requests.get(
            url + f'assessment/{id}',
            headers={
                'Authorization': token, 
                'accept':'*/*'
            }
    )
    res.raise_for_status()
    return res.json()
