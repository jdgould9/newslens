#api_client.py
#Jack Gould
#Fetches news data from API

import requests
import sys
from database_controller import update_job_status

def call_news_api(args, key, use_mock=False):
    if use_mock:
        from mock_news import get_mock_response
        return get_mock_response()

    base_url = 'https://api.currentsapi.services/v1/search?'

    payload = {
        'language' : 'en',
        'page_size' : '50',
        'apiKey' : key,
        'keywords' : getattr(args, 'keywords', None),
        'start_date' : getattr(args, 'start_date', None),
        'end_date' : getattr(args, 'end_date', None),
        'country' : getattr(args, 'country', None),
        'type' : getattr(args, 'content_type', None),
        'category' : getattr(args, 'news_category', None),
        'domain' : getattr(args, 'domain', None),
        'domain_not' : getattr(args, 'domain_not', None)
    }
    
    print(f"Formed API call with payload {payload}")

    response = requests.get(base_url, params=payload)
    response.raise_for_status()
        
    print(f"Response URL: {response.url}")
    return response.json()

